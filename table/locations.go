// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/twmb/murmur3"
)

const (
	hashBinaryStringBits = 20
	entropyDirLength     = 4
	entropyDirDepth      = 3
)

type LocationProvider interface {
	NewDataLocation(dataFileName string) string
	NewTableMetadataFileLocation(newVersion int) (string, error)
	NewMetadataLocation(metadataFileName string) string
}

type simpleLocationProvider struct {
	tableLoc     *url.URL
	tableProps   iceberg.Properties
	dataPath     *url.URL
	metadataPath *url.URL
}

func (slp *simpleLocationProvider) NewDataLocation(dataFileName string) string {
	return slp.dataPath.JoinPath(dataFileName).String()
}

func (slp *simpleLocationProvider) NewTableMetadataFileLocation(newVersion int) (string, error) {
	if newVersion < 0 {
		return "", fmt.Errorf("%w: table metadata version %d must be a non-negative integer",
			iceberg.ErrInvalidArgument, newVersion)
	}

	newUUID, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	compression := slp.tableProps.Get(MetadataCompressionKey, MetadataCompressionDefault)
	var ext string
	switch compression {
	case MetadataCompressionCodecNone:
		ext = ".metadata.json"
	case MetadataCompressionCodecGzip:
		ext = ".gz.metadata.json"
	default:
		return "", fmt.Errorf("unsupported write metadata compression codec: %s", compression)
	}

	fname := fmt.Sprintf("%05d-%s%s", newVersion, newUUID, ext)

	return slp.NewMetadataLocation(fname), nil
}

func (slp *simpleLocationProvider) NewMetadataLocation(metadataFileName string) string {
	return slp.metadataPath.JoinPath(metadataFileName).String()
}

func newSimpleLocationProvider(tableLoc *url.URL, tableProps iceberg.Properties) (*simpleLocationProvider, error) {
	out := &simpleLocationProvider{
		tableLoc:   tableLoc,
		tableProps: tableProps,
	}

	var err error
	if propPath, ok := tableProps[WriteDataPathKey]; ok {
		out.dataPath, err = url.Parse(propPath)
		if err != nil {
			return nil, err
		}
	} else {
		out.dataPath = out.tableLoc.JoinPath("data")
	}

	if propPath, ok := tableProps[WriteMetadataPathKey]; ok {
		out.metadataPath, err = url.Parse(propPath)
		if err != nil {
			return nil, err
		}
	} else {
		out.metadataPath = out.tableLoc.JoinPath("metadata")
	}

	return out, nil
}

type objectStoreLocationProvider struct {
	*simpleLocationProvider

	includePartitionPaths bool
}

func computeHash(dataFileName string) string {
	// Bitwise AND to combat sign-extension; bitwise OR to preserve leading zeroes
	topMask := 1 << hashBinaryStringBits
	hashCode := int(murmur3.Sum32([]byte(dataFileName)))&(topMask-1) | topMask

	// Convert to binary string and take the last hashBinaryStringBits
	binaryStr := strconv.FormatInt(int64(hashCode), 2)

	return dirsFromHash(binaryStr[len(binaryStr)-hashBinaryStringBits:])
}

func dirsFromHash(fileHash string) string {
	// Divides hash into directories for optimized orphan removal operation
	totalEntropyLength := entropyDirDepth * entropyDirLength

	hashWithDirs := make([]string, 0)
	for i := 0; i < totalEntropyLength; i += entropyDirLength {
		hashWithDirs = append(hashWithDirs, fileHash[i:i+entropyDirLength])
	}

	if len(fileHash) > totalEntropyLength {
		hashWithDirs = append(hashWithDirs, fileHash[totalEntropyLength:])
	}

	return strings.Join(hashWithDirs, "/")
}

func (p *objectStoreLocationProvider) NewDataLocation(dataFileName string) string {
	if path.Dir(dataFileName) != "." {
		return p.simpleLocationProvider.NewDataLocation(dataFileName)
	}

	hashedPath := computeHash(dataFileName)
	if p.includePartitionPaths {
		return p.simpleLocationProvider.dataPath.JoinPath(hashedPath, dataFileName).String()
	} else {
		return p.simpleLocationProvider.dataPath.JoinPath(hashedPath + "-" + dataFileName).String()
	}
}

func newObjectStoreLocationProvider(tableLoc *url.URL, tableProps iceberg.Properties) (*objectStoreLocationProvider, error) {
	slp, err := newSimpleLocationProvider(tableLoc, tableProps)
	if err != nil {
		return nil, err
	}

	return &objectStoreLocationProvider{
		simpleLocationProvider: slp,
		includePartitionPaths: tableProps.GetBool(WriteObjectStorePartitionedPathsKey,
			WriteObjectStorePartitionedPathsDefault),
	}, nil
}

func LoadLocationProvider(tableLocation string, tableProps iceberg.Properties) (LocationProvider, error) {
	u, err := url.Parse(tableLocation)
	if err != nil {
		return nil, err
	}

	if tableProps.GetBool(ObjectStoreEnabledKey, ObjectStoreEnabledDefault) {
		return newObjectStoreLocationProvider(u, tableProps)
	}

	return newSimpleLocationProvider(u, tableProps)
}
