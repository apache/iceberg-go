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
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/twmb/murmur3"
)

const (
	hashBits         = 20
	entropyDirLength = 4
	entropyDirDepth  = 3
	hashPathLength   = hashBits + entropyDirDepth
	hashNibbleMask   = (1 << entropyDirLength) - 1
	// Binary representation of all 4-bit values, indexed by nibble.
	binaryHashNibbles = "0000000100100011010001010110011110001001101010111100110111101111"
)

type LocationProvider interface {
	// NewDataLocation returns the location for a data file. partitionPath is the
	// file's partition path, such as "a=1/b=2", or empty for an unpartitioned file.
	NewDataLocation(dataFileName, partitionPath string) string
	NewTableMetadataFileLocation(newVersion int) (string, error)
	NewMetadataLocation(metadataFileName string) string
}

type simpleLocationProvider struct {
	tableProps   iceberg.Properties
	dataPath     string
	metadataPath string
}

// joinLocationPath appends components using forward slashes.
// It trims trailing slashes from the base but does not otherwise clean,
// encode, or decode the local path or URI.
func joinLocationPath(location string, elem ...string) string {
	base := strings.TrimRight(location, "/")
	// The common zero- and one-component cases already need just one
	// allocation. Keep their direct concatenation path.
	if len(elem) < 2 {
		return base + "/" + strings.Join(elem, "/")
	}

	size := len(base) + len(elem)
	for _, component := range elem {
		size += len(component)
	}

	var b strings.Builder
	b.Grow(size)
	b.WriteString(base)
	b.WriteByte('/')
	for i, component := range elem {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(component)
	}

	return b.String()
}

func (slp *simpleLocationProvider) NewDataLocation(dataFileName, partitionPath string) string {
	if partitionPath == "" {
		return joinLocationPath(slp.dataPath, dataFileName)
	}

	return joinLocationPath(slp.dataPath, partitionPath, dataFileName)
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
	case MetadataCompressionCodecZstd:
		ext = ".zstd.metadata.json"
	default:
		return "", fmt.Errorf("unsupported write metadata compression codec: %s", compression)
	}

	fname := fmt.Sprintf("%05d-%s%s", newVersion, newUUID, ext)

	return slp.NewMetadataLocation(fname), nil
}

func (slp *simpleLocationProvider) NewMetadataLocation(metadataFileName string) string {
	return joinLocationPath(slp.metadataPath, metadataFileName)
}

func newSimpleLocationProvider(tableLoc string, tableProps iceberg.Properties) *simpleLocationProvider {
	return &simpleLocationProvider{
		tableProps:   tableProps,
		dataPath:     tableProps.Get(WriteDataPathKey, joinLocationPath(tableLoc, "data")),
		metadataPath: tableProps.Get(WriteMetadataPathKey, joinLocationPath(tableLoc, "metadata")),
	}
}

type objectStoreLocationProvider struct {
	*simpleLocationProvider

	includePartitionPaths bool
	context               string
}

func computeHash(dataFileName string) string {
	hashCode := murmur3.Sum32([]byte(dataFileName)) & ((1 << hashBits) - 1)

	// Format the hash directly into the final directory layout. The fixed
	// buffer contains 20 bits and one separator for each of the three
	// four-bit entropy directories.
	// Masking the hash is equivalent to the old sentinel-bit approach because
	// each nibble is copied explicitly, including leading zeroes.
	var hashPath [hashPathLength]byte
	copy(hashPath[0:entropyDirLength], binaryHashNibbles[((hashCode>>(hashBits-entropyDirLength))&hashNibbleMask)*entropyDirLength:])
	hashPath[entropyDirLength] = '/'
	copy(hashPath[entropyDirLength+1:2*entropyDirLength+1], binaryHashNibbles[((hashCode>>(hashBits-2*entropyDirLength))&hashNibbleMask)*entropyDirLength:])
	hashPath[2*entropyDirLength+1] = '/'
	copy(hashPath[2*entropyDirLength+2:3*entropyDirLength+2], binaryHashNibbles[((hashCode>>(hashBits-3*entropyDirLength))&hashNibbleMask)*entropyDirLength:])
	hashPath[3*entropyDirLength+2] = '/'
	copy(hashPath[3*entropyDirLength+3:4*entropyDirLength+3], binaryHashNibbles[((hashCode>>(hashBits-4*entropyDirLength))&hashNibbleMask)*entropyDirLength:])
	copy(hashPath[4*entropyDirLength+3:5*entropyDirLength+3], binaryHashNibbles[(hashCode&hashNibbleMask)*entropyDirLength:])

	return string(hashPath[:])
}

// NewDataLocation places the entropy hash right under the data path, like the
// Java implementation. External data paths also include the database/table
// context, so two tables sharing a storage prefix do not share file locations.
func (p *objectStoreLocationProvider) NewDataLocation(dataFileName, partitionPath string) string {
	if !p.includePartitionPaths {
		hash := computeHash(dataFileName)
		if p.context != "" {
			return joinLocationPath(p.dataPath, hash, p.context, dataFileName)
		}

		return joinLocationPath(p.dataPath, hash+"-"+dataFileName)
	}

	if partitionPath != "" {
		dataFileName = partitionPath + "/" + dataFileName
	}

	if p.context != "" {
		return joinLocationPath(p.dataPath, computeHash(dataFileName), p.context, dataFileName)
	}

	return joinLocationPath(p.dataPath, computeHash(dataFileName), dataFileName)
}

// objectStorePathContext matches the database/table suffix Java adds when
// write.data.path is outside the table location. Skip URI scheme and authority
// without interpreting percent escapes, queries, or fragments in path names.
func objectStorePathContext(tableLoc string) string {
	location := strings.TrimRight(tableLoc, "/")
	if scheme := strings.Index(location, "://"); scheme >= 0 {
		_, path, ok := strings.Cut(location[scheme+3:], "/")
		if !ok {
			return ""
		}
		location = path
	}

	location = strings.Trim(location, "/")
	lastSlash := strings.LastIndexByte(location, '/')
	if lastSlash < 0 {
		return location
	}

	parent := location[:lastSlash]
	parentSlash := strings.LastIndexByte(parent, '/')

	return parent[parentSlash+1:] + "/" + location[lastSlash+1:]
}

func locationIsWithinTable(location, tableLoc string) bool {
	location = strings.TrimRight(location, "/")
	tableLoc = strings.TrimRight(tableLoc, "/")

	return location == tableLoc || strings.HasPrefix(location, tableLoc+"/")
}

func newObjectStoreLocationProvider(tableLoc string, tableProps iceberg.Properties) *objectStoreLocationProvider {
	simple := newSimpleLocationProvider(tableLoc, tableProps)
	var context string
	if !locationIsWithinTable(simple.dataPath, tableLoc) {
		context = objectStorePathContext(tableLoc)
	}

	return &objectStoreLocationProvider{
		simpleLocationProvider: simple,
		context:                context,
		includePartitionPaths: tableProps.GetBool(WriteObjectStorePartitionedPathsKey,
			WriteObjectStorePartitionedPathsDefault),
	}
}

func LoadLocationProvider(tableLocation string, tableProps iceberg.Properties) (LocationProvider, error) {
	if tableProps.GetBool(ObjectStoreEnabledKey, ObjectStoreEnabledDefault) {
		return newObjectStoreLocationProvider(tableLocation, tableProps), nil
	}

	return newSimpleLocationProvider(tableLocation, tableProps), nil
}
