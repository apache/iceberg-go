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

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
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

	fname := fmt.Sprintf("%05d-%s.metadata.json", newVersion, newUUID)

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
