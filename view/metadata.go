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

package view

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

// LoadMetadata reads and parses a view metadata file from the specified location.
//
// Returns the loaded Metadata or an error if the file cannot be read or parsed.
func LoadMetadata(ctx context.Context,
	props iceberg.Properties,
	metadataLocation string,
	name string,
	namespace string,
) (_ Metadata, err error) {
	fs, err := io.LoadFS(ctx, props, metadataLocation)
	if err != nil {
		return nil, fmt.Errorf("error loading view metadata: %w", err)
	}

	inputFile, err := fs.Open(metadataLocation)
	if err != nil {
		return nil, fmt.Errorf("%s.%s: error encountered loading view metadata: %w", namespace, name, err)
	}
	defer internal.CheckedClose(inputFile, &err)

	var m metadata
	if err := json.NewDecoder(inputFile).Decode(&m); err != nil {
		return nil, fmt.Errorf("error encountered decoding view metadata: %w", err)
	}

	return &m, nil
}

// CreateMetadata creates a new view metadata file and writes it to storage.
//
// Returns the full path to the created metadata file, or an error if creation fails.
//
// Note: This function only supports creating new views with format version 1.
// It does not support updating existing view metadata.
func CreateMetadata(
	ctx context.Context,
	catalogName string,
	nsIdent []string,
	schema *iceberg.Schema,
	viewSQL string,
	loc string,
	props iceberg.Properties,
) (metadataLocation string, err error) {
	versionId := int64(1)
	timestampMs := time.Now().UnixMilli()

	viewVersion := Version{
		VersionID:   versionId,
		TimestampMs: timestampMs,
		SchemaID:    schema.ID,
		Summary:     map[string]string{"sql": viewSQL},
		Representations: []SQLRepresentation{
			{Type: "sql", SQL: viewSQL, Dialect: "default"},
		},
		DefaultCatalog:   &catalogName,
		DefaultNamespace: nsIdent,
	}

	metadataLocation = loc + "/metadata/view-" + uuid.New().String() + ".metadata.json"

	viewUUID := uuid.New().String()
	viewMetadata := metadata{
		UUID:             viewUUID,
		FmtVersion:       1,
		Loc:              loc,
		SchemaList:       []*iceberg.Schema{schema},
		CurrentVersionId: versionId,
		VersionList:      []Version{viewVersion},
		VersionLogList: []VersionLogEntry{
			{
				TimestampMs: timestampMs,
				VersionID:   versionId,
			},
		},
		Props: props,
	}

	viewMetadataBytes, err := json.Marshal(viewMetadata)
	if err != nil {
		return "", fmt.Errorf("failed to marshal view metadata: %w", err)
	}

	fs, err := io.LoadFS(ctx, props, metadataLocation)
	if err != nil {
		return "", fmt.Errorf("failed to load filesystem for view metadata: %w", err)
	}

	wfs, ok := fs.(io.WriteFileIO)
	if !ok {
		return "", errors.New("filesystem IO does not support writing")
	}

	out, err := wfs.Create(metadataLocation)
	if err != nil {
		return "", fmt.Errorf("failed to create view metadata file: %w", err)
	}
	defer internal.CheckedClose(out, &err)

	if _, err := out.Write(viewMetadataBytes); err != nil {
		return "", fmt.Errorf("failed to write view metadata: %w", err)
	}

	return metadataLocation, nil
}
