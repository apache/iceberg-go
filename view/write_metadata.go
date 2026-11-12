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
	"io"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// WriteMetadata writes a new view metadata file and returns its location and
// filesystem. The directory comes from table.WriteMetadataPathKey, or defaults
// to <view location>/metadata. Each file has a fresh view-<uuid>.metadata.json
// name. ioProps configures the filesystem and is not persisted in the metadata.
//
// Failed writes and closes remove the incomplete file without inheriting
// cancellation. After a successful write, the caller owns publication and may
// use the returned filesystem to remove the file after a confirmed failed
// commit. A commit with an unknown outcome must retain the file.
func WriteMetadata(ctx context.Context, metadata Metadata, ioProps iceberg.Properties) (string, icebergio.WriteFileIO, error) {
	return WriteMetadataWithRegistry(ctx, metadata, ioProps, nil)
}

// WriteMetadataWithRegistry writes a new view metadata file through the
// selected FileIO registry. A nil registry uses the process-wide default.
// The registry pointer is shared; later Register and Unregister calls are
// visible to this write.
func WriteMetadataWithRegistry(
	ctx context.Context, metadata Metadata, ioProps iceberg.Properties,
	registry *icebergio.Registry,
) (string, icebergio.WriteFileIO, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if metadata == nil {
		return "", nil, fmt.Errorf("%w: view metadata is required", ErrInvalidViewMetadata)
	}
	directory := metadata.Properties().Get(table.WriteMetadataPathKey, "")
	if directory == "" {
		directory = strings.TrimRight(metadata.Location(), "/") + "/metadata"
	}
	// Append without URL encoding or path cleaning, as for table metadata.
	// Object keys and native file paths can contain literal spaces, ? and #.
	location := strings.TrimRight(directory, "/") + "/view-" + uuid.NewString() + ".metadata.json"
	data, err := json.Marshal(metadata)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal view metadata: %w", err)
	}
	loadFS := icebergio.LoadFS
	if registry != nil {
		loadFS = registry.LoadFS
	}
	fs, err := loadFS(ctx, ioProps, location)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load filesystem for view metadata: %w", err)
	}
	wfs, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return "", nil, errors.New("filesystem IO does not support writing")
	}
	if err := writeViewMetadata(ctx, wfs, location, data); err != nil {
		return "", nil, err
	}

	return location, wfs, nil
}

func writeViewMetadata(ctx context.Context, fs icebergio.WriteFileIO, location string, data []byte) (err error) {
	writer, err := fs.Create(ctx, location)
	if err != nil {
		return fmt.Errorf("failed to create view metadata file: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, fs.Remove(context.WithoutCancel(ctx), location))
		}
	}()
	defer internal.CheckedClose(writer, &err)

	n, err := writer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write view metadata: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("failed to write view metadata: %w", io.ErrShortWrite)
	}

	return nil
}
