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

package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

type CreateTableCfg struct {
	Location      string
	PartitionSpec *iceberg.PartitionSpec
	SortOrder     table.SortOrder
	Properties    iceberg.Properties
}

func GetMetadataLoc(location string, newVersion uint) string {
	return fmt.Sprintf("%s/metadata/%05d-%s.metadata.json",
		location, newVersion, uuid.New().String())
}

func WriteMetadata(ctx context.Context, metadata table.Metadata, loc string, props iceberg.Properties) error {
	fs, err := io.LoadFS(ctx, props, loc)
	if err != nil {
		return err
	}

	wfs, ok := fs.(io.WriteFileIO)
	if !ok {
		return errors.New("filesystem IO does not support writing")
	}

	out, err := wfs.Create(loc)
	if err != nil {
		return nil
	}

	defer out.Close()

	return json.NewEncoder(out).Encode(metadata)
}

func UpdateTableMetadata(base table.Metadata, updates []table.Update, metadataLoc string) (table.Metadata, error) {
	bldr, err := table.MetadataBuilderFromBase(base)
	if err != nil {
		return nil, err
	}

	for _, update := range updates {
		if err := update.Apply(bldr); err != nil {
			return nil, err
		}
	}

	if bldr.HasChanges() {
		if metadataLoc != "" {
			maxMetadataLogEntries := max(1,
				base.Properties().GetInt(
					table.MetadataPreviousVersionsMaxKey, table.MetadataPreviousVersionsMaxDefault))

			bldr.TrimMetadataLogs(maxMetadataLogEntries + 1).
				AppendMetadataLog(table.MetadataLogEntry{
					MetadataFile: metadataLoc,
					TimestampMs:  base.LastUpdatedMillis(),
				})
		}
		if base.LastUpdatedMillis() == bldr.LastUpdatedMS() {
			bldr.SetLastUpdatedMS()
		}
	}

	return bldr.Build()
}

// (\d+)            -> version number
// -                -> separator
// ([\w-]{36})      -> UUID (36 characters, including hyphens)
// (?:\.\w+)?       -> optional codec name
// \.metadata\.json -> file extension
var tableMetadataFileNameRegex = regexp.MustCompile(`^(\d+)-([\w-]{36})(?:\.\w+)?\.metadata\.json`)

func ParseMetadataVersion(location string) int {
	fileName := path.Base(location)
	matches := tableMetadataFileNameRegex.FindStringSubmatch(fileName)

	if len(matches) != 3 {
		return -1
	}

	if _, err := uuid.Parse(matches[2]); err != nil {
		return -1
	}

	v, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1
	}

	return v
}
