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
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// Entries returns every manifest entry in the current snapshot, including
// entries marked deleted. This exposes commit history that data_files and
// delete_files intentionally hide.
func (i InspectTable) Entries(ctx context.Context) (array.RecordReader, error) {
	partitionType := inspectPartitionType(i.tbl.metadata)
	schema := EntriesSchema(partitionType)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect entries: build arrow schema: %w", err)
	}

	rr, err := i.manifestEntryReader(ctx, arrowSchema, false, nil,
		func(bldr *array.RecordBuilder, entry iceberg.ManifestEntry) error {
			bldr.Field(0).(*array.Int32Builder).Append(int32(entry.Status()))
			appendEntriesOptionalInt64(bldr.Field(1).(*array.Int64Builder), entry.SnapshotID())
			appendEntriesOptionalInt64(bldr.Field(2).(*array.Int64Builder), entry.SequenceNum())
			appendInspectOptionalInt64(bldr.Field(3).(*array.Int64Builder), entry.FileSequenceNum())

			return appendContentFile(bldr.Field(4).(*array.StructBuilder), partitionType, entry.DataFile())
		})
	if err != nil {
		return nil, fmt.Errorf("inspect entries: %w", err)
	}

	return rr, nil
}

// EntriesSchema returns the schema of the entries metadata table.
func EntriesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 0, Name: "status", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 1, Name: "snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 3, Name: "sequence_number", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "file_sequence_number", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "data_file", Type: inspectContentFileType(partitionType), Required: true},
	)
}

func appendEntriesOptionalInt64(builder *array.Int64Builder, value int64) {
	if value < 0 {
		builder.AppendNull()

		return
	}
	builder.Append(value)
}
