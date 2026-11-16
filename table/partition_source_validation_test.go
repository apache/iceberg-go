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

package table_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestNewMetadataRejectsIncompatiblePartitionSource(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "source", Type: iceberg.PrimitiveTypes.String})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "source_day", Transform: iceberg.DayTransform{},
	})

	_, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, t.TempDir(), nil)
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
	require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
	require.ErrorContains(t, err, "invalid source type string for transform day")
}

func TestNewMetadataAcceptsPointerPartitionSource(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "source", Type: &iceberg.StringType{}})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "source_bucket", Transform: iceberg.BucketTransform{NumBuckets: 8},
	})

	_, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, t.TempDir(), nil)
	require.NoError(t, err)
}

func TestWriteRecordsRejectsIncompatiblePartitionSource(t *testing.T) {
	loc := t.TempDir()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "source", Type: iceberg.PrimitiveTypes.String})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "source_day", Transform: iceberg.IdentityTransform{},
	})
	metadata, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, loc, nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"transform":"identity"`)
	encoded = bytes.ReplaceAll(encoded, []byte(`"transform":"identity"`), []byte(`"transform":"day"`))
	metadata, err = table.ParseMetadataBytes(encoded)
	require.NoError(t, err)

	tbl := table.New(table.Identifier{"test", "invalid_partition"}, metadata,
		filepath.Join(loc, "metadata", "v1.metadata.json"),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil)
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "source", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	records := func(yield func(arrow.RecordBatch, error) bool) {
		record, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[{"source":"abc"}]`))
		if err != nil {
			yield(nil, err)

			return
		}
		yield(record, nil)
	}

	for _, tt := range []struct {
		name string
		opts []table.WriteRecordOption
	}{
		{name: "fanout"},
		{name: "clustered", opts: []table.WriteRecordOption{table.WithClusteredWrite()}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var writeErr error
			for dataFile, err := range table.WriteRecords(context.Background(), tbl, arrowSchema, records, tt.opts...) {
				if err != nil {
					writeErr = err

					break
				}
				t.Fatalf("wrote a file with an incompatible partition: %#v", dataFile.Partition())
			}
			require.ErrorIs(t, writeErr, iceberg.ErrInvalidTransform)
			require.ErrorContains(t, writeErr, "invalid source type string for transform day")
		})
	}
}

func TestFileToDataFileRejectsIncompatiblePartitionSource(t *testing.T) {
	ctx := context.Background()
	fs := iceio.LocalFS{}
	path := filepath.Join(t.TempDir(), "data.parquet")
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "source", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[{"source":"abc"}]`))
	require.NoError(t, err)
	defer record.Release()
	arrowTable := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
	defer arrowTable.Release()
	output, err := fs.Create(ctx, path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(arrowTable, output, arrowTable.NumRows(), nil, pqarrow.DefaultWriterProps()))

	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "source", Type: iceberg.PrimitiveTypes.String})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "source_day", Transform: iceberg.DayTransform{},
	})
	for _, tt := range []struct {
		name string
		opts []table.FileToDataFileOption
	}{
		{name: "inferred partition"},
		{name: "supplied partition", opts: []table.FileToDataFileOption{table.WithFilePartitionValues(map[int]any{1000: nil})}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dataFile, err := table.FileToDataFileWithOptions(ctx, fs, path, schema, spec, 0, nil, tt.opts...)
			require.ErrorIs(t, err, iceberg.ErrInvalidTransform)
			require.ErrorContains(t, err, "invalid source type string for transform day")
			require.Nil(t, dataFile)
		})
	}
}
