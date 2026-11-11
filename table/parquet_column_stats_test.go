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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func statsSettingsSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "kept", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "display name", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "parent", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 5, Name: "child", Type: iceberg.PrimitiveTypes.Int32},
			{ID: 6, Name: "other", Type: iceberg.PrimitiveTypes.Int32},
		}}},
		iceberg.NestedField{ID: 7, Name: "items", Type: iceberg.NewListType(8, iceberg.PrimitiveTypes.Int32, false)},
		iceberg.NestedField{ID: 9, Name: "properties", Type: iceberg.NewMapType(10, iceberg.PrimitiveTypes.String, 11, iceberg.PrimitiveTypes.Int32, false)},
	)
}

func statsSettingsRecord(t *testing.T, schema *iceberg.Schema) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	arrowSchema, err := SchemaToArrowSchemaWithOptions(schema, ArrowSchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	for _, value := range []int32{1, 2} {
		builder.Field(0).(*array.Int32Builder).Append(value)
		builder.Field(1).(*array.Int32Builder).Append(value)
		builder.Field(2).(*array.Int32Builder).Append(value)
		parent := builder.Field(3).(*array.StructBuilder)
		parent.Append(true)
		parent.FieldBuilder(0).(*array.Int32Builder).Append(value)
		parent.FieldBuilder(1).(*array.Int32Builder).Append(value)
		items := builder.Field(4).(*array.ListBuilder)
		items.Append(true)
		items.ValueBuilder().(*array.Int32Builder).Append(value)
		properties := builder.Field(5).(*array.MapBuilder)
		properties.Append(true)
		properties.KeyBuilder().(*array.StringBuilder).Append("key")
		properties.ItemBuilder().(*array.Int32Builder).Append(value)
	}

	return arrowSchema, builder.NewRecordBatch()
}

func writeStatsSettingsFile(t *testing.T, mode string, props iceberg.Properties) (*iceberg.Schema, iceberg.DataFile) {
	t.Helper()
	loc := t.TempDir()
	schema := statsSettingsSchema()
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, props)
	require.NoError(t, err)
	schema = meta.CurrentSchema()
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	arrowSchema, record := statsSettingsRecord(t, schema)

	if mode == "task" {
		fileSchema, err := iceberg.SanitizeColumnNames(schema)
		require.NoError(t, err)
		writer, err := newDataFileWriter(loc, iceio.LocalFS{}, builder, props, withFileSchema(fileSchema))
		require.NoError(t, err)
		dataFile, err := writer.writeFile(t.Context(), nil, WriteTask{
			Uuid: uuid.New(), Schema: schema, FileCount: 1, Batches: []arrow.RecordBatch{record},
		})
		require.NoError(t, err)

		return schema, dataFile
	}

	writeUUID := uuid.New()
	factory, err := newWriterFactory(loc, recordWritingArgs{
		sc: arrowSchema, fs: iceio.LocalFS{}, writeUUID: &writeUUID,
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					return
				}
			}
		},
	}, builder, schema, 1024*1024)
	require.NoError(t, err)
	defer factory.closeAll()
	output := make(chan iceberg.DataFile, 1)
	writer := factory.newRollingDataWriter(t.Context(), "", nil, output)
	require.NoError(t, writer.Add(record))
	record.Release()
	require.NoError(t, writer.closeAndWait())

	return schema, <-output
}

func TestParquetColumnStatisticsWriterPaths(t *testing.T) {
	prefix := ParquetColumnStatsEnabledKeyPrefix + "."
	for _, mode := range []string{"task", "rolling"} {
		for _, enabled := range []bool{true, false} {
			name := mode + "/defaults"
			props := iceberg.Properties{}
			if !enabled {
				name = mode + "/configured"
				props = iceberg.Properties{
					prefix + "id":                 "false",
					prefix + "display name":       "FALSE",
					prefix + "parent.child":       "1",
					prefix + "parent.other":       "TrUe",
					prefix + "items.element":      "false",
					prefix + "items.list.element": "true",
					prefix + "properties.value":   "false",
					prefix + "missing":            "false",
					ParquetCompressionColumnCodecKeyPrefix + ".items.element": "gzip",
				}
			}
			t.Run(name, func(t *testing.T) {
				schema, dataFile := writeStatsSettingsFile(t, mode, props)
				reader, err := file.OpenParquetFile(dataFile.FilePath(), false)
				require.NoError(t, err)
				defer reader.Close()
				meta := reader.MetaData()
				require.Equal(t, 1, meta.NumRowGroups())
				rowGroup := meta.RowGroup(0)
				want := map[string]bool{
					"id": enabled, "kept": true, "display_x20name": enabled,
					"parent.child": enabled, "parent.other": true,
					"items.list.element":       enabled,
					"properties.key_value.key": true, "properties.key_value.value": enabled,
				}
				assert.Equal(t, len(want), rowGroup.NumColumns())
				for col := range rowGroup.NumColumns() {
					path := meta.Schema.Column(col).Path()
					expected, ok := want[path]
					require.True(t, ok, "unexpected column %s", path)
					chunk, err := rowGroup.ColumnChunk(col)
					require.NoError(t, err)
					if !enabled && path == "items.list.element" {
						assert.Equal(t, compress.Codecs.Gzip, chunk.Compression())
					}
					statsSet, err := chunk.StatsSet()
					require.NoError(t, err)
					assert.Equal(t, expected, statsSet, path)
				}
				assert.Contains(t, dataFile.LowerBoundValues(), 2)
				assert.Contains(t, dataFile.UpperBoundValues(), 2)
				assert.Equal(t, int64(2), dataFile.ValueCounts()[1])
				if enabled {
					assert.Contains(t, dataFile.LowerBoundValues(), 1)
					assert.Contains(t, dataFile.NullValueCounts(), 1)
				} else {
					assert.NotContains(t, dataFile.LowerBoundValues(), 1)
					assert.NotContains(t, dataFile.UpperBoundValues(), 1)
					assert.NotContains(t, dataFile.NullValueCounts(), 1)
				}

				// Missing file and row-group statistics must not cause false pruning.
				for _, tc := range []struct {
					column string
					index  int
					keep   bool
				}{
					{"id", 0, !enabled},
					{"kept", 1, false},
				} {
					pred := iceberg.EqualTo(iceberg.Reference(tc.column), int32(999))
					fileEval, err := newInclusiveMetricsEvaluator(schema, pred, true, false)
					require.NoError(t, err)
					mayMatch, err := fileEval(dataFile)
					require.NoError(t, err)
					assert.Equal(t, tc.keep, mayMatch, "file %s", tc.column)
					bound, err := iceberg.BindExpr(schema, pred, true)
					require.NoError(t, err)
					rowEval := &inclusiveMetricsEval{expr: bound}
					mayMatch, err = rowEval.TestRowGroup(rowGroup, []int{tc.index})
					require.NoError(t, err)
					assert.Equal(t, tc.keep, mayMatch, "row group %s", tc.column)
				}
			})
		}
	}
}

func writeStatsSettingsDeleteFile(t *testing.T, mode string, content iceberg.ManifestEntryContent) iceberg.DataFile {
	t.Helper()
	loc := t.TempDir()
	prefix := ParquetColumnStatsEnabledKeyPrefix + "."
	props := iceberg.Properties{prefix + "id": "false", prefix + "file_path": "false", prefix + "pos": "false"}
	meta, err := NewMetadata(statsSettingsSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, props)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	fileSchema := iceberg.PositionalDeleteSchema
	if content == iceberg.EntryContentEqDeletes {
		field, ok := meta.CurrentSchema().FindFieldByName("id")
		require.True(t, ok)
		fileSchema = iceberg.NewSchema(0, field)
	}
	arrowSchema, err := SchemaToArrowSchemaWithOptions(fileSchema, ArrowSchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer rb.Release()
	if content == iceberg.EntryContentEqDeletes {
		rb.Field(0).(*array.Int32Builder).Append(1)
	} else {
		rb.Field(0).(*array.StringBuilder).Append("file://table/data/a.parquet")
		rb.Field(1).(*array.Int64Builder).Append(3)
	}
	record := rb.NewRecordBatch()

	if mode == "task" {
		var writer *defaultDataFileWriter
		if content == iceberg.EntryContentEqDeletes {
			writer, err = newEqualityDeleteWriter(loc, iceio.LocalFS{}, builder, props,
				withFileSchema(fileSchema), withEqualityFieldIDs([]int{1}))
		} else {
			writer, err = newPositionDeleteWriter(loc, iceio.LocalFS{}, builder, props)
		}
		require.NoError(t, err)
		dataFile, err := writer.writeFile(t.Context(), nil, WriteTask{
			Uuid: uuid.New(), Schema: fileSchema, FileCount: 1, Batches: []arrow.RecordBatch{record},
		})
		require.NoError(t, err)

		return dataFile
	}

	writeUUID := uuid.New()
	opts := []writerFactoryOption{withContentType(content), withFactoryFileSchema(fileSchema)}
	if content == iceberg.EntryContentEqDeletes {
		opts = append(opts, withFactoryEqualityFieldIDs([]int{1}))
	}
	factory, err := newWriterFactory(loc, recordWritingArgs{
		sc: arrowSchema, fs: iceio.LocalFS{}, writeUUID: &writeUUID,
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					return
				}
			}
		},
	}, builder, fileSchema, 1024*1024, opts...)
	require.NoError(t, err)
	defer factory.closeAll()
	output := make(chan iceberg.DataFile, 1)
	writer := factory.newRollingDataWriter(t.Context(), "", nil, output)
	require.NoError(t, writer.Add(record))
	record.Release()
	require.NoError(t, writer.closeAndWait())

	return <-output
}

func TestParquetColumnStatisticsSettingsDoNotAffectDeleteFiles(t *testing.T) {
	for _, mode := range []string{"task", "rolling"} {
		for _, tc := range []struct {
			name    string
			content iceberg.ManifestEntryContent
		}{
			{"position", iceberg.EntryContentPosDeletes},
			{"equality", iceberg.EntryContentEqDeletes},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				dataFile := writeStatsSettingsDeleteFile(t, mode, tc.content)
				reader, err := file.OpenParquetFile(dataFile.FilePath(), false)
				require.NoError(t, err)
				defer reader.Close()
				rowGroup := reader.MetaData().RowGroup(0)
				for col := range rowGroup.NumColumns() {
					chunk, err := rowGroup.ColumnChunk(col)
					require.NoError(t, err)
					statsSet, err := chunk.StatsSet()
					require.NoError(t, err)
					assert.True(t, statsSet)
				}
				if tc.content == iceberg.EntryContentPosDeletes {
					assert.Contains(t, dataFile.LowerBoundValues(), iceberg.DeleteFilePathFieldID)
					assert.Contains(t, dataFile.LowerBoundValues(), iceberg.DeleteFilePosFieldID)
					require.NotNil(t, dataFile.ReferencedDataFile())
					assert.Equal(t, "file://table/data/a.parquet", *dataFile.ReferencedDataFile())
				} else {
					assert.Contains(t, dataFile.LowerBoundValues(), 1)
				}
			})
		}
	}
}

func TestParquetColumnStatisticsAcrossRowGroups(t *testing.T) {
	prefix := ParquetColumnStatsEnabledKeyPrefix + "."
	for _, mode := range []string{"task", "rolling"} {
		t.Run(mode, func(t *testing.T) {
			props := iceberg.Properties{ParquetRowGroupLimitKey: "1"}
			props[prefix+"id"] = "false"
			props[prefix+"parent.child"] = "false"
			schema, dataFile := writeStatsSettingsFile(t, mode, props)
			reader, err := file.OpenParquetFile(dataFile.FilePath(), false)
			require.NoError(t, err)
			defer reader.Close()

			meta := reader.MetaData()
			require.Equal(t, 2, meta.NumRowGroups())
			want := map[string]bool{}
			want["id"] = false
			want["kept"] = true
			want["display_x20name"] = true
			want["parent.child"] = false
			want["parent.other"] = true
			want["items.list.element"] = true
			want["properties.key_value.key"] = true
			want["properties.key_value.value"] = true

			pred := iceberg.EqualTo(iceberg.Reference("id"), int32(999))
			bound, err := iceberg.BindExpr(schema, pred, true)
			require.NoError(t, err)
			rowEval := &inclusiveMetricsEval{expr: bound}
			for group := range meta.NumRowGroups() {
				rowGroup := meta.RowGroup(group)
				require.EqualValues(t, 1, rowGroup.NumRows())
				require.Equal(t, len(want), rowGroup.NumColumns())
				for col := range rowGroup.NumColumns() {
					path := meta.Schema.Column(col).Path()
					expected, ok := want[path]
					require.True(t, ok, "unexpected column %s", path)
					chunk, err := rowGroup.ColumnChunk(col)
					require.NoError(t, err)
					statsSet, err := chunk.StatsSet()
					require.NoError(t, err)
					assert.Equal(t, expected, statsSet, "row group %d, column %s", group, path)
				}
				mayMatch, err := rowEval.TestRowGroup(rowGroup, []int{0})
				require.NoError(t, err)
				assert.True(t, mayMatch, "row group %d must not be pruned without id stats", group)
			}
			assert.Len(t, dataFile.SplitOffsets(), 2)
			assert.NotContains(t, dataFile.LowerBoundValues(), 1)
			assert.NotContains(t, dataFile.UpperBoundValues(), 1)
			assert.Contains(t, dataFile.LowerBoundValues(), 2)
			assert.Equal(t, int64(2), dataFile.ValueCounts()[1])
		})
	}
}
