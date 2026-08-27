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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writerProjectionOptions() SchemaOptions {
	return SchemaOptions{
		DowncastTimestamp: true,
		IncludeFieldIDs:   true,
		UseWriteDefault:   true,
	}
}

func writerFieldIDMeta(id string) arrow.Metadata {
	return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: id})
}

func projectionTestRecord(t *testing.T, mem memory.Allocator, schema *arrow.Schema, json []byte) arrow.RecordBatch {
	t.Helper()

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	require.NoError(t, builder.UnmarshalJSON(json))

	return builder.NewRecordBatch()
}

func projectionTestSchema(t *testing.T, schema *iceberg.Schema) *arrow.Schema {
	t.Helper()

	arrowSchema, err := SchemaToArrowSchemaWithOptions(schema, ArrowSchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)

	return arrowSchema
}

func assertProjectedBatch(t *testing.T, requested, provided *iceberg.Schema, batch arrow.RecordBatch, target *arrow.Schema, wantReuse bool) arrow.RecordBatch {
	t.Helper()

	got, err := toRequestedSchema(context.Background(), requested, provided, batch, writerProjectionOptions(), target)
	require.NoError(t, err)
	if wantReuse {
		assert.Same(t, batch, got)
	} else {
		assert.NotSame(t, batch, got)
	}
	assert.True(t, arrowSchemaEqual(got.Schema(), target), "expected schema: %s\ngot: %s", target, got.Schema())

	return got
}

func TestToRequestedSchemaWriteFastPath(t *testing.T) {
	tests := []struct {
		name      string
		requested *iceberg.Schema
		provided  *iceberg.Schema
		batch     func(*testing.T, memory.Allocator) arrow.RecordBatch
		wantReuse bool
		check     func(*testing.T, arrow.RecordBatch)
	}{
		{
			name: "exact Arrow schema",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := projectionTestSchema(t, iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				))

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1}`))
			},
			wantReuse: true,
		},
		{
			name: "exact multi-column Arrow schema",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := projectionTestSchema(t, iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
				))

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1, "name": "one"}`))
			},
			wantReuse: true,
		},
		{
			name: "exact nested Arrow schema",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "profile", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String},
					{ID: 4, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
				}}},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "profile", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String},
					{ID: 4, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
				}}},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := projectionTestSchema(t, iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
					iceberg.NestedField{ID: 2, Name: "profile", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
						{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String},
						{ID: 4, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
					}}},
				))

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1, "profile": {"name": "one", "age": 42}}`))
			},
			wantReuse: true,
		},
		{
			name: "required field with nullable batch",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := arrow.NewSchema([]arrow.Field{{
					Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: writerFieldIDMeta("1"),
				}}, nil)

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1}`))
			},
		},
		{
			name: "reordered columns",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: writerFieldIDMeta("2")},
					{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: writerFieldIDMeta("1")},
				}, nil)

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1, "name": "one"}`))
			},
		},
		{
			name: "missing optional field",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := arrow.NewSchema([]arrow.Field{{
					Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: writerFieldIDMeta("1"),
				}}, nil)

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1}`))
			},
			check: func(t *testing.T, got arrow.RecordBatch) {
				assert.True(t, got.Column(1).IsNull(0))
			},
		},
		{
			name: "missing write-default field",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true, WriteDefault: "default"},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := arrow.NewSchema([]arrow.Field{{
					Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: writerFieldIDMeta("1"),
				}}, nil)

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1}`))
			},
			check: func(t *testing.T, got arrow.RecordBatch) {
				assert.Equal(t, "default", got.Column(1).(*array.String).Value(0))
			},
		},
		{
			name: "timestamp nanoseconds require downcast",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampNs},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := arrow.NewSchema([]arrow.Field{{
					Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true, Metadata: writerFieldIDMeta("1"),
				}}, nil)
				builder := array.NewRecordBuilder(mem, schema)
				builder.Field(0).(*array.TimestampBuilder).Append(-1_500)
				batch := builder.NewRecordBatch()
				builder.Release()

				return batch
			},
			check: func(t *testing.T, got arrow.RecordBatch) {
				assert.Equal(t, arrow.Timestamp(-2), got.Column(0).(*array.Timestamp).Value(0))
			},
		},
		{
			name: "large list requires offset conversion",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "items", Type: &iceberg.ListType{
					ElementID: 2, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true,
				}},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "items", Type: &iceberg.ListType{
					ElementID: 2, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true,
				}},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				target := projectionTestSchema(t, iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "items", Type: &iceberg.ListType{
						ElementID: 2, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true,
					}},
				))
				listType := target.Field(0).Type.(*arrow.ListType)
				schema := arrow.NewSchema([]arrow.Field{{
					Name: "items", Type: arrow.LargeListOfField(listType.ElemField()), Nullable: true,
					Metadata: writerFieldIDMeta("1"),
				}}, nil)

				return projectionTestRecord(t, mem, schema, []byte(`{"items": [1, 2]}`))
			},
		},
		{
			name: "field ID metadata mismatch",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				schema := arrow.NewSchema([]arrow.Field{{
					Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
				}}, nil)

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1}`))
			},
		},
		{
			name: "top-level metadata mismatch",
			requested: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			),
			provided: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			),
			batch: func(t *testing.T, mem memory.Allocator) arrow.RecordBatch {
				field := arrow.Field{
					Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: writerFieldIDMeta("1"),
				}
				metadata := arrow.MetadataFrom(map[string]string{"source": "input"})
				schema := arrow.NewSchema([]arrow.Field{field}, &metadata)

				return projectionTestRecord(t, mem, schema, []byte(`{"id": 1}`))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			target := projectionTestSchema(t, tt.requested)
			batch := tt.batch(t, mem)
			got := assertProjectedBatch(t, tt.requested, tt.provided, batch, target, tt.wantReuse)
			if tt.check != nil {
				tt.check(t, got)
			}
			got.Release()
			batch.Release()
		})
	}
}

func TestToRequestedSchemaMatchesSchemaToArrowSchema(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	requested := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "profile", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String},
			{ID: 4, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
		}}},
	)
	inputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "profile", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		), Nullable: true},
	}, nil)
	batch := projectionTestRecord(t, mem, inputSchema, []byte(`{"id": 1, "profile": {"name": "one", "age": 42}}`))
	defer batch.Release()

	got, err := ToRequestedSchema(context.Background(), requested, requested, batch, writerProjectionOptions())
	require.NoError(t, err)
	defer got.Release()

	expected, err := SchemaToArrowSchemaWithOptions(requested, ArrowSchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	assert.True(t, got.Schema().Equal(expected), "expected schema: %s\ngot: %s", expected, got.Schema())
	assert.True(t, got.Schema().Metadata().Equal(expected.Metadata()))
}

type captureWriteDataFileFormat struct {
	tblutils.FileFormat
	batches []arrow.RecordBatch
	writer  *captureFileWriter
}

func (f *captureWriteDataFileFormat) WriteDataFile(_ context.Context, _ iceio.WriteFileIO, _ map[int]any, _ tblutils.WriteFileInfo, batches []arrow.RecordBatch) (iceberg.DataFile, error) {
	f.batches = batches

	return nil, nil
}

func (f *captureWriteDataFileFormat) NewFileWriter(_ context.Context, _ iceio.WriteFileIO, _ map[int]any, _ tblutils.WriteFileInfo, _ *arrow.Schema) (tblutils.FileWriter, error) {
	f.writer = &captureFileWriter{}

	return f.writer, nil
}

type captureFileWriter struct {
	batch arrow.RecordBatch
}

func (w *captureFileWriter) Write(batch arrow.RecordBatch) error {
	w.batch = batch

	return nil
}

func (w *captureFileWriter) BytesWritten() int64              { return 0 }
func (w *captureFileWriter) Close() (iceberg.DataFile, error) { return nil, nil }
func (w *captureFileWriter) Abort() error                     { return nil }

func TestDefaultDataFileWriterReusesExactBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec()
	metadata, err := NewMetadata(schema, &spec, UnsortedSortOrder, t.TempDir(), iceberg.Properties{})
	require.NoError(t, err)
	metaBuilder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)

	format := &captureWriteDataFileFormat{FileFormat: tblutils.GetFileFormat(iceberg.ParquetFile)}
	writer, err := newDataFileWriter(t.TempDir(), iceio.LocalFS{}, metaBuilder, iceberg.Properties{}, withFormat(format))
	require.NoError(t, err)

	arrowSchema := projectionTestSchema(t, schema)
	record := projectionTestRecord(t, mem, arrowSchema, []byte(`{"id": 1}`))
	defer record.Release()
	// writeFile releases both the task batch and the returned batch. The fast
	// path returns this same record, so retain it for the extra release.
	record.Retain()

	_, err = writer.writeFile(t.Context(), nil, WriteTask{
		Uuid: uuid.New(), ID: 1, FileCount: 1, Schema: schema,
		Batches: []arrow.RecordBatch{record},
	})
	require.NoError(t, err)
	require.Len(t, format.batches, 1)
	assert.Same(t, record, format.batches[0])
}

func TestRollingDataWriterReusesExactBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec()
	metadata, err := NewMetadata(schema, &spec, UnsortedSortOrder, t.TempDir(), iceberg.Properties{})
	require.NoError(t, err)
	metaBuilder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	arrowSchema := projectionTestSchema(t, schema)
	writeUUID := uuid.New()
	factory, err := newWriterFactory(t.TempDir(), recordWritingArgs{
		sc:        arrowSchema,
		fs:        iceio.LocalFS{},
		writeUUID: &writeUUID,
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					return
				}
			}
		},
	}, metaBuilder, schema, 1024*1024)
	require.NoError(t, err)
	format := &captureWriteDataFileFormat{FileFormat: tblutils.GetFileFormat(iceberg.ParquetFile)}
	factory.format = format

	output := make(chan iceberg.DataFile, 1)
	writer := factory.newRollingDataWriter(t.Context(), "", nil, output)
	record := projectionTestRecord(t, mem, arrowSchema, []byte(`{"id": 1}`))
	defer record.Release()

	require.NoError(t, writer.Add(record))
	require.NoError(t, writer.closeAndWait())
	require.NoError(t, factory.closeAll())
	require.NotNil(t, format.writer)
	assert.Same(t, record, format.writer.batch)
}
