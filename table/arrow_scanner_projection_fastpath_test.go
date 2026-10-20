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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func projectionPrimitiveSchemas(t testing.TB) (*iceberg.Schema, *arrow.Schema, *arrow.Schema) {
	t.Helper()
	requested := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "city", Type: iceberg.PrimitiveTypes.String, Doc: "city label"},
	)
	output, err := SchemaToArrowSchemaWithOptions(requested, ArrowSchemaOptions{})
	require.NoError(t, err)

	fields := output.Fields()
	for i := range fields {
		fields[i].Metadata = arrow.NewMetadata(
			[]string{ArrowParquetFieldIDKey}, []string{strconv.Itoa(i + 1)})
	}
	meta := arrow.NewMetadata([]string{"parquet-source"}, []string{"source-metadata"})
	input := arrow.NewSchema(fields, &meta)

	return requested, input, output
}

func TestCanReuseFlatProjection(t *testing.T) {
	requested, input, output := projectionPrimitiveSchemas(t)
	require.True(t, canReuseFlatProjection(requested, requested, input, output))

	fields := input.Fields()
	fields[0], fields[1] = fields[1], fields[0]
	require.False(t, canReuseFlatProjection(requested, requested, arrow.NewSchema(fields, nil), output),
		"reordered columns must be projected")

	fields = input.Fields()
	fields[0].Metadata = arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"12"})
	require.False(t, canReuseFlatProjection(requested, requested, arrow.NewSchema(fields, nil), output),
		"incorrect Arrow field IDs must not bypass projection")

	fields = input.Fields()
	fields[0].Name = "previous_name"
	require.False(t, canReuseFlatProjection(requested, requested, arrow.NewSchema(fields, nil), output),
		"renamed Arrow fields must be projected")

	fields = input.Fields()
	fields[0].Type = arrow.PrimitiveTypes.Int32
	require.False(t, canReuseFlatProjection(requested, requested, arrow.NewSchema(fields, nil), output),
		"mismatched physical Arrow types must not be rewrapped")

	fields = input.Fields()
	fields[1].Name = fields[0].Name
	require.False(t, canReuseFlatProjection(requested, requested, arrow.NewSchema(fields, nil), output),
		"duplicate Arrow field names must not bypass projection")

	fields = append(input.Fields(), arrow.Field{Name: "unexpected", Type: arrow.PrimitiveTypes.Int32})
	require.False(t, canReuseFlatProjection(requested, requested, arrow.NewSchema(fields, nil), output),
		"extra input columns must still follow schema projection")

	oldName := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "previous_name", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "city", Type: iceberg.PrimitiveTypes.String},
	)
	require.False(t, canReuseFlatProjection(requested, oldName, input, output),
		"renamed Iceberg fields must be projected")

	oldType := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "city", Type: iceberg.PrimitiveTypes.String},
	)
	require.False(t, canReuseFlatProjection(requested, oldType, input, output),
		"type promotions must be projected")

	missing := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	require.False(t, canReuseFlatProjection(requested, missing, input, output),
		"missing fields may need defaults")

	nested := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "struct", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int64},
		}},
	})
	nestedArrow, err := SchemaToArrowSchema(nested, nil, false, false)
	require.NoError(t, err)
	require.False(t, canReuseFlatProjection(nested, nested, nestedArrow, nestedArrow),
		"nested fields require the full projection visitor")
}

func TestReuseFlatProjectionPreservesRowsAndMetadata(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	requested, inputSchema, outputSchema := projectionPrimitiveSchemas(t)
	builder := array.NewRecordBuilder(mem, inputSchema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 8, 9}, nil)
	cities := builder.Field(1).(*array.StringBuilder)
	cities.Append("Berlin")
	cities.AppendNull()
	cities.Append("Vienna")
	batch := builder.NewRecordBatch()
	defer batch.Release()

	require.True(t, canReuseFlatProjection(requested, requested, batch.Schema(), outputSchema))
	expected, err := ToRequestedSchema(context.Background(), requested, requested, batch, SchemaOptions{})
	require.NoError(t, err)
	defer expected.Release()
	got := array.NewRecordBatch(outputSchema, batch.Columns(), batch.NumRows())
	defer got.Release()

	require.True(t, array.RecordEqual(expected, got))
	require.True(t, arrowSchemaEqual(expected.Schema(), got.Schema()))
	require.Same(t, batch.Column(0), got.Column(0), "input arrays must be reused")
	require.Same(t, batch.Column(1), got.Column(1), "input arrays must be reused")
	require.Equal(t, 1, got.Column(1).NullN())
}

func BenchmarkFlatPrimitiveProjection(b *testing.B) {
	for _, width := range []int{4, 32} {
		b.Run(fmt.Sprintf("fields=%d", width), func(b *testing.B) {
			fields := make([]iceberg.NestedField, width)
			for i := range fields {
				fields[i] = iceberg.NestedField{
					ID: i + 1, Name: fmt.Sprintf("field_%d", i), Type: iceberg.PrimitiveTypes.Int64,
				}
			}
			requested := iceberg.NewSchema(1, fields...)
			output, err := SchemaToArrowSchema(requested, nil, false, false)
			if err != nil {
				b.Fatal(err)
			}
			inputFields := output.Fields()
			for i := range inputFields {
				inputFields[i].Metadata = arrow.NewMetadata(
					[]string{ArrowParquetFieldIDKey}, []string{strconv.Itoa(i + 1)})
			}
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(inputFields, nil))
			defer builder.Release()
			values := make([]int64, 1024)
			for i := range values {
				values[i] = int64(i)
			}
			for i := range inputFields {
				builder.Field(i).(*array.Int64Builder).AppendValues(values, nil)
			}
			batch := builder.NewRecordBatch()
			defer batch.Release()
			if !canReuseFlatProjection(requested, requested, batch.Schema(), output) {
				b.Fatal("expected zero-copy projection")
			}

			b.Run("schema_visitor", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := ToRequestedSchema(context.Background(), requested, requested, batch, SchemaOptions{})
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
			b.Run("zero_copy", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out := array.NewRecordBatch(output, batch.Columns(), batch.NumRows())
					out.Release()
				}
			})
		})
	}
}
