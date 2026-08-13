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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeColEncoderMatchesGenericForNullFastPathTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name  string
		build func(memory.Allocator, encoderArrayShape) arrow.Array
	}{
		{name: "int8", build: buildInt8Array},
		{name: "int16", build: buildInt16Array},
		{name: "int32", build: buildInt32Array},
		{name: "int64", build: buildInt64Array},
		{name: "float32", build: buildFloat32Array},
		{name: "float64", build: buildFloat64Array},
		{name: "date32", build: buildDate32Array},
		{name: "date64", build: buildDate64Array},
		{name: "time32", build: buildTime32Array},
		{name: "time64", build: buildTime64Array},
		{name: "timestamp", build: buildTimestampArray},
	}

	for _, tt := range tests {
		for _, shape := range encoderArrayShapes {
			t.Run(tt.name+"/"+shape.name, func(t *testing.T) {
				arr := tt.build(mem, shape.shape)
				defer arr.Release()

				encoder := makeColEncoder(arr)

				var fast, generic bytes.Buffer
				for row := 0; row < arr.Len(); row++ {
					fast.Reset()
					generic.Reset()

					encoder(&fast, row)
					encodeArrowValue(&generic, arr, row)

					assert.Equal(t, generic.Bytes(), fast.Bytes(), "row %d", row)
				}
			})
		}
	}
}

func TestMakeArrowFieldEncoderHonorsNullStructParents(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	childBuilder := array.NewInt64Builder(mem)
	childBuilder.Append(123)
	childBuilder.Append(456)
	child := childBuilder.NewArray()
	childBuilder.Release()
	defer child.Release()
	structArray, err := array.NewStructArrayWithNulls(
		[]arrow.Array{child}, []string{"id"}, memory.NewBufferBytes([]byte{0x02}), 1, 0)
	require.NoError(t, err)
	structType := structArray.DataType().(*arrow.StructType)

	schema := arrow.NewSchema([]arrow.Field{{Name: "person", Type: structType}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{structArray}, 2)
	structArray.Release()
	defer record.Release()

	assert.False(t, record.Column(0).(*array.Struct).Field(0).IsNull(0), "child storage must remain valid under the null parent")

	encoder, err := makeArrowFieldEncoder(record, arrowFieldRef{path: []int{0, 0}}, 1, "person.id", "")
	require.NoError(t, err)

	var got bytes.Buffer
	encoder(&got, 0)
	assert.Equal(t, []byte{0}, got.Bytes())

	got.Reset()
	encoder(&got, 1)
	var want bytes.Buffer
	encodeArrowValue(&want, record.Column(0).(*array.Struct).Field(0), 1)
	assert.Equal(t, want.Bytes(), got.Bytes())
}

type encoderArrayShape int

const (
	encoderArrayNullsAtEnds encoderArrayShape = iota
	encoderArrayAllNull
	encoderArrayAllValid
)

var encoderArrayShapes = []struct {
	name  string
	shape encoderArrayShape
}{
	{name: "nulls-at-ends", shape: encoderArrayNullsAtEnds},
	{name: "all-null", shape: encoderArrayAllNull},
	{name: "all-valid", shape: encoderArrayAllValid},
}

type testArrayBuilder[T any] interface {
	Append(T)
	AppendNull()
	AppendNulls(int)
	NewArray() arrow.Array
	Release()
}

func buildFastPathArray[T any](builder testArrayBuilder[T], shape encoderArrayShape, values ...T) arrow.Array {
	defer builder.Release()

	switch shape {
	case encoderArrayNullsAtEnds:
		builder.AppendNull()
		builder.Append(values[0])
		builder.Append(values[1])
		builder.AppendNull()
	case encoderArrayAllNull:
		builder.AppendNulls(4)
	case encoderArrayAllValid:
		for _, v := range values {
			builder.Append(v)
		}
	}

	return builder.NewArray()
}

func buildInt8Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt8Builder(mem), shape, int8(7), int8(-3), int8(0), int8(4))
}

func buildInt16Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt16Builder(mem), shape, int16(7), int16(-3), int16(0), int16(4))
}

func buildInt32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt32Builder(mem), shape, int32(7), int32(-3), int32(0), int32(4))
}

func buildInt64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt64Builder(mem), shape, int64(7), int64(-3), int64(0), int64(4))
}

func buildFloat32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewFloat32Builder(mem), shape, float32(7.5), float32(-3.25), float32(0), float32(4))
}

func buildFloat64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewFloat64Builder(mem), shape, 7.5, -3.25, 0, 4)
}

func buildDate32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewDate32Builder(mem), shape, arrow.Date32(7), arrow.Date32(-3), arrow.Date32(0), arrow.Date32(4))
}

func buildDate64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewDate64Builder(mem), shape, arrow.Date64(7), arrow.Date64(-3), arrow.Date64(0), arrow.Date64(4))
}

func buildTime32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(
		array.NewTime32Builder(mem, arrow.FixedWidthTypes.Time32s.(*arrow.Time32Type)),
		shape, arrow.Time32(7), arrow.Time32(-3), arrow.Time32(0), arrow.Time32(4))
}

func buildTime64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(
		array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type)),
		shape, arrow.Time64(7), arrow.Time64(-3), arrow.Time64(0), arrow.Time64(4))
}

func buildTimestampArray(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(
		array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType)),
		shape, arrow.Timestamp(7), arrow.Timestamp(-3), arrow.Timestamp(0), arrow.Timestamp(4))
}

func TestReadAllEqualityDeleteFilesRejectsEmptyEqualityFieldIDs(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"mem://default/table/delete/empty-equality-fields.parquet",
		iceberg.ParquetFile, nil, nil, nil, 1, 128,
	)
	require.NoError(t, err)
	deleteFile := builder.EqualityFieldIDs(nil).Build()

	_, err = readAllEqualityDeleteFiles(
		t.Context(),
		iceio.NewMemFS(),
		schema,
		nil,
		[]FileScanTask{{EqualityDeleteFiles: []iceberg.DataFile{deleteFile}}},
		1,
	)
	require.ErrorIs(t, err, ErrEmptyEqualityFieldIDs)
	require.ErrorContains(t, err, "empty-equality-fields.parquet")
}

func TestProcessEqualityDeletesUsesStructuralFieldPaths(t *testing.T) {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	first := builder.NewArray()
	builder.Append(2)
	second := builder.NewArray()
	builder.Release()
	var key bytes.Buffer
	encodeArrowValue(&key, first, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{first, second}, 1)
	first.Release()
	second.Release()

	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "left", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "right", Type: iceberg.PrimitiveTypes.Int64},
	)
	process, err := processEqualityDeletesColumnarForFile(context.Background(), []*equalityDeleteSet{{
		keys:     set[string]{key.String(): {}},
		fieldIDs: []int{1},
		colNames: []string{"id"},
	}}, fileSchema, "data.parquet")
	require.NoError(t, err)

	result, err := process(record)
	require.NoError(t, err)
	assert.Zero(t, result.NumRows())
	result.Release()
}

func TestProcessEqualityDeletesRejectsMismatchedFieldMetadata(t *testing.T) {
	process, err := processEqualityDeletesColumnarForFile(context.Background(), []*equalityDeleteSet{{
		keys:     make(set[string]),
		fieldIDs: []int{1},
		colNames: []string{"id", "other"},
	}}, iceberg.NewSchema(0), "data.parquet")

	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Nil(t, process)
}

func TestResolveArrowFieldUsesFieldIDBeforeName(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "renamed", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 4, Name: "record", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Int64},
		}}},
	)

	refs := indexArrowFields(schema)
	ref, err := resolveArrowField(refs, 2, "id", "data.parquet")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, ref.path)

	ref, err = resolveArrowField(refs, 3, "record.value", "data.parquet")
	require.NoError(t, err)
	assert.Equal(t, []int{2, 0}, ref.path)
}

func TestResolveArrowFieldRejectsAmbiguousOrMismatchedIDs(t *testing.T) {
	tests := []struct {
		name      string
		schema    *iceberg.Schema
		fieldID   int
		fieldName string
		wantPath  []int
		wantErr   error
	}{
		{
			name: "renamed field uses matching ID",
			schema: iceberg.NewSchema(0, iceberg.NestedField{
				ID: 7, Name: "renamed", Type: iceberg.PrimitiveTypes.Int64,
			}),
			fieldID: 7, fieldName: "old_name", wantPath: []int{0},
		},
		{
			name: "missing ID is rejected even if a name matches",
			schema: iceberg.NewSchema(0, iceberg.NestedField{
				ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64,
			}),
			fieldID: 1, fieldName: "id", wantErr: errors.New("not found"),
		},
		{
			name: "literal dotted name is distinct from nested path",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "user.id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "user", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				}}},
			),
			fieldID: 1, fieldName: "user.id", wantPath: []int{0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ref, err := resolveArrowField(indexArrowFields(test.schema), test.fieldID, test.fieldName, "data.parquet")
			if test.wantErr != nil {
				require.Error(t, err)
				if test.wantErr == ErrAmbiguousEqualityColumn {
					assert.ErrorIs(t, err, test.wantErr)
				}
				assert.Contains(t, err.Error(), test.wantErr.Error())
				assert.Contains(t, err.Error(), "data.parquet")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, test.wantPath, ref.path)
		})
	}
}

func TestResolveArrowFieldRejectsDuplicateMetadataIDs(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "left", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"})},
		{Name: "right", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"})},
	}, nil)

	_, err := resolveArrowField(indexArrowFieldsByMetadata(schema), 1, "id", "data.parquet")
	require.ErrorIs(t, err, ErrAmbiguousEqualityColumn)
	require.ErrorContains(t, err, "data.parquet")
}
