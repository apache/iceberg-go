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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
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
		[]FileScanTask{{EqualityDeleteFiles: []iceberg.DataFile{deleteFile}}},
		1,
	)
	require.ErrorIs(t, err, ErrEmptyEqualityFieldIDs)
	require.ErrorContains(t, err, "empty-equality-fields.parquet")
}
