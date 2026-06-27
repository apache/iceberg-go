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
	"github.com/stretchr/testify/require"
)

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

func TestMakeColEncoderMatchesGenericEncodingForNullSpecializedTypes(t *testing.T) {
	mem := memory.DefaultAllocator

	tests := []struct {
		name  string
		build func() arrow.Array
	}{
		{
			name: "int8",
			build: func() arrow.Array {
				b := array.NewInt8Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(7)

				return b.NewArray()
			},
		},
		{
			name: "int16",
			build: func() arrow.Array {
				b := array.NewInt16Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(70)

				return b.NewArray()
			},
		},
		{
			name: "int32",
			build: func() arrow.Array {
				b := array.NewInt32Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(700)

				return b.NewArray()
			},
		},
		{
			name: "int64",
			build: func() arrow.Array {
				b := array.NewInt64Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(7000)

				return b.NewArray()
			},
		},
		{
			name: "float32",
			build: func() arrow.Array {
				b := array.NewFloat32Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(3.25)

				return b.NewArray()
			},
		},
		{
			name: "float64",
			build: func() arrow.Array {
				b := array.NewFloat64Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(6.5)

				return b.NewArray()
			},
		},
		{
			name: "date32",
			build: func() arrow.Array {
				b := array.NewDate32Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(arrow.Date32(10))

				return b.NewArray()
			},
		},
		{
			name: "date64",
			build: func() arrow.Array {
				b := array.NewDate64Builder(mem)
				defer b.Release()
				b.AppendNull()
				b.Append(arrow.Date64(20))

				return b.NewArray()
			},
		},
		{
			name: "time32",
			build: func() arrow.Array {
				b := array.NewTime32Builder(mem, &arrow.Time32Type{Unit: arrow.Millisecond})
				defer b.Release()
				b.AppendNull()
				b.Append(arrow.Time32(30))

				return b.NewArray()
			},
		},
		{
			name: "time64",
			build: func() arrow.Array {
				b := array.NewTime64Builder(mem, &arrow.Time64Type{Unit: arrow.Microsecond})
				defer b.Release()
				b.AppendNull()
				b.Append(arrow.Time64(40))

				return b.NewArray()
			},
		},
		{
			name: "timestamp",
			build: func() arrow.Array {
				b := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
				defer b.Release()
				b.AppendNull()
				b.Append(arrow.Timestamp(50))

				return b.NewArray()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tt.build()
			defer arr.Release()

			enc := makeColEncoder(arr)
			for _, row := range []int{0, 1} {
				var got bytes.Buffer
				enc(&got, row)

				var want bytes.Buffer
				encodeArrowValue(&want, arr, row)

				require.Equal(t, want.Bytes(), got.Bytes())
			}
		})
	}
}
