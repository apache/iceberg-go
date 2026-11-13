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

package internal_test

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteDataFileRecordsNaNValueCounts writes float and double columns
// through the full ParquetFileWriter path. Parquet statistics carry no NaN
// count, so the writer counts NaN values itself; like Java, it records a count,
// zero included, for each float leaf outside a list or map whose metrics mode
// isn't none.
func TestWriteDataFileRecordsNaNValueCounts(t *testing.T) {
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "d", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 2, Name: "f", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 3, Name: "s", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 4, Name: "x", Type: iceberg.PrimitiveTypes.Float64},
		)},
		iceberg.NestedField{ID: 5, Name: "l", Type: iceberg.NewListType(6, iceberg.PrimitiveTypes.Float64, false)},
		iceberg.NestedField{ID: 7, Name: "n", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 8, Name: "z", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 9, Name: "i", Type: iceberg.PrimitiveTypes.Int32},
	)
	arrowSchema, err := table.SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	nan := math.NaN()
	type row struct {
		d, x, n, z float64
		f          float32
		dNull      bool
		sNull      bool
		list       []float64
	}
	batch := func(rows []row) arrow.RecordBatch {
		bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer bldr.Release()

		for _, r := range rows {
			if r.dNull {
				bldr.Field(0).AppendNull()
			} else {
				bldr.Field(0).(*array.Float64Builder).Append(r.d)
			}
			bldr.Field(1).(*array.Float32Builder).Append(r.f)
			sb := bldr.Field(2).(*array.StructBuilder)
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Float64Builder).Append(r.x)
			lb := bldr.Field(3).(*array.ListBuilder)
			lb.Append(true)
			for _, v := range r.list {
				lb.ValueBuilder().(*array.Float64Builder).Append(v)
			}
			bldr.Field(4).(*array.Float64Builder).Append(r.n)
			bldr.Field(5).(*array.Float64Builder).Append(r.z)
			bldr.Field(6).(*array.Int32Builder).Append(1)
		}

		rec := bldr.NewRecordBatch()
		defer rec.Release()

		// StructBuilder.Append(false) appends a null child too, so null struct
		// rows are set afterwards over the built child: a NaN then sits under a
		// null struct row without belonging to it.
		valid := make([]byte, bitutil.BytesForBits(int64(len(rows))))
		nulls := 0
		for i, r := range rows {
			if r.sNull {
				nulls++
			} else {
				bitutil.SetBit(valid, i)
			}
		}
		structData := rec.Column(2).Data()
		withNulls := array.NewData(structData.DataType(), structData.Len(),
			[]*memory.Buffer{memory.NewBufferBytes(valid)}, structData.Children(), nulls, 0)
		defer withNulls.Release()
		structArr := array.MakeFromData(withNulls)
		defer structArr.Release()

		cols := slices.Clone(rec.Columns())
		cols[2] = structArr

		return array.NewRecordBatch(arrowSchema, cols, rec.NumRows())
	}

	first := batch([]row{
		{d: 1, f: float32(nan), x: nan, list: []float64{nan}, n: nan, z: 1},
		{d: nan, f: 2, x: nan, sNull: true, list: []float64{nan, 1}, n: nan, z: 2},
	})
	defer first.Release()
	second := batch([]row{
		{dNull: true, f: 3, x: 1, list: []float64{nan}, n: nan, z: 3},
		{d: nan, f: 4, x: nan, n: nan, z: 4},
	})
	defer second.Release()

	// The NaN under the null struct row must really be in the child array,
	// or the s.x count below would pass without the null check.
	structCol := first.Column(2).(*array.Struct)
	require.True(t, structCol.IsNull(1))
	require.True(t, math.IsNaN(structCol.Field(0).(*array.Float64).Value(1)))

	full := internal.MetricsMode{Typ: internal.MetricModeFull}
	counts := internal.MetricsMode{Typ: internal.MetricModeCounts}
	statsCols := map[int]internal.StatisticsCollector{
		1: {FieldID: 1, IcebergTyp: iceberg.PrimitiveTypes.Float64, ColName: "d", Mode: full},
		2: {FieldID: 2, IcebergTyp: iceberg.PrimitiveTypes.Float32, ColName: "f", Mode: counts},
		4: {FieldID: 4, IcebergTyp: iceberg.PrimitiveTypes.Float64, ColName: "s.x", Mode: counts},
		6: {FieldID: 6, IcebergTyp: iceberg.PrimitiveTypes.Float64, ColName: "l.element", Mode: counts},
		7: {FieldID: 7, IcebergTyp: iceberg.PrimitiveTypes.Float64, ColName: "n", Mode: internal.MetricsMode{Typ: internal.MetricModeNone}},
		8: {FieldID: 8, IcebergTyp: iceberg.PrimitiveTypes.Float64, ColName: "z", Mode: full},
		9: {FieldID: 9, IcebergTyp: iceberg.PrimitiveTypes.Int32, ColName: "i", Mode: full},
	}

	fm := internal.GetFileFormat(iceberg.ParquetFile)
	df, err := fm.WriteDataFile(context.Background(), iceio.NewMemFS(), nil, internal.WriteFileInfo{
		FileSchema: iceSchema,
		Spec:       *iceberg.UnpartitionedSpec,
		FileName:   "nan.parquet",
		StatsCols:  statsCols,
		WriteProps: fm.GetWriteProperties(iceberg.Properties{}),
		Content:    iceberg.EntryContentData,
	}, []arrow.RecordBatch{first, second})
	require.NoError(t, err)

	// d: NaN in both batches, and the NULL row is not a NaN. f: counts mode
	// still records NaN counts. s.x: three NaN child values, one of them
	// under the null struct row. z: no NaN still records a zero, which is
	// what lets a metrics evaluator rule NaN out. l.element is in a list and
	// n has metrics mode none, so neither is recorded, nor is the int column.
	assert.Equal(t, map[int]int64{1: 2, 2: 1, 4: 2, 8: 0}, df.NaNValueCounts())

	// Parquet already leaves NaN out of float min/max, so the bounds hold
	// only the finite values.
	lower, upper := df.LowerBoundValues(), df.UpperBoundValues()
	d := func(b []byte) float64 {
		lit, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float64, b)
		require.NoError(t, err)

		return float64(lit.(iceberg.Float64Literal))
	}
	assert.Equal(t, 1.0, d(lower[1]))
	assert.Equal(t, 1.0, d(upper[1]))
}
