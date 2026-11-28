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
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNDVValueBytesMatchesLiteral checks that every fast path in ndvValueBytes
// hashes exactly the bytes the literal path (getArrowValueAsIcebergLiteral then
// MarshalBinary) produces, so sketches don't change.
func TestNDVValueBytesMatchesLiteral(t *testing.T) {
	mem := memory.DefaultAllocator
	fixed4, err := iceberg.FixedTypeOf(4)
	require.NoError(t, err)
	dec, err := iceberg.DecimalTypeOf(38, 4)
	require.NoError(t, err)

	fromJSON := func(dt arrow.DataType, js string) arrow.Array {
		arr, _, err := array.FromJSON(mem, dt, strings.NewReader(js))
		require.NoError(t, err)
		t.Cleanup(arr.Release)

		return arr
	}

	decs := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 38, Scale: 4})
	defer decs.Release()
	decs.AppendNull()
	for _, v := range []int64{0, 1, -1, 127, 128, -128, -129, 255, -255, -256, 32767, -32768, -32769, math.MaxInt64, math.MinInt64} {
		decs.Append(decimal128.FromI64(v))
	}
	decs.Append(decimal128.New(math.MaxInt64, math.MaxUint64)) // max int128
	decs.Append(decimal128.New(math.MinInt64, 0))              // min int128
	decs.Append(decimal128.New(0, 1<<63))                      // 2^63
	decs.Append(decimal128.New(-1, 1<<63))                     // -2^63
	decs.Append(decimal128.New(-1, 0x7f00000000000000))
	decArr := decs.NewArray()
	defer decArr.Release()

	ts := func(unit arrow.TimeUnit) arrow.DataType { return &arrow.TimestampType{Unit: unit} }
	tsJSON := `[null, 0, -1, 1, 1700000000123456, -1700000000123456]`

	tests := []struct {
		name string
		typ  iceberg.Type
		arr  arrow.Array
	}{
		{"boolean", iceberg.PrimitiveTypes.Bool, fromJSON(arrow.FixedWidthTypes.Boolean, `[true, null, false]`)},
		{"int", iceberg.PrimitiveTypes.Int32, fromJSON(arrow.PrimitiveTypes.Int32, `[0, -1, 1, null, 2147483647, -2147483648]`)},
		{"long", iceberg.PrimitiveTypes.Int64, fromJSON(arrow.PrimitiveTypes.Int64, `[0, -1, null, 9223372036854775807, -9223372036854775808]`)},
		{"float", iceberg.PrimitiveTypes.Float32, fromJSON(arrow.PrimitiveTypes.Float32, `[0, -0.0, -1.25, null, 3.4e38]`)},
		{"double", iceberg.PrimitiveTypes.Float64, fromJSON(arrow.PrimitiveTypes.Float64, `[0, -0.0, 0.1, null, -1e300]`)},
		{"date", iceberg.PrimitiveTypes.Date, fromJSON(arrow.FixedWidthTypes.Date32, `[0, -1, 18000, null, -719162]`)},
		{"time", iceberg.PrimitiveTypes.Time, fromJSON(arrow.FixedWidthTypes.Time64us, `[0, null, 86399999999]`)},
		{"timestamp_s", iceberg.PrimitiveTypes.Timestamp, fromJSON(ts(arrow.Second), `[null, 0, -1, 1, 1700000000]`)},
		{"timestamp_ms", iceberg.PrimitiveTypes.TimestampTz, fromJSON(ts(arrow.Millisecond), `[null, 0, -1, 1, 1700000000123]`)},
		{"timestamp_us", iceberg.PrimitiveTypes.Timestamp, fromJSON(ts(arrow.Microsecond), tsJSON)},
		{"timestamp_ns_to_us", iceberg.PrimitiveTypes.Timestamp, fromJSON(ts(arrow.Nanosecond), tsJSON)},
		{"timestamp_us_to_ns", iceberg.PrimitiveTypes.TimestampNs, fromJSON(ts(arrow.Microsecond), tsJSON)},
		{"timestamp_ns", iceberg.PrimitiveTypes.TimestampTzNs, fromJSON(ts(arrow.Nanosecond), tsJSON)},
		{"decimal", dec, decArr},
		{"string", iceberg.PrimitiveTypes.String, fromJSON(arrow.BinaryTypes.String, `["", "é", null, "value"]`)},
		{"large_string", iceberg.PrimitiveTypes.String, fromJSON(arrow.BinaryTypes.LargeString, `["", "é", null, "value"]`)},
		{"binary", iceberg.PrimitiveTypes.Binary, fromJSON(arrow.BinaryTypes.Binary, `["", "AAEC", null, "/w=="]`)},
		{"large_binary", iceberg.PrimitiveTypes.Binary, fromJSON(arrow.BinaryTypes.LargeBinary, `["", "AAEC", null]`)},
		{"fixed", fixed4, fromJSON(&arrow.FixedSizeBinaryType{ByteWidth: 4}, `["AAAAAA==", null, "3q2+7w=="]`)},
		{"uuid", iceberg.PrimitiveTypes.UUID, fromJSON(extensions.NewUUIDType(), `["f79c3e09-677c-4bbd-a479-3f349cb785e7", null, "00000000-0000-0000-0000-000000000001"]`)},
	}

	var scratch [16]byte
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Also check a slice, so values are read at a non-zero array offset.
			sliced := array.NewSlice(tc.arr, 1, int64(tc.arr.Len()))
			defer sliced.Release()
			for _, arr := range []arrow.Array{tc.arr, sliced} {
				for row := range arr.Len() {
					if arr.IsNull(row) {
						continue
					}
					lit, err := getArrowValueAsIcebergLiteral(arr, row, tc.typ)
					require.NoError(t, err)
					want, err := lit.MarshalBinary()
					require.NoError(t, err)

					got, ok := ndvValueBytes(scratch[:0], arr, row, tc.typ)
					require.True(t, ok, "no fast path for row %d", row)
					assert.Equal(t, want, got, "row %d (%v)", row, lit)
				}
			}
		})
	}
}

func BenchmarkAddRecordToSketches(b *testing.B) {
	const rows = 8192
	mem := memory.DefaultAllocator
	dec, _ := iceberg.DecimalTypeOf(18, 2)

	longs := array.NewInt64Builder(mem)
	strs := array.NewStringBuilder(mem)
	decs := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 18, Scale: 2})
	tss := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond})
	for i := range rows {
		longs.Append(int64(i) - rows/2)
		strs.Append("value-" + strconv.Itoa(i))
		decs.Append(decimal128.FromI64(int64(i) * 12345))
		tss.Append(arrow.Timestamp(1_700_000_000_000_000 + int64(i)))
	}

	cols := []struct {
		name string
		typ  iceberg.Type
		arr  arrow.Array
	}{
		{"long", iceberg.PrimitiveTypes.Int64, longs.NewArray()},
		{"string", iceberg.PrimitiveTypes.String, strs.NewArray()},
		{"decimal", dec, decs.NewArray()},
		{"timestamp", iceberg.PrimitiveTypes.Timestamp, tss.NewArray()},
	}
	for _, c := range cols {
		b.Run(c.name, func(b *testing.B) {
			sc := arrow.NewSchema([]arrow.Field{{Name: "v", Type: c.arr.DataType(), Nullable: true}}, nil)
			rec := array.NewRecordBatch(sc, []arrow.Array{c.arr}, rows)
			defer rec.Release()
			field := iceberg.NestedField{ID: 1, Name: "v", Type: c.typ}
			b.ReportAllocs()
			for b.Loop() {
				targets := map[int]*ndvTarget{0: {field: field, sketch: stats.NewSketch()}}
				if err := addRecordToSketches(rec, targets); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestNDVValueBytesStringFallsBackForConvertedTypes checks that strings under a
// uuid, binary or fixed column take the literal path, which converts them, so
// the fast path never hashes the raw string bytes there.
func TestNDVValueBytesStringFallsBackForConvertedTypes(t *testing.T) {
	fixed16, err := iceberg.FixedTypeOf(16)
	require.NoError(t, err)

	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		arr, _, err := array.FromJSON(memory.DefaultAllocator, dt,
			strings.NewReader(`["f79c3e09-677c-4bbd-a479-3f349cb785e7"]`))
		require.NoError(t, err)
		defer arr.Release()

		for _, typ := range []iceberg.Type{iceberg.PrimitiveTypes.UUID, iceberg.PrimitiveTypes.Binary, fixed16} {
			_, ok := ndvValueBytes(nil, arr, 0, typ)
			assert.False(t, ok, "%s under %s", dt, typ)
		}
	}
}
