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

package internal

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mkVar builds a variant.Value from a Go value via a fresh builder.
func mkVar(t *testing.T, raw any) variant.Value {
	t.Helper()
	var b variant.Builder
	require.NoError(t, b.Append(raw))
	v, err := b.Build()
	require.NoError(t, err)

	return v
}

func mkVars(t *testing.T, raws ...any) []variant.Value {
	t.Helper()
	out := make([]variant.Value, len(raws))
	for i, r := range raws {
		out[i] = mkVar(t, r)
	}

	return out
}

// bigI64 is large enough to force Int64 encoding (> 2^32).
const bigI64 = int64(5_000_000_000)

func TestAnalyzeEmptyAndAllNull(t *testing.T) {
	_, ok := AnalyzeVariantShredding(nil)
	assert.False(t, ok, "empty sample must not shred")

	_, ok = AnalyzeVariantShredding(mkVars(t, nil, nil, nil))
	assert.False(t, ok, "all variant-null must not shred")
}

func TestAnalyzeUniformObject(t *testing.T) {
	sample := mkVars(t,
		map[string]any{"a": bigI64, "b": "x"},
		map[string]any{"a": bigI64, "b": "y"},
		map[string]any{"a": bigI64, "b": "z"},
	)
	dt, ok := AnalyzeVariantShredding(sample)
	require.True(t, ok)

	want := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	assert.Truef(t, arrow.TypeEqual(want, dt), "want %s got %s", want, dt)
}

func TestAnalyzeRareFieldDropped(t *testing.T) {
	var sample []variant.Value
	for i := 0; i < 100; i++ {
		sample = append(sample, mkVar(t, map[string]any{"common": bigI64}))
	}
	for i := 0; i < 5; i++ { // 5/105 = 4.8% < 10% floor
		sample = append(sample, mkVar(t, map[string]any{"common": bigI64, "rare": "z"}))
	}
	dt, ok := AnalyzeVariantShredding(sample)
	require.True(t, ok)

	st, isStruct := dt.(*arrow.StructType)
	require.True(t, isStruct)
	_, hasCommon := st.FieldsByName("common")
	_, hasRare := st.FieldsByName("rare")
	assert.True(t, hasCommon, "common (100%%) must be kept")
	assert.False(t, hasRare, "rare (<10%%) must be dropped")
}

func TestAnalyzeIntegerWidening(t *testing.T) {
	// field "n" is small in some rows (Int8) and large in others (Int64).
	sample := mkVars(t,
		map[string]any{"n": int64(5)},
		map[string]any{"n": int64(5)},
		map[string]any{"n": bigI64},
	)
	dt, ok := AnalyzeVariantShredding(sample)
	require.True(t, ok)
	st := dt.(*arrow.StructType)
	f, _ := st.FieldsByName("n")
	require.Len(t, f, 1)
	assert.Truef(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int64, f[0].Type),
		"widening must pick Int64, got %s", f[0].Type)
}

func TestAnalyzeMixedTypeMajority(t *testing.T) {
	// "v" is an int in 7 rows, a string in 3 -> majority int wins.
	var sample []variant.Value
	for i := 0; i < 7; i++ {
		sample = append(sample, mkVar(t, map[string]any{"v": bigI64}))
	}
	for i := 0; i < 3; i++ {
		sample = append(sample, mkVar(t, map[string]any{"v": "s"}))
	}
	dt, ok := AnalyzeVariantShredding(sample)
	require.True(t, ok)
	st := dt.(*arrow.StructType)
	f, _ := st.FieldsByName("v")
	require.Len(t, f, 1)
	assert.Truef(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int64, f[0].Type),
		"majority int must win, got %s", f[0].Type)
}

func TestAnalyzeScalarRoot(t *testing.T) {
	dt, ok := AnalyzeVariantShredding(mkVars(t, bigI64, bigI64, bigI64))
	require.True(t, ok)
	assert.Truef(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int64, dt),
		"uniform scalar root must shred as the scalar, got %s", dt)
}

func TestAnalyzeNestedObject(t *testing.T) {
	sample := mkVars(t,
		map[string]any{"a": bigI64, "c": map[string]any{"x": bigI64}},
		map[string]any{"a": bigI64, "c": map[string]any{"x": bigI64}},
	)
	dt, ok := AnalyzeVariantShredding(sample)
	require.True(t, ok)

	want := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "c", Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		), Nullable: true},
	)
	assert.Truef(t, arrow.TypeEqual(want, dt), "want %s got %s", want, dt)
}

func TestAnalyzeArrayRoot(t *testing.T) {
	sample := mkVars(t,
		[]any{bigI64, bigI64},
		[]any{bigI64},
	)
	dt, ok := AnalyzeVariantShredding(sample)
	require.True(t, ok)
	want := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	assert.Truef(t, arrow.TypeEqual(want, dt), "want %s got %s", want, dt)
}

func TestDecimalArrowType(t *testing.T) {
	// precision = intDigits + scale, clamped to 38; always Decimal128 (pqarrow
	// maps it to INT32/INT64/FLBA by precision; Decimal32/64 are not writable).
	cases := []struct {
		intDigits, scale int
		want             arrow.DataType
	}{
		{3, 2, &arrow.Decimal128Type{Precision: 5, Scale: 2}},
		{10, 4, &arrow.Decimal128Type{Precision: 14, Scale: 4}},
		{30, 10, &arrow.Decimal128Type{Precision: 38, Scale: 8}},
	}
	for _, c := range cases {
		got := decimalArrowType(&fieldInfo{maxDecimalIntDigits: c.intDigits, maxDecimalScale: c.scale})
		assert.Truef(t, arrow.TypeEqual(c.want, got), "intDigits=%d scale=%d: want %s got %s",
			c.intDigits, c.scale, c.want, got)
	}
}

// TestInferredTypesActuallyShred checks each inferred type is accepted by arrow-go's
// builder and actually shreds (residual value null), not falls to residual or panics.
func TestInferredTypesActuallyShred(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cases := []struct {
		name string
		raw  any
	}{
		{"bool", true},
		{"int", bigI64},
		{"double", float64(3.5)},
		{"string", "hello"},
		{"binary", []byte{1, 2, 3}},
		{"object", map[string]any{"a": bigI64, "b": "x"}},
		{"array", []any{bigI64, bigI64}},
		{"nested", map[string]any{"a": bigI64, "c": map[string]any{"x": bigI64}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := mkVar(t, c.raw)
			inner, ok := AnalyzeVariantShredding([]variant.Value{v})
			require.Truef(t, ok, "should infer a shredded type for %s", c.name)

			st := extensions.NewShreddedVariantType(inner)
			bldr := extensions.NewVariantBuilder(mem, st) // panics if inner is unsupported
			defer bldr.Release()
			bldr.Append(mkVar(t, c.raw))
			arr := bldr.NewArray().(*extensions.VariantArray)
			defer arr.Release()

			require.True(t, arr.IsShredded())
			// Fully shredded: the residual value column is null at row 0.
			assert.Truef(t, arr.UntypedValues().IsNull(0),
				"%s: expected fully shredded (residual value null)", c.name)

			// Data integrity: reassembling through the reader must reproduce the
			// original value, not just produce a non-null typed column.
			round, err := extensions.UnshredVariant(arr, mem)
			require.NoError(t, err)
			defer round.Release()
			rv, err := round.Value(0)
			require.NoError(t, err)
			want, err := v.MarshalJSON()
			require.NoError(t, err)
			got, err := rv.MarshalJSON()
			require.NoError(t, err)
			assert.JSONEqf(t, string(want), string(got), "%s round-trip value", c.name)
		})
	}
}

// TestInferredTypedScalarsShred round-trips the decimal, temporal, and uuid leaf
// types end to end: build a real value, infer, shred, reassemble via UnshredVariant.
func TestInferredTypedScalarsShred(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	cases := []struct {
		name  string
		build func(*variant.Builder) error
	}{
		{"decimal4", func(b *variant.Builder) error { return b.AppendDecimal4(2, decimal.Decimal32(12345)) }},          // 123.45 -> Decimal32
		{"decimal8", func(b *variant.Builder) error { return b.AppendDecimal8(4, decimal.Decimal64(12345678901234)) }}, // prec 14 -> Decimal64
		{"decimal16", func(b *variant.Builder) error {
			return b.AppendDecimal16(2, decimal128.FromI64(1234567890123456789)) // prec 19 -> Decimal128
		}},
		{"date", func(b *variant.Builder) error { return b.AppendDate(arrow.Date32(19000)) }},
		{"time", func(b *variant.Builder) error { return b.AppendTimeMicro(arrow.Time64(43200000000)) }},
		{"ts_utc", func(b *variant.Builder) error {
			return b.AppendTimestamp(arrow.Timestamp(1700000000000000), true, true)
		}},
		{"ts_ntz", func(b *variant.Builder) error {
			return b.AppendTimestamp(arrow.Timestamp(1700000000000000), true, false)
		}},
		{"uuid", func(b *variant.Builder) error {
			return b.AppendUUID(uuid.MustParse("12345678-1234-1234-1234-123456789abc"))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mk := func() variant.Value {
				var b variant.Builder
				require.NoError(t, c.build(&b))
				v, err := b.Build()
				require.NoError(t, err)

				return v
			}
			v := mk()
			inner, ok := AnalyzeVariantShredding([]variant.Value{v})
			require.Truef(t, ok, "%s should infer a shredded type", c.name)

			st := extensions.NewShreddedVariantType(inner)
			bldr := extensions.NewVariantBuilder(mem, st)
			defer bldr.Release()
			bldr.Append(mk())
			arr := bldr.NewArray().(*extensions.VariantArray)
			defer arr.Release()

			require.True(t, arr.IsShredded())
			assert.Truef(t, arr.UntypedValues().IsNull(0), "%s: expected fully shredded", c.name)

			round, err := extensions.UnshredVariant(arr, mem)
			require.NoError(t, err)
			defer round.Release()
			rv, err := round.Value(0)
			require.NoError(t, err)
			want, err := v.MarshalJSON()
			require.NoError(t, err)
			got, err := rv.MarshalJSON()
			require.NoError(t, err)
			assert.JSONEqf(t, string(want), string(got), "%s round-trip value", c.name)
		})
	}
}

// TestAnalyzeFieldCap exercises the >maxShreddedFields cap: 301 fields keep exactly
// 300, evicting the alphabetically-largest on the count tie.
func TestAnalyzeFieldCap(t *testing.T) {
	row := make(map[string]any, maxShreddedFields+1)
	for i := 0; i <= maxShreddedFields; i++ {
		row[fmt.Sprintf("f%03d", i)] = bigI64
	}
	dt, ok := AnalyzeVariantShredding(mkVars(t, row, row))
	require.True(t, ok)
	st, isStruct := dt.(*arrow.StructType)
	require.True(t, isStruct)
	assert.Equal(t, maxShreddedFields, st.NumFields(), "capped at maxShreddedFields")

	_, hasFirst := st.FieldsByName("f000")
	_, hasLast := st.FieldsByName(fmt.Sprintf("f%03d", maxShreddedFields))
	assert.True(t, hasFirst, "alphabetically-smallest kept")
	assert.False(t, hasLast, "alphabetically-largest evicted on the count tie")
}
