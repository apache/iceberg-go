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
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// buildVariantExtractRec builds a "payload" variant column of shreddedType (nil => unshredded) from rows, plus the bound extract term for path/typ.
func buildVariantExtractRec(t testing.TB, mem memory.Allocator, shreddedType *extensions.VariantType, path string, typ iceberg.PrimitiveType, rows []map[string]any) (arrow.RecordBatch, iceberg.VariantExtractColumn) {
	t.Helper()
	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}})

	vt := shreddedType
	if vt == nil {
		vt = extensions.NewDefaultVariantType()
	}
	vb := extensions.NewVariantBuilder(mem, vt)
	for _, row := range rows {
		if row == nil {
			vb.AppendNull()

			continue
		}
		var b variant.Builder
		require.NoError(t, b.Append(row))
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	pArr := vb.NewArray()
	vb.Release()

	md := arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"2"})
	arrSchema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: pArr.DataType(), Nullable: true, Metadata: md}}, nil)
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{pArr}, int64(pArr.Len()))
	pArr.Release()

	term, err := iceberg.Extract("payload", path, typ).Bind(iceSchema, true)
	require.NoError(t, err)
	col := iceberg.VariantExtractColumn{Term: term.(iceberg.BoundExtract), FieldID: 100, Name: "_x", SourcePath: []string{"payload"}}

	return rec, col
}

func shredStruct(fields ...arrow.Field) *extensions.VariantType {
	return extensions.NewShreddedVariantType(arrow.StructOf(fields...))
}

// TestExtractFastPathParity: fast-path output must match the per-row walk on every branch; wantFast asserts whether it fires.
func TestExtractFastPathParity(t *testing.T) {
	i64 := arrow.PrimitiveTypes.Int64

	cases := []struct {
		name     string
		shred    *extensions.VariantType
		path     string
		typ      iceberg.PrimitiveType
		rows     []map[string]any
		wantFast bool
	}{
		{
			name:  "exact-match int64 shredded",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, {"a": int64(2)}, {"a": int64(3), "city": "x"}},
			wantFast: true,
		},
		{
			name:  "exact-match with absent field yields null",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, {"b": int64(9)}, {"a": int64(3)}},
			wantFast: true,
		},
		{
			name: "nested exact-match int64 shredded",
			shred: shredStruct(arrow.Field{Name: "a", Type: arrow.StructOf(
				arrow.Field{Name: "b", Type: i64},
			)}),
			path: "$.a.b", typ: iceberg.PrimitiveTypes.Int64,
			rows: []map[string]any{
				{"a": map[string]any{"b": int64(7)}},
				{"a": map[string]any{"b": int64(8)}},
			},
			wantFast: true,
		},
		{
			name:  "promotion int32->int64 skips fast path",
			shred: shredStruct(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int32(1)}, {"a": int32(2)}},
			wantFast: false,
		},
		{
			name:  "int64 extracted as float64 skips fast path (iceberg nulls it)",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Float64,
			rows:     []map[string]any{{"a": int64(5)}},
			wantFast: false,
		},
		{
			name:  "unshredded skips fast path",
			shred: nil,
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, {"a": int64(2)}},
			wantFast: false,
		},
		{
			name:  "field-level residual (mixed types) skips fast path",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, {"a": "not-an-int"}, {"a": int64(3)}},
			wantFast: false,
		},
		{
			name:  "null row folds into validity",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, nil, {"a": int64(3)}},
			wantFast: true,
		},
		{
			name:  "null row and absent field both null in merged mask",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, nil, {"b": int64(9)}, {"a": int64(4)}},
			wantFast: true,
		},
		{
			name: "nested absent intermediate folds into validity",
			shred: shredStruct(arrow.Field{Name: "a", Type: arrow.StructOf(
				arrow.Field{Name: "b", Type: i64},
			)}),
			path: "$.a.b", typ: iceberg.PrimitiveTypes.Int64,
			rows: []map[string]any{
				{"a": map[string]any{"b": int64(7)}},
				{"b": int64(9)},
				{"a": map[string]any{"b": int64(3)}},
			},
			wantFast: true,
		},
		{
			name:  "field absent from shredded schema skips fast path",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.c", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, {"a": int64(2)}},
			wantFast: false,
		},
		{
			name:  "string clean shredded",
			shred: shredStruct(arrow.Field{Name: "a", Type: arrow.BinaryTypes.String}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.String,
			rows:     []map[string]any{{"a": "x"}, {"a": "yy"}, {"a": "zzz"}},
			wantFast: true,
		},
		{
			name:  "string with null row folds into validity",
			shred: shredStruct(arrow.Field{Name: "a", Type: arrow.BinaryTypes.String}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.String,
			rows:     []map[string]any{{"a": "x"}, nil, {"a": "zzz"}},
			wantFast: true,
		},
		{
			name:  "mask allocated then bail on field residual",
			shred: shredStruct(arrow.Field{Name: "a", Type: i64}),
			path:  "$.a", typ: iceberg.PrimitiveTypes.Int64,
			rows:     []map[string]any{{"a": int64(1)}, nil, {"a": "str"}},
			wantFast: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			rec, col := buildVariantExtractRec(t, mem, tc.shred, tc.path, tc.typ, tc.rows)
			defer rec.Release()
			varr := resolveVariantSource(rec, col.Term.Ref().Field().ID, col.SourcePath).(*extensions.VariantArray)
			dt, err := TypeToArrowType(tc.typ, false, false)
			require.NoError(t, err)

			ref := tryShreddedTypedColumn(varr, col.Term.VariantPath(), dt, mem)
			require.Equal(t, tc.wantFast, ref != nil, "fast path firing")

			got, _, err := buildExtractColumn(ctx, col, rec, mem)
			require.NoError(t, err)
			defer got.Release()

			want, err := extractColumnValuesPerRow(varr, col, dt, mem)
			require.NoError(t, err)
			defer want.Release()

			require.Truef(t, array.Equal(got, want),
				"fast/columnar output diverges from per-row reference\n got=%v\nwant=%v", got, want)

			if ref != nil {
				require.True(t, sharesDataBuffers(got, ref), "fast path must share the typed column's data (no per-row rebuild)")
				ref.Release()
			}
		})
	}
}

// TestFastPathRootResidualObjectFallsBack: a row whose whole object is in the root residual (value present, typed_value null) must fall back, not be nulled.
func TestFastPathRootResidualObjectFallsBack(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	vt := extensions.NewShreddedVariantType(arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}))
	build := func(obj map[string]any) ([]byte, []byte) {
		var b variant.Builder
		require.NoError(t, b.Append(obj))
		v, err := b.Build()
		require.NoError(t, err)

		return v.Metadata().Bytes(), v.Bytes()
	}
	m0, _ := build(map[string]any{"a": int64(1)})
	m1, v1 := build(map[string]any{"a": int64(5)})

	sb := array.NewStructBuilder(mem, vt.StorageType().(*arrow.StructType))
	defer sb.Release()
	metaB := sb.FieldBuilder(0).(*array.BinaryBuilder)
	valB := sb.FieldBuilder(1).(*array.BinaryBuilder)
	tvB := sb.FieldBuilder(2).(*array.StructBuilder)
	aB := tvB.FieldBuilder(0).(*array.StructBuilder)
	aValB := aB.FieldBuilder(0).(*array.BinaryBuilder)
	aTypedB := aB.FieldBuilder(1).(*array.Int64Builder)

	// row 0: shredded {a:1}
	sb.Append(true)
	metaB.Append(m0)
	valB.AppendNull()
	tvB.Append(true)
	aB.Append(true)
	aValB.AppendNull()
	aTypedB.Append(1)
	// row 1: whole object {a:5} in the root residual, typed_value null
	sb.Append(true)
	metaB.Append(m1)
	valB.Append(v1)
	tvB.AppendNulls(1)

	storage := sb.NewStructArray()
	defer storage.Release()
	varr := array.NewExtensionArrayWithStorage(vt, storage).(*extensions.VariantArray)
	defer varr.Release()

	md := arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"2"})
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "payload", Type: vt, Nullable: true, Metadata: md}}, nil), []arrow.Array{varr}, 2)
	defer rec.Release()

	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}})
	term, err := iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Int64).Bind(iceSchema, true)
	require.NoError(t, err)
	col := iceberg.VariantExtractColumn{Term: term.(iceberg.BoundExtract), FieldID: 100, Name: "_x", SourcePath: []string{"payload"}}
	dt, err := TypeToArrowType(iceberg.PrimitiveTypes.Int64, false, false)
	require.NoError(t, err)

	require.Nil(t, tryShreddedTypedColumn(varr, col.Term.VariantPath(), dt, mem), "must not fast-path a root-residual object row")

	got, _, err := buildExtractColumn(ctx, col, rec, mem)
	require.NoError(t, err)
	defer got.Release()
	want, err := extractColumnValuesPerRow(varr, col, dt, mem)
	require.NoError(t, err)
	defer want.Release()

	require.Truef(t, array.Equal(got, want), "got=%v want=%v", got, want)
	require.False(t, got.IsNull(1), "root-residual row must be extracted, not nulled")
	require.EqualValues(t, 5, got.(*array.Int64).Value(1))
}

// TestFastPathDecimalScaleNearMissFallsBack: a shredded decimal whose scale differs from the target (arrow.TypeEqual near-miss) must fall back, not return the mistyped column.
func TestFastPathDecimalScaleNearMissFallsBack(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	shredType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	vt := extensions.NewShreddedVariantType(arrow.StructOf(arrow.Field{Name: "a", Type: shredType}))

	var b variant.Builder
	require.NoError(t, b.Append(map[string]any{"a": int64(1)}))
	v, err := b.Build()
	require.NoError(t, err)
	meta := v.Metadata().Bytes()

	sb := array.NewStructBuilder(mem, vt.StorageType().(*arrow.StructType))
	defer sb.Release()
	metaB := sb.FieldBuilder(0).(*array.BinaryBuilder)
	valB := sb.FieldBuilder(1).(*array.BinaryBuilder)
	tvB := sb.FieldBuilder(2).(*array.StructBuilder)
	aB := tvB.FieldBuilder(0).(*array.StructBuilder)
	aValB := aB.FieldBuilder(0).(*array.BinaryBuilder)
	aDecB := aB.FieldBuilder(1).(*array.Decimal128Builder)

	sb.Append(true)
	metaB.Append(meta)
	valB.AppendNull()
	tvB.Append(true)
	aB.Append(true)
	aValB.AppendNull()
	aDecB.Append(decimal128.FromI64(150)) // 1.50 at scale 2

	storage := sb.NewStructArray()
	defer storage.Release()
	varr := array.NewExtensionArrayWithStorage(vt, storage).(*extensions.VariantArray)
	defer varr.Release()

	md := arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"2"})
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "payload", Type: vt, Nullable: true, Metadata: md}}, nil), []arrow.Array{varr}, 1)
	defer rec.Release()

	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}})
	term, err := iceberg.Extract("payload", "$.a", iceberg.DecimalTypeOf(10, 4)).Bind(iceSchema, true)
	require.NoError(t, err)
	col := iceberg.VariantExtractColumn{Term: term.(iceberg.BoundExtract), FieldID: 100, Name: "_x", SourcePath: []string{"payload"}}
	dt, err := TypeToArrowType(iceberg.DecimalTypeOf(10, 4), false, false)
	require.NoError(t, err)

	require.Nil(t, tryShreddedTypedColumn(varr, col.Term.VariantPath(), dt, mem), "scale near-miss must not fast-path")

	got, _, err := buildExtractColumn(ctx, col, rec, mem)
	require.NoError(t, err)
	defer got.Release()
	want, err := extractColumnValuesPerRow(varr, col, dt, mem)
	require.NoError(t, err)
	defer want.Release()
	require.Truef(t, array.Equal(got, want), "got=%v want=%v", got, want)
}

// TestFastPathTimestampTzNearMissFallsBack: a shredded tz-aware timestamp extracted as a zoneless timestamp (arrow.TypeEqual near-miss on tz) must fall back.
func TestFastPathTimestampTzNearMissFallsBack(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	shredType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	vt := extensions.NewShreddedVariantType(arrow.StructOf(arrow.Field{Name: "a", Type: shredType}))

	var b variant.Builder
	require.NoError(t, b.Append(map[string]any{"a": int64(1)}))
	v, err := b.Build()
	require.NoError(t, err)
	meta := v.Metadata().Bytes()

	sb := array.NewStructBuilder(mem, vt.StorageType().(*arrow.StructType))
	defer sb.Release()
	metaB := sb.FieldBuilder(0).(*array.BinaryBuilder)
	valB := sb.FieldBuilder(1).(*array.BinaryBuilder)
	tvB := sb.FieldBuilder(2).(*array.StructBuilder)
	aB := tvB.FieldBuilder(0).(*array.StructBuilder)
	aValB := aB.FieldBuilder(0).(*array.BinaryBuilder)
	aTsB := aB.FieldBuilder(1).(*array.TimestampBuilder)

	sb.Append(true)
	metaB.Append(meta)
	valB.AppendNull()
	tvB.Append(true)
	aB.Append(true)
	aValB.AppendNull()
	aTsB.Append(arrow.Timestamp(1_000_000))

	storage := sb.NewStructArray()
	defer storage.Release()
	varr := array.NewExtensionArrayWithStorage(vt, storage).(*extensions.VariantArray)
	defer varr.Release()

	md := arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"2"})
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "payload", Type: vt, Nullable: true, Metadata: md}}, nil), []arrow.Array{varr}, 1)
	defer rec.Release()

	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}})
	term, err := iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Timestamp).Bind(iceSchema, true)
	require.NoError(t, err)
	col := iceberg.VariantExtractColumn{Term: term.(iceberg.BoundExtract), FieldID: 100, Name: "_x", SourcePath: []string{"payload"}}
	dt, err := TypeToArrowType(iceberg.PrimitiveTypes.Timestamp, false, false)
	require.NoError(t, err)

	require.Nil(t, tryShreddedTypedColumn(varr, col.Term.VariantPath(), dt, mem), "tz near-miss must not fast-path")

	got, _, err := buildExtractColumn(ctx, col, rec, mem)
	require.NoError(t, err)
	defer got.Release()
	want, err := extractColumnValuesPerRow(varr, col, dt, mem)
	require.NoError(t, err)
	defer want.Release()
	require.Truef(t, array.Equal(got, want), "got=%v want=%v", got, want)
}

// sharesDataBuffers reports whether a and b share their non-validity buffers (validity may differ after a mask merge).
func sharesDataBuffers(a, b arrow.Array) bool {
	ba, bb := a.Data().Buffers(), b.Data().Buffers()
	if len(ba) != len(bb) || len(ba) < 2 {
		return false
	}
	for i := 1; i < len(ba); i++ {
		if ba[i] != bb[i] {
			return false
		}
	}

	return true
}
