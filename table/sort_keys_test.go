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
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveSortKeys_Unsorted(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	keys, err := resolveSortKeys(UnsortedSortOrder, schema)
	require.NoError(t, err)
	assert.Nil(t, keys, "unsorted order must produce no sort keys")
}

func TestResolveSortKeys_DirectionAndNullOrder(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	order, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, Direction: SortDESC, NullOrder: NullsFirst},
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsLast},
	})
	require.NoError(t, err)

	keys, err := resolveSortKeys(order, schema)
	require.NoError(t, err)
	require.Len(t, keys, 2)

	assert.Equal(t, 1, keys[0].ColumnIndex, "first key should target column index 1 (source id 2)")
	assert.Equal(t, compute.SortOrderDescending, keys[0].Order)
	assert.Equal(t, compute.SortNullsAtStart, keys[0].NullPlacement)

	assert.Equal(t, 0, keys[1].ColumnIndex, "second key should target column index 0 (source id 1)")
	assert.Equal(t, compute.SortOrderAscending, keys[1].Order)
	assert.Equal(t, compute.SortNullsAtEnd, keys[1].NullPlacement)
}

func TestResolveSortKeys_MissingSourceID(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	order, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{99}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst},
	})
	require.NoError(t, err)

	_, err = resolveSortKeys(order, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source id 99")
}

func TestAllTopLevelSortSources(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "loc", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 3, Name: "lat", Type: iceberg.PrimitiveTypes.Float64},
		)},
	)
	for _, tt := range []struct {
		name    string
		sources []int
		want    bool
	}{
		{"top-level", []int{1}, true},
		{"nested", []int{3}, false},
		{"top-level then nested", []int{1, 3}, false},
		{"missing", []int{99}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fields := make([]SortField, len(tt.sources))
			for i, id := range tt.sources {
				fields[i] = identSort(id, SortASC, NullsFirst)
			}
			order, err := NewSortOrder(1, fields)
			require.NoError(t, err)
			assert.Equal(t, tt.want, allTopLevelSortSources(order, schema.Fields()))
		})
	}
}

func identSort(sourceID int, dir SortDirection, null NullOrder) SortField {
	return SortField{
		SourceIDs: []int{sourceID}, Transform: iceberg.IdentityTransform{},
		Direction: dir, NullOrder: null,
	}
}

func TestNewRowComparator_Resolve(t *testing.T) {
	type key struct {
		path       []int
		desc       bool
		nullsFirst bool
	}
	for _, tt := range []struct {
		name   string
		schema *iceberg.Schema
		// sc overrides the Arrow schema derived from schema.
		sc          *arrow.Schema
		fields      []SortField
		want        []key
		errIs       error
		errContains string
	}{
		{
			name: "direction and null order",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.String},
			),
			fields: []SortField{identSort(2, SortDESC, NullsFirst), identSort(1, SortASC, NullsLast)},
			want:   []key{{path: []int{1}, desc: true, nullsFirst: true}, {path: []int{0}}},
		},
		{
			name: "struct child",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "loc", Type: iceberg.NewStructType(
					iceberg.NestedField{ID: 3, Name: "lat", Type: iceberg.PrimitiveTypes.Float64},
					iceberg.NestedField{ID: 4, Name: "lon", Type: iceberg.PrimitiveTypes.Float64},
				)},
			),
			fields: []SortField{identSort(4, SortASC, NullsLast)},
			want:   []key{{path: []int{1, 1}}},
		},
		{
			name: "deeply nested",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "outer", Required: true, Type: iceberg.NewStructType(
					iceberg.NestedField{ID: 2, Name: "inner", Required: true, Type: iceberg.NewStructType(
						iceberg.NestedField{ID: 3, Name: "val", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					)},
				)},
			),
			fields: []SortField{identSort(3, SortASC, NullsLast)},
			want:   []key{{path: []int{0, 0, 0}}},
		},
		{
			name: "struct children matched by name",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "s", Type: iceberg.NewStructType(
					iceberg.NestedField{ID: 2, Name: "x", Type: iceberg.PrimitiveTypes.Int64},
					iceberg.NestedField{ID: 3, Name: "y", Type: iceberg.PrimitiveTypes.Int64},
				)},
			),
			sc: arrow.NewSchema([]arrow.Field{{Name: "s", Nullable: true, Type: arrow.StructOf(
				arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			)}}, nil),
			fields: []SortField{identSort(2, SortASC, NullsLast)},
			want:   []key{{path: []int{0, 1}}},
		},
		{
			name: "missing source id",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			),
			fields:      []SortField{identSort(99, SortASC, NullsFirst)},
			errContains: "source id 99 is not a column",
		},
		{
			name: "missing from record schema",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "s", Type: iceberg.NewStructType(
					iceberg.NestedField{ID: 2, Name: "x", Type: iceberg.PrimitiveTypes.Int64},
				)},
			),
			sc: arrow.NewSchema([]arrow.Field{{Name: "s", Nullable: true, Type: arrow.StructOf(
				arrow.Field{Name: "z", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			)}}, nil),
			fields:      []SortField{identSort(2, SortASC, NullsFirst)},
			errContains: `column "s.x" not found`,
		},
		{
			name: "list element rejected",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "ids", Required: true, Type: &iceberg.ListType{
					ElementID: 2, Element: iceberg.PrimitiveTypes.Int64, ElementRequired: true,
				}},
			),
			fields: []SortField{identSort(2, SortASC, NullsLast)},
			errIs:  iceberg.ErrInvalidArgument,
		},
		{
			name: "map value rejected",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "props", Required: true, Type: &iceberg.MapType{
					KeyID: 2, KeyType: iceberg.PrimitiveTypes.String,
					ValueID: 3, ValueType: iceberg.PrimitiveTypes.Int64,
				}},
			),
			fields: []SortField{identSort(3, SortASC, NullsLast)},
			errIs:  iceberg.ErrInvalidArgument,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			order, err := NewSortOrder(1, tt.fields)
			require.NoError(t, err)
			sc := tt.sc
			if sc == nil {
				sc, err = SchemaToArrowSchema(tt.schema, nil, true, false)
				require.NoError(t, err)
			}

			rows, err := newRowComparator(order, tt.schema, sc)
			if tt.errIs != nil || tt.errContains != "" {
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}
				require.ErrorContains(t, err, tt.errContains)

				return
			}
			require.NoError(t, err)

			got := make([]key, len(rows))
			for i, k := range rows {
				got[i] = key{path: k.path, desc: k.desc, nullsFirst: k.nullsFirst}
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// sortTestSchema has a top-level double f and a nullable struct loc whose
// child lat is an int32.
func sortTestSchema() (*iceberg.Schema, *arrow.Schema) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "f", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 3, Name: "loc", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 4, Name: "lat", Type: iceberg.PrimitiveTypes.Int32},
		)},
	)
	sc, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		panic(err)
	}

	return schema, sc
}

type sortTestRow struct {
	id    int32
	f     float64
	lat   int32
	locOK bool
}

// sortTestBatch builds rows. A row with locOK false has a null loc whose
// lat child still holds lat, so a comparator that ignores the parent's
// validity would order it by lat.
func sortTestBatch(mem memory.Allocator, sc *arrow.Schema, rows []sortTestRow) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()
	loc := bldr.Field(2).(*array.StructBuilder)
	for _, r := range rows {
		bldr.Field(0).(*array.Int32Builder).Append(r.id)
		bldr.Field(1).(*array.Float64Builder).Append(r.f)
		// AppendValues, unlike Append(false), leaves the children alone.
		loc.AppendValues([]bool{r.locOK})
		loc.FieldBuilder(0).(*array.Int32Builder).Append(r.lat)
	}

	return bldr.NewRecordBatch()
}

func batchIDs(rec arrow.RecordBatch) []int32 {
	return append([]int32(nil), rec.Column(0).(*array.Int32).Int32Values()...)
}

func TestSortRecordBatch(t *testing.T) {
	negNaN := math.Copysign(math.NaN(), -1)
	nullParent := []sortTestRow{
		{id: 1, lat: 20, locOK: true},
		{id: 2, lat: 15, locOK: false},
		{id: 3, lat: 10, locOK: true},
	}
	for _, tt := range []struct {
		name   string
		fields []SortField
		rows   []sortTestRow
		want   []int32
	}{
		{
			name:   "nested key",
			fields: []SortField{identSort(4, SortASC, NullsLast)},
			rows:   []sortTestRow{{id: 1, lat: 30, locOK: true}, {id: 2, lat: 10, locOK: true}, {id: 3, lat: 20, locOK: true}},
			want:   []int32{2, 3, 1},
		},
		{
			name:   "null parent sorts as null key/nulls last",
			fields: []SortField{identSort(4, SortASC, NullsLast)},
			rows:   nullParent,
			want:   []int32{3, 1, 2},
		},
		{
			name:   "null parent sorts as null key/nulls first",
			fields: []SortField{identSort(4, SortASC, NullsFirst)},
			rows:   nullParent,
			want:   []int32{2, 3, 1},
		},
		{
			name:   "null parent sorts as null key/descending",
			fields: []SortField{identSort(4, SortDESC, NullsLast)},
			rows:   nullParent,
			want:   []int32{1, 3, 2},
		},
		{
			// The spec orders -NaN < -Inf < -x < -0 < 0 < x < Inf < NaN.
			name:   "spec float order",
			fields: []SortField{identSort(2, SortASC, NullsFirst)},
			rows: []sortTestRow{
				{id: 7, f: math.NaN()},
				{id: 5, f: 1},
				{id: 4, f: 0},
				{id: 3, f: math.Copysign(0, -1)},
				{id: 2, f: math.Inf(-1)},
				{id: 1, f: negNaN},
				{id: 6, f: math.Inf(1)},
			},
			want: []int32{1, 2, 3, 4, 5, 6, 7},
		},
		{
			name:   "stable on ties",
			fields: []SortField{identSort(4, SortASC, NullsFirst)},
			rows: []sortTestRow{
				{id: 1, lat: 2, locOK: true},
				{id: 2, lat: 1, locOK: true},
				{id: 3, lat: 2, locOK: true},
				{id: 4, lat: 1, locOK: true},
			},
			want: []int32{2, 4, 1, 3},
		},
		{
			name:   "second key breaks ties",
			fields: []SortField{identSort(4, SortASC, NullsFirst), identSort(2, SortDESC, NullsFirst)},
			rows: []sortTestRow{
				{id: 3, lat: 1, f: 1, locOK: true},
				{id: 1, lat: 0, locOK: false},
				{id: 2, lat: 1, f: 2, locOK: true},
			},
			want: []int32{1, 2, 3},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			schema, sc := sortTestSchema()
			order, err := NewSortOrder(1, tt.fields)
			require.NoError(t, err)
			keys, err := newNestedSortKeys(order, schema, sc)
			require.NoError(t, err)
			rows, err := newRowComparator(order, schema, sc)
			require.NoError(t, err)

			batch := sortTestBatch(mem, sc, tt.rows)
			defer batch.Release()
			sorted, err := sortRecordBatch(ctx, batch, keys)
			require.NoError(t, err)
			defer sorted.Release()
			byComparator, err := sortRecordBatchByComparator(ctx, batch, rows)
			require.NoError(t, err)
			defer byComparator.Release()

			assert.Equal(t, tt.want, batchIDs(sorted))
			assert.Equal(t, tt.want, batchIDs(byComparator))
		})
	}
}

func TestSortRecordBatch_AlreadySortedIsNotCopied(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	schema, sc := sortTestSchema()
	order, err := NewSortOrder(1, []SortField{identSort(4, SortASC, NullsLast)})
	require.NoError(t, err)
	keys, err := newNestedSortKeys(order, schema, sc)
	require.NoError(t, err)

	batch := sortTestBatch(mem, sc, []sortTestRow{
		{id: 1, lat: 1, locOK: true}, {id: 2, lat: 2, locOK: true}, {id: 3, locOK: false},
	})
	defer batch.Release()
	sorted, err := sortRecordBatch(ctx, batch, keys)
	require.NoError(t, err)
	defer sorted.Release()

	assert.Same(t, batch, sorted)
}

// TestSortRecordBatch_Slice sorts a sliced batch, whose struct column and
// its children carry a non-zero offset.
func TestSortRecordBatch_Slice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	schema, sc := sortTestSchema()
	order, err := NewSortOrder(1, []SortField{identSort(4, SortASC, NullsFirst)})
	require.NoError(t, err)
	keys, err := newNestedSortKeys(order, schema, sc)
	require.NoError(t, err)

	batch := sortTestBatch(mem, sc, []sortTestRow{
		{id: 1, lat: -5, locOK: true},
		{id: 2, lat: -9, locOK: false},
		{id: 3, lat: 30, locOK: true},
		{id: 4, lat: 0, locOK: false},
		{id: 5, lat: 10, locOK: true},
	})
	defer batch.Release()
	slice := batch.NewSlice(2, 5)
	defer slice.Release()

	sorted, err := sortRecordBatch(ctx, slice, keys)
	require.NoError(t, err)
	defer sorted.Release()

	assert.Equal(t, []int32{4, 5, 3}, batchIDs(sorted))
}

// TestArrowFloatOrderMatchesRowComparator checks that the write path's two
// sort paths, Arrow's kernel for top-level keys and the row comparator for
// nested ones, order float keys the same way, ties included.
func TestArrowFloatOrderMatchesRowComparator(t *testing.T) {
	negNaN := math.Copysign(math.NaN(), -1)
	negZero := math.Copysign(0, -1)
	specials := []float64{math.NaN(), 1, negZero, math.Inf(1), 0, negNaN, -1, math.Inf(-1), 1, math.NaN(), negZero, 0}

	// 4096 rows without nulls takes Arrow's radix path for a single key.
	rng := rand.New(rand.NewPCG(1, 2))
	large := make([]float64, 4096)
	for i := range large {
		if i%8 == 0 {
			large[i] = specials[rng.IntN(len(specials))]
		} else {
			large[i] = float64(rng.IntN(64) - 32)
		}
	}

	for _, typ := range []struct {
		name string
		ice  iceberg.Type
		arr  arrow.DataType
	}{
		{"float64", iceberg.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64},
		{"float32", iceberg.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float32},
	} {
		for _, in := range []struct {
			name   string
			values []float64
			nulls  bool
		}{
			{"specials with nulls", specials, true},
			{"large without nulls", large, false},
		} {
			for _, dir := range []SortDirection{SortASC, SortDESC} {
				for _, nulls := range []NullOrder{NullsFirst, NullsLast} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", typ.name, in.name, dir, nulls), func(t *testing.T) {
						mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
						defer mem.AssertSize(t, 0)
						ctx := compute.WithAllocator(t.Context(), mem)

						schema := iceberg.NewSchema(0,
							iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
							iceberg.NestedField{ID: 2, Name: "f", Type: typ.ice},
						)
						sc, err := SchemaToArrowSchema(schema, nil, true, false)
						require.NoError(t, err)
						require.True(t, arrow.TypeEqual(typ.arr, sc.Field(1).Type))
						order, err := NewSortOrder(1, []SortField{identSort(2, dir, nulls)})
						require.NoError(t, err)

						bldr := array.NewRecordBuilder(mem, sc)
						for i, v := range in.values {
							bldr.Field(0).(*array.Int32Builder).Append(int32(i))
							switch fb := bldr.Field(1).(type) {
							case *array.Float64Builder:
								fb.Append(v)
							case *array.Float32Builder:
								fb.Append(float32(v))
							}
							if in.nulls && i%5 == 0 {
								bldr.Field(0).(*array.Int32Builder).Append(int32(len(in.values) + i))
								bldr.Field(1).AppendNull()
							}
						}
						batch := bldr.NewRecordBatch()
						bldr.Release()
						defer batch.Release()

						keys, err := resolveSortKeys(order, schema)
						require.NoError(t, err)
						byArrow, err := compute.SortRecordBatch(ctx, batch, keys)
						require.NoError(t, err)
						defer byArrow.Release()

						rows, err := newRowComparator(order, schema, sc)
						require.NoError(t, err)
						byComparator, err := sortRecordBatchByComparator(ctx, batch, rows)
						require.NoError(t, err)
						defer byComparator.Release()

						assert.Equal(t, batchIDs(byComparator), batchIDs(byArrow))
					})
				}
			}
		}
	}
}

// TestSortRecordBatchMatchesRowComparator checks hoisted nested keys
// against the row comparator on random rows with nulls at every level of
// a two-deep struct, children holding values under null parents, and
// slices at offsets that are not byte-aligned.
func TestSortRecordBatchMatchesRowComparator(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "outer", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 3, Name: "inner", Type: iceberg.NewStructType(
				iceberg.NestedField{ID: 4, Name: "v", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 5, Name: "u", Type: iceberg.PrimitiveTypes.UUID},
			)},
			iceberg.NestedField{ID: 6, Name: "s", Type: iceberg.PrimitiveTypes.String},
		)},
	)
	sc, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	rng := rand.New(rand.NewPCG(3, 4))
	bldr := array.NewRecordBuilder(mem, sc)
	outer := bldr.Field(1).(*array.StructBuilder)
	inner := outer.FieldBuilder(0).(*array.StructBuilder)
	v := inner.FieldBuilder(0).(*array.Int64Builder)
	u := inner.FieldBuilder(1).(*extensions.UUIDBuilder)
	str := outer.FieldBuilder(1).(*array.StringBuilder)
	const n = 1000
	for i := range n {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		// AppendValues leaves the children alone, so they hold values
		// under a null parent.
		outer.AppendValues([]bool{rng.IntN(6) != 0})
		inner.AppendValues([]bool{rng.IntN(6) != 0})
		if rng.IntN(8) == 0 {
			v.AppendNull()
		} else {
			v.Append(rng.Int64N(16))
		}
		if rng.IntN(8) == 0 {
			u.AppendNull()
		} else {
			u.Append(uuid.UUID{byte(rng.IntN(4)), byte(rng.IntN(4))})
		}
		str.Append(fmt.Sprintf("s%d", rng.IntN(8)))
	}
	batch := bldr.NewRecordBatch()
	bldr.Release()
	defer batch.Release()

	for _, tc := range []struct {
		name   string
		fields []SortField
	}{
		{"deep", []SortField{identSort(4, SortASC, NullsFirst)}},
		{"deep desc nulls last", []SortField{identSort(4, SortDESC, NullsLast)}},
		{"uuid extension", []SortField{identSort(5, SortASC, NullsFirst)}},
		{"multi-key", []SortField{
			identSort(6, SortDESC, NullsFirst), identSort(4, SortASC, NullsLast), identSort(5, SortDESC, NullsFirst),
		}},
		{"mixed top-level and nested", []SortField{identSort(4, SortASC, NullsLast), identSort(1, SortDESC, NullsLast)}},
	} {
		order, err := NewSortOrder(1, tc.fields)
		require.NoError(t, err)
		keys, err := newNestedSortKeys(order, schema, sc)
		require.NoError(t, err)
		rows, err := newRowComparator(order, schema, sc)
		require.NoError(t, err)

		for _, sl := range []struct{ start, end int64 }{{0, n}, {3, n}, {13, 701}} {
			t.Run(fmt.Sprintf("%s/slice=%d-%d", tc.name, sl.start, sl.end), func(t *testing.T) {
				in := batch.NewSlice(sl.start, sl.end)
				defer in.Release()

				got, err := sortRecordBatch(ctx, in, keys)
				require.NoError(t, err)
				defer got.Release()
				want, err := sortRecordBatchByComparator(ctx, in, rows)
				require.NoError(t, err)
				defer want.Release()

				assert.Equal(t, batchIDs(want), batchIDs(got))
			})
		}
	}
}

// sortRecordBatchByComparator stably sorts batch with the row comparator,
// as a reference to check sortRecordBatch against.
func sortRecordBatchByComparator(ctx context.Context, batch arrow.RecordBatch, rows rowComparator) (arrow.RecordBatch, error) {
	// With a single batch, a row's rowRef is its row index.
	compare := rows.bind([]arrow.RecordBatch{batch})
	indices := make([]int64, batch.NumRows())
	for i := range indices {
		indices[i] = int64(i)
	}
	slices.SortStableFunc(indices, func(x, y int64) int { return compare(rowRef(x), rowRef(y)) })

	identity := true
	for i, idx := range indices {
		if idx != int64(i) {
			identity = false

			break
		}
	}
	if identity {
		batch.Retain()

		return batch, nil
	}

	return partitionBatchByKey(ctx)(batch, indices)
}
