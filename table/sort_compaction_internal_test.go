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
	"encoding/json"
	"iter"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedCompactionRecords_NestedAndNulls sorts rows spread over several
// batches, with nulls in the sort key and a struct column (which Arrow's
// chunked Take cannot gather without concatenating) carried along.
func TestSortedCompactionRecords_NestedAndNulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "s", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "x", Type: iceberg.PrimitiveTypes.String},
		}}},
	)
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.StructOf(arrow.Field{Name: "x", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
	}, nil)

	inputs := []string{
		`[{"id": 4, "s": {"x": "d"}}, {"id": null, "s": {"x": "n"}}, {"id": 1, "s": {"x": "a"}}]`,
		`[{"id": 3, "s": {"x": "c"}}]`,
		`[{"id": 5, "s": null}, {"id": 2, "s": {"x": "b"}}]`,
	}
	records := func(yield func(arrow.RecordBatch, error) bool) {
		for _, in := range inputs {
			rec, _, err := array.RecordFromJSON(mem, sc, strings.NewReader(in))
			if err != nil {
				yield(nil, err)

				return
			}
			if !yield(rec, nil) {
				return
			}
		}
	}

	order, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
		Direction: SortASC, NullOrder: NullsFirst,
	}})
	require.NoError(t, err)

	// mem tracks only the input batches: arrow-go's sort_indices kernel does
	// not release its scratch buffers, so compute allocations stay untracked.
	ctx := t.Context()
	var got []map[string]any
	var sizes []int64
	for rec, err := range sortedCompactionRecords(ctx, *iceberg.UnpartitionedSpec, schema, order, 4, records) {
		require.NoError(t, err)
		sizes = append(sizes, rec.NumRows())
		b, err := rec.MarshalJSON()
		rec.Release()
		require.NoError(t, err)
		var rows []map[string]any
		require.NoError(t, json.Unmarshal(b, &rows))
		got = append(got, rows...)
	}

	assert.Equal(t, []int64{4, 2}, sizes)
	want := []map[string]any{
		{"id": nil, "s": map[string]any{"x": "n"}},
		{"id": float64(1), "s": map[string]any{"x": "a"}},
		{"id": float64(2), "s": map[string]any{"x": "b"}},
		{"id": float64(3), "s": map[string]any{"x": "c"}},
		{"id": float64(4), "s": map[string]any{"x": "d"}},
		{"id": float64(5), "s": nil},
	}
	assert.Equal(t, want, got)
}

func TestCompareFloat_IcebergOrder(t *testing.T) {
	// Ascending order required by the Iceberg spec.
	want := []float64{
		math.Copysign(math.NaN(), -1), math.Inf(-1), -1, math.Copysign(0, -1), 0, 1, math.Inf(1), math.NaN(),
	}
	for i, x := range want {
		for j, y := range want {
			assert.Equal(t, cmpInts(i, j), compareFloat(x, y), "compare(%v, %v)", x, y)
		}
	}
}

func cmpInts(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// TestSortedCompactionRecords_FloatOrder sorts a float column holding NaN,
// infinities and a null, which Arrow's sort kernel would order with NaN
// next to the nulls rather than where the Iceberg spec puts it.
func TestSortedCompactionRecords_FloatOrder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "v", Type: iceberg.PrimitiveTypes.Float64})
	sc := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)

	tests := []struct {
		name      string
		direction SortDirection
		nullOrder NullOrder
		want      []string
	}{
		{"asc nulls first", SortASC, NullsFirst, []string{"null", "-Inf", "-1", "1", "+Inf", "NaN"}},
		{"desc nulls last", SortDESC, NullsLast, []string{"NaN", "+Inf", "1", "-1", "-Inf", "null"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := func(yield func(arrow.RecordBatch, error) bool) {
				bldr := array.NewFloat64Builder(mem)
				defer bldr.Release()
				bldr.AppendValues([]float64{math.NaN(), 1, 0, math.Inf(-1), math.Inf(1), -1}, []bool{true, true, false, true, true, true})
				col := bldr.NewArray()
				defer col.Release()
				yield(array.NewRecordBatch(sc, []arrow.Array{col}, int64(col.Len())), nil)
			}
			order, err := NewSortOrder(1, []SortField{{
				SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
				Direction: tt.direction, NullOrder: tt.nullOrder,
			}})
			require.NoError(t, err)

			var got []string
			for rec, err := range sortedCompactionRecords(t.Context(), *iceberg.UnpartitionedSpec, schema, order, 100, records) {
				require.NoError(t, err)
				col := rec.Column(0).(*array.Float64)
				for i := range col.Len() {
					if col.IsNull(i) {
						got = append(got, "null")
					} else {
						got = append(got, col.ValueStr(i))
					}
				}
				rec.Release()
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestClaimableSortOrderID(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 3, Name: "a", Type: iceberg.PrimitiveTypes.Float64},
	)
	field := func(id int, transform iceberg.Transform) SortField {
		return SortField{SourceIDs: []int{id}, Transform: transform, Direction: SortASC, NullOrder: NullsFirst}
	}

	tests := []struct {
		name    string
		fields  []SortField
		claimed bool
	}{
		{"identity fields", []SortField{field(1, iceberg.IdentityTransform{}), field(3, iceberg.IdentityTransform{})}, true},
		{"order-preserving last field", []SortField{field(1, iceberg.IdentityTransform{}), field(2, iceberg.DayTransform{})}, true},
		// Sorting by ts orders rows that share a day, so id is not sorted within a day.
		{"order-preserving field before another", []SortField{field(2, iceberg.DayTransform{}), field(1, iceberg.IdentityTransform{})}, false},
		{"transform does not preserve order", []SortField{field(1, iceberg.BucketTransform{NumBuckets: 4})}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := NewSortOrder(1, tt.fields)
			require.NoError(t, err)
			meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, order, "s3://bucket/t", nil)
			require.NoError(t, err)

			want := UnsortedSortOrderID
			if tt.claimed {
				want = meta.SortOrder().OrderID()
			}
			assert.Equal(t, want, claimableSortOrderID(meta, meta.SortOrder()))
		})
	}
}

func TestPresortedPartitions(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "p", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "p",
	})
	order, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	meta, err := NewMetadata(schema, &spec, order, "s3://bucket/t", nil)
	require.NoError(t, err)
	order, spec = meta.SortOrder(), meta.PartitionSpec()

	task := func(path, partition string, sortOrderID *int) FileScanTask {
		bldr, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
			map[int]any{1000: partition}, nil, nil, 1, 1)
		require.NoError(t, err)
		if sortOrderID != nil {
			bldr.SortOrderID(*sortOrderID)
		}

		return FileScanTask{File: bldr.Build()}
	}
	id, other := order.OrderID(), order.OrderID()+1

	a1, b1, a2 := task("a1", "a", &id), task("b1", "b", &id), task("a2", "a", &id)
	parts, ok := presortedPartitions(meta, spec, order, []FileScanTask{a1, b1, a2}, 2)
	require.True(t, ok)
	require.Len(t, parts, 2)
	assert.Equal(t, []string{"a1", "a2"}, []string{parts[0][0].File.FilePath(), parts[0][1].File.FilePath()})
	assert.Equal(t, "b1", parts[1][0].File.FilePath())

	_, ok = presortedPartitions(meta, spec, order, []FileScanTask{a1, b1, a2}, 1)
	assert.False(t, ok, "a partition with more files than streams may be open must be sorted, not merged")
	_, ok = presortedPartitions(meta, spec, order, []FileScanTask{a1, task("a3", "a", nil)}, 2)
	assert.False(t, ok, "a file without sort_order_id must be sorted, not merged")
	_, ok = presortedPartitions(meta, spec, order, []FileScanTask{a1, task("a3", "a", &other)}, 2)
	assert.False(t, ok, "a file sorted by another order must be sorted, not merged")

	truncated, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst},
		{SourceIDs: []int{2}, Transform: iceberg.TruncateTransform{Width: 1}, Direction: SortASC, NullOrder: NullsFirst},
	})
	require.NoError(t, err)
	truncMeta, err := NewMetadata(schema, &spec, truncated, "s3://bucket/t", nil)
	require.NoError(t, err)
	truncated = truncMeta.SortOrder()
	truncID := truncated.OrderID()
	require.Equal(t, truncID, claimableSortOrderID(truncMeta, truncated))
	_, ok = presortedPartitions(truncMeta, spec, truncated,
		[]FileScanTask{task("t1", "a", &truncID), task("t2", "a", &truncID)}, 2)
	assert.False(t, ok, "files sorted by a transform may be unsorted by its source column, so they must be sorted, not merged")
}

var mergeTestSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "src", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

func mergeTestComparator(t *testing.T, direction SortDirection, nullOrder NullOrder) rowComparator {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "src", Type: iceberg.PrimitiveTypes.Int64},
	)
	order, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: direction, NullOrder: nullOrder,
	}})
	require.NoError(t, err)
	rows, err := newRowComparator(order, schema, mergeTestSchema)
	require.NoError(t, err)

	return rows
}

// idStream yields one batch per entry of batches, each row tagged with src.
func idStream(mem memory.Allocator, src int, batches ...[]any) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for _, ids := range batches {
			rows := make([]map[string]any, len(ids))
			for i, id := range ids {
				rows[i] = map[string]any{"id": id, "src": src}
			}
			b, err := json.Marshal(rows)
			if err != nil {
				yield(nil, err)

				return
			}
			rec, _, err := array.RecordFromJSON(mem, mergeTestSchema, strings.NewReader(string(b)))
			if err != nil {
				yield(nil, err)

				return
			}
			if !yield(rec, nil) {
				return
			}
		}
	}
}

type mergedRows struct {
	ids, srcs []any
	sizes     []int64
}

func runMerge(t *testing.T, rows rowComparator, batchRows int64, streams ...iter.Seq2[arrow.RecordBatch, error]) (mergedRows, error) {
	t.Helper()

	paths := make([]string, len(streams))
	for i := range paths {
		paths[i] = "f" + string(rune('0'+i)) + ".parquet"
	}
	var (
		got     mergedRows
		lastErr error
	)
	mergeSortedStreams(t.Context(), rows, paths, streams, batchRows, func(rec arrow.RecordBatch, err error) bool {
		if err != nil {
			lastErr = err

			return false
		}
		defer rec.Release()
		got.sizes = append(got.sizes, rec.NumRows())
		ids, srcs := rec.Column(0).(*array.Int64), rec.Column(1).(*array.Int64)
		for i := range ids.Len() {
			if ids.IsNull(i) {
				got.ids = append(got.ids, nil)
			} else {
				got.ids = append(got.ids, ids.Value(i))
			}
			got.srcs = append(got.srcs, srcs.Value(i))
		}

		return true
	})

	return got, lastErr
}

func TestMergeSortedStreams_Interleaved(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	got, err := runMerge(t, mergeTestComparator(t, SortASC, NullsFirst), 4,
		idStream(mem, 0, []any{1, 4}, []any{7}),
		idStream(mem, 1, []any{}, []any{2, 3}, []any{9}),
		idStream(mem, 2, []any{5, 6, 8}),
		idStream(mem, 3),
	)
	require.NoError(t, err)
	assert.Equal(t, []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9)}, got.ids)
	assert.Equal(t, []int64{4, 4, 1}, got.sizes)
}

func TestMergeSortedStreams_TiesKeepStreamOrder(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	got, err := runMerge(t, mergeTestComparator(t, SortASC, NullsFirst), 100,
		idStream(mem, 0, []any{1, 2}),
		idStream(mem, 1, []any{1, 2}),
	)
	require.NoError(t, err)
	assert.Equal(t, []any{int64(1), int64(1), int64(2), int64(2)}, got.ids)
	assert.Equal(t, []any{int64(0), int64(1), int64(0), int64(1)}, got.srcs)
}

func TestMergeSortedStreams_DisjointStreamsFillOneBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	got, err := runMerge(t, mergeTestComparator(t, SortASC, NullsFirst), 100,
		idStream(mem, 1, []any{4, 5, 6}),
		idStream(mem, 0, []any{1, 2, 3}),
	)
	require.NoError(t, err)
	assert.Equal(t, []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)}, got.ids)
	assert.Equal(t, []int64{6}, got.sizes)
}

func TestMergeSortedStreams_DescendingNullsLast(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	got, err := runMerge(t, mergeTestComparator(t, SortDESC, NullsLast), 2,
		idStream(mem, 0, []any{5, nil}),
		idStream(mem, 1, []any{6, 2}, []any{nil}),
	)
	require.NoError(t, err)
	assert.Equal(t, []any{int64(6), int64(5), int64(2), nil, nil}, got.ids)
}

func TestMergeSortedStreams_RejectsUnsortedInput(t *testing.T) {
	tests := []struct {
		name    string
		batches [][]any
	}{
		{"within a batch", [][]any{{1, 3, 2}}},
		{"across batches", [][]any{{1, 5}, {3}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			_, err := runMerge(t, mergeTestComparator(t, SortASC, NullsFirst), 100,
				idStream(mem, 0, []any{0, 4}),
				idStream(mem, 1, tt.batches...),
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "f1.parquet")
		})
	}
}

func TestMergeSortedStreams_EarlyStopReleases(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	streams := []iter.Seq2[arrow.RecordBatch, error]{
		idStream(mem, 0, []any{1, 3}, []any{5, 7}),
		idStream(mem, 1, []any{2, 4}, []any{6, 8}),
	}
	batches := 0
	ok := mergeSortedStreams(t.Context(), mergeTestComparator(t, SortASC, NullsFirst), []string{"a", "b"}, streams, 3,
		func(rec arrow.RecordBatch, err error) bool {
			require.NoError(t, err)
			rec.Release()
			batches++

			return false
		})
	assert.False(t, ok)
	assert.Equal(t, 1, batches)
}
