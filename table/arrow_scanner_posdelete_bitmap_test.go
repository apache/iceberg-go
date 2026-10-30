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
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCombinePositionalDeleteBitmap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deletes  []uint64
		spans    []internal.RowGroupSpan
		batches  []int64
		want     [][]int64
		wantNext int64
	}{
		{name: "empty batch", batches: []int64{0}, want: [][]int64{nil}},
		{name: "no deletes", batches: []int64{4}, want: [][]int64{nil}, wantNext: 4},
		{name: "deletes outside batch", deletes: []uint64{10}, batches: []int64{4}, want: [][]int64{nil}, wantNext: 4},
		{name: "first row", deletes: []uint64{0}, batches: []int64{4}, want: [][]int64{{1, 2, 3}}, wantNext: 4},
		{name: "backfill", deletes: []uint64{2}, batches: []int64{4}, want: [][]int64{{0, 1, 3}}, wantNext: 4},
		{name: "last row", deletes: []uint64{3}, batches: []int64{4}, want: [][]int64{{0, 1, 2}}, wantNext: 4},
		{name: "all rows", deletes: []uint64{0, 1, 2}, batches: []int64{3}, want: [][]int64{{}}, wantNext: 3},
		{name: "across batches", deletes: []uint64{3}, batches: []int64{3, 2}, want: [][]int64{nil, {1}}, wantNext: 5},
		{
			name: "pruned row groups", deletes: []uint64{4},
			spans:   []internal.RowGroupSpan{{FirstRowPos: 0, NumRows: 2}, {FirstRowPos: 4, NumRows: 2}},
			batches: []int64{2, 2}, want: [][]int64{nil, {1}}, wantNext: 6,
		},
		{
			name: "64-bit positions", deletes: []uint64{1 << 32},
			spans:   []internal.RowGroupSpan{{FirstRowPos: 1<<32 - 1, NumRows: 3}},
			batches: []int64{3}, want: [][]int64{{0, 2}}, wantNext: 1<<32 + 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			bitmap := dv.NewRoaringPositionBitmap()
			for _, position := range tc.deletes {
				bitmap.Set(position)
			}
			cursor := (&rowPositionSource{spans: tc.spans}).cursor()
			for i, nrows := range tc.batches {
				indices := combinePositionalDeleteBitmap(mem, bitmap, cursor, nrows)
				if tc.want[i] == nil {
					require.Nil(t, indices, "a clean batch must use the passthrough path")

					continue
				}
				require.NotNil(t, indices, "an empty array must still remove an entirely deleted batch")
				if len(tc.want[i]) == 0 {
					assert.Zero(t, indices.Len())
				} else {
					assert.Equal(t, tc.want[i], indices.(*array.Int64).Int64Values())
				}
				indices.Release()
			}
			assert.Equal(t, tc.wantNext, cursor.next(), "every input row must advance the shared position source")
		})
	}
}

func TestProcessPositionalDeleteBitmapAcrossBatches(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)
	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(3)
	process := processPositionalDeleteBitmap(ctx, bitmap, (&rowPositionSource{}).cursor())

	for i, values := range [][]int64{{10, 11, 12}, {13, 14}} {
		batch := checkedInt64RecordBatch(mem, values...)
		out, err := process(batch)
		require.NoError(t, err)
		if i == 0 {
			assert.Same(t, batch, out, "the clean first batch must retain its input")
			assert.Equal(t, values, out.Column(0).(*array.Int64).Int64Values())
		} else {
			assert.Equal(t, []int64{14}, out.Column(0).(*array.Int64).Int64Values())
		}
		out.Release()
	}
}

func TestCombinePositionalDeleteBitmapLargeBatches(t *testing.T) {
	const nrows = 1_024
	for _, tc := range []struct {
		name       string
		start, end uint64
	}{
		{name: "first row", start: 0, end: 1},
		{name: "leading run", start: 0, end: 512},
		{name: "middle row", start: 512, end: 513},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			bitmap := dv.NewRoaringPositionBitmap()
			bitmap.SetRange(tc.start, tc.end)
			var want []int64
			for i := range uint64(nrows) {
				if i < tc.start || i >= tc.end {
					want = append(want, int64(i))
				}
			}

			indices := combinePositionalDeleteBitmap(mem, bitmap, (&rowPositionSource{}).cursor(), nrows)
			require.NotNil(t, indices)
			defer indices.Release()
			assert.Equal(t, want, indices.(*array.Int64).Int64Values())
		})
	}
}

type positionDeleteAllocationTracker struct {
	memory.Allocator
	largest int
}

func (a *positionDeleteAllocationTracker) Allocate(size int) []byte {
	a.largest = max(a.largest, size)

	return a.Allocator.Allocate(size)
}

func (a *positionDeleteAllocationTracker) Reallocate(size int, b []byte) []byte {
	a.largest = max(a.largest, size)

	return a.Allocator.Reallocate(size, b)
}

func TestCombinePositionalDeleteBitmapAllocationBounds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		nrows      int64
		start, end uint64
	}{
		{name: "clean", nrows: 1_024},
		{name: "first row", nrows: 1_024, start: 0, end: 1},
		{name: "middle row", nrows: 1_024, start: 512, end: 513},
		{name: "first row non-power-of-two batch", nrows: 1_025, start: 0, end: 1},
		{name: "middle row non-power-of-two batch", nrows: 1_025, start: 512, end: 513},
		{name: "leading run", nrows: 1_024, start: 0, end: 512},
		{name: "all rows", nrows: 1_024, start: 0, end: 1_024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer checked.AssertSize(t, 0)
			mem := &positionDeleteAllocationTracker{Allocator: checked}
			bitmap := dv.NewRoaringPositionBitmap()
			bitmap.SetRange(tc.start, tc.end)
			indices := combinePositionalDeleteBitmap(mem, bitmap, (&rowPositionSource{}).cursor(), tc.nrows)
			if tc.start == tc.end {
				require.Nil(t, indices)
				assert.Zero(t, mem.largest, "a clean batch must not allocate Arrow buffers")

				return
			}
			require.NotNil(t, indices)
			defer indices.Release()
			survivors := int(tc.nrows) - int(tc.end-tc.start)
			assert.Equal(t, survivors, indices.Len())
			if survivors == 0 {
				assert.Zero(t, mem.largest, "a fully deleted batch must not allocate Arrow buffers")
			} else {
				// Arrow aligns buffers to 64 bytes. Check the largest request,
				// before NewArray can shrink an oversized values buffer.
				assert.LessOrEqual(t, mem.largest, arrow.Int64Traits.BytesRequired(survivors)+63)
			}
		})
	}
}

func TestCollectPosDeleteBitmapRejectsInvalidPositions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column func(memory.Allocator) arrow.Array
		want   string
	}{
		{name: "nil chunk", want: "nil pos column chunk"},
		{name: "wrong type", column: func(mem memory.Allocator) arrow.Array {
			return int32Array(mem, 1)
		}, want: "unsupported pos column type"},
		{name: "negative", column: func(mem memory.Allocator) arrow.Array {
			return int64Array(mem, -1)
		}, want: "negative pos -1"},
		{name: "null", column: func(mem memory.Allocator) arrow.Array {
			builder := array.NewInt64Builder(mem)
			defer builder.Release()
			builder.AppendNull()

			return builder.NewArray()
		}, want: "null pos in position delete file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			var chunk *arrow.Chunked
			if tc.column != nil {
				column := tc.column(mem)
				defer column.Release()
				chunk = arrow.NewChunked(column.DataType(), []arrow.Array{column})
				defer chunk.Release()
			}

			bitmap, err := collectPosDeleteBitmap(positionDeletes{chunk})
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.ErrorContains(t, err, tc.want)
			assert.Nil(t, bitmap)
		})
	}
}

func TestCollectPosDeleteBitmapDeduplicates64BitPositions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	first := int64Array(mem, 0, 2, 2, 1<<32)
	defer first.Release()
	second := int64Array(mem, 1<<32, math.MaxInt64)
	defer second.Release()
	column := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{first, second})
	defer column.Release()

	bitmap, err := collectPosDeleteBitmap(positionDeletes{column, column})
	require.NoError(t, err)
	assert.Equal(t, []uint64{0, 2, 1 << 32, math.MaxInt64}, slices.Collect(bitmap.Positions()))
}

func BenchmarkCombinePositionalDeleteBitmap(b *testing.B) {
	const nrows = 65_536
	for _, name := range []string{"clean", "first", "middle", "all"} {
		b.Run(name, func(b *testing.B) {
			bitmap := dv.NewRoaringPositionBitmap()
			switch name {
			case "first":
				bitmap.Set(0)
			case "middle":
				bitmap.Set(nrows / 2)
			case "all":
				bitmap.SetRange(0, nrows)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				indices := combinePositionalDeleteBitmap(memory.DefaultAllocator, bitmap, (&rowPositionSource{}).cursor(), nrows)
				if indices != nil {
					positionDeleteSplitReuseBenchmarkSink = int64(indices.Len())
					indices.Release()
				}
			}
		})
	}
}
