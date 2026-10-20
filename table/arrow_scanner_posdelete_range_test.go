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
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table/dv"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The previous per-row implementation is preserved here to measure the same
// scan path and compare its output and cursor advancement against the range path.
func combinePositionalDeleteBitmapBefore(
	mem memory.Allocator,
	deletes *dv.RoaringPositionBitmap,
	cursor *rowPositionCursor,
	nrows int64,
) arrow.Array {
	var bldr *array.Int64Builder

	for i := range nrows {
		if deletes.Contains(uint64(cursor.next())) {
			if bldr == nil {
				bldr = array.NewInt64Builder(mem)
				if i > 0 {
					bldr.Resize(int(nrows - 1))
				}
				for j := range i {
					bldr.UnsafeAppend(j)
				}
			}

			continue
		}

		if bldr != nil {
			if bldr.Cap() == 0 {
				bldr.Resize(int(nrows - i))
			}
			bldr.UnsafeAppend(i)
		}
	}
	if bldr == nil {
		return nil
	}
	defer bldr.Release()

	return bldr.NewArray()
}

func TestCombinePositionalDeleteBitmapRangeParity(t *testing.T) {
	const boundary uint64 = 1 << 32
	cases := []struct {
		name       string
		start      int64
		batches    []int64
		positions  []uint64
		rangeStart uint64
		rangeEnd   uint64
		spans      []tblutils.RowGroupSpan
	}{
		{name: "empty", batches: []int64{0, 0}},
		{name: "clean across batches", batches: []int64{4, 64, 11}, positions: []uint64{1000}},
		{name: "sparse across batches", batches: []int64{8, 65, 16}, positions: []uint64{1, 33, 88}},
		{name: "all deleted", batches: []int64{3, 5}, rangeEnd: 8},
		{name: "single surviving row", batches: []int64{9}, rangeStart: 1, rangeEnd: 9},
		{name: "non-byte-aligned dense deletes", batches: []int64{7, 3}, rangeStart: 0, rangeEnd: 9},
		{
			name:       "dense deletes across a 32-bit boundary",
			start:      int64(boundary - 4),
			batches:    []int64{8},
			rangeStart: boundary - 3,
			rangeEnd:   boundary + 4,
		},
		{name: "partially deleted", batches: []int64{65, 64}, rangeStart: 32, rangeEnd: 98},
		{name: "crosses 32-bit buckets", start: int64(boundary - 4), batches: []int64{3, 8}, positions: []uint64{boundary - 3, boundary - 1, boundary, boundary + 5}},
		{name: "high positions", start: 1 << 40, batches: []int64{3, 8}, positions: []uint64{1<<40 + 1, 1<<40 + 10}},
		{
			name:    "pruned row groups",
			batches: []int64{4, 4},
			spans: []tblutils.RowGroupSpan{
				{FirstRowPos: 10, NumRows: 4},
				{FirstRowPos: int64(boundary), NumRows: 4},
			},
			positions: []uint64{11, boundary + 1, boundary + 3},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bitmap := dv.NewRoaringPositionBitmap()
			for _, pos := range tc.positions {
				bitmap.Set(pos)
			}
			if tc.rangeEnd > tc.rangeStart {
				bitmap.SetRange(tc.rangeStart, tc.rangeEnd)
			}
			checkPosDeleteRangeParity(t, bitmap, tc.start, tc.spans, tc.batches)
		})
	}
}

func TestCombinePositionalDeleteBitmapRangeRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(1020))
	for i := range 200 {
		start := []int64{0, 73, 1<<32 - 32, 1 << 40}[i%4]
		nrows := int64(rng.Intn(256))
		bitmap := dv.NewRoaringPositionBitmap()
		for range rng.Intn(128) {
			bitmap.Set(uint64(start) + uint64(rng.Intn(int(nrows)+3)))
		}
		checkPosDeleteRangeParity(t, bitmap, start, nil, []int64{nrows / 2, nrows - nrows/2})
	}
}

func checkPosDeleteRangeParity(t *testing.T, bitmap *dv.RoaringPositionBitmap,
	start int64, spans []tblutils.RowGroupSpan, batches []int64,
) {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	source := &rowPositionSource{spans: spans}
	gotCursor, wantCursor := source.cursor(), source.cursor()
	gotCursor.consumed, wantCursor.consumed = start, start

	for _, nrows := range batches {
		got := combinePositionalDeleteBitmap(mem, bitmap, gotCursor, nrows)
		want := combinePositionalDeleteBitmapBefore(mem, bitmap, wantCursor, nrows)
		if want == nil {
			require.Nil(t, got)
		} else {
			require.NotNil(t, got)
			assert.Equal(t, want.(*array.Int64).Int64Values(), got.(*array.Int64).Int64Values())
			got.Release()
			want.Release()
		}
		assert.Equal(t, wantCursor.consumed, gotCursor.consumed)
		assert.Equal(t, wantCursor.spanIdx, gotCursor.spanIdx)
	}
}

var positionalDeleteRangeBenchmarkSink int64

func BenchmarkCombinePositionalDeleteBitmapRange(b *testing.B) {
	const nrows int64 = 65_536
	for _, tc := range []struct {
		name  string
		setup func(*dv.RoaringPositionBitmap)
	}{
		{name: "clean", setup: func(bm *dv.RoaringPositionBitmap) { bm.Set(uint64(nrows + 100)) }},
		{name: "sparse", setup: func(bm *dv.RoaringPositionBitmap) {
			bm.Set(1)
			bm.Set(uint64(nrows - 1))
		}},
		{name: "half", setup: func(bm *dv.RoaringPositionBitmap) { bm.SetRange(0, uint64(nrows/2)) }},
		{name: "all", setup: func(bm *dv.RoaringPositionBitmap) { bm.SetRange(0, uint64(nrows)) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bitmap := dv.NewRoaringPositionBitmap()
			tc.setup(bitmap)
			for _, bench := range []struct {
				name string
				fn   func(memory.Allocator, *dv.RoaringPositionBitmap, *rowPositionCursor, int64) arrow.Array
			}{
				{name: "before", fn: combinePositionalDeleteBitmapBefore},
				{name: "after", fn: combinePositionalDeleteBitmap},
			} {
				b.Run(bench.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						cursor := (&rowPositionSource{}).cursor()
						indices := bench.fn(memory.DefaultAllocator, bitmap, cursor, nrows)
						if indices != nil {
							positionalDeleteRangeBenchmarkSink = int64(indices.Len())
							indices.Release()
						}
					}
				})
			}
		})
	}
}
