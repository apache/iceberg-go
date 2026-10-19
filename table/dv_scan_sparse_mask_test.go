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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeletionVectorHugeFileCleanBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(1 << 39)
	ctx := compute.WithAllocator(context.Background(), mem)
	filter := filterByDeletionVector(ctx, bitmap, 1<<40, (&rowPositionSource{}).cursor())

	in := checkedInt64RecordBatch(mem, 0, 1, 2)
	out, err := filter(in)
	require.NoError(t, err)
	assert.Same(t, in, out, "a clean batch should pass through")
	out.Release()
}

func TestDeletionVectorSkipsCleanBatches(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(4)
	ctx := compute.WithAllocator(context.Background(), mem)
	filter := filterByDeletionVector(ctx, bitmap, 9, (&rowPositionSource{}).cursor())

	for i, test := range []struct {
		values []int64
		want   []int64
		clean  bool
	}{
		{values: []int64{0, 1, 2}, want: []int64{0, 1, 2}, clean: true},
		{values: []int64{3, 4, 5}, want: []int64{3, 5}},
		{values: []int64{6, 7, 8}, want: []int64{6, 7, 8}, clean: true},
	} {
		in := checkedInt64RecordBatch(mem, test.values...)
		out, err := filter(in)
		require.NoError(t, err, "batch %d", i)
		if test.clean {
			assert.Same(t, in, out, "batch %d", i)
		}
		assert.Equal(t, test.want, out.Column(0).(*array.Int64).Int64Values())
		out.Release()
	}
}

func TestDeletionVectorPrunedBatchSkipsCleanRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(53)
	ctx := compute.WithAllocator(context.Background(), mem)
	source := &rowPositionSource{spans: []internal.RowGroupSpan{
		{FirstRowPos: 40, NumRows: 3},
		{FirstRowPos: 52, NumRows: 3},
	}}
	filter := filterByDeletionVector(ctx, bitmap, 100, source.cursor())

	first := checkedInt64RecordBatch(mem, 40, 41, 42)
	firstOut, err := filter(first)
	require.NoError(t, err)
	assert.Same(t, first, firstOut)
	firstOut.Release()

	second := checkedInt64RecordBatch(mem, 52, 53, 54)
	secondOut, err := filter(second)
	require.NoError(t, err)
	assert.Equal(t, []int64{52, 54}, secondOut.Column(0).(*array.Int64).Int64Values())
	secondOut.Release()
}
