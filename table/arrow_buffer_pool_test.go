// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"iter"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestArrowBufferPoolReusesZeroedBuffers(t *testing.T) {
	pool := &arrowBufferPool{backing: memory.NewGoAllocator()}
	buf := pool.Allocate(80)
	require.Equal(t, 128, cap(buf))
	for i := range buf {
		buf[i] = 17
	}
	pool.Free(buf)

	reused := pool.Allocate(65)
	require.Same(t, &buf[0], &reused[0])
	require.Equal(t, make([]byte, 65), reused)
	reused[0] = 42
	grown := pool.Reallocate(100, reused)
	require.Same(t, &reused[0], &grown[0])
	require.Equal(t, byte(42), grown[0])
	require.Equal(t, make([]byte, 99), grown[1:])
	moved := pool.Reallocate(129, grown)
	require.NotSame(t, &grown[0], &moved[0])
	require.Equal(t, byte(42), moved[0])
	require.Equal(t, make([]byte, 128), moved[1:])

	pool.Free(moved)
	pool.close()
	pool.Free(pool.Allocate(129))
	for i := range pool.buckets {
		require.Zero(t, pool.buckets[i].count)
	}
}

func TestReadTasksArrowBufferReuse(t *testing.T) {
	for _, tc := range []struct {
		name      string
		optIn     bool
		allocator memory.Allocator
		wantPool  bool
	}{
		{name: "default", allocator: memory.NewGoAllocator()},
		{name: "opted in", optIn: true, allocator: memory.NewGoAllocator(), wantPool: true},
		{name: "custom allocator", optIn: true, allocator: memory.NewCheckedAllocator(memory.NewGoAllocator())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan, _ := residualBindingTestScan(t)
			if tc.optIn {
				WithArrowBufferReuse()(scan)
			}
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
			var pool *arrowBufferPool
			scan.readRecords = func(_ *arrowScan, ctx context.Context, _ []FileScanTask) (
				*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error,
			) {
				allocator := compute.GetAllocator(ctx)
				pool, _ = allocator.(*arrowBufferPool)
				if tc.wantPool {
					require.NotNil(t, pool)
				} else {
					require.Same(t, tc.allocator, allocator)
				}
				return schema, func(yield func(arrow.RecordBatch, error) bool) {
					builder := array.NewInt64Builder(allocator)
					defer builder.Release()
					builder.Append(42)
					data := builder.NewInt64Array()
					defer data.Release()
					yield(array.NewRecordBatch(schema, []arrow.Array{data}, 1), nil)
				}, nil
			}

			_, records, err := scan.ReadTasks(compute.WithAllocator(t.Context(), tc.allocator), nil)
			require.NoError(t, err)
			var retained arrow.RecordBatch
			for batch, err := range records {
				require.NoError(t, err)
				retained = batch
				break
			}
			require.NotNil(t, retained)
			require.Equal(t, int64(42), retained.Column(0).(*array.Int64).Value(0))
			if tc.wantPool {
				require.True(t, pool.closed.Load())
			}
			retained.Release()
			if checked, ok := tc.allocator.(*memory.CheckedAllocator); ok {
				checked.AssertSize(t, 0)
			}
		})
	}
}
