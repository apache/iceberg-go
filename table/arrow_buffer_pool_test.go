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
	"bytes"
	"context"
	"iter"
	"math/bits"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestArrowBufferPoolReusesZeroedBuffers(t *testing.T) {
	pool := &arrowBufferPool{backing: memory.NewGoAllocator()}
	buf := pool.Allocate(80)
	require.Equal(t, 128, cap(buf))
	full := buf[:cap(buf)]
	for i := range full {
		full[i] = 17
	}
	pool.Free(buf)

	reused := pool.Allocate(65)
	require.Same(t, &buf[0], &reused[0])
	require.Equal(t, make([]byte, cap(reused)), reused[:cap(reused)])
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

func BenchmarkReadTasksArrowBufferReuse(b *testing.B) {
	meta, fs, _, tasks, wantRows := benchmarkSplitParquetScanWithRowGroups(b, 16, 8192)
	table := New(Identifier{"benchmark"}, meta, "", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
	for _, tc := range []struct {
		name    string
		workers int
	}{
		{name: "one_worker", workers: 1},
		{name: "four_workers", workers: 4},
	} {
		for _, workload := range []struct {
			name string
			rows int64
		}{
			{name: "full", rows: wantRows},
			{name: "filtered", rows: wantRows / 4},
		} {
			for _, mode := range []string{"default", "reuse"} {
				b.Run(workload.name+"/"+tc.name+"/"+mode, func(b *testing.B) {
					options := []ScanOption{WithMaxConcurrency(tc.workers), WithArrowBatchSize(8192)}
					if workload.rows != wantRows {
						options = append(options, WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("id"), wantRows-workload.rows)))
					}
					if mode == "reuse" {
						options = append(options, WithArrowBufferReuse())
					}
					benchmarkReadTasksBufferReuse(b, table.Scan(options...), tasks, workload.rows)
				})
			}
		}
	}
}

func benchmarkReadTasksBufferReuse(b *testing.B, scan *Scan, tasks []FileScanTask, wantRows int64) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		_, records, err := scan.ReadTasks(b.Context(), tasks)
		require.NoError(b, err)
		var rows int64
		for record, err := range records {
			require.NoError(b, err)
			rows += record.NumRows()
			record.Release()
		}
		require.Equal(b, wantRows, rows)
	}
}

func TestArrowBufferPoolBoundsEachSizeClass(t *testing.T) {
	pool := &arrowBufferPool{backing: memory.NewGoAllocator()}
	defer pool.close()

	for _, size := range []int{1, 64, 65, 1025, 1 << arrowBufferMaxClass} {
		class := max(arrowBufferMinClass, bits.Len(uint(size-1)))
		bucket := &pool.buckets[class-arrowBufferMinClass]
		held := make([][]byte, arrowBuffersPerClass+3)
		for i := range held {
			held[i] = pool.Allocate(size)
			require.Len(t, held[i], size)
			full := held[i][:cap(held[i])]
			for j := range full {
				full[j] = byte(i + 1)
			}
		}
		for _, buf := range held {
			pool.Free(buf)
		}
		require.Equal(t, arrowBuffersPerClass, bucket.count,
			"size %d must not retain more than the configured buffer count", size)

		for range arrowBuffersPerClass {
			reused := pool.Allocate(size)
			require.Equal(t, make([]byte, cap(reused)), reused[:cap(reused)],
				"size %d must clear the full buffer, including its spare capacity", size)
		}
		require.Zero(t, bucket.count)
	}

	oversized := pool.Allocate((1 << arrowBufferMaxClass) + 1)
	pool.Free(oversized)
	for i := range pool.buckets {
		require.Zero(t, pool.buckets[i].count, "oversized allocations must bypass the cache")
	}
}

func TestArrowBufferPoolReallocateShrinkAndRegrow(t *testing.T) {
	pool := &arrowBufferPool{backing: memory.NewGoAllocator()}
	defer pool.close()

	buf := pool.Allocate(100)
	for i := range buf {
		buf[i] = 7
	}
	shrunk := pool.Reallocate(64, buf)
	require.Same(t, &buf[0], &shrunk[0])
	require.Len(t, shrunk, 64)

	grown := pool.Reallocate(100, shrunk)
	require.Same(t, &buf[0], &grown[0])
	require.Equal(t, bytes.Repeat([]byte{7}, 64), grown[:64])
	require.Equal(t, make([]byte, 36), grown[64:])

	pool.close()
	pool.Free(grown)
	require.Equal(t, byte(7), grown[0], "closing must not mutate live Arrow buffers")
	for i := range pool.buckets {
		require.Zero(t, pool.buckets[i].count)
	}
}

func TestArrowBufferPoolConcurrentFreeAndClose(t *testing.T) {
	pool := &arrowBufferPool{backing: memory.NewGoAllocator()}
	const workers = 8
	const rounds = 200
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			for i := range rounds {
				buf := pool.Allocate(65 + i%3)
				buf[0] = byte(i)
				pool.Free(buf)
			}
		}()
	}
	close(start)
	pool.close()
	wg.Wait()
	pool.close()

	require.True(t, pool.closed.Load())
	for i := range pool.buckets {
		require.Zero(t, pool.buckets[i].count)
	}
}
