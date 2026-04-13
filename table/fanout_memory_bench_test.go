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

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// gatedReader is a RecordReader whose Next() blocks on a channel, giving
// the test control over when each batch enters the fanout pipeline. Between
// sends the test can measure HeapInuse to observe whether previous batches
// were released (new code) or are still retained (old code with
// function-scoped defer).
type gatedReader struct {
	schema *arrow.Schema
	ch     <-chan arrow.RecordBatch
	cur    arrow.RecordBatch
	refs   int64
}

func (r *gatedReader) Retain()                        { r.refs++ }
func (r *gatedReader) Release()                       { r.refs-- }
func (r *gatedReader) Schema() *arrow.Schema          { return r.schema }
func (r *gatedReader) Err() error                     { return nil }
func (r *gatedReader) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *gatedReader) Record() arrow.RecordBatch      { return r.cur }

func (r *gatedReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	batch, ok := <-r.ch
	if !ok {
		return false
	}
	r.cur = batch
	return true
}

// BenchmarkFanoutMemory feeds N large record batches through the partitioned
// fanout writer one at a time, measuring HeapInuse after each batch is
// consumed. Before the processRecord refactor, defer record.Release() was
// function-scoped in fanout(), so batches accumulated in memory until the
// function returned — HeapInuse grew linearly. After the fix, each batch is
// released when processRecord returns, so HeapInuse stays flat.
func BenchmarkFanoutMemory(b *testing.B) {
	mem := memory.DefaultAllocator
	ctx := context.Background()

	const (
		recordsPerBatch = 50_000
		numBatches      = 100
	)

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 3, Name: "host", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "value", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.DayTransform{}, Name: "ts_day"},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1001, Transform: iceberg.IdentityTransform{}, Name: "host"},
	)

	hosts := []string{"host-a.example.com", "host-b.example.com", "host-c.example.com"}
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	createBatch := func(offset int) arrow.RecordBatch {
		idB := array.NewInt64Builder(mem)
		tsB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
		hostB := array.NewStringBuilder(mem)
		valB := array.NewStringBuilder(mem)

		defer idB.Release()
		defer tsB.Release()
		defer hostB.Release()
		defer valB.Release()

		for i := range recordsPerBatch {
			idB.Append(int64(offset*recordsPerBatch + i))
			ts := baseTime.Add(time.Duration(i%10) * 24 * time.Hour)
			tsB.Append(arrow.Timestamp(ts.UnixMicro()))
			hostB.Append(hosts[i%len(hosts)])
			valB.Append(fmt.Sprintf("payload-%06d-abcdefghijklmnopqrstuvwxyz-0123456789-abcdefghijklmnopqrstuvwxyz-%06d", i, offset))
		}

		return array.NewRecordBatch(arrSchema, []arrow.Array{
			idB.NewArray(),
			tsB.NewArray(),
			hostB.NewArray(),
			valB.NewArray(),
		}, recordsPerBatch)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		loc := filepath.ToSlash(b.TempDir())

		cat, err := catalog.Load(ctx, "bench", iceberg.Properties{
			"type":        "sql",
			"uri":         ":memory:",
			"sql.dialect": "sqlite",
			"sql.driver":  "sqlite",
			"warehouse":   "file://" + loc,
		})
		if err != nil {
			b.Fatalf("create catalog: %v", err)
		}

		ns := table.Identifier{"bench"}
		if err := cat.CreateNamespace(ctx, ns, iceberg.Properties{}); err != nil {
			b.Fatalf("create namespace: %v", err)
		}

		tbl, err := cat.CreateTable(ctx, table.Identifier{"bench", "mem_test"}, icebergSchema,
			catalog.WithPartitionSpec(&spec),
		)
		if err != nil {
			b.Fatalf("create table: %v", err)
		}

		// Gated channel: we send one batch at a time and measure memory
		// between sends.
		batchCh := make(chan arrow.RecordBatch)
		reader := &gatedReader{schema: arrSchema, ch: batchCh, refs: 1}

		// Run Append in the background — it blocks on the gated reader.
		errCh := make(chan error, 1)
		go func() {
			_, err := tbl.Append(ctx, reader, iceberg.Properties{})
			errCh <- err
		}()

		runtime.GC()
		var baseline runtime.MemStats
		runtime.ReadMemStats(&baseline)

		var peakHeapInuse uint64

		for j := range numBatches {
			batch := createBatch(j)
			batchCh <- batch

			// Let the fanout worker process the batch.
			time.Sleep(5 * time.Millisecond)

			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peakHeapInuse {
				peakHeapInuse = m.HeapInuse
			}
		}

		close(batchCh)
		if err := <-errCh; err != nil {
			b.Fatalf("append: %v", err)
		}

		b.ReportMetric(float64(peakHeapInuse-baseline.HeapInuse)/(1024*1024), "peak-heap-delta-MB")
		b.ReportMetric(float64(numBatches), "batches")
		b.ReportMetric(float64(numBatches*recordsPerBatch), "total_records")
	}
}
