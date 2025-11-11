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
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// BenchmarkPartitionedWriteThroughput benchmarks the full table.Append() path
// including partition path generation, which exercises the optimizations in
// PartitionToPath (cached PartitionType, pre-escaped names, strings.Builder)
func BenchmarkPartitionedWriteThroughput(b *testing.B) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	// Define Iceberg schema matching reproducer
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 3, Name: "host", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "status_code", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 5, Name: "bytes_sent", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 6, Name: "user_agent", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	// Define Arrow schema (must match Iceberg schema types)
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status_code", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "bytes_sent", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "user_agent", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	// Define partition spec (partitioned by day(ts) and host)
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Transform: iceberg.DayTransform{}, Name: "ts_day"},
		iceberg.PartitionField{SourceID: 3, FieldID: 1001, Transform: iceberg.IdentityTransform{}, Name: "host"},
	)

	// Helper to create a batch of records
	createBatch := func(numRecords int) arrow.RecordBatch {
		idB := array.NewInt64Builder(mem)
		tsB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
		hostB := array.NewStringBuilder(mem)
		statusB := array.NewInt32Builder(mem)
		bytesB := array.NewInt64Builder(mem)
		uaB := array.NewStringBuilder(mem)

		defer idB.Release()
		defer tsB.Release()
		defer hostB.Release()
		defer statusB.Release()
		defer bytesB.Release()
		defer uaB.Release()

		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		hosts := []string{"example.com", "foo.test.org", "demo.net"}
		userAgents := []string{"Mozilla/5.0", "curl/7.68.0", "python-requests/2.25.1"}

		for i := 0; i < numRecords; i++ {
			idB.Append(int64(i))
			// Spread timestamps across multiple days to create multiple partitions
			ts := baseTime.Add(time.Duration(i%10) * 24 * time.Hour)
			tsB.Append(arrow.Timestamp(ts.UnixMicro()))
			hostB.Append(hosts[i%len(hosts)])
			statusB.Append(200 + int32(i%5))
			bytesB.Append(int64(1000 + i%5000))
			uaB.Append(userAgents[i%len(userAgents)])
		}

		return array.NewRecordBatch(arrSchema, []arrow.Array{
			idB.NewArray(),
			tsB.NewArray(),
			hostB.NewArray(),
			statusB.NewArray(),
			bytesB.NewArray(),
			uaB.NewArray(),
		}, int64(numRecords))
	}

	// Run benchmarks with different batch sizes
	benchSizes := []struct {
		name       string
		numRecords int
	}{
		{"100K_records", 100_000},
		{"500K_records", 500_000},
		{"2.5M_records", 2_500_000},
	}

	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) {
			// Setup warehouse directory and catalog
			loc := filepath.ToSlash(b.TempDir())

			// Create in-memory SQL catalog (like sql_test.go examples)
			cat, err := catalog.Load(ctx, "benchmark", iceberg.Properties{
				"type":        "sql",
				"uri":         ":memory:",
				"sql.dialect": "sqlite",
				"sql.driver":  "sqlite",
				"warehouse":   "file://" + loc,
			})
			if err != nil {
				b.Fatalf("Failed to create catalog: %v", err)
			}

			// Create namespace
			ns := table.Identifier{"benchmark"}
			err = cat.CreateNamespace(ctx, ns, iceberg.Properties{})
			if err != nil {
				b.Fatalf("Failed to create namespace: %v", err)
			}

			// Create table with partition spec
			tableID := table.Identifier{"benchmark", "partitioned_table"}
			tbl, err := cat.CreateTable(ctx, tableID, icebergSchema,
				catalog.WithPartitionSpec(&spec),
			)
			if err != nil {
				b.Fatalf("Failed to create table: %v", err)
			}

			// Pre-create batch to avoid measuring batch creation time
			testBatch := createBatch(bs.numRecords)
			defer testBatch.Release()

			b.ResetTimer()
			b.ReportAllocs()

			totalRecords := int64(0)
			for i := 0; i < b.N; i++ {
				// Use table.Append() - this triggers getPartitionMap() which calls
				// PartitionToPath() for EVERY row, exercising our optimizations
				reader, err := array.NewRecordReader(arrSchema, []arrow.RecordBatch{testBatch})
				if err != nil {
					b.Fatalf("Failed to create reader: %v", err)
				}

				// This is the hot path that benefits from our optimizations:
				// 1. Calls getPartitionMap() which iterates every row
				// 2. For each row, calls PartitionToPath() via partitionPath()
				// 3. PartitionToPath now uses: cached PartitionType, pre-escaped names, strings.Builder
				newTable, err := tbl.Append(ctx, reader, iceberg.Properties{})
				if err != nil {
					reader.Release()
					b.Fatalf("Append error: %v", err)
				}

				reader.Release()
				tbl = newTable
				totalRecords += int64(bs.numRecords)
			}

			b.StopTimer()

			// Report throughput
			recordsPerOp := float64(bs.numRecords)
			recordsPerSec := recordsPerOp / (b.Elapsed().Seconds() / float64(b.N))
			b.ReportMetric(recordsPerSec, "records/sec")
			b.ReportMetric(float64(totalRecords), "total_records")
		})
	}
}
