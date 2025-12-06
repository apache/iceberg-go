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
	"github.com/apache/iceberg-go/table/internal"
)

// Common benchmark configuration
var benchSizes = []struct {
	name       string
	numRecords int
}{
	{"100K_records", 100_000},
	{"500K_records", 500_000},
	{"2.5M_records", 2_500_000},
}

// runBenchmark is a helper function to run a benchmark with a given schema and batch creator
func runBenchmark(b *testing.B, icebergSchema *iceberg.Schema, arrSchema *arrow.Schema, createBatch func(int) arrow.RecordBatch) {
	ctx := context.Background()

	// Define partition spec (partitioned by day(ts) and host)
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Transform: iceberg.DayTransform{}, Name: "ts_day"},
		iceberg.PartitionField{SourceID: 3, FieldID: 1001, Transform: iceberg.IdentityTransform{}, Name: "host"},
	)

	for _, bs := range benchSizes {
		b.Run(bs.name, func(b *testing.B) {
			// Setup warehouse directory and catalog
			loc := filepath.ToSlash(b.TempDir())

			// Create in-memory SQL catalog
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
				reader, err := array.NewRecordReader(arrSchema, []arrow.RecordBatch{testBatch})
				if err != nil {
					b.Fatalf("Failed to create reader: %v", err)
				}

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

// BenchmarkPartitionedWriteThroughput_Simple benchmarks with simple primitive types only
func BenchmarkPartitionedWriteThroughput_Simple(b *testing.B) {
	mem := memory.DefaultAllocator

	// Define Iceberg schema with only primitive types
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

	runBenchmark(b, icebergSchema, arrSchema, createBatch)
}

// BenchmarkPartitionedWriteThroughput_ListPrimitive benchmarks with a list of primitive types
func BenchmarkPartitionedWriteThroughput_ListPrimitive(b *testing.B) {
	mem := memory.DefaultAllocator

	// Define Iceberg schema with list<string>
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 3, Name: "host", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "status_code", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 5, Name: "bytes_sent", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 6, Name: "user_agent", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 7, Name: "tags", Type: &iceberg.ListType{
			ElementID:       8,
			Element:         iceberg.PrimitiveTypes.String,
			ElementRequired: true,
		}, Required: false},
	)

	// Define Arrow schema
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status_code", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "bytes_sent", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "user_agent", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	}, nil)

	// Helper to create a batch of records
	createBatch := func(numRecords int) arrow.RecordBatch {
		idB := array.NewInt64Builder(mem)
		tsB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
		hostB := array.NewStringBuilder(mem)
		statusB := array.NewInt32Builder(mem)
		bytesB := array.NewInt64Builder(mem)
		uaB := array.NewStringBuilder(mem)
		tagsB := array.NewListBuilder(mem, arrow.BinaryTypes.String)
		tagsValueB := tagsB.ValueBuilder().(*array.StringBuilder)

		defer idB.Release()
		defer tsB.Release()
		defer hostB.Release()
		defer statusB.Release()
		defer bytesB.Release()
		defer uaB.Release()
		defer tagsB.Release()

		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		hosts := []string{"example.com", "foo.test.org", "demo.net"}
		userAgents := []string{"Mozilla/5.0", "curl/7.68.0", "python-requests/2.25.1"}
		tagsList := []string{"tag_0", "tag_1", "tag_2", "tag_3", "tag_4"}

		for i := 0; i < numRecords; i++ {
			idB.Append(int64(i))
			ts := baseTime.Add(time.Duration(i%10) * 24 * time.Hour)
			tsB.Append(arrow.Timestamp(ts.UnixMicro()))
			hostB.Append(hosts[i%len(hosts)])
			statusB.Append(200 + int32(i%5))
			bytesB.Append(int64(1000 + i%5000))
			uaB.Append(userAgents[i%len(userAgents)])

			// Add 2-5 tags per record
			numTags := 2 + (i % 4)
			tagsB.Append(true)
			for j := 0; j < numTags; j++ {
				tagsValueB.Append(tagsList[j%len(tagsList)])
			}
		}

		return array.NewRecordBatch(arrSchema, []arrow.Array{
			idB.NewArray(),
			tsB.NewArray(),
			hostB.NewArray(),
			statusB.NewArray(),
			bytesB.NewArray(),
			uaB.NewArray(),
			tagsB.NewArray(),
		}, int64(numRecords))
	}

	runBenchmark(b, icebergSchema, arrSchema, createBatch)
}

// BenchmarkPartitionedWriteThroughput_ListStruct benchmarks with a list of structs containing nested lists
func BenchmarkPartitionedWriteThroughput_ListStruct(b *testing.B) {
	mem := memory.DefaultAllocator

	// Resource struct type: {type: string, id: list<string>}
	resourceStruct := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 8, Name: "type", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 9, Name: "id", Type: &iceberg.ListType{
				ElementID:       10,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: true,
			}, Required: true},
		},
	}

	// List of resources: list<struct<type: string, id: list<string>>>
	resourcesListType := &iceberg.ListType{
		ElementID:       11,
		Element:         resourceStruct,
		ElementRequired: true,
	}

	// Define Iceberg schema with complex nested types
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 3, Name: "host", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "status_code", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 5, Name: "bytes_sent", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 6, Name: "user_agent", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 7, Name: "resources", Type: resourcesListType, Required: false},
	)

	// Define Arrow schema
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status_code", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "bytes_sent", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "user_agent", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "resources", Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "id", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
		)), Nullable: true},
	}, nil)

	// Helper to create a batch of records
	createBatch := func(numRecords int) arrow.RecordBatch {
		idB := array.NewInt64Builder(mem)
		tsB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
		hostB := array.NewStringBuilder(mem)
		statusB := array.NewInt32Builder(mem)
		bytesB := array.NewInt64Builder(mem)
		uaB := array.NewStringBuilder(mem)

		// Build resources: list<struct<type: string, id: list<string>>>
		resourcesB := array.NewListBuilder(mem, arrow.StructOf(
			arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "id", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
		))
		resourceStructB := resourcesB.ValueBuilder().(*array.StructBuilder)
		resourceTypeB := resourceStructB.FieldBuilder(0).(*array.StringBuilder)
		resourceIdListB := resourceStructB.FieldBuilder(1).(*array.ListBuilder)
		resourceIdB := resourceIdListB.ValueBuilder().(*array.StringBuilder)

		defer idB.Release()
		defer tsB.Release()
		defer hostB.Release()
		defer statusB.Release()
		defer bytesB.Release()
		defer uaB.Release()
		defer resourcesB.Release()

		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		hosts := []string{"example.com", "foo.test.org", "demo.net"}
		userAgents := []string{"Mozilla/5.0", "curl/7.68.0", "python-requests/2.25.1"}
		resourceTypes := []string{"dataset", "station", "network"}

		for i := 0; i < numRecords; i++ {
			idB.Append(int64(i))
			ts := baseTime.Add(time.Duration(i%10) * 24 * time.Hour)
			tsB.Append(arrow.Timestamp(ts.UnixMicro()))
			hostB.Append(hosts[i%len(hosts)])
			statusB.Append(200 + int32(i%5))
			bytesB.Append(int64(1000 + i%5000))
			uaB.Append(userAgents[i%len(userAgents)])

			// Add resources - vary between 1-3 resources per record
			numResources := 1 + (i % 3)
			resourcesB.Append(true)
			for j := 0; j < numResources; j++ {
				resourceStructB.Append(true)
				resourceTypeB.Append(resourceTypes[j%len(resourceTypes)])

				// Add 2-4 IDs per resource
				numIds := 2 + (j % 3)
				resourceIdListB.Append(true)
				for k := 0; k < numIds; k++ {
					resourceIdB.Append("id_" + string(rune('A'+k)))
				}
			}
		}

		return array.NewRecordBatch(arrSchema, []arrow.Array{
			idB.NewArray(),
			tsB.NewArray(),
			hostB.NewArray(),
			statusB.NewArray(),
			bytesB.NewArray(),
			uaB.NewArray(),
			resourcesB.NewArray(),
		}, int64(numRecords))
	}

	runBenchmark(b, icebergSchema, arrSchema, createBatch)
}

// BenchmarkPartitionedWriteThroughput_MapPrimitive benchmarks with a map of primitive types
func BenchmarkPartitionedWriteThroughput_MapPrimitive(b *testing.B) {
	mem := memory.DefaultAllocator

	// Define Iceberg schema with list<string>
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 3, Name: "host", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "status_code", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 5, Name: "bytes_sent", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 6, Name: "user_agent", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 7, Name: "tags", Type: &iceberg.MapType{
			KeyID:         8,
			KeyType:       iceberg.PrimitiveTypes.String,
			ValueID:       9,
			ValueType:     iceberg.PrimitiveTypes.String,
			ValueRequired: false,
		}, Required: false},
	)

	// Define Arrow schema
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status_code", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "bytes_sent", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "user_agent", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "tags", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
	}, nil)

	// Helper to create a batch of records
	createBatch := func(numRecords int) arrow.RecordBatch {
		idB := array.NewInt64Builder(mem)
		tsB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
		hostB := array.NewStringBuilder(mem)
		statusB := array.NewInt32Builder(mem)
		bytesB := array.NewInt64Builder(mem)
		uaB := array.NewStringBuilder(mem)
		tagsB := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		tagsKeyB := tagsB.KeyBuilder().(*array.StringBuilder)
		tagsItemB := tagsB.ItemBuilder().(*array.StringBuilder)

		defer idB.Release()
		defer tsB.Release()
		defer hostB.Release()
		defer statusB.Release()
		defer bytesB.Release()
		defer uaB.Release()
		defer tagsB.Release()
		defer tagsKeyB.Release()
		defer tagsItemB.Release()

		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		hosts := []string{"example.com", "foo.test.org", "demo.net"}
		userAgents := []string{"Mozilla/5.0", "curl/7.68.0", "python-requests/2.25.1"}
		tagsKeys := []string{"tag_0", "tag_1", "tag_2", "tag_3", "tag_4"}
		tagsItems := []string{"item_0", "item_1", "item_2", "item_3", "item_4", "item_5", "item_6", "item_7", "item_8", "item_9"}

		for i := 0; i < numRecords; i++ {
			idB.Append(int64(i))
			ts := baseTime.Add(time.Duration(i%10) * 24 * time.Hour)
			tsB.Append(arrow.Timestamp(ts.UnixMicro()))
			hostB.Append(hosts[i%len(hosts)])
			statusB.Append(200 + int32(i%5))
			bytesB.Append(int64(1000 + i%5000))
			uaB.Append(userAgents[i%len(userAgents)])

			// Add 2-5 tags per record
			numTags := 2 + (i % 4)
			tagsB.Append(true)
			for j := 0; j < numTags; j++ {
				tagsKeyB.Append(tagsKeys[j%len(tagsKeys)])
				tagsItemB.Append(tagsItems[j%len(tagsItems)])
			}
		}

		return array.NewRecordBatch(arrSchema, []arrow.Array{
			idB.NewArray(),
			tsB.NewArray(),
			hostB.NewArray(),
			statusB.NewArray(),
			bytesB.NewArray(),
			uaB.NewArray(),
			tagsB.NewArray(),
		}, int64(numRecords))
	}

	runBenchmark(b, icebergSchema, arrSchema, createBatch)
}

// BenchmarkPartitionedWriteThroughput_PartitionCount tests how write performance scales with partition count
func BenchmarkPartitionedWriteThroughput_PartitionCount(b *testing.B) {
	mem := memory.DefaultAllocator
	ctx := context.Background()

	// Define Iceberg schema
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 3, Name: "partition_key", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 4, Name: "value", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	// Define Arrow schema
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "partition_key", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Partition spec - only by partition_key
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{SourceID: 3, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "partition_key"},
	)

	numRecords := 100_000

	partitionTests := []struct {
		name           string
		partitionCount int
	}{
		{"25_partitions", 25},
		{"100_partitions", 100},
		{"250_partitions", 250},
		{"1000_partitions", 1000},
	}

	for _, pt := range partitionTests {
		b.Run(pt.name, func(b *testing.B) {
			// Helper to create a batch with specified partition distribution
			createBatch := func(numRecords, numPartitions int) arrow.RecordBatch {
				idB := array.NewInt64Builder(mem)
				tsB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
				partKeyB := array.NewInt32Builder(mem)
				valueB := array.NewInt64Builder(mem)

				defer idB.Release()
				defer tsB.Release()
				defer partKeyB.Release()
				defer valueB.Release()

				baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

				for i := 0; i < numRecords; i++ {
					idB.Append(int64(i))
					tsB.Append(arrow.Timestamp(baseTime.Add(time.Duration(i) * time.Second).UnixMicro()))
					partKeyB.Append(int32(i % numPartitions))
					valueB.Append(int64(i * 1000))
				}

				return array.NewRecordBatch(arrSchema, []arrow.Array{
					idB.NewArray(),
					tsB.NewArray(),
					partKeyB.NewArray(),
					valueB.NewArray(),
				}, int64(numRecords))
			}

			// Setup
			loc := filepath.ToSlash(b.TempDir())

			cat, err := catalog.Load(ctx, "benchmark", iceberg.Properties{
				"type":                              "sql",
				"uri":                               ":memory:",
				"sql.dialect":                       "sqlite",
				"sql.driver":                        "sqlite",
				"warehouse":                         "file://" + loc,
				internal.ParquetCompressionKey:      "zstd",
				internal.ParquetCompressionLevelKey: "3",
			})
			if err != nil {
				b.Fatalf("Failed to create catalog: %v", err)
			}

			ns := table.Identifier{"benchmark"}
			err = cat.CreateNamespace(ctx, ns, iceberg.Properties{})
			if err != nil {
				b.Fatalf("Failed to create namespace: %v", err)
			}

			tableID := table.Identifier{"benchmark", "partition_scale_test"}
			tbl, err := cat.CreateTable(ctx, tableID, icebergSchema,
				catalog.WithPartitionSpec(&spec),
			)
			if err != nil {
				b.Fatalf("Failed to create table: %v", err)
			}

			testBatch := createBatch(numRecords, pt.partitionCount)
			defer testBatch.Release()

			b.ResetTimer()
			b.ReportAllocs()

			totalRecords := int64(0)
			for i := 0; i < b.N; i++ {
				reader, err := array.NewRecordReader(arrSchema, []arrow.RecordBatch{testBatch})
				if err != nil {
					b.Fatalf("Failed to create reader: %v", err)
				}

				newTable, err := tbl.Append(ctx, reader, iceberg.Properties{})
				if err != nil {
					reader.Release()
					b.Fatalf("Append error: %v", err)
				}

				reader.Release()
				tbl = newTable
				totalRecords += int64(numRecords)
			}

			b.StopTimer()

			recordsPerOp := float64(numRecords)
			recordsPerSec := recordsPerOp / (b.Elapsed().Seconds() / float64(b.N))
			b.ReportMetric(recordsPerSec, "records/sec")
			b.ReportMetric(float64(pt.partitionCount), "partitions")
			b.ReportMetric(float64(totalRecords), "total_records")
		})
	}
}
