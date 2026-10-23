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
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
)

var partitionSummaryBenchmarkResult string

func BenchmarkPartitionSummary(b *testing.B) {
	cases := []struct {
		name    string
		metrics updateMetrics
	}{
		{"empty", updateMetrics{}},
		{"append", updateMetrics{addedFileSize: 123456, addedDataFiles: 10, addedRecords: 1000}},
		{"overwrite", updateMetrics{
			addedFileSize: 123456, removedFileSize: 654321,
			addedDataFiles: 10, removedDataFiles: 20,
			addedRecords: 1000, deletedRecords: 2000,
		}},
		{"all", updateMetrics{
			addedFileSize: 123456, removedFileSize: 654321,
			addedDataFiles: 10, removedDataFiles: 20,
			addedEqDeleteFiles: 3, removedEqDeleteFiles: 4,
			addedPosDeleteFiles: 5, removedPosDeleteFiles: 6,
			addedDeleteFiles: 8, removedDeleteFiles: 10,
			addedRecords: 1000, deletedRecords: 2000,
			addedPosDeletes: 300, removedPosDeletes: 400,
			addedEqDeletes: 500, removedEqDeletes: 600,
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var collector SnapshotSummaryCollector
			b.ReportAllocs()
			for b.Loop() {
				partitionSummaryBenchmarkResult = collector.partitionSummary(&tc.metrics)
			}
		})
	}
}

var snapshotSummaryPathBenchmarkResult string

func BenchmarkSnapshotSummaryPartitionPath(b *testing.B) {
	for _, fieldCount := range []int{1, 3, 16, 64} {
		fields := make([]iceberg.NestedField, fieldCount)
		partitionFields := make([]iceberg.PartitionField, fieldCount)
		partition := make(map[int]any, fieldCount)
		for i := range fields {
			name := "field_" + strconv.Itoa(i)
			fields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: iceberg.PrimitiveTypes.Int32}
			partitionFields[i] = iceberg.PartitionField{
				SourceIDs: []int{i + 1}, FieldID: 1000 + i, Name: name,
				Transform: iceberg.IdentityTransform{},
			}
			partition[1000+i] = int32(i)
		}
		schema := iceberg.NewSchema(0, fields...)
		spec := iceberg.NewPartitionSpec(partitionFields...)

		b.Run(fmt.Sprintf("fields=%d", fieldCount), func(b *testing.B) {
			b.Run("baseline", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					snapshotSummaryPathBenchmarkResult = spec.PartitionToPath(
						newPartitionRecord(partition, spec.PartitionType(schema)), schema)
				}
			})

			b.Run("cold", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var collector SnapshotSummaryCollector
					snapshotSummaryPathBenchmarkResult = collector.partitionPath(partition, schema, spec)
				}
			})

			b.Run("cached", func(b *testing.B) {
				var collector SnapshotSummaryCollector
				snapshotSummaryPathBenchmarkResult = collector.partitionPath(partition, schema, spec)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					snapshotSummaryPathBenchmarkResult = collector.partitionPath(partition, schema, spec)
				}
			})
		})
	}
}

func BenchmarkSnapshotSummaryPartitionPathEscaped(b *testing.B) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String,
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "region",
		Transform: iceberg.IdentityTransform{},
	})

	for _, tc := range []struct {
		name, value string
	}{
		{name: "plain", value: "berlin"},
		{name: "escaped", value: "berlin/east +%?#"},
		{name: "unicode", value: "München/東京"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			partition := map[int]any{1000: tc.value}
			want := spec.PartitionToPath(newPartitionRecord(partition, spec.PartitionType(schema)), schema)
			var collector SnapshotSummaryCollector
			if got := collector.partitionPath(partition, schema, spec); got != want {
				b.Fatalf("partition path changed: got %q, want %q", got, want)
			}

			b.Run("baseline", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					snapshotSummaryPathBenchmarkResult = spec.PartitionToPath(
						newPartitionRecord(partition, spec.PartitionType(schema)), schema)
				}
			})

			b.Run("cached", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					snapshotSummaryPathBenchmarkResult = collector.partitionPath(partition, schema, spec)
				}
			})
		})
	}
}
