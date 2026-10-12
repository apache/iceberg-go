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

import "testing"

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
