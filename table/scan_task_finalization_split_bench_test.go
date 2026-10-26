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
	"testing"

	"github.com/apache/iceberg-go"
)

var finalizeTaskRangeSplitBenchmarkSink []FileScanTask

// BenchmarkFinalizeTaskRangeParquetSplits isolates local scan task finalization.
// Files and split offsets are built before timing. No residual evaluation or
// manifest I/O is included, so the result measures split/append overhead.
func BenchmarkFinalizeTaskRangeParquetSplits(b *testing.B) {
	const (
		rowGroupBytes int64 = 4096
		targetSize    int64 = 16384
	)

	for _, tc := range []struct {
		files, groups, splittablePercent int
	}{
		{8192, 32, 0},
		{8192, 32, 50},
		{8192, 32, 100},
		{2048, 128, 100},
	} {
		b.Run(fmt.Sprintf("files=%d/groups=%d/splittable=%d",
			tc.files, tc.groups, tc.splittablePercent), func(b *testing.B) {
			offsets := make([]int64, tc.groups)
			for i := range offsets {
				offsets[i] = int64(i) * rowGroupBytes
			}
			fileSize := int64(tc.groups) * rowGroupBytes
			tasks := make([]FileScanTask, tc.files)
			for i := range tasks {
				file := finalizationTestDataFile(
					b, *iceberg.UnpartitionedSpec,
					fmt.Sprintf("mem://finalization-splits/data-%d.parquet", i),
					nil, fileSize, offsets,
				)
				task := FileScanTask{File: file, Length: fileSize}
				if i%100 >= tc.splittablePercent {
					task.Start = 1
					task.Length--
				}
				tasks[i] = task
			}

			expectedTasks := 0
			for _, task := range tasks {
				if splits, ok := splitParquetScanTask(task, targetSize); ok {
					expectedTasks += len(splits)
				} else {
					expectedTasks++
				}
			}

			scan := &Scan{}
			b.ReportAllocs()
			b.ReportMetric(float64(tc.files), "files/op")
			b.ResetTimer()
			for b.Loop() {
				result := scan.finalizeTaskRange(tasks, 0, len(tasks), nil, targetSize)
				if result.err != nil {
					b.Fatal(result.err)
				}
				if len(result.tasks) != expectedTasks {
					b.Fatalf("got %d tasks, want %d", len(result.tasks), expectedTasks)
				}
				finalizeTaskRangeSplitBenchmarkSink = result.tasks
			}
		})
	}
}
