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

var finalizeTaskRangeDirectSplitBenchmarkSink []FileScanTask

// BenchmarkFinalizeTaskRangeDirectParquetSplits measures scan-task finalization
// with prebuilt Parquet metadata, excluding manifest reads and file decoding.
// Cases include split-heavy, mixed, and unsplittable task ranges. Use the same
// benchmark on base and head to compare per-file split-buffer allocations.
func BenchmarkFinalizeTaskRangeDirectParquetSplits(b *testing.B) {
	const groupBytes int64 = 8 << 10
	for _, tc := range []struct {
		files, groups, splitPercent int
	}{
		{1024, 4, 100},
		{1024, 32, 100},
		{1024, 128, 100},
		{8192, 4, 100},
		{8192, 32, 100},
		{8192, 32, 50},
		{8192, 32, 0},
	} {
		b.Run(fmt.Sprintf("files=%d/groups=%d/split=%d", tc.files, tc.groups, tc.splitPercent), func(b *testing.B) {
			offsets := make([]int64, tc.groups)
			for i := range offsets {
				offsets[i] = int64(i) * groupBytes
			}
			fileSize := int64(tc.groups) * groupBytes
			targetSize := 2 * groupBytes
			tasks := make([]FileScanTask, tc.files)
			for i := range tasks {
				builder, err := iceberg.NewDataFileBuilder(
					*iceberg.UnpartitionedSpec,
					iceberg.EntryContentData,
					fmt.Sprintf("mem://finalize-splits/data-%d.parquet", i),
					iceberg.ParquetFile, nil, nil, nil, 1, fileSize,
				)
				if err != nil {
					b.Fatal(err)
				}
				task := FileScanTask{File: builder.SplitOffsets(offsets).Build(), Length: fileSize}
				if i%100 >= tc.splitPercent {
					task.Start = 1
					task.Length--
				}
				tasks[i] = task
			}
			expectedTasks := 0
			for _, task := range tasks {
				if splits, split := splitParquetScanTask(task, targetSize); split {
					expectedTasks += len(splits)
				} else {
					expectedTasks++
				}
			}
			scan := &Scan{}
			probe := scan.finalizeTaskRange(tasks, 0, len(tasks), nil, targetSize)
			if probe.err != nil || len(probe.tasks) != expectedTasks {
				b.Fatalf("finalized %d tasks, expected %d (err=%v)", len(probe.tasks), expectedTasks, probe.err)
			}

			b.ReportAllocs()
			b.ReportMetric(float64(tc.files), "files/op")
			b.ResetTimer()
			for b.Loop() {
				result := scan.finalizeTaskRange(tasks, 0, len(tasks), nil, targetSize)
				finalizeTaskRangeDirectSplitBenchmarkSink = result.tasks
			}
		})
	}
}
