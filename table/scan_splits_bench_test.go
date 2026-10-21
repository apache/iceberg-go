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

var splitRemoteScanTasksBenchmarkSink []FileScanTask

func BenchmarkSplitRemoteScanTasksSplitOffsets(b *testing.B) {
	const filesPerOp = 1024

	for _, offsetCount := range []int{1, 32, 128, 512} {
		for _, publicGetter := range []bool{false, true} {
			b.Run(fmt.Sprintf("offsets=%d/public=%t", offsetCount, publicGetter), func(b *testing.B) {
				fileSize := int64(offsetCount+1) * 64
				offsets := make([]int64, offsetCount)
				for i := range offsets {
					offsets[i] = int64(i+1) * 64
				}

				makeFile := func(i int) iceberg.DataFile {
					dataFileBuilder, err := iceberg.NewDataFileBuilder(
						*iceberg.UnpartitionedSpec,
						iceberg.EntryContentData,
						fmt.Sprintf("mem://benchmark/split-offsets-%d.parquet", i),
						iceberg.ParquetFile,
						nil,
						nil,
						nil,
						1,
						fileSize,
					)
					if err != nil {
						b.Fatal(err)
					}
					file := dataFileBuilder.
						ColumnSizes(map[int]int64{1: fileSize}).
						ValueCounts(map[int]int64{1: 1}).
						NullValueCounts(map[int]int64{1: 0}).
						NaNValueCounts(map[int]int64{1: 0}).
						LowerBoundValues(map[int][]byte{1: {1}}).
						UpperBoundValues(map[int][]byte{1: {2}}).
						SplitOffsets(offsets).
						Build()
					if publicGetter {
						file = splitOffsetsPublicBenchmarkFile{DataFile: file}
					}

					return file
				}
				makeTasks := func() []FileScanTask {
					tasks := make([]FileScanTask, filesPerOp)
					for i := range tasks {
						tasks[i] = FileScanTask{File: makeFile(i), Start: 0, Length: fileSize}
					}

					return tasks
				}

				probe := makeTasks()
				wantTasksPerFile := 1
				if offsetCount > 1 {
					splits, ok := splitParquetScanTask(probe[0], fileSize/2)
					if !ok {
						b.Fatal("expected split offsets to produce scan tasks")
					}
					wantTasksPerFile = len(splits)
				}
				wantTasks := filesPerOp * wantTasksPerFile

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					// Rebuild distinct files outside the timer so each timed call measures
					// first access to per-file metadata, matching scan planning.
					b.StopTimer()
					tasks := makeTasks()
					b.StartTimer()

					result := splitRemoteScanTasks(tasks, fileSize/2)

					b.StopTimer()
					if len(result) != wantTasks {
						b.Fatalf("splitRemoteScanTasks() returned %d tasks, want %d", len(result), wantTasks)
					}
					splitRemoteScanTasksBenchmarkSink = result
					b.StartTimer()
				}
				b.ReportMetric(filesPerOp, "files/op")
				b.ReportMetric(float64(offsetCount), "split_offsets/file")
			})
		}
	}
}

// Embedding only the public DataFile interface intentionally hides
// DataFileSplitOffsetsRef, forcing the defensive-copy fallback for comparison.
type splitOffsetsPublicBenchmarkFile struct{ iceberg.DataFile }

// BenchmarkSplitRemoteScanTasksBatch measures the split planner with many
// Parquet files and shared row-group layouts. Input files are built before
// timing, and the benchmark reports the complete output task slice.
func BenchmarkSplitRemoteScanTasksBatch(b *testing.B) {
	const rowGroupBytes int64 = 4096

	for _, tc := range []struct {
		files, groups, splittablePercent int
	}{
		{1024, 4, 100},
		{1024, 32, 100},
		{1024, 128, 100},
		{8192, 4, 100},
		{8192, 32, 100},
		{8192, 32, 50},
		{8192, 32, 0},
	} {
		b.Run(fmt.Sprintf("files=%d/groups=%d/splittable=%d", tc.files, tc.groups, tc.splittablePercent),
			func(b *testing.B) {
				offsets := make([]int64, tc.groups)
				for i := range offsets {
					offsets[i] = int64(i) * rowGroupBytes
				}
				fileSize := int64(tc.groups) * rowGroupBytes
				targetSize := int64(8192)
				if tc.groups > 4 {
					targetSize = 16384
				}

				tasks := make([]FileScanTask, tc.files)
				for i := range tasks {
					builder, err := iceberg.NewDataFileBuilder(
						*iceberg.UnpartitionedSpec,
						iceberg.EntryContentData,
						fmt.Sprintf("mem://split-batch/data-%d.parquet", i),
						iceberg.ParquetFile, nil, nil, nil, 1, fileSize,
					)
					if err != nil {
						b.Fatal(err)
					}
					task := FileScanTask{File: builder.SplitOffsets(offsets).Build(), Length: fileSize}
					if i%100 >= tc.splittablePercent {
						task.Start = 1
						task.Length--
					}
					tasks[i] = task
				}

				probe := splitRemoteScanTasks(tasks, targetSize)
				if len(probe) < len(tasks) {
					b.Fatalf("split planner dropped tasks: got %d, want at least %d", len(probe), len(tasks))
				}

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					splitRemoteScanTasksBenchmarkSink = splitRemoteScanTasks(tasks, targetSize)
				}
				b.ReportMetric(float64(tc.files), "files/op")
			})
	}
}
