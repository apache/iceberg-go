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
