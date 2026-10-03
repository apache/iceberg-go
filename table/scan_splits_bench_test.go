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
		b.Run(fmt.Sprintf("offsets=%d", offsetCount), func(b *testing.B) {
			fileSize := int64(offsetCount+1) * 64
			offsets := make([]int64, offsetCount)
			for i := range offsets {
				offsets[i] = int64(i+1) * 64
			}

			dataFileBuilder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec,
				iceberg.EntryContentData,
				"mem://benchmark/split-offsets.parquet",
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
			file := dataFileBuilder.SplitOffsets(offsets).Build()
			tasks := make([]FileScanTask, filesPerOp)
			for i := range tasks {
				tasks[i] = FileScanTask{File: file, Start: 0, Length: fileSize}
			}

			wantTasksPerFile := 1
			if offsetCount > 1 {
				splits, ok := splitParquetScanTask(tasks[0], fileSize/2)
				if !ok {
					b.Fatal("expected split offsets to produce scan tasks")
				}
				wantTasksPerFile = len(splits)
			}
			wantTasks := filesPerOp * wantTasksPerFile

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result := splitRemoteScanTasks(tasks, fileSize/2)
				if len(result) != wantTasks {
					b.Fatalf("splitRemoteScanTasks() returned %d tasks, want %d", len(result), wantTasks)
				}
				splitRemoteScanTasksBenchmarkSink = result
			}
			b.ReportMetric(filesPerOp, "files/op")
			b.ReportMetric(float64(offsetCount), "split_offsets/file")
		})
	}
}
