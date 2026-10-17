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
	"testing"

	"github.com/apache/iceberg-go"
)

func TestSplitParquetScanTaskCapacity(t *testing.T) {
	tests := []struct {
		name           string
		rowGroups      int
		rowGroupSize   int64
		targetSize     int64
		expectedSplits int
		maxCapacity    int
	}{
		{
			name:           "coalesced_small_row_groups",
			rowGroups:      4096,
			rowGroupSize:   1 << 20,
			targetSize:     128 << 20,
			expectedSplits: 32,
			maxCapacity:    65,
		},
		{
			name:           "large_row_groups",
			rowGroups:      64,
			rowGroupSize:   96 << 20,
			targetSize:     128 << 20,
			expectedSplits: 64,
			maxCapacity:    64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offsets := make([]int64, tt.rowGroups)
			for i := range offsets {
				offsets[i] = int64(i) * tt.rowGroupSize
			}
			fileSize := int64(tt.rowGroups) * tt.rowGroupSize
			file := splitTestDataFile(t, iceberg.ParquetFile, fileSize, offsets)
			task := FileScanTask{File: file, Length: fileSize}

			result, ok := splitParquetScanTask(task, tt.targetSize)
			if !ok || len(result) != tt.expectedSplits {
				t.Fatalf("expected %d splits, got %d (ok=%v)", tt.expectedSplits, len(result), ok)
			}
			if cap(result) > tt.maxCapacity {
				t.Fatalf("allocated capacity %d, want at most %d", cap(result), tt.maxCapacity)
			}
			for i, split := range result {
				if split.Length <= 0 || (i == 0 && split.Start != 0) {
					t.Fatalf("invalid split %d: start=%d length=%d", i, split.Start, split.Length)
				}
				if i > 0 && result[i-1].Start+result[i-1].Length != split.Start {
					t.Fatalf("split %d is not contiguous with split %d", i-1, i)
				}
			}
			last := result[len(result)-1]
			if got := last.Start + last.Length; got != fileSize {
				t.Fatalf("split coverage ended at %d, want %d", got, fileSize)
			}
		})
	}
}

var splitParquetScanTaskBenchmarkSink []FileScanTask

func BenchmarkSplitParquetScanTask(b *testing.B) {
	tests := []struct {
		name         string
		rowGroups    int
		rowGroupSize int64
		targetSize   int64
	}{
		{"coalesced_4096x1MiB", 4096, 1 << 20, 128 << 20},
		{"coalesced_1024x1MiB", 1024, 1 << 20, 128 << 20},
		{"not_coalesced_4096x96MiB", 4096, 96 << 20, 128 << 20},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			offsets := make([]int64, tt.rowGroups)
			for i := range offsets {
				offsets[i] = int64(i) * tt.rowGroupSize
			}
			fileSize := int64(tt.rowGroups) * tt.rowGroupSize
			builder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec,
				iceberg.EntryContentData,
				"mem://table/split-benchmark.parquet",
				iceberg.ParquetFile,
				nil, nil, nil,
				int64(tt.rowGroups),
				fileSize,
			)
			if err != nil {
				b.Fatal(err)
			}
			builder.SplitOffsets(offsets)
			task := FileScanTask{File: builder.Build(), Length: fileSize}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				splits, ok := splitParquetScanTask(task, tt.targetSize)
				if !ok {
					b.Fatal("expected to split parquet task")
				}
				splitParquetScanTaskBenchmarkSink = splits
			}
		})
	}
}

func TestSplitParquetScanTaskSkewedCapacity(t *testing.T) {
	const rowGroups = 4096
	const targetSize int64 = 128 << 20
	offsets := make([]int64, rowGroups)
	var fileSize int64
	for i := range offsets {
		offsets[i] = fileSize
		if i%2 == 0 {
			fileSize += 1 << 20
		} else {
			fileSize += 96 << 20
		}
	}

	file := splitTestDataFile(t, iceberg.ParquetFile, fileSize, offsets)
	splits, ok := splitParquetScanTask(FileScanTask{File: file, Length: fileSize}, targetSize)
	if !ok {
		t.Fatal("expected skewed Parquet row groups to split")
	}
	if len(splits) != 2048 {
		t.Fatalf("got %d splits, want 2048", len(splits))
	}
	if cap(splits) > 3105 {
		t.Fatalf("allocated capacity %d, want at most 3105", cap(splits))
	}

	var nextStart int64
	for i, split := range splits {
		if split.Start != nextStart || split.Length <= 0 {
			t.Fatalf("invalid split %d: start=%d length=%d, expected start=%d", i, split.Start, split.Length, nextStart)
		}
		nextStart += split.Length
	}
	if nextStart != fileSize {
		t.Fatalf("splits cover %d bytes, want %d", nextStart, fileSize)
	}
}

func BenchmarkSplitParquetScanTaskSkewed(b *testing.B) {
	const rowGroups = 4096
	const targetSize int64 = 128 << 20
	offsets := make([]int64, rowGroups)
	var fileSize int64
	for i := range offsets {
		offsets[i] = fileSize
		if i%2 == 0 {
			fileSize += 1 << 20
		} else {
			fileSize += 96 << 20
		}
	}

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		"mem://table/skewed-split-benchmark.parquet",
		iceberg.ParquetFile,
		nil, nil, nil,
		rowGroups,
		fileSize,
	)
	if err != nil {
		b.Fatal(err)
	}
	builder.SplitOffsets(offsets)
	task := FileScanTask{File: builder.Build(), Length: fileSize}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		splits, ok := splitParquetScanTask(task, targetSize)
		if !ok {
			b.Fatal("expected to split Parquet task")
		}
		splitParquetScanTaskBenchmarkSink = splits
	}
}
