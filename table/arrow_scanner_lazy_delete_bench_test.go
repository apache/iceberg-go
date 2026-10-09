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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
)

const (
	lazyPositionDeleteBenchmarkTaskCount     = 10_000
	lazyPositionDeleteBenchmarkFileCount     = 1_000
	lazyPositionDeleteBenchmarkTasksPerFile  = lazyPositionDeleteBenchmarkTaskCount / lazyPositionDeleteBenchmarkFileCount
	lazyPositionDeleteBenchmarkDataFileBytes = 128
)

type lazyPositionDeleteBenchmarkFixture struct {
	fs    *iceio.MemFS
	tasks []FileScanTask
}

func newLazyPositionDeleteBenchmarkFixture(b *testing.B) lazyPositionDeleteBenchmarkFixture {
	b.Helper()

	fs := iceio.NewMemFS()
	deleteFiles := make([]iceberg.DataFile, lazyPositionDeleteBenchmarkFileCount)
	for deleteIndex := range deleteFiles {
		deletePath := fmt.Sprintf("mem://benchmark/deletes/delete-%04d.parquet", deleteIndex)
		var content strings.Builder
		content.WriteByte('[')
		for taskOffset := range lazyPositionDeleteBenchmarkTasksPerFile {
			if taskOffset > 0 {
				content.WriteByte(',')
			}
			taskIndex := deleteIndex + taskOffset*lazyPositionDeleteBenchmarkFileCount
			fmt.Fprintf(&content,
				`{"file_path":"mem://benchmark/data/data-%05d.parquet","pos":0}`,
				taskIndex)
		}
		content.WriteByte(']')

		benchmarkWritePosDeleteParquet(b, fs, deletePath, content.String())
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			deletePath, iceberg.ParquetFile, nil, nil, nil,
			lazyPositionDeleteBenchmarkTasksPerFile, lazyPositionDeleteBenchmarkDataFileBytes)
		if err != nil {
			b.Fatal(err)
		}
		deleteFiles[deleteIndex] = builder.Build()
	}

	tasks := make([]FileScanTask, lazyPositionDeleteBenchmarkTaskCount)
	for taskIndex := range tasks {
		dataPath := fmt.Sprintf("mem://benchmark/data/data-%05d.parquet", taskIndex)
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
			dataPath, iceberg.ParquetFile, nil, nil, nil, 1, lazyPositionDeleteBenchmarkDataFileBytes)
		if err != nil {
			b.Fatal(err)
		}
		tasks[taskIndex] = FileScanTask{
			File:        builder.Build(),
			DeleteFiles: []iceberg.DataFile{deleteFiles[taskIndex%lazyPositionDeleteBenchmarkFileCount]},
		}
	}

	return lazyPositionDeleteBenchmarkFixture{fs: fs, tasks: tasks}
}

func benchmarkWritePosDeleteParquet(b *testing.B, fs *iceio.MemFS, path, content string) {
	b.Helper()

	record := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, content)
	defer record.Release()
	tbl := array.NewTableFromRecords(PositionalDeleteArrowSchema, []arrow.RecordBatch{record})
	defer tbl.Release()

	file, err := fs.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := pqarrow.WriteTable(tbl, file, record.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkLazyPositionDeleteLoading(b *testing.B) {
	fixture := newLazyPositionDeleteBenchmarkFixture(b)

	b.Run("eager_all_delete_files", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			deletes, err := readAllDeleteFiles(b.Context(), fixture.fs, fixture.tasks, 16)
			if err != nil {
				b.Fatal(err)
			}
			releasePerFilePosDeletes(deletes)
		}
	})

	b.Run("lazy_unread_iterator", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			loader := newLazyPositionDeleteLoader(fixture.fs, fixture.tasks)
			if len(loader.files) != lazyPositionDeleteBenchmarkFileCount {
				b.Fatalf("expected %d cached delete files, got %d",
					lazyPositionDeleteBenchmarkFileCount, len(loader.files))
			}
			loader.release()
		}
	})

	b.Run("lazy_first_task", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			loader := newLazyPositionDeleteLoader(fixture.fs, fixture.tasks)
			bitmap, err := loader.load(b.Context(), 0)
			if err != nil {
				b.Fatal(err)
			}
			if bitmap == nil || bitmap.Cardinality() != 1 {
				b.Fatalf("expected one indexed delete for the first task")
			}
			loader.release()
		}
	})
}

var positionDeleteSplitReuseBenchmarkSink int64

func BenchmarkPositionDeleteBitmapNoDeletes(b *testing.B) {
	for _, taskCount := range []int{1, 10_000} {
		b.Run(fmt.Sprintf("tasks=%d", taskCount), func(b *testing.B) {
			tasks := make([]FileScanTask, taskCount)
			dataFile := &mockDataFile{path: "mem://benchmark/data/no-deletes.parquet"}
			for i := range tasks {
				tasks[i].File = dataFile
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				loader := newLazyPositionDeleteLoader(nil, tasks)
				if loader != nil {
					b.Fatal("expected no position delete loader")
				}
				loader.release()
			}
		})
	}
}

// BenchmarkPositionDeleteBitmapUnion isolates merging already decoded delete
// files. Separate high-bit buckets make repeated copies of the growing union
// visible as the number of applicable delete files increases.
func BenchmarkPositionDeleteBitmapUnion(b *testing.B) {
	const (
		dataPath         = "mem://benchmark/data/union.parquet"
		positionsPerFile = 16_384
	)

	for _, fileCount := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			loader := &lazyPositionDeleteLoader{
				files: make(map[string]*lazyPositionDeleteFile, fileCount),
			}
			paths := make([]string, fileCount)
			for i := range paths {
				paths[i] = fmt.Sprintf("delete-%d.parquet", i)
				bitmap := dv.NewRoaringPositionBitmap()
				for position := range positionsPerFile {
					bitmap.Set(uint64(i)<<32 | uint64(position*37))
				}
				cached := &lazyPositionDeleteFile{
					bitmaps: map[string]*dv.RoaringPositionBitmap{dataPath: bitmap},
				}
				cached.once.Do(func() {})
				loader.files[paths[i]] = cached
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				bitmap, err := loader.buildIndex(b.Context(), dataPath, paths)
				if err != nil {
					b.Fatal(err)
				}
				if bitmap.Cardinality() != int64(fileCount*positionsPerFile) {
					b.Fatal("merged bitmap has unexpected cardinality")
				}
				positionDeleteSplitReuseBenchmarkSink = bitmap.Cardinality()
			}
		})
	}
}

// BenchmarkPositionDeleteBitmapCachedLoad measures the per-split lookup after
// the delete files and merged bitmap are already cached. Each operation visits
// 16 splits of the same data file without any Parquet decoding or row filtering.
func BenchmarkPositionDeleteBitmapCachedLoad(b *testing.B) {
	const (
		dataPath = "mem://benchmark/data/cached-position-delete-index.parquet"
		splits   = 16
	)

	for _, fileCount := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			deleteFiles := make([]iceberg.DataFile, fileCount)
			for i := range deleteFiles {
				deleteFiles[i] = &mockDataFile{
					path:        fmt.Sprintf("mem://benchmark/deletes/cached-index-%02d.parquet", i),
					contentType: iceberg.EntryContentPosDeletes,
				}
			}
			tasks := make([]FileScanTask, splits)
			for i := range tasks {
				tasks[i] = FileScanTask{
					File: &mockDataFile{path: dataPath}, DeleteFiles: deleteFiles,
					Start: int64(i * 128), Length: 128,
				}
			}
			loader := newLazyPositionDeleteLoader(nil, tasks)
			defer loader.release()
			for i, deleteFile := range deleteFiles {
				cached := loader.files[deleteFile.FilePath()]
				bitmap := dv.NewRoaringPositionBitmap()
				bitmap.Set(uint64(i))
				cached.bitmaps = map[string]*dv.RoaringPositionBitmap{dataPath: bitmap}
				cached.once.Do(func() {})
			}
			if _, err := loader.load(b.Context(), 0); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for taskIndex := range tasks {
					bitmap, err := loader.load(b.Context(), taskIndex)
					if err != nil {
						b.Fatal(err)
					}
					positionDeleteSplitReuseBenchmarkSink = bitmap.Cardinality()
				}
			}
		})
	}
}

func BenchmarkPositionDeleteBitmapReuseAcrossSplits(b *testing.B) {
	const deleteRows = 100_000

	deletePath := "mem://benchmark/deletes/split-reuse.parquet"
	dataPath := "mem://benchmark/data/split-reuse.parquet"
	fs := iceio.NewMemFS()

	var content strings.Builder
	content.Grow(deleteRows * 72)
	content.WriteByte('[')
	for pos := range deleteRows {
		if pos > 0 {
			content.WriteByte(',')
		}
		fmt.Fprintf(&content, `{"file_path":"%s","pos":%d}`, dataPath, pos)
	}
	content.WriteByte(']')
	benchmarkWritePosDeleteParquet(b, fs, deletePath, content.String())

	deleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, deleteRows, 128)
	if err != nil {
		b.Fatal(err)
	}
	deleteFile := deleteBuilder.Build()

	dataBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		dataPath, iceberg.ParquetFile, nil, nil, nil, deleteRows*2, 16*128)
	if err != nil {
		b.Fatal(err)
	}
	dataFile := dataBuilder.Build()
	targets := map[string]struct{}{dataPath: {}}

	for _, splitCount := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("splits=%d", splitCount), func(b *testing.B) {
			tasks := make([]FileScanTask, splitCount)
			for i := range tasks {
				tasks[i] = FileScanTask{
					File:        dataFile,
					DeleteFiles: []iceberg.DataFile{deleteFile},
					Start:       int64(i * 128),
					Length:      128,
				}
			}

			b.Run("task_local_hash_sets", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					deletes, err := readDeletesForPaths(b.Context(), fs, deleteFile, targets)
					if err != nil {
						b.Fatal(err)
					}
					positions := positionDeletes{deletes[dataPath]}
					for range splitCount {
						set, err := collectPosDeletePositions(positions)
						if err != nil {
							releasePosDeletes(deletes)
							b.Fatal(err)
						}
						positionDeleteSplitReuseBenchmarkSink = int64(len(set))
					}
					releasePosDeletes(deletes)
				}
			})

			b.Run("shared_roaring_bitmap", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					loader := newLazyPositionDeleteLoader(fs, tasks)
					for taskIndex := range tasks {
						bitmap, err := loader.load(b.Context(), taskIndex)
						if err != nil {
							loader.release()
							b.Fatal(err)
						}
						if bitmap == nil {
							loader.release()
							b.Fatal("expected position delete bitmap")
						}
						positionDeleteSplitReuseBenchmarkSink = bitmap.Cardinality()
					}
					loader.release()
				}
			})
		})
	}
}
