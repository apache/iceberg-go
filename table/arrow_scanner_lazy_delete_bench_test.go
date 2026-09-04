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
			deletes, err := loader.load(b.Context(), fixture.tasks[0])
			if err != nil {
				b.Fatal(err)
			}
			if len(deletes) != 1 {
				b.Fatalf("expected one delete chunk for the first task, got %d", len(deletes))
			}
			loader.release()
		}
	})
}
