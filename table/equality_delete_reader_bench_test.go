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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func BenchmarkReadEqualityDeleteFile(b *testing.B) {
	for _, numRows := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			tableSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
				iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String},
			)
			arrowSchema, err := SchemaToArrowSchema(tableSchema, nil, true, false)
			if err != nil {
				b.Fatal(err)
			}

			path := filepath.Join(b.TempDir(), "equality-delete.parquet")
			mem := memory.DefaultAllocator
			builder := array.NewRecordBuilder(mem, arrowSchema)
			idBuilder := builder.Field(0).(*array.Int64Builder)
			nameBuilder := builder.Field(1).(*array.StringBuilder)
			payloadBuilder := builder.Field(2).(*array.StringBuilder)
			payload := strings.Repeat("payload-", 64)
			for i := range numRows {
				idBuilder.Append(int64(i))
				nameBuilder.Append(fmt.Sprintf("user-%08d", i))
				payloadBuilder.Append(payload)
			}
			rec := builder.NewRecordBatch()
			builder.Release()
			defer rec.Release()

			tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
			defer tbl.Release()
			file, err := (iceio.LocalFS{}).Create(path)
			if err != nil {
				b.Fatal(err)
			}
			if err := pqarrow.WriteTable(tbl, file, 16_384,
				parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}

			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			dataFileBuilder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
				path, iceberg.ParquetFile, nil, nil, nil, int64(numRows), info.Size())
			if err != nil {
				b.Fatal(err)
			}
			dataFileBuilder.EqualityFieldIDs([]int{1, 2})
			dataFile := dataFileBuilder.Build()

			for _, benchmark := range []struct {
				name string
				read func(context.Context, iceio.IO, *iceberg.Schema, iceberg.NameMapping, iceberg.DataFile, []int) (set[string], []string, error)
			}{
				{name: "streamed-projection", read: readEqualityDeleteFile},
				{name: "materialized-table", read: readEqualityDeleteFileMaterialized},
			} {
				b.Run(benchmark.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						keys, _, err := benchmark.read(context.Background(), iceio.LocalFS{}, tableSchema, nil, dataFile, []int{1, 2})
						if err != nil {
							b.Fatal(err)
						}
						if len(keys) != numRows {
							b.Fatalf("got %d keys, want %d", len(keys), numRows)
						}
					}
				})
			}
		})
	}
}

var equalityDeleteLoadingBenchmarkSink int

type equalityDeleteLoadingBenchmarkInput struct {
	fs          *countingEqualityDeleteOpenFS
	tableSchema *iceberg.Schema
	tasks       []FileScanTask
	deleteFiles int
}

func BenchmarkLazyEqualityDeleteLoading(b *testing.B) {
	input := newEqualityDeleteLoadingBenchmarkInput(b)

	b.Run("eager/all_tasks", func(b *testing.B) {
		input.fs.opens.Store(0)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			perTask, err := readAllEqualityDeleteFiles(
				b.Context(), input.fs, input.tableSchema, nil, input.tasks, 16)
			if err != nil {
				b.Fatal(err)
			}

			totalKeys := 0
			for _, sets := range perTask {
				for _, deleteSet := range sets {
					totalKeys += len(deleteSet.keys)
				}
			}
			equalityDeleteLoadingBenchmarkSink = totalKeys
		}
		b.StopTimer()
		b.ReportMetric(float64(input.deleteFiles), "delete_files/op")
		b.ReportMetric(float64(input.fs.opens.Load())/float64(b.N), "opens/op")
	})

	for _, benchmark := range []struct {
		name      string
		taskCount int
	}{
		{name: "lazy/unread", taskCount: 0},
		{name: "lazy/first_task", taskCount: 1},
		{name: "lazy/ten_tasks", taskCount: 10},
		{name: "lazy/full_scan", taskCount: len(input.tasks)},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			input.fs.opens.Store(0)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				loader, err := newLazyEqualityDeleteLoader(
					input.fs, input.tableSchema, nil, input.tasks)
				if err != nil {
					b.Fatal(err)
				}

				totalKeys, err := loadEqualityDeleteTasksConcurrently(
					b.Context(), loader, input.tasks[:benchmark.taskCount], 16)
				if err != nil {
					b.Fatal(err)
				}
				equalityDeleteLoadingBenchmarkSink = totalKeys
			}
			b.StopTimer()
			b.ReportMetric(float64(input.deleteFiles), "delete_files/op")
			b.ReportMetric(float64(input.fs.opens.Load())/float64(b.N), "opens/op")
		})
	}
}

func loadEqualityDeleteTasksConcurrently(
	ctx context.Context,
	loader *lazyEqualityDeleteLoader,
	tasks []FileScanTask,
	concurrency int,
) (int, error) {
	if len(tasks) == 0 {
		return 0, nil
	}

	taskChan := make(chan FileScanTask, len(tasks))
	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)
	totals := make(chan int, len(tasks))
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	workerCount := min(concurrency, len(tasks))
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for task := range taskChan {
				sets, err := loader.load(ctx, task)
				if err != nil {
					select {
					case errs <- err:
					default:
					}

					return
				}

				total := 0
				for _, deleteSet := range sets {
					total += len(deleteSet.keys)
				}
				totals <- total
			}
		}()
	}
	wg.Wait()
	close(totals)

	total := 0
	for count := range totals {
		total += count
	}

	select {
	case err := <-errs:
		return total, err
	default:
		return total, nil
	}
}

func newEqualityDeleteLoadingBenchmarkInput(b *testing.B) equalityDeleteLoadingBenchmarkInput {
	b.Helper()

	const (
		deleteFileCount = 1_000
		taskCount       = 10_000
	)
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	fs := &countingEqualityDeleteOpenFS{MemFS: iceio.NewMemFS()}
	firstPath := "mem://benchmark-lazy-equality/delete-0000.parquet"
	writeEqualityDeleteParquetToMemFS(b, fs.MemFS, firstPath,
		`[{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}, {"id": 6}, {"id": 7}, {"id": 8}, {"id": 9}]`)
	file, err := fs.MemFS.Open(firstPath)
	if err != nil {
		b.Fatal(err)
	}
	contents, err := io.ReadAll(file)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		b.Fatal(err)
	}

	deleteFiles := make([]iceberg.DataFile, deleteFileCount)
	for i := range deleteFileCount {
		path := fmt.Sprintf("mem://benchmark-lazy-equality/delete-%04d.parquet", i)
		if i > 0 {
			if err := fs.MemFS.WriteFile(path, contents); err != nil {
				b.Fatal(err)
			}
		}
		deleteFiles[i] = newEqualityDeleteSetAssemblyTestFile(b, path, []int{1})
	}

	tasks := make([]FileScanTask, taskCount)
	for i := range tasks {
		tasks[i] = FileScanTask{
			EqualityDeleteFiles: []iceberg.DataFile{deleteFiles[i%deleteFileCount]},
		}
	}

	return equalityDeleteLoadingBenchmarkInput{
		fs:          fs,
		tableSchema: tableSchema,
		tasks:       tasks,
		deleteFiles: deleteFileCount,
	}
}

func benchEqDeletes(
	b *testing.B,
	buildRec func(memory.Allocator, int) arrow.RecordBatch,
	buildDel func(int) *equalityDeleteSet,
	fileSchema *iceberg.Schema,
) {
	b.Helper()
	benchEqDeletesForFile(b, buildRec, buildDel, fileSchema)
}

func benchEqDeletesForFile(
	b *testing.B,
	buildRec func(memory.Allocator, int) arrow.RecordBatch,
	buildDel func(int) *equalityDeleteSet,
	fileSchema *iceberg.Schema,
) {
	b.Helper()

	dataRows := []int{1_000, 100_000, 1_000_000}
	deleteRows := []int{10, 100, 10_000}

	for _, nData := range dataRows {
		for _, nDel := range deleteRows {
			if nDel > nData {
				continue
			}

			b.Run(fmt.Sprintf("rows=%d/deletes=%d", nData, nDel), func(b *testing.B) {
				mem := memory.NewGoAllocator()
				ctx := compute.WithAllocator(context.Background(), mem)
				rec := buildRec(mem, nData)
				defer rec.Release()

				delSets := []*equalityDeleteSet{buildDel(nDel)}
				filterFn, err := processEqualityDeletesColumnarForFile(ctx, delSets, fileSchema, "bench.parquet")
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for range b.N {
					rec.Retain()
					result, err := filterFn(rec)
					if err != nil {
						b.Fatal(err)
					}

					result.Release()
				}
			})
		}
	}
}

func benchIntFileSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.Int64},
	)
}

func benchStringFileSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
}

func buildBenchRecordInt(mem memory.Allocator, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	catBldr := bldr.Field(1).(*array.Int64Builder)

	for i := range numRows {
		idBldr.Append(int64(i))
		catBldr.Append(int64(i % 100))
	}

	return bldr.NewRecordBatch()
}

func buildBenchDeleteSetInt(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := range numDeletes {
		buf.Reset()
		buf.WriteByte(1)
		_ = binary.Write(&buf, binary.BigEndian, int64(i*3))
		buf.WriteByte(1)
		_ = binary.Write(&buf, binary.BigEndian, int64((i*3)%100))
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{
		keys:     keys,
		fieldIDs: []int{1, 2},
		colNames: []string{"id", "category"},
	}
}

func buildBenchRecordString(mem memory.Allocator, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	nameBldr := bldr.Field(1).(*array.StringBuilder)

	for i := range numRows {
		idBldr.Append(int64(i))
		nameBldr.Append(fmt.Sprintf("user-%08d", i))
	}

	return bldr.NewRecordBatch()
}

func buildBenchDeleteSetString(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := range numDeletes {
		buf.Reset()
		buf.WriteByte(1)
		_ = binary.Write(&buf, binary.BigEndian, int64(i*3))
		buf.WriteByte(1)
		s := fmt.Sprintf("user-%08d", i*3)
		_ = binary.Write(&buf, binary.BigEndian, int32(len(s)))
		buf.WriteString(s)
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{
		keys:     keys,
		fieldIDs: []int{1, 2},
		colNames: []string{"id", "name"},
	}
}

func buildBenchDeleteSetIntNoMatch(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := range numDeletes {
		buf.Reset()
		buf.WriteByte(1)
		_ = binary.Write(&buf, binary.BigEndian, int64(i*3))
		buf.WriteByte(1)
		_ = binary.Write(&buf, binary.BigEndian, int64((i*3+1)%100))
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{
		keys:     keys,
		fieldIDs: []int{1, 2},
		colNames: []string{"id", "category"},
	}
}

func buildBenchDeleteSetStringNoMatch(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := range numDeletes {
		buf.Reset()
		buf.WriteByte(1)
		_ = binary.Write(&buf, binary.BigEndian, int64(i*3))
		buf.WriteByte(1)
		name := fmt.Sprintf("user-%08d", i*3+1)
		_ = binary.Write(&buf, binary.BigEndian, int32(len(name)))
		buf.WriteString(name)
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{
		keys:     keys,
		fieldIDs: []int{1, 2},
		colNames: []string{"id", "name"},
	}
}

func BenchmarkProcessEqualityDeletesInt(b *testing.B) {
	benchEqDeletes(b, buildBenchRecordInt, buildBenchDeleteSetInt, benchIntFileSchema())
}

func BenchmarkProcessEqualityDeletesString(b *testing.B) {
	benchEqDeletes(b, buildBenchRecordString, buildBenchDeleteSetString, benchStringFileSchema())
}

func BenchmarkProcessEqualityDeletesNoMatchInt(b *testing.B) {
	benchEqDeletesForFile(b, buildBenchRecordInt, buildBenchDeleteSetIntNoMatch, benchIntFileSchema())
}

func BenchmarkProcessEqualityDeletesNoMatchString(b *testing.B) {
	benchEqDeletesForFile(b, buildBenchRecordString, buildBenchDeleteSetStringNoMatch, benchStringFileSchema())
}
