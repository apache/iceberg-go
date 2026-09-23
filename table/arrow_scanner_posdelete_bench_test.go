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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

func BenchmarkReadDeletesWithFilePathFilter(b *testing.B) {
	const (
		numPaths    = 1_000
		rowsPerPath = 1_024
	)

	memFS, deleteFile, pathNames := benchmarkPositionDeleteFileWithPaths(b, numPaths, rowsPerPath)
	ctx := tblutils.WithTableProperties(context.Background(), iceberg.Properties{
		ParquetBatchSizeKey: "65536",
	})

	for _, targetCount := range []int{1, 10, 100, 1_000} {
		b.Run(fmt.Sprintf("targets=%d", targetCount), func(b *testing.B) {
			targets := make(map[string]struct{}, targetCount)
			for _, path := range pathNames[:targetCount] {
				targets[path] = struct{}{}
			}

			b.Run("all paths", func(b *testing.B) {
				benchmarkReadDeletesWithFilePathFilter(b, ctx, memFS, deleteFile, nil)
			})
			b.Run("target paths", func(b *testing.B) {
				benchmarkReadDeletesWithFilePathFilter(b, ctx, memFS, deleteFile, targets)
			})
		})
	}
}

func benchmarkReadDeletesWithFilePathFilter(
	b *testing.B,
	ctx context.Context,
	fs iceio.IO,
	dataFile iceberg.DataFile,
	targets map[string]struct{},
) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		deletes, err := readDeletesForPaths(ctx, fs, dataFile, targets)
		if err != nil {
			b.Fatal(err)
		}
		releasePosDeletes(deletes)
	}
}

func benchmarkPositionDeleteFileWithPaths(b *testing.B, numPaths, rowsPerPath int) (*iceio.MemFS, iceberg.DataFile, []string) {
	b.Helper()

	mem := memory.DefaultAllocator
	pathNames := make([]string, numPaths)
	for i := range numPaths {
		pathNames[i] = fmt.Sprintf("mem://bucket/data/data-%04d.parquet", i)
	}

	pathBuilder := array.NewStringBuilder(mem)
	posBuilder := array.NewInt64Builder(mem)
	for pathIdx, path := range pathNames {
		for pos := range rowsPerPath {
			pathBuilder.Append(path)
			posBuilder.Append(int64(pathIdx*rowsPerPath + pos))
		}
	}
	paths := pathBuilder.NewStringArray()
	pathBuilder.Release()
	positions := posBuilder.NewInt64Array()
	posBuilder.Release()
	record := array.NewRecordBatch(PositionalDeleteArrowSchema,
		[]arrow.Array{paths, positions}, int64(numPaths*rowsPerPath))
	paths.Release()
	positions.Release()
	defer record.Release()
	tbl := array.NewTableFromRecords(PositionalDeleteArrowSchema, []arrow.RecordBatch{record})
	defer tbl.Release()

	deletePath := "mem://bucket/deletes/file-path-filter-benchmark.parquet"
	memFS := iceio.NewMemFS()
	fw, err := memFS.Create(deletePath)
	if err != nil {
		b.Fatal(err)
	}
	if err := pqarrow.WriteTable(tbl, fw, int64(rowsPerPath),
		parquet.NewWriterProperties(
			parquet.WithStats(true),
			parquet.WithMaxRowGroupLength(int64(rowsPerPath)),
		),
		pqarrow.DefaultWriterProps()); err != nil {
		_ = fw.Close()
		b.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		b.Fatal(err)
	}

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil,
		int64(numPaths*rowsPerPath), 128)
	if err != nil {
		b.Fatal(err)
	}

	return memFS, builder.Build(), pathNames
}

func BenchmarkReadDeletesProjected(b *testing.B) {
	for _, numRows := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			memFS, dataFile := benchmarkPositionDeleteFile(b, numRows)
			ctx := tblutils.WithTableProperties(context.Background(), iceberg.Properties{
				ParquetBatchSizeKey: "65536",
			})

			b.Run("before", func(b *testing.B) {
				benchmarkReadDeletes(b, ctx, memFS, dataFile, readDeletesBefore)
			})
			b.Run("after", func(b *testing.B) {
				benchmarkReadDeletes(b, ctx, memFS, dataFile, readDeletes)
			})
		})
	}
}

func benchmarkReadDeletes(
	b *testing.B,
	ctx context.Context,
	memFS *iceio.MemFS,
	dataFile iceberg.DataFile,
	read func(context.Context, iceio.IO, iceberg.DataFile) (map[string]*arrow.Chunked, error),
) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		deletes, err := read(ctx, memFS, dataFile)
		if err != nil {
			b.Fatal(err)
		}
		releasePosDeletes(deletes)
	}
}

func benchmarkPositionDeleteFile(b *testing.B, numRows int) (*iceio.MemFS, iceberg.DataFile) {
	b.Helper()

	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "unused", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	pathBuilder := array.NewStringBuilder(mem)
	posBuilder := array.NewInt64Builder(mem)
	unusedBuilder := array.NewStringBuilder(mem)
	unusedValue := strings.Repeat("unused-value-", 16)
	for i := range numRows {
		pathBuilder.Append("mem://bucket/data/data.parquet")
		posBuilder.Append(int64(i))
		unusedBuilder.Append(unusedValue)
	}
	paths := pathBuilder.NewStringArray()
	pathBuilder.Release()
	positions := posBuilder.NewInt64Array()
	posBuilder.Release()
	unused := unusedBuilder.NewStringArray()
	unusedBuilder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{paths, positions, unused}, int64(numRows))
	paths.Release()
	positions.Release()
	unused.Release()
	defer record.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
	defer tbl.Release()

	deletePath := "mem://bucket/deletes/benchmark.parquet"
	memFS := iceio.NewMemFS()
	fw, err := memFS.Create(deletePath)
	if err != nil {
		b.Fatal(err)
	}
	if err := pqarrow.WriteTable(tbl, fw, int64(numRows),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()); err != nil {
		_ = fw.Close()
		b.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		b.Fatal(err)
	}

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, int64(numRows), 128)
	if err != nil {
		b.Fatal(err)
	}

	return memFS, builder.Build()
}

func readDeletesBefore(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (_ map[string]*arrow.Chunked, err error) {
	src, err := tblutils.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, err
	}
	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdr.Close() }()

	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()
	tbl, err = array.UnifyTableDicts(compute.GetAllocator(ctx), tbl)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	filePathIndex, posIndex, err := positionDeleteColumnIndices(tbl.Schema())
	if err != nil {
		return nil, err
	}

	return groupPosDeletesByFilePath(ctx, tbl.Column(filePathIndex).Data(), tbl.Column(posIndex).Data())
}

func BenchmarkGroupPosDeletesByFilePath(b *testing.B) {
	const numRows = 1_000_000
	const maxBeforePaths = 1_000
	chunkConfigs := []struct {
		name           string
		filePathChunks int
		posChunks      int
	}{
		{name: "1x1", filePathChunks: 1, posChunks: 1},
		{name: "64x65", filePathChunks: 64, posChunks: 65},
	}

	for _, numPaths := range []int{1, 10, 100, 1_000, 10_000} {
		for _, chunks := range chunkConfigs {
			b.Run(fmt.Sprintf("rows=%d/paths=%d/chunks=%s", numRows, numPaths, chunks.name), func(b *testing.B) {
				mem := memory.DefaultAllocator
				pathNames := make([]string, numPaths)
				for i := range numPaths {
					pathNames[i] = fmt.Sprintf("file-%05d.parquet", i)
				}

				filePaths := make([]string, numRows)
				positions := make([]int64, numRows)
				for i := range numRows {
					filePaths[i] = pathNames[i%numPaths]
					positions[i] = int64(i)
				}

				filePathChunks := makeStringChunks(mem, filePaths, chunks.filePathChunks)
				filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, filePathChunks)
				defer func() {
					filePathCol.Release()
					for _, chunk := range filePathChunks {
						chunk.Release()
					}
				}()

				posChunks := makeInt64Chunks(mem, positions, chunks.posChunks)
				posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, posChunks)
				defer func() {
					posCol.Release()
					for _, chunk := range posChunks {
						chunk.Release()
					}
				}()

				ctx := compute.WithAllocator(context.Background(), mem)
				b.Run("before", func(b *testing.B) {
					if numPaths > maxBeforePaths {
						b.Skipf("the old implementation would scan about %d rows per iteration", numRows*numPaths)
					}
					benchmarkGroupPosDeletesByFilePath(b, ctx, filePathCol, posCol, groupPosDeletesByFilePathBefore)
				})
				b.Run("after", func(b *testing.B) {
					benchmarkGroupPosDeletesByFilePath(b, ctx, filePathCol, posCol, groupPosDeletesByFilePath)
				})
			})
		}
	}
}

func benchmarkGroupPosDeletesByFilePath(
	b *testing.B,
	ctx context.Context,
	filePathCol, posCol *arrow.Chunked,
	group func(context.Context, *arrow.Chunked, *arrow.Chunked) (map[string]*arrow.Chunked, error),
) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		deletes, err := group(ctx, filePathCol, posCol)
		if err != nil {
			b.Fatal(err)
		}
		releasePosDeletes(deletes)
	}
}

// groupPosDeletesByFilePathBefore is the scan-per-path implementation that
// was replaced by groupPosDeletesByFilePath. Keep it in the benchmark so
// before/after numbers remain reproducible from a single checkout.
func groupPosDeletesByFilePathBefore(ctx context.Context, filePathCol, posCol *arrow.Chunked) (map[string]*arrow.Chunked, error) {
	filePaths := compute.NewDatum(filePathCol)
	unique, err := compute.Unique(ctx, filePaths)
	filePaths.Release()
	if err != nil {
		return nil, err
	}

	result, ok := unique.(*compute.ArrayDatum)
	if !ok {
		unique.Release()

		return nil, fmt.Errorf("unique file_path result is %s", unique.Kind())
	}

	uniquePaths := result.MakeArray()
	unique.Release()
	defer uniquePaths.Release()

	paths, err := filePathValues(uniquePaths)
	if err != nil {
		return nil, err
	}

	results := make(map[string]*arrow.Chunked, uniquePaths.Len())
	for i := range uniquePaths.Len() {
		sc, err := scalar.GetScalar(uniquePaths, i)
		if err != nil {
			releasePosDeletes(results)

			return nil, err
		}
		scDatum := compute.NewDatum(sc)
		if releasable, ok := sc.(scalar.Releasable); ok {
			releasable.Release()
		}

		mask, err := compute.CallFunction(ctx, "equal", nil,
			compute.NewDatumWithoutOwning(filePathCol), scDatum)
		scDatum.Release()
		if err != nil {
			releasePosDeletes(results)

			return nil, err
		}

		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(posCol),
			mask, *compute.DefaultFilterOptions())
		mask.Release()
		if err != nil {
			releasePosDeletes(results)

			return nil, err
		}

		filteredChunked, ok := filtered.(*compute.ChunkedDatum)
		if !ok {
			filtered.Release()
			releasePosDeletes(results)

			return nil, fmt.Errorf("filtered position delete result is %s", filtered.Kind())
		}
		filteredChunked.Value.Retain()
		results[paths.Value(i)] = filteredChunked.Value
		filtered.Release()
	}

	return results, nil
}

func makeStringChunks(mem memory.Allocator, values []string, numChunks int) []arrow.Array {
	boundaries := benchmarkChunkBoundaries(len(values), numChunks)
	chunks := make([]arrow.Array, numChunks)
	for i := range numChunks {
		chunks[i] = stringArray(mem, values[boundaries[i]:boundaries[i+1]]...)
	}

	return chunks
}

func makeInt64Chunks(mem memory.Allocator, values []int64, numChunks int) []arrow.Array {
	boundaries := benchmarkChunkBoundaries(len(values), numChunks)
	chunks := make([]arrow.Array, numChunks)
	for i := range numChunks {
		chunks[i] = int64Array(mem, values[boundaries[i]:boundaries[i+1]]...)
	}

	return chunks
}

func benchmarkChunkBoundaries(length, numChunks int) []int {
	boundaries := make([]int, numChunks+1)
	for i := range boundaries {
		boundaries[i] = i * length / numChunks
	}

	return boundaries
}

func BenchmarkProcessPositionalDeletes(b *testing.B) {
	const numRows = 64 * 1024
	deleteHeavy := benchmarkPositionalDeleteSet(numRows, numRows-1)
	allDeleted := benchmarkPositionalDeleteSet(numRows)

	for _, tc := range []struct {
		name    string
		deletes set[int64]
	}{
		{name: "clean", deletes: set[int64]{numRows: {}}},
		{name: "partial", deletes: set[int64]{numRows / 2: {}}},
		{name: "delete-heavy", deletes: deleteHeavy},
		{name: "all-deleted", deletes: allDeleted},
	} {
		b.Run(tc.name, func(b *testing.B) {
			batch := benchmarkPositionalDeleteBatch(memory.DefaultAllocator, numRows)
			defer batch.Release()

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				process := processPositionalDeletes(ctx, tc.deletes, (&rowPositionSource{}).cursor())
				batch.Retain()
				out, err := process(batch)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

var collectPosDeletePositionsBenchmarkSink int

func BenchmarkCollectPosDeletePositions(b *testing.B) {
	for _, tc := range []struct {
		name            string
		numRows         int
		numChunks       int
		uniquePositions int
	}{
		{name: "rows=1K/chunks=1/unique", numRows: 1_000, numChunks: 1, uniquePositions: 1_000},
		{name: "rows=100K/chunks=4/unique", numRows: 100_000, numChunks: 4, uniquePositions: 100_000},
		{name: "rows=1M/chunks=16/unique", numRows: 1_000_000, numChunks: 16, uniquePositions: 1_000_000},
		{name: "rows=1K/chunks=1/duplicate", numRows: 1_000, numChunks: 1, uniquePositions: 1},
		{name: "rows=100K/chunks=4/duplicate", numRows: 100_000, numChunks: 4, uniquePositions: 1},
		{name: "rows=1M/chunks=16/duplicate", numRows: 1_000_000, numChunks: 16, uniquePositions: 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			values := make([]int64, tc.numRows)
			for i := range values {
				values[i] = int64(i % tc.uniquePositions)
			}
			chunks := makeInt64Chunks(memory.DefaultAllocator, values, tc.numChunks)
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
			defer func() {
				posCol.Release()
				for _, chunk := range chunks {
					chunk.Release()
				}
			}()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				deletes, err := collectPosDeletePositions(positionDeletes{posCol})
				if err != nil {
					b.Fatal(err)
				}
				collectPosDeletePositionsBenchmarkSink = len(deletes)
			}
		})
	}
}

func benchmarkPositionalDeleteSet(numRows int64, survivors ...int64) set[int64] {
	deletes := make(set[int64], int(numRows))
	for i := range numRows {
		deletes[i] = struct{}{}
	}
	for _, pos := range survivors {
		delete(deletes, pos)
	}

	return deletes
}

func benchmarkPositionalDeleteBatch(mem memory.Allocator, numRows int64) arrow.RecordBatch {
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.Reserve(int(numRows))
	for i := range numRows {
		bldr.Append(i)
	}

	values := bldr.NewArray()
	defer values.Release()

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
	}}, nil)

	return array.NewRecordBatch(schema, []arrow.Array{values}, numRows)
}
