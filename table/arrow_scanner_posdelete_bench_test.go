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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

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

	for _, tc := range []struct {
		name    string
		deletes set[int64]
	}{
		{name: "clean", deletes: set[int64]{numRows: {}}},
		{name: "partial", deletes: set[int64]{numRows / 2: {}}},
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
