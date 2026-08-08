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
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkGroupPosDeletesByFilePath(b *testing.B) {
	const numRows = 1_000_000
	chunkConfigs := []struct {
		name           string
		filePathChunks int
		posChunks      int
	}{
		{name: "1x1", filePathChunks: 1, posChunks: 1},
		{name: "64x65", filePathChunks: 64, posChunks: 65},
	}

	for _, numPaths := range []int{1, 10, 100, 1_000} {
		for _, chunks := range chunkConfigs {
			b.Run(fmt.Sprintf("rows=%d/paths=%d/chunks=%s", numRows, numPaths, chunks.name), func(b *testing.B) {
				mem := memory.DefaultAllocator
				pathNames := make([]string, numPaths)
				for i := range numPaths {
					pathNames[i] = fmt.Sprintf("file-%04d.parquet", i)
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
				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					deletes, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
					if err != nil {
						b.Fatal(err)
					}
					releasePosDeletes(deletes)
				}
			})
		}
	}
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
