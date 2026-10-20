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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestPosDeleteDictionaryPathCache(t *testing.T) {
	for _, dictionarySize := range []int{3, maxPositionDeleteDictionaryCacheEntries + 1} {
		t.Run(fmt.Sprintf("dictionary-entries=%d", dictionarySize), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			pathNames := make([]string, dictionarySize)
			for i := range pathNames {
				pathNames[i] = fmt.Sprintf("other-%d.parquet", i)
			}
			pathNames[0] = "file-a.parquet"
			pathNames[1] = "file-b.parquet"
			pathNames[2] = "file-a.parquet" // Different indexes with the same value.

			dictionary := stringArray(mem, pathNames...)
			defer dictionary.Release()
			indexes := int32Array(mem, 0, 0, 1, 2, 1, 2, 0)
			defer indexes.Release()
			dictType := &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int32,
				ValueType: arrow.BinaryTypes.String,
			}
			pathArray := array.NewDictionaryArray(dictType, indexes, dictionary)
			defer pathArray.Release()
			pathCol := arrow.NewChunked(dictType, []arrow.Array{pathArray})
			defer pathCol.Release()

			posArray := int64Array(mem, 11, 12, 13, 14, 15, 16, 17)
			defer posArray.Release()
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArray})
			defer posCol.Release()

			acc := newPosDeleteAccumulator(ctx, map[string]struct{}{"file-a.parquet": {}})
			defer acc.release()
			require.NoError(t, acc.appendChunked(ctx, pathCol, posCol))
			grouped := acc.finish()
			defer releasePosDeletes(grouped)
			require.Len(t, grouped, 1)
			require.Equal(t, []int64{11, 12, 14, 16, 17}, int64Values(grouped["file-a.parquet"]))
		})
	}
}

func TestPosDeleteDictionaryPathCacheValidatesEveryPosition(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	dictionary := stringArray(mem, "file-a.parquet")
	defer dictionary.Release()
	indexes := int32Array(mem, 0, 0)
	defer indexes.Release()
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	pathArray := array.NewDictionaryArray(dictType, indexes, dictionary)
	defer pathArray.Release()
	pathCol := arrow.NewChunked(dictType, []arrow.Array{pathArray})
	defer pathCol.Release()

	posArray := int64Array(mem, 1, -2)
	defer posArray.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArray})
	defer posCol.Release()

	acc := newPosDeleteAccumulator(ctx, nil)
	defer acc.release()
	err := acc.appendChunked(ctx, pathCol, posCol)
	require.ErrorContains(t, err, "negative pos -2")
}

func TestPosDeleteDictionaryPathCacheRejectsNullIndices(t *testing.T) {
	for _, dictionarySize := range []int{1, maxPositionDeleteDictionaryCacheEntries + 1} {
		t.Run(fmt.Sprintf("dictionary-entries=%d", dictionarySize), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			paths := make([]string, dictionarySize)
			for i := range paths {
				paths[i] = fmt.Sprintf("file-%d.parquet", i)
			}
			dictionary := stringArray(mem, paths...)
			defer dictionary.Release()

			indexBuilder := array.NewInt32Builder(mem)
			indexBuilder.Append(0)
			indexBuilder.AppendNull()
			indexes := indexBuilder.NewArray()
			indexBuilder.Release()
			defer indexes.Release()

			dictType := &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int32,
				ValueType: arrow.BinaryTypes.String,
			}
			pathArray := array.NewDictionaryArray(dictType, indexes, dictionary)
			defer pathArray.Release()
			pathCol := arrow.NewChunked(dictType, []arrow.Array{pathArray})
			defer pathCol.Release()

			posArray := int64Array(mem, 1, 2)
			defer posArray.Release()
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArray})
			defer posCol.Release()

			acc := newPosDeleteAccumulator(ctx, nil)
			defer acc.release()
			err := acc.appendChunked(ctx, pathCol, posCol)
			// Column-level validation rejects null dictionary indices before either cache path runs.
			require.ErrorContains(t, err, "null file_path in position delete file")
		})
	}
}

func BenchmarkGroupPosDeletesByDictionaryIndex(b *testing.B) {
	const numRows = 262_144
	for _, numPaths := range []int{1, 16, 512, maxPositionDeleteDictionaryCacheEntries + 1} {
		b.Run(fmt.Sprintf("paths=%d", numPaths), func(b *testing.B) {
			mem := memory.DefaultAllocator
			pathNames := make([]string, numPaths)
			for i := range pathNames {
				pathNames[i] = fmt.Sprintf("s3://bucket/table/data/%05d.parquet", i)
			}
			dictionary := stringArray(mem, pathNames...)
			defer dictionary.Release()

			indexes := make([]int32, numRows)
			positions := make([]int64, numRows)
			for i := range indexes {
				indexes[i] = int32(i % numPaths)
				positions[i] = int64(i)
			}
			indexArray := int32Array(mem, indexes...)
			defer indexArray.Release()
			dictType := &arrow.DictionaryType{
				IndexType: arrow.PrimitiveTypes.Int32,
				ValueType: arrow.BinaryTypes.String,
			}
			pathArray := array.NewDictionaryArray(dictType, indexArray, dictionary)
			defer pathArray.Release()
			pathCol := arrow.NewChunked(dictType, []arrow.Array{pathArray})
			defer pathCol.Release()

			posArray := int64Array(mem, positions...)
			defer posArray.Release()
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArray})
			defer posCol.Release()

			ctx := compute.WithAllocator(b.Context(), mem)
			benchmarkGroupPosDeletesByFilePath(b, ctx, pathCol, posCol, groupPosDeletesByFilePath)
		})
	}
}
