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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupPosDeletesByFilePathSupportsStringLayouts(t *testing.T) {
	for _, tc := range []struct {
		name        string
		filePathCol func(memory.Allocator) (*arrow.Chunked, func())
	}{
		{
			name: "string chunks",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				chunkA := stringArray(mem, "file-a.parquet", "file-b.parquet")
				chunkB := stringArray(mem, "file-a.parquet", "file-c.parquet")
				chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
				}
			},
		},
		{
			name: "large string chunks",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				chunkA := largeStringArray(mem, "file-a.parquet", "file-b.parquet")
				chunkB := largeStringArray(mem, "file-a.parquet", "file-c.parquet")
				chunked := arrow.NewChunked(arrow.BinaryTypes.LargeString, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
				}
			},
		},
		{
			name: "dictionary chunks",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := stringArray(mem, "file-a.parquet", "file-b.parquet", "file-c.parquet")
				idxA := int32Array(mem, 0, 1)
				idxB := int32Array(mem, 0, 2)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.String,
				}
				chunkA := array.NewDictionaryArray(dictType, idxA, dict)
				chunkB := array.NewDictionaryArray(dictType, idxB, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
					dict.Release()
					idxA.Release()
					idxB.Release()
				}
			},
		},
		{
			name: "large string dictionary chunks",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := largeStringArray(mem, "file-a.parquet", "file-b.parquet", "file-c.parquet")
				idxA := int32Array(mem, 0, 1)
				idxB := int32Array(mem, 0, 2)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.LargeString,
				}
				chunkA := array.NewDictionaryArray(dictType, idxA, dict)
				chunkB := array.NewDictionaryArray(dictType, idxB, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
					dict.Release()
					idxA.Release()
					idxB.Release()
				}
			},
		},
		{
			name: "distinct dictionaries per chunk",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				// Each chunk carries its own dictionary with a different
				// vocabulary and order — the shape before UnifyTableDicts runs.
				// The Go-level dedup must not assume a unified dictionary.
				dictA := stringArray(mem, "file-a.parquet", "file-b.parquet")
				dictB := stringArray(mem, "file-c.parquet", "file-a.parquet")
				idxA := int32Array(mem, 0, 1)
				idxB := int32Array(mem, 1, 0)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.String,
				}
				chunkA := array.NewDictionaryArray(dictType, idxA, dictA)
				chunkB := array.NewDictionaryArray(dictType, idxB, dictB)
				chunked := arrow.NewChunked(dictType, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
					dictA.Release()
					dictB.Release()
					idxA.Release()
					idxB.Release()
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A checked allocator turns any leak in groupPosDeletesByFilePath
			// (e.g. an unreleased equal/filter mask) into a test failure.
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			// Registered first so it runs last (t.Cleanup is LIFO), after every
			// release below. Releases go through t.Cleanup so a require failure
			// can't skip them.
			t.Cleanup(func() { mem.AssertSize(t, 0) })
			ctx := compute.WithAllocator(t.Context(), mem)

			filePathCol, releaseFilePathCol := tc.filePathCol(mem)
			t.Cleanup(releaseFilePathCol)

			posA := int64Array(mem, 1, 2)
			posB := int64Array(mem, 3, 4)
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posA, posB})
			t.Cleanup(func() {
				posCol.Release()
				posA.Release()
				posB.Release()
			})

			got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
			require.NoError(t, err)
			t.Cleanup(func() { releasePosDeletes(got) })

			assert.Equal(t, []int64{1, 3}, int64Values(got["file-a.parquet"]))
			assert.Equal(t, []int64{2}, int64Values(got["file-b.parquet"]))
			assert.Equal(t, []int64{4}, int64Values(got["file-c.parquet"]))
		})
	}
}

func TestGroupPosDeletesByFilePathRejectsUnsupportedFilePathLayout(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	filePathArr := int32Array(mem, 1)
	filePathCol := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{filePathArr})
	t.Cleanup(func() {
		filePathCol.Release()
		filePathArr.Release()
	})

	posArr := int64Array(mem, 1)
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	t.Cleanup(func() {
		posCol.Release()
		posArr.Release()
	})

	_, err := groupPosDeletesByFilePath(t.Context(), filePathCol, posCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "unsupported file_path column type")
}

func TestGroupPosDeletesByFilePathRejectsNullFilePath(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	bldr.Append("file-a.parquet")
	bldr.AppendNull()
	filePathArr := bldr.NewStringArray()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	t.Cleanup(func() {
		filePathCol.Release()
		filePathArr.Release()
	})

	posArr := int64Array(mem, 1, 2)
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	t.Cleanup(func() {
		posCol.Release()
		posArr.Release()
	})

	_, err := groupPosDeletesByFilePath(t.Context(), filePathCol, posCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "null file_path")
}

func TestGroupPosDeletesByFilePathRejectsNullDictionaryValue(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	// The index array is fully valid, but row 1 points at a null entry in the
	// dictionary value array. This exercises the dict.IsNull(GetValueIndex(i))
	// guard, which a plain null-index check would miss.
	dbldr := array.NewStringBuilder(mem)
	defer dbldr.Release()
	dbldr.Append("file-a.parquet")
	dbldr.AppendNull()
	dictVals := dbldr.NewStringArray()
	idx := int32Array(mem, 0, 1)
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	chunk := array.NewDictionaryArray(dictType, idx, dictVals)
	filePathCol := arrow.NewChunked(dictType, []arrow.Array{chunk})
	t.Cleanup(func() {
		filePathCol.Release()
		chunk.Release()
		dictVals.Release()
		idx.Release()
	})

	posArr := int64Array(mem, 1, 2)
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	t.Cleanup(func() {
		posCol.Release()
		posArr.Release()
	})

	_, err := groupPosDeletesByFilePath(t.Context(), filePathCol, posCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "null file_path dictionary value")
}

// TestProcessPositionalDeletesAcrossBatches is the regression net for the
// positional-delete index bug: processPositionalDeletes applies deletes one Arrow
// batch at a time, but the surviving-row indices must index into the *current
// batch*, not into the whole file. combinePositionalDeletes therefore has to
// rebase the global file positions [start, end) to batch-local coordinates.
//
// Before the rebase, the second and later batches of a single data file passed
// indices >= the batch length into compute.Take and the scan failed with
// "index error: N out of bounds" (N being a multiple of the parquet batch size,
// e.g. 131072). This test feeds two consecutive batches with a delete located in
// the *second* batch — exactly the case the old code got wrong.
func TestProcessPositionalDeletesAcrossBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Two batches: global positions 0,1,2 then 3,4. Delete global position 3
	// (the first row of the SECOND batch). The survivors are 0,1,2 and 4.
	batches := []arrow.RecordBatch{
		mustLoadRecordBatchFromJSON(schema, `[{"val": 10}, {"val": 11}, {"val": 12}]`),
		mustLoadRecordBatchFromJSON(schema, `[{"val": 13}, {"val": 14}]`),
	}
	expected := []string{
		`[{"val":10},{"val":11},{"val":12}]`,
		`[{"val":14}]`,
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	// processPositionalDeletes owns (releases) each input batch it is handed.
	deletes := set[int64]{3: {}}
	processFn := processPositionalDeletes(ctx, deletes)

	for i, b := range batches {
		out, err := processFn(b)
		require.NoErrorf(t, err, "batch %d must not return an out-of-bounds error", i)

		gotJSON, err := out.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEq(t, expected[i], string(gotJSON))

		out.Release()
	}
}

func stringArray(mem memory.Allocator, values ...string) *array.String {
	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewStringArray()
}

func largeStringArray(mem memory.Allocator, values ...string) *array.LargeString {
	bldr := array.NewLargeStringBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewLargeStringArray()
}

func int32Array(mem memory.Allocator, values ...int32) *array.Int32 {
	bldr := array.NewInt32Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewInt32Array()
}

func int64Array(mem memory.Allocator, values ...int64) *array.Int64 {
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewInt64Array()
}

func int64Values(chunked *arrow.Chunked) []int64 {
	var out []int64
	for _, chunk := range chunked.Chunks() {
		out = append(out, chunk.(*array.Int64).Int64Values()...)
	}

	return out
}
