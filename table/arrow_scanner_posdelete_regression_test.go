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
		name                  string
		filePathCol           func(memory.Allocator) (*arrow.Chunked, func())
		checkComputeAllocator bool
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
			name:                  "dictionary chunks",
			checkComputeAllocator: true,
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
			name:                  "large string dictionary chunks",
			checkComputeAllocator: true,
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.DefaultAllocator
			ctx := t.Context()
			if tc.checkComputeAllocator {
				checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
				ctx = compute.WithAllocator(ctx, checked)
				defer checked.AssertSize(t, 0)
			}

			filePathCol, releaseFilePathCol := tc.filePathCol(mem)
			defer releaseFilePathCol()
			posA := int64Array(mem, 1, 2)
			defer posA.Release()
			posB := int64Array(mem, 3, 4)
			defer posB.Release()
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posA, posB})
			defer posCol.Release()

			got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
			require.NoError(t, err)
			defer releasePosDeletes(got)

			assert.Equal(t, []int64{1, 3}, int64Values(got["file-a.parquet"]))
			assert.Equal(t, []int64{2}, int64Values(got["file-b.parquet"]))
			assert.Equal(t, []int64{4}, int64Values(got["file-c.parquet"]))
		})
	}
}

func TestGroupPosDeletesByFilePathRejectsUnsupportedFilePathLayout(t *testing.T) {
	for _, tc := range []struct {
		name        string
		filePathCol func(memory.Allocator) (*arrow.Chunked, func())
		want        string
	}{
		{
			name: "unsupported primitive",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				arr := int32Array(mem, 1)
				chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
				}
			},
			want: "unsupported file_path column type",
		},
		{
			name: "null string",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				arr := nullableStringArray(mem, "file-a.parquet", "")
				chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
				}
			},
			want: "null file_path",
		},
		{
			name: "null dictionary value",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := nullableStringArray(mem, "", "file-a.parquet")
				idx := int32Array(mem, 0)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.String,
				}
				arr := array.NewDictionaryArray(dictType, idx, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
					dict.Release()
					idx.Release()
				}
			},
			want: "null file_path dictionary value",
		},
		{
			name: "non-string dictionary",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := int32Array(mem, 1)
				idx := int32Array(mem, 0)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.PrimitiveTypes.Int32,
				}
				arr := array.NewDictionaryArray(dictType, idx, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
					dict.Release()
					idx.Release()
				}
			},
			want: "file_path column is not string",
		},
		{
			name: "string view",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				arr := stringViewArray(mem, "file-a.parquet")
				chunked := arrow.NewChunked(arrow.BinaryTypes.StringView, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
				}
			},
			want: "unsupported file_path column type",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			ctx := t.Context()
			filePathCol, releaseFilePathCol := tc.filePathCol(mem)
			defer releaseFilePathCol()
			posArr := int64Array(mem, 1)
			defer posArr.Release()
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
			defer posCol.Release()

			_, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestCollectPosDeletePositionsRejectsUnsupportedPosType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	posArr := int32Array(mem, 1, 2)
	defer posArr.Release()

	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := collectPosDeletePositions(positionDeletes{posCol})
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "unsupported pos column type")
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
	processFn := processPositionalDeletes(ctx, deletes, (&rowPositionSource{}).cursor())

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

func stringViewArray(mem memory.Allocator, values ...string) *array.StringView {
	bldr := array.NewStringViewBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewStringViewArray()
}

func nullableStringArray(mem memory.Allocator, values ...string) *array.String {
	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	for _, v := range values {
		if v == "" {
			bldr.AppendNull()
		} else {
			bldr.Append(v)
		}
	}

	return bldr.NewStringArray()
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
