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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnrichRecordsWithPosDeleteFields(t *testing.T) {
	testSchema := arrow.NewSchema([]arrow.Field{
		{Name: "first_name", Type: &arrow.StringType{}, Nullable: false},
		{Name: "last_name", Type: &arrow.StringType{}, Nullable: false},
		{Name: "age", Type: &arrow.Int32Type{}, Nullable: true},
	}, nil)
	schemaWithPosDelete := arrow.NewSchema(append(testSchema.Fields(),
		arrow.Field{Name: "file_path", Type: &arrow.StringType{}, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(2147483546)})},
		arrow.Field{Name: "pos", Type: &arrow.Int64Type{}, Nullable: false, Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(2147483545)})},
	), nil)

	testCases := []struct {
		name            string
		inputBatches    []arrow.RecordBatch
		expectedOutputs []arrow.RecordBatch
	}{
		{
			name:            "one empty record batch",
			inputBatches:    []arrow.RecordBatch{mustLoadRecordBatchFromJSON(testSchema, `[]`)},
			expectedOutputs: []arrow.RecordBatch{mustLoadRecordBatchFromJSON(schemaWithPosDelete, `[]`)},
		},
		{
			name:            "batch of one",
			inputBatches:    []arrow.RecordBatch{mustLoadRecordBatchFromJSON(testSchema, `[{"first_name": "alan", "last_name": "gopher", "age": 7}]`)},
			expectedOutputs: []arrow.RecordBatch{mustLoadRecordBatchFromJSON(schemaWithPosDelete, `[{"first_name": "alan", "last_name": "gopher", "age": 7, "file_path": "file://test_path.parquet", "pos": 0}]`)},
		},
		{
			name: "batch of many",
			inputBatches: []arrow.RecordBatch{mustLoadRecordBatchFromJSON(testSchema, `[{"first_name": "alan", "last_name": "gopher", "age": 7},
{"first_name": "steve", "last_name": "gopher", "age": 5},
{"first_name": "dead", "last_name": "gopher", "age": 95}]`)},
			expectedOutputs: []arrow.RecordBatch{mustLoadRecordBatchFromJSON(schemaWithPosDelete, `[{"first_name": "alan", "last_name": "gopher", "age": 7, "file_path": "file://test_path.parquet", "pos": 0},
{"first_name": "steve", "last_name": "gopher", "age": 5, "file_path": "file://test_path.parquet", "pos": 1},
{"first_name": "dead", "last_name": "gopher", "age": 95, "file_path": "file://test_path.parquet", "pos": 2}]`)},
		},
		{
			name: "many batches",
			inputBatches: []arrow.RecordBatch{
				mustLoadRecordBatchFromJSON(testSchema, `[{"first_name": "alan", "last_name": "gopher", "age": 7},
{"first_name": "steve", "last_name": "gopher", "age": 5},
{"first_name": "dead", "last_name": "gopher", "age": 95}]`),
				mustLoadRecordBatchFromJSON(testSchema, `[{"first_name": "matt", "last_name": "gopher", "age": 2},
{"first_name": "alex", "last_name": "gopher", "age": 10}]`),
			},
			expectedOutputs: []arrow.RecordBatch{
				mustLoadRecordBatchFromJSON(schemaWithPosDelete, `[{"first_name": "alan", "last_name": "gopher", "age": 7, "file_path": "file://test_path.parquet", "pos": 0},
{"first_name": "steve", "last_name": "gopher", "age": 5, "file_path": "file://test_path.parquet", "pos": 1},
{"first_name": "dead", "last_name": "gopher", "age": 95, "file_path": "file://test_path.parquet", "pos": 2}]`),
				mustLoadRecordBatchFromJSON(schemaWithPosDelete, `[{"first_name": "matt", "last_name": "gopher", "age": 2, "file_path": "file://test_path.parquet", "pos": 3},
{"first_name": "alex", "last_name": "gopher", "age": 10, "file_path": "file://test_path.parquet", "pos": 4}]`),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			ctx := compute.WithAllocator(t.Context(), mem)
			defer mem.AssertSize(t, 0)
			defer func() {
				for _, b := range tc.inputBatches {
					b.Release()
				}
			}()

			enrichFn := enrichRecordsWithPosDeleteFields(ctx, &mockDataFile{path: "file://test_path.parquet"}, (&rowPositionSource{}).cursor())
			for i, b := range tc.inputBatches {
				out, err := enrichFn(b)
				require.NoError(t, err)

				assert.Equal(t, schemaWithPosDelete, out.Schema())
				assert.Equal(t, out.NumRows(), b.NumRows())

				expectedOutputJSON, err := tc.expectedOutputs[i].MarshalJSON()
				require.NoError(t, err)

				outAsJSON, err := out.MarshalJSON()
				require.NoError(t, err)

				assert.Equal(t, string(expectedOutputJSON), string(outAsJSON))
				out.Release()
			}
		})
	}
}

// mustLoadRecordBatchFromJSON is a convenience wrapper around array.RecordFromJSON that returns the RecordBatch only
// to make it friendlier to table-driven tests. In case of error parsing the json content, it panics.
func mustLoadRecordBatchFromJSON(schema *arrow.Schema, content string) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	recordBatch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(content))
	if err != nil {
		panic("failed to load test data from JSON: " + err.Error())
	}

	return recordBatch
}

// chunkedPosDelete allocates a positional-delete *arrow.Chunked against mem so
// the CheckedAllocator can prove leaks. Returns a chunked that owns one Int64
// array of `positions` values.
func chunkedPosDelete(t *testing.T, mem memory.Allocator, positions []int64) *arrow.Chunked {
	t.Helper()
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(positions, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	return arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{arr})
}

// TestReleasePerFilePosDeletes verifies the helper releases every Arrow chunk
// it holds, leaving the allocator at zero outstanding bytes. Three cases:
// populated map, nil map (must not panic), and a positionDeletes slice
// containing a nil chunk (must not panic).
//
// This is the regression net for GetRecords' error-return paths: a future
// change that drops a perFilePosDeletes map on the floor without calling
// this helper will reintroduce the leak fixed by #1051.
func TestReleasePerFilePosDeletes(t *testing.T) {
	t.Run("populated map releases all chunks", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		m := perFilePosDeletes{
			"file://a.parquet": positionDeletes{
				chunkedPosDelete(t, mem, []int64{0, 1, 2}),
				chunkedPosDelete(t, mem, []int64{10, 11}),
			},
			"file://b.parquet": positionDeletes{
				chunkedPosDelete(t, mem, []int64{42}),
			},
		}

		releasePerFilePosDeletes(m)
	})

	t.Run("nil map does not panic", func(t *testing.T) {
		require.NotPanics(t, func() { releasePerFilePosDeletes(nil) })
	})

	t.Run("nil chunk in slice does not panic", func(t *testing.T) {
		// Defensive: production code paths never insert a nil *arrow.Chunked
		// into the map. This subtest pins the guard so a future caller (the
		// in-flight readAllDeletionVectors merger, or a refactor of readDeletes)
		// can't silently NPE the cleanup path.
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		m := perFilePosDeletes{
			"file://a.parquet": positionDeletes{
				nil,
				chunkedPosDelete(t, mem, []int64{7}),
				nil,
			},
		}

		require.NotPanics(t, func() { releasePerFilePosDeletes(m) })
	})
}
