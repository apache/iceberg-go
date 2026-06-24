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
	"errors"
	iofs "io/fs"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failAfterGoodCloseFS struct {
	*iceio.MemFS

	goodPath string
	badPath  string

	goodClosed     chan struct{}
	goodClosedOnce sync.Once
}

func (f *failAfterGoodCloseFS) Open(name string) (iceio.File, error) {
	if name == f.goodPath {
		file, err := f.MemFS.Open(name)
		if err != nil {
			return nil, err
		}

		return &closeSignalFile{
			File: file,
			onClose: func() {
				f.goodClosedOnce.Do(func() { close(f.goodClosed) })
			},
		}, nil
	}

	if name == f.badPath {
		select {
		case <-f.goodClosed:
		case <-time.After(5 * time.Second):
			return nil, &iofs.PathError{
				Op:   "open",
				Path: name,
				Err:  errors.New("timed out waiting for good delete file to close"),
			}
		}

		return nil, &iofs.PathError{Op: "open", Path: name, Err: iofs.ErrNotExist}
	}

	return f.MemFS.Open(name)
}

type closeSignalFile struct {
	iceio.File
	onClose func()
}

func (f *closeSignalFile) Close() error {
	err := f.File.Close()
	f.onClose()

	return err
}

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

func writePosDeleteParquetToMemFS(t *testing.T, memFS *iceio.MemFS, path, content string) {
	t.Helper()

	rec := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, content)
	defer rec.Release()

	tbl := array.NewTableFromRecords(PositionalDeleteArrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	fw, err := memFS.Create(path)
	require.NoError(t, err)

	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
	require.NoError(t, fw.Close())
}

func newPosDeleteFile(t *testing.T, path string, count, size int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, count, size)
	require.NoError(t, err)

	return builder.Build()
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

func TestReadAllDeleteFilesReturnsPartialDeletesOnError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	ctx := compute.WithAllocator(t.Context(), mem)
	goodDeletePath := "mem://bucket/deletes/good.parquet"
	badDeletePath := "mem://bucket/deletes/missing.parquet"
	dataPath := "mem://bucket/data/data.parquet"

	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFS(t, memFS, goodDeletePath, `[
		{"file_path": "`+dataPath+`", "pos": 1},
		{"file_path": "`+dataPath+`", "pos": 3}
	]`)
	testFS := &failAfterGoodCloseFS{
		MemFS:      memFS,
		goodPath:   goodDeletePath,
		badPath:    badDeletePath,
		goodClosed: make(chan struct{}),
	}
	tasks := []FileScanTask{{
		DeleteFiles: []iceberg.DataFile{
			newPosDeleteFile(t, goodDeletePath, 2, 128),
			newPosDeleteFile(t, badDeletePath, 1, 128),
		},
	}}

	// concurrency=2 is enough to observe the partial result deterministically:
	// the good worker closes first, then the bad worker returns its error.
	deletesPerFile, err := readAllDeleteFiles(ctx, testFS, tasks, 2)
	require.Error(t, err)
	require.NotNil(t, deletesPerFile)
	require.Contains(t, deletesPerFile, dataPath)
	require.Len(t, deletesPerFile[dataPath], 1)

	chunks := deletesPerFile[dataPath][0].Chunks()
	require.Len(t, chunks, 1)
	pos, ok := chunks[0].(*array.Int64)
	require.True(t, ok)
	assert.Equal(t, []int64{1, 3}, pos.Int64Values())

	releasePerFilePosDeletes(deletesPerFile)
	mem.AssertSize(t, 0)
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
