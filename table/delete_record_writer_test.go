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
	"errors"
	"fmt"
	iofs "io/fs"
	"iter"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func deleteErrorRecord(t *testing.T, schema *arrow.Schema, data string) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	record, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(record.Release)

	return record
}

func TestBinPackRecordsReturnsReadError(t *testing.T) {
	for _, count := range []int{0, 1, 6} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
			record := deleteErrorRecord(t, schema, `[{"id":1}]`)
			readErr := errors.New("source failed")
			input := func(yield func(arrow.RecordBatch, error) bool) {
				for range count {
					if !yield(record, nil) {
						return
					}
				}
				require.False(t, yield(nil, readErr), "the source must stop at its first error")
			}
			batches, failures := 0, 0
			for batch, err := range binPackRecords(input, 2, 1) {
				if err != nil {
					require.ErrorIs(t, err, readErr)
					require.Nil(t, batch)
					failures++
				} else {
					batches++
					releasePackedRecords(batch)
				}
			}
			require.Equal(t, max(0, count-2), batches)
			require.Equal(t, 1, failures)
		})
	}
}

func TestBinPackRecordsEarlyStopReleasesBufferedRecords(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := deleteErrorRecord(t, schema, `[{"id":1}]`)
	consumed := 0
	input := func(yield func(arrow.RecordBatch, error) bool) {
		for range 50 {
			consumed++
			if !yield(record, nil) {
				return
			}
		}
	}
	for batch, err := range binPackRecords(input, 2, 1) {
		require.NoError(t, err)
		releasePackedRecords(batch)

		break
	}
	require.Equal(t, 3, consumed)
}

type deleteErrorFS struct {
	icebergio.LocalFS
	createErr  error
	removeErr  error
	createGate <-chan struct{}
	created    atomic.Int32
	removed    int
}

func (f *deleteErrorFS) Create(ctx context.Context, location string) (icebergio.FileWriter, error) {
	f.created.Add(1)
	if f.createGate != nil {
		<-f.createGate
	}
	if f.createErr != nil {
		return nil, f.createErr
	}

	return f.LocalFS.Create(ctx, location)
}

func (f *deleteErrorFS) Remove(ctx context.Context, location string) error {
	f.removed++

	return errors.Join(ctx.Err(), f.LocalFS.Remove(ctx, location), f.removeErr)
}

func deleteWriterForTest(t *testing.T, kind string, location string, fs icebergio.WriteFileIO, records iter.Seq2[arrow.RecordBatch, error]) iter.Seq2[iceberg.DataFile, error] {
	t.Helper()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, location,
		iceberg.Properties{PropertyFormatVersion: "2", WriteDeleteTargetFileSizeBytesKey: "1"})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(metadata, location+"/metadata/v1.metadata.json")
	require.NoError(t, err)
	inputSchema := schema
	if kind == "position" {
		inputSchema = iceberg.PositionalDeleteSchema
	}
	sc, err := SchemaToArrowSchema(inputSchema, nil, true, false)
	require.NoError(t, err)
	args := recordWritingArgs{sc: sc, itr: records, fs: fs}
	if kind == "position" {
		return positionDeleteRecordsToDataFiles(t.Context(), location, builder, nil, args)
	}
	files, err := equalityDeleteRecordsToDataFiles(t.Context(), location, builder, schema, []int{1}, args)
	require.NoError(t, err)

	return files
}

func TestDeleteWritersReturnSourceErrorsAndRemoveCompletedFiles(t *testing.T) {
	for _, kind := range []string{"position", "equality"} {
		for _, count := range []int{0, 1, defaultBinPackLookback + 5} {
			t.Run(fmt.Sprintf("%s/%d", kind, count), func(t *testing.T) {
				schema, data := PositionalDeleteArrowSchema, `[{"file_path":"data.parquet","pos":1}]`
				if kind == "equality" {
					schema = arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
					data = `[{"id":1}]`
				}
				record := deleteErrorRecord(t, schema, data)
				readErr := errors.New("read failed")
				input := func(yield func(arrow.RecordBatch, error) bool) {
					for range count {
						if !yield(record, nil) {
							return
						}
					}
					yield(nil, readErr)
				}
				location := t.TempDir()
				fs := &deleteErrorFS{}
				failures := 0
				for file, err := range deleteWriterForTest(t, kind, location, fs, input) {
					require.Nil(t, file, "a failed operation must not publish file descriptors")
					require.ErrorIs(t, err, readErr)
					failures++
				}
				require.Equal(t, 1, failures)
				require.EqualValues(t, max(0, count-defaultBinPackLookback), fs.created.Load())
				require.EqualValues(t, fs.created.Load(), fs.removed)
				require.NoError(t, filepath.WalkDir(location, func(path string, entry iofs.DirEntry, err error) error {
					require.NoError(t, err)
					require.True(t, entry.IsDir(), "uncommitted file remains: %s", path)

					return nil
				}))
			})
		}
	}
}

func TestDeleteWriterReturnsCleanupError(t *testing.T) {
	record := deleteErrorRecord(t, PositionalDeleteArrowSchema, `[{"file_path":"data.parquet","pos":1}]`)
	readErr, removeErr := errors.New("read failed"), errors.New("remove failed")
	input := func(yield func(arrow.RecordBatch, error) bool) {
		for range defaultBinPackLookback + 1 {
			if !yield(record, nil) {
				return
			}
		}
		yield(nil, readErr)
	}
	for _, err := range deleteWriterForTest(t, "position", t.TempDir(), &deleteErrorFS{removeErr: removeErr}, input) {
		require.ErrorIs(t, err, readErr)
		require.ErrorIs(t, err, removeErr)
	}
}

func TestDeleteWriterFailureReleasesQueuedRecords(t *testing.T) {
	record := deleteErrorRecord(t, PositionalDeleteArrowSchema, `[{"file_path":"data.parquet","pos":1}]`)
	writeErr := errors.New("create failed")
	input := func(yield func(arrow.RecordBatch, error) bool) {
		for range 100 {
			if !yield(record, nil) {
				return
			}
		}
	}
	failures := 0
	for _, err := range deleteWriterForTest(t, "position", t.TempDir(), &deleteErrorFS{createErr: writeErr}, input) {
		require.ErrorIs(t, err, writeErr)
		failures++
	}
	require.Equal(t, 1, failures)
}

func TestDeleteWriterReturnsSourceAndWriterFailures(t *testing.T) {
	record := deleteErrorRecord(t, PositionalDeleteArrowSchema, `[{"file_path":"data.parquet","pos":1}]`)
	readErr, writeErr := errors.New("source failed"), errors.New("writer failed")
	readFinished := make(chan struct{})
	input := func(yield func(arrow.RecordBatch, error) bool) {
		defer close(readFinished)
		for range defaultBinPackLookback + 1 {
			if !yield(record, nil) {
				return
			}
		}
		yield(nil, readErr)
	}
	fs := &deleteErrorFS{createGate: readFinished, createErr: writeErr}
	failures := 0
	for _, err := range deleteWriterForTest(t, "position", t.TempDir(), fs, input) {
		require.ErrorIs(t, err, readErr)
		require.ErrorIs(t, err, writeErr)
		failures++
	}
	require.Equal(t, 1, failures)
}

func TestDeleteWriterCanceledBeforeInput(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := func(func(arrow.RecordBatch, error) bool) { t.Error("canceled write read the input") }
	files := positionDeleteRecordsToDataFiles(ctx, t.TempDir(), newPositionDeleteUnpartitionedMetadata(t, 2), nil,
		recordWritingArgs{itr: input, fs: icebergio.LocalFS{}})
	failures := 0
	for file, err := range files {
		require.Nil(t, file)
		require.ErrorIs(t, err, context.Canceled)
		failures++
	}
	require.Equal(t, 1, failures)
}

func TestDeleteWriterCancellationAtEndOfInput(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := func(func(arrow.RecordBatch, error) bool) { cancel() }
	files := positionDeleteRecordsToDataFiles(ctx, t.TempDir(), newPositionDeleteUnpartitionedMetadata(t, 2), nil,
		recordWritingArgs{itr: input, fs: icebergio.LocalFS{}})
	failures := 0
	for file, err := range files {
		require.Nil(t, file)
		require.ErrorIs(t, err, context.Canceled)
		failures++
	}
	require.Equal(t, 1, failures)
}

func TestDeleteWriterCancellationStopsBufferedInput(t *testing.T) {
	record := deleteErrorRecord(t, PositionalDeleteArrowSchema, `[{"file_path":"data.parquet","pos":1}]`)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	consumed := 0
	input := func(yield func(arrow.RecordBatch, error) bool) {
		for range 100 {
			consumed++
			if !yield(record, nil) {
				return
			}
			cancel()
		}
	}
	files := positionDeleteRecordsToDataFiles(ctx, t.TempDir(), newPositionDeleteUnpartitionedMetadata(t, 2), nil,
		recordWritingArgs{itr: input, fs: icebergio.LocalFS{}})
	failures := 0
	for file, err := range files {
		require.Nil(t, file)
		require.ErrorIs(t, err, context.Canceled)
		failures++
	}
	require.Equal(t, 1, failures)
	require.Equal(t, 2, consumed)
}

func TestDeleteWriterPreservesCancellationCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("upstream canceled")
	cancel(cause)
	files := positionDeleteRecordsToDataFiles(ctx, t.TempDir(), newPositionDeleteUnpartitionedMetadata(t, 2), nil,
		recordWritingArgs{itr: func(func(arrow.RecordBatch, error) bool) { t.Error("read canceled input") }, fs: icebergio.LocalFS{}})
	failures := 0
	for file, err := range files {
		require.Nil(t, file)
		require.ErrorIs(t, err, cause)
		failures++
	}
	require.Equal(t, 1, failures)
}
