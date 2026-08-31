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
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingOpenMemFS struct {
	*iceio.MemFS
	opens        atomic.Int64
	trackedPath  string
	trackedOpens atomic.Int64
}

func (f *countingOpenMemFS) Open(name string) (iceio.File, error) {
	f.opens.Add(1)
	if name == f.trackedPath {
		f.trackedOpens.Add(1)
	}

	return f.MemFS.Open(name)
}

func newLazyDataFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	return builder.Build()
}

func writeLazyDataParquetToMemFS(t *testing.T, fs *iceio.MemFS, path string, start, count int) iceberg.DataFile {
	t.Helper()

	dataSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "value",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: false,
		Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"}),
	}}, nil)
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	for i := range count {
		bldr.Append(int64(start + i))
	}
	values := bldr.NewArray()
	defer values.Release()
	record := array.NewRecordBatch(dataSchema, []arrow.Array{values}, int64(count))
	defer record.Release()
	tbl := array.NewTableFromRecords(dataSchema, []arrow.RecordBatch{record})
	defer tbl.Release()

	file, err := fs.Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, file, int64(count),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
	require.NoError(t, file.Close())

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, int64(count), 128)
	require.NoError(t, err)

	return builder.Build()
}

func TestLazyPositionDeleteLoaderDefersReadsAndSharesResults(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	const (
		deletePath = "mem://bucket/deletes/shared.parquet"
		dataPathA  = "mem://bucket/data/a.parquet"
		dataPathB  = "mem://bucket/data/b.parquet"
	)
	memFS := &countingOpenMemFS{MemFS: iceio.NewMemFS()}
	writePosDeleteParquetToMemFS(t, memFS.MemFS, deletePath, `[
		{"file_path": "`+dataPathA+`", "pos": 1},
		{"file_path": "`+dataPathB+`", "pos": 3}
	]`)

	deleteFile := newPosDeleteFile(t, deletePath, 2, 128)
	tasks := []FileScanTask{
		{
			File:        newLazyDataFile(t, dataPathA),
			DeleteFiles: []iceberg.DataFile{deleteFile, deleteFile},
		},
		{
			File:        newLazyDataFile(t, dataPathB),
			DeleteFiles: []iceberg.DataFile{deleteFile},
		},
	}
	loader := newLazyPositionDeleteLoader(memFS, tasks)

	assert.Zero(t, memFS.opens.Load(), "constructing the scan loader must not open delete files")

	gotA, err := loader.load(ctx, tasks[0])
	require.NoError(t, err)
	require.Len(t, gotA, 1, "duplicate delete references must be read once per task")
	assert.Equal(t, []int64{1}, int64Values(gotA[0]))
	assert.Equal(t, int64(1), memFS.opens.Load())

	gotB, err := loader.load(ctx, tasks[1])
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	assert.Equal(t, []int64{3}, int64Values(gotB[0]))
	assert.Equal(t, int64(1), memFS.opens.Load(), "shared delete files must use one read")

	loader.release()
}

func TestLazyPositionDeleteLoaderSingleflightsConcurrentLoads(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	const (
		deletePath = "mem://bucket/deletes/concurrent.parquet"
		dataPath   = "mem://bucket/data/a.parquet"
	)
	fs := &countingOpenMemFS{MemFS: iceio.NewMemFS()}
	writePosDeleteParquetToMemFS(t, fs.MemFS, deletePath,
		`[{"file_path":"`+dataPath+`","pos":7}]`)
	deleteFile := newPosDeleteFile(t, deletePath, 1, 128)
	task := FileScanTask{
		File:        newLazyDataFile(t, dataPath),
		DeleteFiles: []iceberg.DataFile{deleteFile},
	}
	loader := newLazyPositionDeleteLoader(fs, []FileScanTask{task})

	const callers = 8
	results := make(chan positionDeletes, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			deletes, err := loader.load(ctx, task)
			results <- deletes
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	for deletes := range results {
		require.Len(t, deletes, 1)
		assert.Equal(t, []int64{7}, int64Values(deletes[0]))
	}
	assert.Equal(t, int64(1), fs.opens.Load(), "concurrent users must share one delete-file read")

	loader.release()
}

func TestLazyPositionDeleteLoaderCachesErrors(t *testing.T) {
	fs := &countingOpenMemFS{MemFS: iceio.NewMemFS()}
	deleteFile := newPosDeleteFile(t, "mem://bucket/deletes/missing.parquet", 1, 128)
	task := FileScanTask{
		File:        newLazyDataFile(t, "mem://bucket/data/a.parquet"),
		DeleteFiles: []iceberg.DataFile{deleteFile},
	}
	loader := newLazyPositionDeleteLoader(fs, []FileScanTask{task})

	first, err := loader.load(context.Background(), task)
	require.Error(t, err)
	assert.Nil(t, first)
	assert.Contains(t, err.Error(), deleteFile.FilePath())
	assert.Equal(t, int64(1), fs.opens.Load())

	second, secondErr := loader.load(context.Background(), task)
	require.Error(t, secondErr)
	assert.Nil(t, second)
	assert.ErrorIs(t, secondErr, err)
	assert.Equal(t, int64(1), fs.opens.Load(), "a failed delete file must not be retried by other tasks")

	loader.release()
}

func TestLazyPositionDeleteLoaderCachesCancellation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx, cancel := context.WithCancel(compute.WithAllocator(t.Context(), mem))
	cancel()

	fs := &countingOpenMemFS{MemFS: iceio.NewMemFS()}
	deletePath := "mem://bucket/deletes/cancelled.parquet"
	writePosDeleteParquetToMemFS(t, fs.MemFS, deletePath, `[{"file_path":"mem://bucket/data/a.parquet","pos":1}]`)
	deleteFile := newPosDeleteFile(t, deletePath, 1, 128)
	task := FileScanTask{
		File:        newLazyDataFile(t, "mem://bucket/data/a.parquet"),
		DeleteFiles: []iceberg.DataFile{deleteFile},
	}
	loader := newLazyPositionDeleteLoader(fs, []FileScanTask{task})

	_, firstErr := loader.load(ctx, task)
	require.ErrorIs(t, firstErr, context.Canceled)
	_, secondErr := loader.load(context.Background(), task)
	require.ErrorIs(t, secondErr, context.Canceled)
	assert.Equal(t, int64(1), fs.opens.Load(), "cancellation must not cause a second read")

	loader.release()
}

func TestArrowScanDefersPositionDeleteReadsUntilIteration(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int64,
	})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec,
		UnsortedSortOrder, "mem://bucket/table", nil)
	require.NoError(t, err)

	const deletePath = "mem://bucket/deletes/one.parquet"
	memFS := &countingOpenMemFS{MemFS: iceio.NewMemFS()}
	writePosDeleteParquetToMemFS(t, memFS.MemFS, deletePath,
		`[{"file_path":"mem://bucket/data/missing.parquet","pos":1}]`)
	deleteFile := newPosDeleteFile(t, deletePath, 1, 128)
	task := FileScanTask{
		File:        newLazyDataFile(t, "mem://bucket/data/missing.parquet"),
		DeleteFiles: []iceberg.DataFile{deleteFile},
	}
	scan := &arrowScan{
		metadata:        metadata,
		fs:              memFS,
		scanSchema:      schema,
		projectedSchema: schema,
		boundRowFilter:  iceberg.AlwaysTrue{},
		rowLimit:        -1,
		concurrency:     1,
	}

	_, records, err := scan.GetRecords(ctx, []FileScanTask{task})
	require.NoError(t, err)
	assert.Zero(t, memFS.opens.Load(), "GetRecords must not read position deletes")

	var iterErr error
	for record, err := range records {
		if record != nil {
			record.Release()
		}
		iterErr = err

		break
	}
	require.Error(t, iterErr, "iteration should reach the missing data file after loading its delete")
	assert.Equal(t, int64(2), memFS.opens.Load(), "the first task should open one delete and one data file")
}

func TestArrowScanReleasesLazyPositionDeletesOnEarlyStop(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int64,
	})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec,
		UnsortedSortOrder, "mem://bucket/table", nil)
	require.NoError(t, err)

	const (
		deletePath = "mem://bucket/deletes/shared.parquet"
		taskCount  = 8
		rowCount   = 4
	)
	memFS := &countingOpenMemFS{
		MemFS:       iceio.NewMemFS(),
		trackedPath: deletePath,
	}
	tasks := make([]FileScanTask, taskCount)
	var deleteJSON strings.Builder
	deleteJSON.WriteByte('[')
	for i := range tasks {
		dataPath := fmt.Sprintf("mem://bucket/data/data-%d.parquet", i)
		tasks[i].File = writeLazyDataParquetToMemFS(t, memFS.MemFS,
			dataPath, i*rowCount, rowCount)
		if i > 0 {
			deleteJSON.WriteByte(',')
		}
		fmt.Fprintf(&deleteJSON, `{"file_path":"%s","pos":0}`, dataPath)
	}
	deleteJSON.WriteByte(']')
	writePosDeleteParquetToMemFS(t, memFS.MemFS, deletePath, deleteJSON.String())
	deleteFile := newPosDeleteFile(t, deletePath, taskCount, 128)
	for i := range tasks {
		tasks[i].DeleteFiles = []iceberg.DataFile{deleteFile}
	}

	scan := &arrowScan{
		metadata:        metadata,
		fs:              memFS,
		scanSchema:      schema,
		projectedSchema: schema,
		boundRowFilter:  iceberg.AlwaysTrue{},
		rowLimit:        -1,
		concurrency:     4,
	}
	_, records, err := scan.GetRecords(ctx, tasks)
	require.NoError(t, err)

	var batches int
	for record, err := range records {
		require.NoError(t, err)
		require.NotNil(t, record)
		record.Release()
		batches++

		break
	}

	assert.Equal(t, 1, batches, "the test must stop after the first batch")
	assert.Equal(t, int64(1), memFS.trackedOpens.Load(),
		"all workers must share one lazily-loaded positional-delete file")
}

func TestLazyPositionDeleteLoaderReleasesChunksWhenIteratorStops(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	loader := &lazyPositionDeleteLoader{files: map[string]*lazyPositionDeleteFile{
		"mem://bucket/deletes/one.parquet": {
			deletes: map[string]*arrow.Chunked{
				"mem://bucket/data/a.parquet": chunkedPosDelete(t, mem, []int64{1}),
			},
		},
	}}

	batch := checkedInt64RecordBatch(mem, 1)
	records := make(chan enumeratedRecord, 1)
	records <- enumeratedRecord{
		Record: internal.Enumerated[arrow.RecordBatch]{
			Value: batch,
			Index: 0,
			Last:  true,
		},
		Task: internal.Enumerated[FileScanTask]{Index: 0, Last: true},
	}
	close(records)

	ctx, cancel := context.WithCancelCause(context.Background())
	itr := createIteratorWithCleanup(ctx, 1, records, nil, cancel, 0, loader.release)
	for record, err := range itr {
		require.NoError(t, err)
		record.Release()

		break
	}
}

func TestArrowScanPreCancelledIteratorTearsDownProducer(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)

	scan := &arrowScan{concurrency: 1, rowLimit: -1}
	tasks := []FileScanTask{{
		File: newLazyDataFile(t, "mem://bucket/data/pre-cancelled.parquet"),
	}}
	records := scan.recordBatchesFromTasksAndDeletes(ctx, tasks, nil, nil, nil, nil)

	done := make(chan error, 1)
	go func() {
		var iterErr error
		for _, err := range records {
			if err != nil {
				iterErr = err
			}
		}
		done <- iterErr
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pre-cancelled scan iterator did not terminate")
	}
}

func TestCreateIteratorReleasesOutOfOrderBatchAfterError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	expectedErr := errors.New("position delete load failed")
	records := make(chan enumeratedRecord, 2)
	records <- enumeratedRecord{
		Record: internal.Enumerated[arrow.RecordBatch]{
			Value: checkedInt64RecordBatch(mem, 1),
			Index: 0,
			Last:  true,
		},
		Task: internal.Enumerated[FileScanTask]{Index: 1, Last: true},
	}
	records <- enumeratedRecord{
		Task: internal.Enumerated[FileScanTask]{Index: 0},
		Err:  expectedErr,
	}
	close(records)

	ctx, cancel := context.WithCancelCause(context.Background())
	var gotErr error
	for _, err := range createIteratorWithCleanup(ctx, 2, records, nil, cancel, 0, nil) {
		gotErr = err
	}

	require.ErrorIs(t, gotErr, expectedErr)
}

func TestCreateIteratorStopsWhenConsumerStops(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	records := make(chan enumeratedRecord, 1)
	records <- enumeratedRecord{
		Record: internal.Enumerated[arrow.RecordBatch]{
			Value: checkedInt64RecordBatch(mem, 1),
			Index: 0,
			Last:  true,
		},
		Task: internal.Enumerated[FileScanTask]{Index: 0, Last: true},
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer func() { cancel(nil) }()
	closed := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(records)
	}()

	go func() {
		for record, err := range createIteratorWithCleanup(ctx, 1, records, nil, cancel, 0, nil) {
			if err == nil && record != nil {
				record.Release()
			}

			break
		}
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("iterator did not stop after consumer termination")
	}
}
