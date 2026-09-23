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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

type reorderGateIO struct {
	iceio.LocalFS

	mu         sync.Mutex
	headPath   string
	gate       chan struct{}
	createGate chan struct{}
	closes     chan<- struct{}
	released   bool
	closed     int
}

func (g *reorderGateIO) arm(headPath string, closes chan<- struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.headPath = headPath
	g.gate = make(chan struct{})
	g.closes = closes
	g.released = false
	g.closed = 0
}

func (g *reorderGateIO) Open(name string) (iceio.File, error) {
	g.mu.Lock()
	headPath, gate := g.headPath, g.gate
	g.mu.Unlock()

	if gate != nil && name == headPath {
		<-gate
	}

	f, err := g.LocalFS.Open(name)
	if err != nil {
		return nil, err
	}

	return &reorderCountingFile{File: f, owner: g}, nil
}

func (g *reorderGateIO) armCreate() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.createGate = make(chan struct{})
}

func (g *reorderGateIO) Create(name string) (iceio.FileWriter, error) {
	g.mu.Lock()
	gate := g.createGate
	g.mu.Unlock()

	if gate != nil {
		<-gate
	}

	return g.LocalFS.Create(name)
}

func (g *reorderGateIO) releaseCreate() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.createGate != nil {
		close(g.createGate)
		g.createGate = nil
	}
}

func (g *reorderGateIO) noteClose() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.closed++
	if g.closes != nil {
		select {
		case g.closes <- struct{}{}:
		default:
		}
	}
}

func (g *reorderGateIO) gatedCloses() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.closed
}

func (g *reorderGateIO) release() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.released {
		return
	}
	g.released = true
	close(g.gate)
}

type reorderCountingFile struct {
	iceio.File

	owner *reorderGateIO
	once  sync.Once
}

func (f *reorderCountingFile) Close() error {
	f.once.Do(f.owner.noteClose)

	return f.File.Close()
}

func newReorderScanFixture(tb testing.TB, dir string, files, rowsPerFile int, fs iceio.IO) (*Table, []FileScanTask) {
	tb.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	arrowSchema, err := SchemaToArrowSchemaWithOptions(schema, ArrowSchemaOptions{IncludeFieldIDs: true})
	require.NoError(tb, err)

	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, dir,
		iceberg.Properties{PropertyFormatVersion: "2", ParquetCompressionKey: "uncompressed"})
	require.NoError(tb, err)

	payload := strings.Repeat("x", 256)
	tasks := make([]FileScanTask, files)
	for i := range files {
		path := fmt.Sprintf("%s/data/file-%03d.parquet", dir, i)
		tasks[i].File = writeReorderDataFile(tb, arrowSchema, path, i*rowsPerFile, rowsPerFile, payload)
	}

	tbl := New(Identifier{"db", "reorder_heap"}, meta, dir+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

	return tbl, tasks
}

func writeReorderDataFile(tb testing.TB, arrowSchema *arrow.Schema, path string, firstID, rows int, payload string) iceberg.DataFile {
	tb.Helper()

	mem := memory.DefaultAllocator
	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	dataBldr := array.NewStringBuilder(mem)
	defer dataBldr.Release()
	for i := range rows {
		idBldr.Append(int64(firstID + i))
		dataBldr.Append(payload)
	}
	idArr := idBldr.NewArray()
	defer idArr.Release()
	dataArr := dataBldr.NewArray()
	defer dataArr.Release()

	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{idArr, dataArr}, int64(rows))
	defer rec.Release()
	arrowTable := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer arrowTable.Release()

	fw, err := iceio.LocalFS{}.Create(path)
	require.NoError(tb, err)
	defer fw.Close()
	require.NoError(tb, pqarrow.WriteTable(arrowTable, fw, int64(rows),
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))

	info, err := os.Stat(path)
	require.NoError(tb, err)

	bldr, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, int64(rows), info.Size())
	require.NoError(tb, err)

	return bldr.Build()
}

func TestArrowScanReorderHeapBoundedWhileHeadTaskLags(t *testing.T) {
	const (
		files       = 32
		rowsPerFile = 256
		numWorkers  = 4
	)

	synctest.Test(t, func(t *testing.T) {
		dir := filepath.ToSlash(t.TempDir())
		gateIO := &reorderGateIO{}
		tbl, tasks := newReorderScanFixture(t, dir, files, rowsPerFile, gateIO)
		gateIO.arm(tasks[0].File.FilePath(), nil)

		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ctx := compute.WithAllocator(context.Background(), mem)
		_, records, err := tbl.Scan(WithMaxConcurrency(numWorkers)).ReadTasks(ctx, tasks)
		require.NoError(t, err)

		type scanResult struct {
			rows int64
			err  error
		}
		done := make(chan scanResult, 1)
		go func() {
			var rows int64
			for rec, err := range records {
				if err != nil {
					done <- scanResult{rows: rows, err: err}

					return
				}
				rows += rec.NumRows()
				rec.Release()
			}
			done <- scanResult{rows: rows}
		}()

		synctest.Wait()

		held := gateIO.gatedCloses()
		bound := (numWorkers - 1) * maxInFlightTasksPerWorker
		if held > bound {
			t.Errorf("scan completed %d of the %d out-of-order tasks while task 0 was gated, %d bytes in Arrow buffers, bound is (numWorkers - 1) x maxInFlightTasksPerWorker = %d x %d = %d",
				held, files-1, mem.CurrentAlloc(), numWorkers-1, maxInFlightTasksPerWorker, bound)
		}

		gateIO.release()
		result := <-done
		require.NoError(t, result.err)
		require.Equal(t, int64(files*rowsPerFile), result.rows)
		mem.AssertSize(t, 0)
	})
}

func TestArrowScanReorderCancelWhileWorkersWaitForTaskCredits(t *testing.T) {
	const (
		files       = 16
		rowsPerFile = 256
		numWorkers  = 4
	)

	synctest.Test(t, func(t *testing.T) {
		dir := filepath.ToSlash(t.TempDir())
		gateIO := &reorderGateIO{}
		tbl, tasks := newReorderScanFixture(t, dir, files, rowsPerFile, gateIO)
		gateIO.arm(tasks[0].File.FilePath(), nil)

		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ctx, cancel := context.WithCancel(compute.WithAllocator(context.Background(), mem))
		defer cancel()
		_, records, err := tbl.Scan(WithMaxConcurrency(numWorkers)).ReadTasks(ctx, tasks)
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			for rec, err := range records {
				if err != nil {
					done <- err

					return
				}
				rec.Release()
			}
			done <- nil
		}()

		synctest.Wait()
		require.Equal(t, numWorkers-1, gateIO.gatedCloses())

		cancel()
		require.ErrorIs(t, <-done, context.Canceled)

		gateIO.release()
		synctest.Wait()
		mem.AssertSize(t, 0)
	})
}

func TestRecordSinkHandsOffCreditOnce(t *testing.T) {
	ctx := context.Background()
	task := tblutils.Enumerated[FileScanTask]{Index: 3}
	out := make(chan enumeratedRecord, 4)
	sink := newRecordSink(out)

	require.NoError(t, sink.reserve(ctx))
	sink.fail(task, errors.New("open failed"))
	failed := <-out
	require.Error(t, failed.Err)
	require.NotNil(t, failed.credits)

	blocked, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, sink.reserve(blocked), context.Canceled)
	failed.credits.release()
	require.NoError(t, sink.reserve(ctx))

	sink.send(enumeratedRecord{Task: task, Record: tblutils.Enumerated[arrow.RecordBatch]{Index: 0}})
	sink.send(enumeratedRecord{Task: task, Record: tblutils.Enumerated[arrow.RecordBatch]{Index: 1, Last: true}})
	sink.fail(task, errors.New("close failed"))
	first, last, afterLast := <-out, <-out, <-out
	require.Nil(t, first.credits)
	require.NotNil(t, last.credits)
	require.Nil(t, afterLast.credits)
	require.ErrorIs(t, sink.reserve(blocked), context.Canceled)
	last.credits.release()
	require.NoError(t, sink.reserve(ctx))
}

func TestExecuteCompactionGroupRecordPipelineBounded(t *testing.T) {
	const (
		files                 = 32
		rowsPerFile           = 256
		numWorkers            = 4
		recordBatchBufferSize = 2
	)

	synctest.Test(t, func(t *testing.T) {
		dir := filepath.ToSlash(t.TempDir())
		gateIO := &reorderGateIO{}
		tbl, tasks := newReorderScanFixture(t, dir, files, rowsPerFile, gateIO)
		gateIO.arm(tasks[0].File.FilePath(), nil)
		gateIO.armCreate()

		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		ctx := compute.WithAllocator(context.Background(), mem)

		type groupResult struct {
			result CompactionGroupResult
			err    error
		}
		done := make(chan groupResult, 1)
		go func() {
			result, err := ExecuteCompactionGroup(ctx, tbl,
				CompactionTaskGroup{PartitionKey: "unpartitioned", Tasks: tasks},
				WithCompactionScanConcurrency(numWorkers),
				WithCompactionRecordBatchBufferSize(recordBatchBufferSize))
			done <- groupResult{result: result, err: err}
		}()

		synctest.Wait()
		if got, want := gateIO.gatedCloses(), (numWorkers-1)*maxInFlightTasksPerWorker; got != want {
			t.Errorf("%d tasks fully read while task 0 was gated in Open, want (workers - 1) x maxInFlightTasksPerWorker = %d", got, want)
		}

		gateIO.release()
		synctest.Wait()
		if got, want := gateIO.gatedCloses(), numWorkers*maxInFlightTasksPerWorker+recordBatchBufferSize+2; got != want {
			t.Errorf("%d tasks fully read while the writer was gated in Create, want workers x maxInFlightTasksPerWorker + recordBatchBufferSize + 2 = %d", got, want)
		}

		gateIO.releaseCreate()
		res := <-done
		require.NoError(t, res.err)
		var rows int64
		for _, df := range res.result.NewDataFiles {
			rows += df.Count()
		}
		require.Equal(t, int64(files*rowsPerFile), rows)
		mem.AssertSize(t, 0)
	})
}
