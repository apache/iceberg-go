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
	"github.com/stretchr/testify/require"
)

const reorderCreditsPerWorker = 2

type reorderGateIO struct {
	iceio.LocalFS

	mu       sync.Mutex
	headPath string
	gate     chan struct{}
	closes   chan<- struct{}
	released bool
	closed   int
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

		return g.LocalFS.Open(name)
	}

	f, err := g.LocalFS.Open(name)
	if err != nil {
		return nil, err
	}

	return &reorderCountingFile{File: f, owner: g}, nil
}

func (g *reorderGateIO) noteClose() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.released {
		return
	}
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
		iceberg.Properties{PropertyFormatVersion: "2"})
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
		bound := numWorkers * reorderCreditsPerWorker
		if held > bound {
			t.Errorf("scan held %d batches from the %d out-of-order tasks while task 0 was gated, %d bytes in Arrow buffers, bound is numWorkers x K = %d x %d = %d",
				held, files-1, mem.CurrentAlloc(), numWorkers, reorderCreditsPerWorker, bound)
		}

		gateIO.release()
		result := <-done
		require.NoError(t, result.err)
		require.Equal(t, int64(files*rowsPerFile), result.rows)
		mem.AssertSize(t, 0)
	})
}
