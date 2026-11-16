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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

type blockedPositionDeleteBatch struct {
	arrow.RecordBatch
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (b *blockedPositionDeleteBatch) Columns() []arrow.Array {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})

	return b.RecordBatch.Columns()
}

func TestPartitionedPositionDeleteFilesPreserveBatchOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
		})
		txn, fs := createTestTransactionWithMemIO(t, spec)
		require.NoError(t, txn.SetProperties(iceberg.Properties{DeleteParquetCompressionKey: "uncompressed"}))
		meta, err := txn.txnMeta()
		require.NoError(t, err)
		const path = "file://table/id=1/data.parquet"
		first, _, err := array.RecordFromJSON(memory.DefaultAllocator, PositionalDeleteArrowSchema,
			strings.NewReader(`[{"file_path":"`+path+`","pos":0}]`))
		require.NoError(t, err)
		defer first.Release()
		second, _, err := array.RecordFromJSON(memory.DefaultAllocator, PositionalDeleteArrowSchema,
			strings.NewReader(`[{"file_path":"`+path+`","pos":1}]`))
		require.NoError(t, err)
		defer second.Release()
		head := &blockedPositionDeleteBatch{RecordBatch: first, started: make(chan struct{}), release: make(chan struct{})}
		input := func(yield func(arrow.RecordBatch, error) bool) {
			if yield(head, nil) {
				yield(second, nil)
			}
		}
		type result struct {
			files []iceberg.DataFile
			err   error
		}
		done := make(chan result, 1)
		go func() {
			var result result
			for df, err := range positionDeleteRecordsToDataFiles(t.Context(), meta.loc, meta,
				map[string]partitionContext{path: {partitionData: map[int]any{1000: int32(1)}, specID: 0}},
				recordWritingArgs{sc: PositionalDeleteArrowSchema, itr: input, fs: fs}) {
				if err != nil {
					result.err = err

					break
				}
				result.files = append(result.files, df)
			}
			done <- result
		}()
		select {
		case <-head.started:
		case outcome := <-done:
			t.Fatalf("writer stopped before processing the first batch: %v", outcome.err)
		}
		synctest.Wait()
		close(head.release)
		outcome := <-done
		require.NoError(t, outcome.err)
		require.Len(t, outcome.files, 1)

		source, err := tblutils.GetFile(t.Context(), fs, outcome.files[0], true)
		require.NoError(t, err)
		reader, err := source.GetReader(t.Context())
		require.NoError(t, err)
		defer reader.Close()
		rows, err := reader.GetRecords(t.Context(), []int{0, 1}, nil)
		require.NoError(t, err)
		defer rows.Release()
		var positions []string
		for rows.Next() {
			batch := rows.RecordBatch()
			for i := range int(batch.NumRows()) {
				require.Equal(t, path, batch.Column(0).ValueStr(i))
				positions = append(positions, batch.Column(1).ValueStr(i))
			}
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"0", "1"}, positions)
	})
}

func TestPositionDeleteFilesSortedByPathAndPosition(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())
	fs := iceio.LocalFS{}
	tbl, tasks := newReorderScanFixture(t, dir, 3, 4, fs)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{ParquetBatchSizeKey: "1"}))

	files := []iceberg.DataFile{tasks[2].File, tasks[1].File, tasks[0].File}
	records, err := txn.makePositionDeleteRecordsForFilter(t.Context(), fs, files,
		iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(0)), true, 3)
	require.NoError(t, err)
	meta, err := txn.txnMeta()
	require.NoError(t, err)
	var deleteFiles []iceberg.DataFile
	for df, err := range positionDeleteRecordsToDataFiles(t.Context(), dir, meta, nil,
		recordWritingArgs{sc: PositionalDeleteArrowSchema, itr: records, fs: fs}) {
		require.NoError(t, err)
		deleteFiles = append(deleteFiles, df)
	}
	require.Len(t, deleteFiles, 1)

	source, err := tblutils.GetFile(t.Context(), fs, deleteFiles[0], true)
	require.NoError(t, err)
	reader, err := source.GetReader(t.Context())
	require.NoError(t, err)
	defer reader.Close()
	rows, err := reader.GetRecords(t.Context(), []int{0, 1}, nil)
	require.NoError(t, err)
	defer rows.Release()

	var gotPaths []string
	var gotPositions []int64
	for rows.Next() {
		batch := rows.RecordBatch()
		paths := batch.Column(0)
		positions := batch.Column(1)
		for i := range paths.Len() {
			gotPaths = append(gotPaths, paths.ValueStr(i))
			pos, err := strconv.ParseInt(positions.ValueStr(i), 10, 64)
			require.NoError(t, err)
			gotPositions = append(gotPositions, pos)
		}
	}
	require.NoError(t, rows.Err())
	var wantPaths []string
	for _, task := range tasks {
		for range 4 {
			wantPaths = append(wantPaths, task.File.FilePath())
		}
	}
	require.Equal(t, wantPaths, gotPaths)
	require.Equal(t, []int64{0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3}, gotPositions)
}

func TestMergeOnReadDeleteWritesSortedPositionDeletes(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())
	fs := iceio.LocalFS{}
	tbl, tasks := newReorderScanFixture(t, dir, 3, 4, fs)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{
		WriteDeleteModeKey:  WriteModeMergeOnRead,
		ParquetBatchSizeKey: "1",
	}))
	require.NoError(t, txn.AddDataFiles(t.Context(), []iceberg.DataFile{tasks[2].File, tasks[1].File, tasks[0].File}, nil))
	require.NoError(t, txn.Delete(t.Context(), iceberg.IsIn(iceberg.Reference("id"), int64(0), int64(4), int64(8)), nil))

	meta, err := txn.txnMeta()
	require.NoError(t, err)
	var deleteFiles []iceberg.DataFile
	for df, err := range meta.currentSnapshot().dataFiles(t.Context(), fs, nil) {
		require.NoError(t, err)
		if df.ContentType() == iceberg.EntryContentPosDeletes {
			deleteFiles = append(deleteFiles, df)
		}
	}
	require.Len(t, deleteFiles, 1)
	source, err := tblutils.GetFile(t.Context(), fs, deleteFiles[0], true)
	require.NoError(t, err)
	reader, err := source.GetReader(t.Context())
	require.NoError(t, err)
	defer reader.Close()
	rows, err := reader.GetRecords(t.Context(), []int{0, 1}, nil)
	require.NoError(t, err)
	defer rows.Release()
	var paths []string
	var positions []string
	for rows.Next() {
		batch := rows.RecordBatch()
		for i := range int(batch.NumRows()) {
			paths = append(paths, batch.Column(0).ValueStr(i))
			positions = append(positions, batch.Column(1).ValueStr(i))
		}
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{tasks[0].File.FilePath(), tasks[1].File.FilePath(), tasks[2].File.FilePath()}, paths)
	require.Equal(t, []string{"0", "0", "0"}, positions)
}
