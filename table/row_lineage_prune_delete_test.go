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

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests guard the interaction between row-group pruning and the
// position-keyed scan steps. Re-enabling pruning while _row_id is projected
// (or for any scan with positional/DV deletes) means processPositionalDeletes
// and filterByDeletionVector see non-contiguous emitted rows; if they kept
// counting positions densely they would target the wrong physical rows. Each
// test prunes a leading row group and deletes a row that lives only in the
// surviving group.

// TestScanPruningWithPositionalDeletes covers a Parquet positional delete whose
// target (pos 6 = id=7) is in the second row group while the filter prunes the
// first. The deleted row must vanish and survivors must keep their _row_id.
func TestScanPruningWithPositionalDeletes(t *testing.T) {
	ctx := context.Background()
	tbl := buildTwoRowGroupV3Table(t)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataPath := tasks[0].File.FilePath()

	posDelPath := tbl.Location() + "/data/pos-del.parquet"
	posSc, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, posDelPath, posSc, fmt.Sprintf(`[{"file_path":%q,"pos":6}]`, dataPath))

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 256)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(b.Build()).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	filter := table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("id"), int64(5)))

	got := rowIDByID(t, tbl, filter)
	assert.Equal(t, map[int64]int64{6: 5, 8: 7, 9: 8, 10: 9}, got,
		"pos-6 (id=7) must be deleted and survivors keep their _row_id under pruning")

	// Same scan without _row_id projected: pruning was already active here before
	// the cursor change, so this guards a pre-existing correctness gap too.
	assert.Equal(t, []int64{6, 8, 9, 10}, idsMatchingFilter(t, tbl, filter),
		"pos-6 (id=7) must be deleted even when _row_id is not projected")
}

// TestScanPruningWithDeletionVector is the v3 deletion-vector analogue: the DV
// drops pos 6 (id=7) in the surviving second row group while the first is pruned.
func TestScanPruningWithDeletionVector(t *testing.T) {
	ctx := context.Background()
	tbl := buildTwoRowGroupV3Table(t)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataPath := tasks[0].File.FilePath()

	w := dv.NewDVWriter(iceio.LocalFS{}, unpartitionedSpecByID)
	w.Add(dataPath, []int64{6}, 0, nil)
	dvFiles, err := w.Flush(ctx, filepath.Join(filepath.Dir(dataPath), "dv-0001.puffin"))
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dvFiles...).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	filter := table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("id"), int64(5)))

	got := rowIDByID(t, tbl, filter)
	assert.Equal(t, map[int64]int64{6: 5, 8: 7, 9: 8, 10: 9}, got,
		"DV pos-6 (id=7) must be deleted and survivors keep their _row_id under pruning")

	// Without _row_id pruning was already active here, so this guards the
	// pre-existing DV-under-pruning gap as well.
	assert.Equal(t, []int64{6, 8, 9, 10}, idsMatchingFilter(t, tbl, filter),
		"DV pos-6 (id=7) must be deleted even when _row_id is not projected")
}

// TestScanPruningSurvivingGroupSpansBatches forces a tiny read batch size so the
// single surviving row group is emitted across several Arrow batches. The cursor
// must keep advancing across batch boundaries, not reset per batch.
func TestScanPruningSurvivingGroupSpansBatches(t *testing.T) {
	ctx := context.Background()
	tbl := buildTwoRowGroupV3Table(t)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.ParquetBatchSizeKey: "2"}))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	got := rowIDByID(t, tbl, table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("id"), int64(5))))
	assert.Equal(t, map[int64]int64{6: 5, 7: 6, 8: 7, 9: 8, 10: 9}, got,
		"_row_id must stay contiguous as one surviving row group spans multiple batches")
}

// TestReadTaskDeletionVectorSupersedesPositionalDeletes locks the spec contract
// PlanFiles enforces (scanner.go) for hand-built tasks too: when a data file has
// both a DV and a positional delete attached, the DV wins and positional deletes
// are ignored. Applying both would also run two row-dropping position steps in
// sequence, so the second cursor would map over an already-shortened batch.
func TestReadTaskDeletionVectorSupersedesPositionalDeletes(t *testing.T) {
	ctx := context.Background()
	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`,
	})
	require.NoError(t, err)
	defer data.Release()
	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataFile := tasks[0].File
	dataPath := dataFile.FilePath()

	// DV deletes pos 0 (id=1); the positional delete targets pos 4 (id=5).
	w := dv.NewDVWriter(iceio.LocalFS{}, unpartitionedSpecByID)
	w.Add(dataPath, []int64{0}, 0, nil)
	dvFiles, err := w.Flush(ctx, filepath.Join(filepath.Dir(dataPath), "dv-0001.puffin"))
	require.NoError(t, err)

	posDelPath := tbl.Location() + "/data/pos-del.parquet"
	posSc, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, posDelPath, posSc, fmt.Sprintf(`[{"file_path":%q,"pos":4}]`, dataPath))
	posDel, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 256)
	require.NoError(t, err)

	task := table.FileScanTask{
		File:                dataFile,
		DeleteFiles:         []iceberg.DataFile{posDel.Build()},
		DeletionVectorFiles: dvFiles,
		Length:              dataFile.FileSizeBytes(),
	}

	_, itr, err := tbl.Scan().ReadTasks(ctx, []table.FileScanTask{task})
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	slices.Sort(ids)

	assert.Equal(t, []int64{2, 3, 4, 5}, ids,
		"DV (pos 0 / id=1) must win and the positional delete (pos 4 / id=5) must be ignored")
}

// Bloom-filter row-group pruning and its span accounting are covered at the
// reader layer in TestBloomFilterRowGroupPruning (table/internal); an end-to-end
// scan cannot prove bloom skipped a group, since a correct scan emits the same
// rows whether or not the empty group was physically read.

// buildTwoRowGroupV3Table writes a v3 row-lineage table with ids 1..10 across two
// 5-row row groups, so an id>5 filter prunes the first group via stats.
func buildTwoRowGroupV3Table(t *testing.T) *table.Table {
	t.Helper()
	ctx := context.Background()
	tbl := newV3RowLineageTestTable(t)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.ParquetRowGroupLimitKey: "5"}))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},` +
			`{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},` +
			`{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	require.Equal(t, 2, parquetRowGroupCount(t, tbl),
		"file must have multiple row groups for the id>5 / id==7 filter to prune one")

	return tbl
}

// idsMatchingFilter scans without row lineage and returns the surviving ids, sorted.
func idsMatchingFilter(t *testing.T, tbl *table.Table, opts ...table.ScanOption) []int64 {
	t.Helper()

	_, itr, err := tbl.Scan(opts...).ToArrowRecords(context.Background())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		col := rec.Column(idIdx[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	slices.Sort(ids)

	return ids
}
