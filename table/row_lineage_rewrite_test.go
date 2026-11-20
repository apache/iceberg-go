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
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newV3RowLineageTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "row_lineage_test"}, meta, metaLoc, fsF, cat)
}

func newV2RowLineageTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "row_lineage_test"}, meta, metaLoc, fsF, cat)
}

func readRowIDsByID(t *testing.T, ctx context.Context, tbl *table.Table) map[int64]int64 {
	return readLineageColumnByID(t, ctx, tbl, iceberg.RowIDColumnName)
}

func readLineageColumnByID(t *testing.T, ctx context.Context, tbl *table.Table, column string) map[int64]int64 {
	t.Helper()

	return readLineageColumnFromScan(t, ctx, tbl.Scan(table.WithRowLineage()), column)
}

func readLineageColumnFromScan(t *testing.T, ctx context.Context, scan *table.Scan, column string) map[int64]int64 {
	t.Helper()

	_, itr, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)

	got := map[int64]int64{}
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		lineageIdx := rec.Schema().FieldIndices(column)
		require.NotEmpty(t, lineageIdx, "%s must be projected", column)

		idCol := rec.Column(idIdx[0]).(*array.Int64)
		lineageCol := rec.Column(lineageIdx[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			require.False(t, lineageCol.IsNull(i), "%s must be non-null for id=%d", column, idCol.Value(i))
			got[idCol.Value(i)] = lineageCol.Value(i)
		}
		rec.Release()
	}

	return got
}

// TestCoWRewritePreservesRowID verifies that a copy-on-write overwrite with a
// row filter preserves the original _row_id and _last_updated_sequence_number
// values in the rewritten file. Surviving rows must keep both values from the
// pre-rewrite snapshot — the rewrite is "physically rewritten", not "logically
// updated", per the v3 spec.
func TestCoWRewritePreservesRowID(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV3RowLineageTestTable(t)

	// Append 3 rows: id=1,2,3
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	initialData, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}, {"id": 3, "data": "c"}]`,
	})
	require.NoError(t, err)
	defer initialData.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(initialData, -1), nil)
	require.NoError(t, err)

	// Verify the append created a valid v3 snapshot with row lineage.
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	require.NotNil(t, snap.FirstRowID, "v3 snapshot must have first-row-id")
	require.NotNil(t, snap.AddedRows, "v3 snapshot must have added-rows")
	assert.Equal(t, int64(0), *snap.FirstRowID)
	assert.Equal(t, int64(3), *snap.AddedRows)

	// Capture the snapshot's sequence number so we can assert preservation
	// after the rewrite. After the append, every row's effective
	// _last_updated_sequence_number should be this value.
	createSeq := snap.SequenceNumber

	// Scan with row lineage to see synthesized _row_id values before the rewrite.
	lineageScan := tbl.Scan(table.WithRowLineage())
	schema, itr, err := lineageScan.ToArrowRecords(ctx)
	require.NoError(t, err)

	rowIDIdx := -1
	for i, f := range schema.Fields() {
		if f.Name == iceberg.RowIDColumnName {
			rowIDIdx = i

			break
		}
	}
	require.GreaterOrEqual(t, rowIDIdx, 0, "_row_id should be in scan projection")

	var originalRowIDs []int64
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(rowIDIdx).(*array.Int64)
		for i := range col.Len() {
			originalRowIDs = append(originalRowIDs, col.Value(i))
		}
		rec.Release()
	}
	require.Equal(t, []int64{0, 1, 2}, originalRowIDs, "initial _row_id should be 0,1,2")

	// CoW overwrite: delete the row where id=2, preserving id=1 and id=3.
	filter := iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	tbl, err = tbl.Delete(ctx, filter, nil)
	require.NoError(t, err)

	snap = tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	require.Greater(t, snap.SequenceNumber, createSeq,
		"sanity: rewrite snapshot must have a higher sequence number than the create snapshot")

	// Scan the result with row lineage. The surviving rows should preserve their
	// original _row_id values: 0 and 2. Their _last_updated_sequence_number must
	// also still report the create snapshot's seq, NOT the rewrite snapshot's
	// seq — the rewrite is physical only, not a logical update.
	lineageScan = tbl.Scan(table.WithRowLineage())
	_, itr, err = lineageScan.ToArrowRecords(ctx)
	require.NoError(t, err)

	var afterRowIDs []int64
	var afterIDs []int64
	var afterSeq []int64
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		rowIDIndices := rec.Schema().FieldIndices(iceberg.RowIDColumnName)
		require.NotEmpty(t, rowIDIndices)
		seqIndices := rec.Schema().FieldIndices(iceberg.LastUpdatedSequenceNumberColumnName)
		require.NotEmpty(t, seqIndices, "_last_updated_sequence_number must be in projection")

		idCol := rec.Column(idIdx[0]).(*array.Int64)
		rowIDCol := rec.Column(rowIDIndices[0]).(*array.Int64)
		seqCol := rec.Column(seqIndices[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			afterIDs = append(afterIDs, idCol.Value(i))
			afterRowIDs = append(afterRowIDs, rowIDCol.Value(i))
			require.False(t, seqCol.IsNull(i),
				"row %d must have a non-null _last_updated_sequence_number after CoW rewrite", i)
			afterSeq = append(afterSeq, seqCol.Value(i))
		}
		rec.Release()
	}

	assert.Equal(t, []int64{1, 3}, afterIDs, "remaining rows should be id=1,3")
	assert.Equal(t, []int64{0, 2}, afterRowIDs,
		"_row_id must be preserved through CoW rewrite: row with id=1 keeps _row_id=0, row with id=3 keeps _row_id=2")
	assert.Equal(t, []int64{createSeq, createSeq}, afterSeq,
		"_last_updated_sequence_number must report the original creation snapshot's sequence number, not the rewrite's")
}

// TestCoWRewritePrunesRowGroupsBeforeLineageSynthesis verifies that the
// rewrite path can prune a non-matching row group without renumbering rows in
// the group that survives. The row filter is still applied after lineage
// synthesis, while Parquet statistics can skip the first group entirely.
func TestCoWRewritePrunesRowGroupsBeforeLineageSynthesis(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV3RowLineageTestTable(t)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{
		table.ParquetRowGroupLimitKey: "5",
	}))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	require.Equal(t, 2, parquetRowGroupCount(t, tbl), "fixture must have two row groups")

	// Delete ids 1..5, which occupy the first row group. The survivor filter
	// can therefore skip that group by statistics while the second group's
	// rows retain their original IDs.
	tbl, err = tbl.Delete(ctx, iceberg.LessThanEqual(iceberg.Reference("id"), int64(5)), nil)
	require.NoError(t, err)

	assert.Equal(t, map[int64]int64{
		6: 5, 7: 6, 8: 7, 9: 8, 10: 9,
	}, readRowIDsByID(t, ctx, tbl))
}

func TestCoWRewriteDoesNotPruneMissingInitialDefault(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV3RowLineageTestTable(t)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{
		table.ParquetRowGroupLimitKey: "5",
	}))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	require.Equal(t, 2, parquetRowGroupCount(t, tbl), "fixture must have two row groups")

	// The existing file has no flag column, but its rows logically read flag=1.
	// The survivor complement is id > 5 AND flag IS NOT NULL, so pruning must
	// not translate the missing physical column to false before projection.
	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"flag"}, iceberg.PrimitiveTypes.Int32, "", false, iceberg.Int32Literal(1)).
		Commit())
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	deleteFilter := iceberg.NewOr(
		iceberg.LessThanEqual(iceberg.Reference("id"), int64(5)),
		iceberg.IsNull(iceberg.Reference("flag")),
	)
	tbl, err = tbl.Delete(ctx, deleteFilter, nil)
	require.NoError(t, err)

	assert.Equal(t, map[int64]int64{
		6: 5, 7: 6, 8: 7, 9: 8, 10: 9,
	}, readRowIDsByID(t, ctx, tbl))
}

func TestCoWRewriteNormalizesTheSurvivorComplement(t *testing.T) {
	ctx := context.Background()
	tbl := buildTwoRowGroupTable(t, newV3RowLineageTestTable(t))

	var err error
	tbl, err = tbl.Delete(ctx,
		iceberg.NotEqualTo(iceberg.Reference("id"), int64(7)), nil)
	require.NoError(t, err)

	assert.Equal(t, map[int64]int64{7: 6}, readRowIDsByID(t, ctx, tbl),
		"the complement of id != 7 must retain only the original row at position 6")
}

// TestCoWRewriteRowIDNextRowIDAccounting verifies that row-id accounting remains
// correct after a CoW rewrite. The overcounting (where next-row-id advances by
// the full manifest row count including preserved survivors) is intentional and
// matches Java's ManifestListWriter.V3Writer behavior.
func TestCoWRewriteRowIDNextRowIDAccounting(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	data, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 10, "data": "x"}, {"id": 20, "data": "y"}, {"id": 30, "data": "z"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	// next-row-id should be 3 after appending 3 rows.
	assert.Equal(t, int64(3), tbl.Metadata().NextRowID())

	// Delete one row via CoW.
	filter := iceberg.EqualTo(iceberg.Reference("id"), int64(20))
	tbl, err = tbl.Delete(ctx, filter, nil)
	require.NoError(t, err)

	// next-row-id advances by the new manifest's added-rows count (2 here),
	// even though the surviving rows preserve their old IDs. Going from 3
	// (after the initial append) to 5 (= prior NextRowID + manifest's added
	// rows). This "wastes" ID space but doesn't violate uniqueness — actual
	// row IDs come from the explicit Parquet column, not the global counter.
	// Mirrors Java's ManifestListWriter.V3Writer.prepare() in
	// table/snapshot_producers.go.
	assert.Equal(t, int64(5), tbl.Metadata().NextRowID(),
		"next-row-id should advance from 3 by the rewrite manifest's 2 added rows")
}

// TestExecuteCompactionGroupPreservesRowID verifies that
// ExecuteCompactionGroup preserves _row_id values through compaction on a v3
// table. After compaction every row should retain its original _row_id even
// though the underlying file paths have changed.
func TestExecuteCompactionGroupPreservesRowID(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Two appends produce two data files in two snapshots; row IDs should
	// span both files (0..1 in the first, 2..3 in the second).
	for i, payload := range []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}]`,
		`[{"id": 3, "data": "c"}, {"id": 4, "data": "d"}]`,
	} {
		data, err := array.TableFromJSON(mem, arrowSchema, []string{payload})
		require.NoError(t, err, "append %d", i)
		t.Cleanup(data.Release)

		tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
		require.NoError(t, err, "append %d", i)
	}

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "two source files for compaction")

	scanTasks := make([]table.FileScanTask, len(tasks))
	var totalSize int64
	for i, st := range tasks {
		scanTasks[i] = st
		totalSize += st.File.FileSizeBytes()
	}
	group := table.CompactionTaskGroup{
		PartitionKey:   "single",
		Tasks:          scanTasks,
		TotalSizeBytes: totalSize,
	}

	gr, err := table.ExecuteCompactionGroup(ctx, tbl, group)
	require.NoError(t, err)
	require.Equal(t, 2, len(gr.OldDataFiles), "both source files should be replaced")
	require.GreaterOrEqual(t, len(gr.NewDataFiles), 1, "compaction should produce at least one output file")

	tx := tbl.NewTransaction()
	rewrite := tx.NewRewrite(nil)
	rewrite.ApplyResult(gr)
	require.NoError(t, rewrite.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// Read back with row lineage projected; every row should retain its
	// pre-compaction _row_id (0,1,2,3).
	_, itr, err := tbl.Scan(table.WithRowLineage()).ToArrowRecords(ctx)
	require.NoError(t, err)

	got := map[int64]int64{}
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		rowIDIdx := rec.Schema().FieldIndices(iceberg.RowIDColumnName)
		require.NotEmpty(t, rowIDIdx, "_row_id must be projected")

		idCol := rec.Column(idIdx[0]).(*array.Int64)
		rowIDCol := rec.Column(rowIDIdx[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			got[idCol.Value(i)] = rowIDCol.Value(i)
		}
		rec.Release()
	}

	assert.Equal(t,
		map[int64]int64{1: 0, 2: 1, 3: 2, 4: 3},
		got,
		"compaction must preserve every row's _row_id")
}

// TestExecuteCompactionGroupPreservesRowIDAcrossV2Upgrade verifies that
// compaction preserves _row_id when a group mixes a pre-upgrade v2-era data
// file (written into a v2-format manifest, no row lineage) with a v3 data file.
//
// The first commit after upgrading to v3 assigns a first_row_id to every data
// manifest in the new v3 manifest list — including the carried-over v2-era one
// (spec: First Row ID Inheritance, "even if the data file is existing"). The
// v2-era manifest keeps its internal format-version 2, so the read path must
// gate row-id inheritance on that manifest-list first_row_id, not on the
// manifest's own version. If it does not, the v2-era file's FirstRowID stays
// nil, the group looks mixed-lineage, and compaction reassigns every _row_id.
func TestExecuteCompactionGroupPreservesRowIDAcrossV2Upgrade(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV2RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Append while still v2: produces a v2-format manifest with no row lineage.
	v2Data, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(v2Data.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(v2Data, -1), nil)
	require.NoError(t, err)

	// Upgrade to v3 in place, then append again. The v3 append is the first v3
	// commit, so its manifest list assigns first_row_id to both the new and the
	// carried-over v2-era manifest.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, tbl.Metadata().FormatVersion())

	v3Data, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 3, "data": "c"}, {"id": 4, "data": "d"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(v3Data.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(v3Data, -1), nil)
	require.NoError(t, err)

	// The planner must see a non-null inherited FirstRowID on BOTH files — the
	// v2-era one included. This is the exact mechanism the inheritance gate
	// restores; without it the v2-era task's FirstRowID is nil.
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "two source files for compaction")

	scanTasks := make([]table.FileScanTask, len(tasks))
	var totalSize int64
	for i, st := range tasks {
		require.NotNilf(t, st.FirstRowID,
			"every data file in a committed v3 snapshot must carry an inherited first_row_id, including the v2-era file (path %s)",
			st.File.FilePath())
		scanTasks[i] = st
		totalSize += st.File.FileSizeBytes()
	}

	// Baseline the engine-assigned _row_id for every row before compaction. The
	// four rows must form one contiguous 0-based lineage with no overlap across the
	// v2->v3 boundary: manifest-list order puts the newest (v3) manifest first, so
	// the v3 file (id 3,4) inherits [0,1] and the carried-over v2-era file (id 1,2)
	// inherits [2,3]. A gap, overlap, or duplicate means inheritance mis-assigned a
	// range.
	pre := readRowIDsByID(t, ctx, tbl)
	assert.Equal(t, map[int64]int64{3: 0, 4: 1, 1: 2, 2: 3}, pre,
		"each row's inherited _row_id must be distinct and contiguous from 0")
	preSeq := readLineageColumnByID(t, ctx, tbl, iceberg.LastUpdatedSequenceNumberColumnName)

	group := table.CompactionTaskGroup{
		PartitionKey:   "single",
		Tasks:          scanTasks,
		TotalSizeBytes: totalSize,
	}
	gr, err := table.ExecuteCompactionGroup(ctx, tbl, group)
	require.NoError(t, err)
	require.Equal(t, 2, len(gr.OldDataFiles), "both source files should be replaced")
	require.GreaterOrEqual(t, len(gr.NewDataFiles), 1, "compaction should produce at least one output file")

	tx = tbl.NewTransaction()
	rewrite := tx.NewRewrite(nil)
	rewrite.ApplyResult(gr)
	require.NoError(t, rewrite.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	post := readRowIDsByID(t, ctx, tbl)
	assert.Equal(t, pre, post,
		"compaction across a v2->v3 upgrade must preserve every row's _row_id, including the v2-era rows")
	assert.Equal(t, preSeq, readLineageColumnByID(t, ctx, tbl, iceberg.LastUpdatedSequenceNumberColumnName))
}

func TestCompactionRejectsIncompleteRowLineage(t *testing.T) {
	tbl := appendRows(t, newV3RowLineageTestTable(t), `[{"id":1},{"id":2}]`)
	tbl = appendRows(t, tbl, `[{"id":3},{"id":4}]`)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	before := parquetFiles(t, tbl.Location())
	for _, tc := range []struct {
		name    string
		clear   func([]table.FileScanTask)
		missing string
	}{
		{"missing data sequence", func(tasks []table.FileScanTask) {
			tasks[0].DataSequenceNumber = nil
		}, "data sequence number"},
		{"mixed first row IDs", func(tasks []table.FileScanTask) {
			tasks[0].FirstRowID = nil
		}, "first row ID"},
		{"both first row IDs missing", func(tasks []table.FileScanTask) {
			for i := range tasks {
				tasks[i].FirstRowID = nil
			}
		}, "first row ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := table.CompactionTaskGroup{Tasks: slices.Clone(tasks)}
			tc.clear(group.Tasks)

			_, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			require.ErrorContains(t, err, tc.missing)
			for _, partial := range []bool{false, true} {
				_, err := tbl.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{group},
					table.RewriteDataFilesOptions{PartialProgress: partial})
				require.ErrorIs(t, err, table.ErrInvalidOperation)
				require.ErrorContains(t, err, tc.missing)
			}
			require.ElementsMatch(t, before, parquetFiles(t, tbl.Location()))
		})
	}
}

func TestCompactionCanAssignFirstRowIDsOnLegacyBranch(t *testing.T) {
	const branch = "legacy"
	tbl := appendRowsOnRef(t, newV2RowLineageTestTable(t), branch, 1, 2)
	tbl = appendRowsOnRef(t, tbl, branch, 3, 4)
	upgrade := tbl.NewTransaction()
	require.NoError(t, upgrade.UpgradeFormatVersion(3))
	tbl, err := upgrade.Commit(t.Context())
	require.NoError(t, err)
	tbl = appendRows(t, tbl, `[{"id":5},{"id":6}]`)
	require.NotNil(t, tbl.CurrentSnapshot().FirstRowID)
	require.Nil(t, tbl.SnapshotByName(branch).FirstRowID)

	scan, err := tbl.Scan(table.WithRowLineage()).UseRef(branch)
	require.NoError(t, err)
	defer scan.Close()
	tasks, err := scan.PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	for _, task := range tasks {
		require.Nil(t, task.FirstRowID)
	}
	before := readLineageColumnFromScan(t, t.Context(), scan, iceberg.LastUpdatedSequenceNumberColumnName)
	require.Equal(t, map[int64]int64{1: 1, 2: 1, 3: 2, 4: 2}, before)

	txn, err := tbl.NewTransactionOnBranch(branch)
	require.NoError(t, err)
	incomplete := slices.Clone(tasks)
	incomplete[0].DataSequenceNumber = nil
	_, err = txn.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{{Tasks: incomplete}}, table.RewriteDataFilesOptions{})
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "data sequence number")

	result, err := txn.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{{Tasks: tasks}},
		table.RewriteDataFilesOptions{PartialProgress: true})
	require.NoError(t, err)
	require.Equal(t, 1, result.RewrittenGroups)
	require.NotNil(t, result.Table.SnapshotByName(branch).FirstRowID)
	compacted, err := result.Table.Scan(table.WithRowLineage()).UseRef(branch)
	require.NoError(t, err)
	defer compacted.Close()
	assert.Equal(t, before, readLineageColumnFromScan(t, t.Context(), compacted, iceberg.LastUpdatedSequenceNumberColumnName))
	assert.Len(t, readLineageColumnFromScan(t, t.Context(), compacted, iceberg.RowIDColumnName), 4)
}

func TestAtomicCompactionChecksLineageBeforeWritingAnyGroup(t *testing.T) {
	tbl := appendRows(t, newV3RowLineageTestTable(t), `[{"id":1},{"id":2}]`)
	tbl = appendRows(t, tbl, `[{"id":3},{"id":4}]`)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	tasks[1].DataSequenceNumber = nil
	before := parquetFiles(t, tbl.Location())

	groups := []table.CompactionTaskGroup{{Tasks: tasks[:1]}, {Tasks: tasks[1:]}}
	_, err = tbl.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{})
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "data sequence number")
	require.ElementsMatch(t, before, parquetFiles(t, tbl.Location()))
}

func TestCompactionCannotReplayAfterFirstRowIDsAreAssigned(t *testing.T) {
	tbl, cat := newConcurrentRewriteTestTable(t)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2)
	upgrade := tbl.NewTransaction()
	require.NoError(t, upgrade.UpgradeFormatVersion(3))
	tbl, err := upgrade.Commit(t.Context())
	require.NoError(t, err)
	require.Nil(t, tbl.CurrentSnapshot().FirstRowID)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Nil(t, tasks[0].FirstRowID)
	var assigned map[int64]int64
	cat.beforeCommit = func() {
		peer := appendRowsOnRef(t, tbl, table.MainBranch, 3, 4)
		assigned = readRowIDsByID(t, t.Context(), peer)
	}

	_, err = tbl.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{{Tasks: tasks}}, table.RewriteDataFilesOptions{})
	require.ErrorIs(t, err, table.ErrTransactionUnusable)
	require.ErrorContains(t, err, "replan")
	current, err := cat.LoadTable(t.Context(), tbl.Identifier())
	require.NoError(t, err)
	require.Len(t, assigned, 4)
	require.Equal(t, assigned, readRowIDsByID(t, t.Context(), current))
}

type rowLineageManifestHook struct {
	iceio.LocalFS
	path   string
	onOpen func()
}

func (f *rowLineageManifestHook) Open(ctx context.Context, name string) (iceio.File, error) {
	if name == f.path && f.onOpen != nil {
		hook := f.onOpen
		f.onOpen = nil
		hook()
	}

	return f.LocalFS.Open(ctx, name)
}

func TestCompactionRejectsRowIDsAssignedDuringStaging(t *testing.T) {
	for _, mode := range []string{"data-files", "with-position-delete"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			tbl, cat := newConcurrentRewriteTestTable(t)
			tbl = appendRows(t, tbl, `[{"id":1},{"id":2}]`)
			if mode == "with-position-delete" {
				tasks, err := tbl.Scan().PlanFiles(ctx)
				require.NoError(t, err)
				require.Len(t, tasks, 1)
				del := writeRowDeltaPositionDelete(t, tbl.Location(), "old", tasks[0].File.FilePath(), []int64{0})
				tx := tbl.NewTransaction()
				require.NoError(t, tx.NewRowDelta(nil).AddDeletes(del).Commit(ctx))
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
			}

			upgrade := tbl.NewTransaction()
			require.NoError(t, upgrade.UpgradeFormatVersion(3))
			tbl, err := upgrade.Commit(ctx)
			require.NoError(t, err)
			require.Nil(t, tbl.CurrentSnapshot().FirstRowID)
			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			worker, err := table.ExecuteCompactionGroup(ctx, tbl, table.CompactionTaskGroup{Tasks: tasks})
			require.NoError(t, err)
			require.Len(t, worker.NewDataFiles, 1)

			hook := &rowLineageManifestHook{path: tbl.CurrentSnapshot().ManifestList}
			wrapped := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(),
				func(context.Context) (iceio.IO, error) { return hook, nil }, cat)
			tx := wrapped.NewTransaction()
			var assigned map[int64]int64
			hook.onOpen = func() {
				rdr := rowsReader(t, tbl, `[{"id":3},{"id":4}]`)
				defer rdr.Release()
				require.NoError(t, tx.Append(ctx, rdr, nil))
				staged, err := tx.StagedTable()
				require.NoError(t, err)
				assigned = readRowIDsByID(t, ctx, staged.Table)
			}

			err = tx.NewRewrite(nil).ApplyResult(worker).Commit(ctx)
			require.NotEmpty(t, assigned, "append must stage while the rewrite reads the old parent")
			require.ErrorIs(t, err, table.ErrCommitFailed)
			require.ErrorContains(t, err, "replan")
			committed, err := tx.Commit(ctx)
			require.NoError(t, err)
			require.Equal(t, assigned, readRowIDsByID(t, ctx, committed))
		})
	}
}

func TestPartialCompactionPreservesFirstRowIDsAssignedByEarlierBatch(t *testing.T) {
	tbl := appendRows(t, newV2RowLineageTestTable(t), `[{"id":1},{"id":2}]`)
	tbl = appendRows(t, tbl, `[{"id":3},{"id":4}]`)
	upgrade := tbl.NewTransaction()
	require.NoError(t, upgrade.UpgradeFormatVersion(3))
	tbl, err := upgrade.Commit(t.Context())
	require.NoError(t, err)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskPath := tasks[1].File.FilePath()
	// The original sequence numbers are still visible on the legacy parent.
	wantSeqs := readLineageColumnByID(t, t.Context(), tbl, iceberg.LastUpdatedSequenceNumberColumnName)

	groups := []table.CompactionTaskGroup{{Tasks: tasks[:1]}, {Tasks: tasks[1:]}}
	result, err := tbl.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress: true,
		MaxCommits:      2,
	})
	require.NoError(t, err)
	require.Len(t, result.CompletedGroups, 2)
	require.Empty(t, result.FailedGroups)
	firstBatchID := *result.Table.CurrentSnapshot().ParentSnapshotID
	firstBatch := result.Table.Scan(table.WithRowLineage(), table.WithSnapshotID(firstBatchID))
	defer firstBatch.Close()
	assigned := readLineageColumnFromScan(t, t.Context(), firstBatch, iceberg.RowIDColumnName)
	require.Len(t, assigned, 4)
	require.Equal(t, assigned, readRowIDsByID(t, t.Context(), result.Table))
	require.Equal(t, wantSeqs, readLineageColumnByID(t, t.Context(), result.Table, iceberg.LastUpdatedSequenceNumberColumnName))
	remainingLineage := result.CompletedGroups[1].SourceRowLineage[taskPath]
	require.NotNil(t, remainingLineage.FirstRowID)
	require.Nil(t, tasks[1].FirstRowID, "refresh must not change the caller's plan")
}

func TestCompactionRejectsWorkerResultAfterFirstRowIDsAreAssigned(t *testing.T) {
	tbl := appendRows(t, newV2RowLineageTestTable(t), `[{"id":1},{"id":2}]`)
	upgrade := tbl.NewTransaction()
	require.NoError(t, upgrade.UpgradeFormatVersion(3))
	tbl, err := upgrade.Commit(t.Context())
	require.NoError(t, err)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	result, err := table.ExecuteCompactionGroup(t.Context(), tbl, table.CompactionTaskGroup{Tasks: tasks})
	require.NoError(t, err)
	require.Len(t, result.OldDataFiles, 1)
	require.Len(t, result.NewDataFiles, 1)
	require.Nil(t, result.OldDataFiles[0].FirstRowID())

	tbl = appendRows(t, tbl, `[{"id":3},{"id":4}]`)
	tx := tbl.NewTransaction()
	err = tx.NewRewrite(nil).ApplyResult(result).Commit(t.Context())
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "replan")
}

func TestCompactionRejectsStaleSourceRowLineage(t *testing.T) {
	for _, tc := range []struct {
		name, mode          string
		changedSequenceOnly bool
	}{
		{name: "atomic", mode: "atomic"},
		{name: "partial", mode: "partial"},
		{name: "distributed", mode: "distributed"},
		{name: "retry", mode: "retry"},
		{name: "changed sequence only", mode: "atomic", changedSequenceOnly: true},
		{name: "distributed changed sequence only", mode: "distributed", changedSequenceOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			tbl, cat := newConcurrentRewriteTestTableVersion(t, 3)
			tbl = appendRows(t, tbl, `[{"id":1},{"id":2}]`)
			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			source := tasks[0].File

			var wantIDs, wantSeqs map[int64]int64
			replaceSource := func() *table.Table {
				remove := tbl.NewTransaction()
				require.NoError(t, remove.NewRewrite(nil).DeleteFile(source).Commit(ctx))
				removed, err := remove.Commit(ctx)
				require.NoError(t, err)

				readd := removed.NewTransaction()
				if tc.changedSequenceOnly {
					require.NoError(t, readd.AddDataFiles(ctx, []iceberg.DataFile{source}, nil))
				} else {
					require.NoError(t, readd.AddDataFilesByPath(ctx, []string{source.FilePath()}, nil, false))
				}
				readded, err := readd.Commit(ctx)
				require.NoError(t, err)
				wantIDs = readRowIDsByID(t, ctx, readded)
				wantSeqs = readLineageColumnByID(t, ctx, readded, iceberg.LastUpdatedSequenceNumberColumnName)

				return readded
			}

			// Exercise the lineage checks using a task with only the source
			// file, scan range, and lineage fields.
			group := table.CompactionTaskGroup{Tasks: []table.FileScanTask{{
				File:               source,
				Start:              tasks[0].Start,
				Length:             tasks[0].Length,
				FirstRowID:         tasks[0].FirstRowID,
				DataSequenceNumber: tasks[0].DataSequenceNumber,
			}}}
			var current *table.Table
			if tc.mode == "retry" {
				cat.beforeCommit = func() { current = replaceSource() }
				_, err = tbl.RewriteDataFiles(ctx, []table.CompactionTaskGroup{group}, table.RewriteDataFilesOptions{})
				require.ErrorIs(t, err, table.ErrTransactionUnusable)
			} else {
				current = replaceSource()
				if tc.mode == "distributed" {
					worker, workerErr := table.ExecuteCompactionGroup(ctx, tbl, group)
					require.NoError(t, workerErr)
					err = current.NewTransaction().NewRewrite(nil).ApplyResult(worker).Commit(ctx)
				} else {
					before := parquetFiles(t, current.Location())
					_, err = current.RewriteDataFiles(ctx, []table.CompactionTaskGroup{group},
						table.RewriteDataFilesOptions{PartialProgress: tc.mode == "partial"})
					require.ElementsMatch(t, before, parquetFiles(t, current.Location()), "rejected rewrite must leave no outputs")
				}
			}
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			if tc.changedSequenceOnly {
				require.Equal(t, readRowIDsByID(t, ctx, tbl), wantIDs)
				require.ErrorContains(t, err, "data sequence number")
			} else {
				require.NotEqual(t, readRowIDsByID(t, ctx, tbl), wantIDs)
				require.ErrorContains(t, err, "first row ID")
			}
			require.NotEqual(t, readLineageColumnByID(t, ctx, tbl, iceberg.LastUpdatedSequenceNumberColumnName), wantSeqs)

			latest, err := cat.LoadTable(ctx, tbl.Identifier())
			require.NoError(t, err)
			require.Equal(t, current.CurrentSnapshot().SnapshotID, latest.CurrentSnapshot().SnapshotID)
			require.Equal(t, wantIDs, readRowIDsByID(t, ctx, latest))
			require.Equal(t, wantSeqs, readLineageColumnByID(t, ctx, latest, iceberg.LastUpdatedSequenceNumberColumnName))
		})
	}
}

type compactionDataFileWithoutRowID struct{ iceberg.DataFile }

func (compactionDataFileWithoutRowID) FirstRowID() *int64 { return nil }

func TestCompactionResultUsesEffectiveTaskRowID(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newV3RowLineageTestTable(t), `[{"id":1},{"id":2}]`)
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].FirstRowID)
	tasks[0].File = compactionDataFileWithoutRowID{tasks[0].File}

	result, err := table.ExecuteCompactionGroup(ctx, tbl, table.CompactionTaskGroup{Tasks: tasks})
	require.NoError(t, err)
	require.Equal(t, tasks[0].FirstRowID, result.SourceRowLineage[tasks[0].File.FilePath()].FirstRowID)
	tx := tbl.NewTransaction()
	// The coordinator must reject a worker result whose lineage was lost in transit.
	withoutLineage := result
	withoutLineage.SourceRowLineage = nil
	err = tx.NewRewrite(nil).ApplyResult(withoutLineage).Commit(ctx)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "missing source row lineage")

	require.NoError(t, tx.NewRewrite(nil).ApplyResult(result).Commit(ctx))
	rewritten, err := tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, readRowIDsByID(t, ctx, tbl), readRowIDsByID(t, ctx, rewritten))
}

func TestCompactionRejectsV2WorkerOutputAfterMetadataUpgrade(t *testing.T) {
	tbl := appendRows(t, newV2RowLineageTestTable(t), `[{"id":1},{"id":2}]`)
	tbl = appendRows(t, tbl, `[{"id":3},{"id":4}]`)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	group := table.CompactionTaskGroup{Tasks: tasks}
	oldResult, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
	require.NoError(t, err)
	require.False(t, oldResult.PreservedRowLineage)

	upgrade := tbl.NewTransaction()
	require.NoError(t, upgrade.UpgradeFormatVersion(3))
	tbl, err = upgrade.Commit(t.Context())
	require.NoError(t, err)
	require.Nil(t, tbl.CurrentSnapshot().FirstRowID)
	before := readLineageColumnByID(t, t.Context(), tbl, iceberg.LastUpdatedSequenceNumberColumnName)
	require.Equal(t, map[int64]int64{1: 1, 2: 1, 3: 2, 4: 2}, before)

	tx := tbl.NewTransaction()
	err = tx.NewRewrite(nil).ApplyResult(oldResult).Commit(t.Context())
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "v3 row lineage preservation")

	result, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
	require.NoError(t, err)
	require.True(t, result.PreservedRowLineage)
	require.NoError(t, tx.NewRewrite(nil).ApplyResult(result).Commit(t.Context()))
	compacted, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, readLineageColumnByID(t, t.Context(), compacted, iceberg.LastUpdatedSequenceNumberColumnName))
}
