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
	"iter"
	"path/filepath"
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
	return readRowIDsByTasks(t, ctx, tbl, nil)
}

func readRowIDsByTasks(t *testing.T, ctx context.Context, tbl *table.Table, tasks []table.FileScanTask) map[int64]int64 {
	t.Helper()

	scan := tbl.Scan(table.WithRowLineage())
	var (
		itr iter.Seq2[arrow.RecordBatch, error]
		err error
	)
	if tasks == nil {
		_, itr, err = scan.ToArrowRecords(ctx)
	} else {
		_, itr, err = scan.ReadTasks(ctx, tasks)
	}
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
		for i := 0; i < int(rec.NumRows()); i++ {
			require.False(t, rowIDCol.IsNull(i), "_row_id must be non-null for id=%d", idCol.Value(i))
			got[idCol.Value(i)] = rowIDCol.Value(i)
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
		for i := 0; i < col.Len(); i++ {
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
		for i := 0; i < int(rec.NumRows()); i++ {
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
		for i := 0; i < int(rec.NumRows()); i++ {
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
	require.Equal(t, 3, tbl.Metadata().Version())

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
}

// TestExecuteCompactionGroupPreservesLineageSubsetOnMixedTasks verifies that
// mixed-task compaction on a v3 table no longer drops row-lineage columns
// for all files in the group. Lineage-capable files remain preserved while
// legacy files without a lineage marker are rewritten in a separate output
// stream.
func TestExecuteCompactionGroupPreservesLineageSubsetOnMixedTasks(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	first, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(first.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(first, -1), nil)
	require.NoError(t, err)

	second, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 3, "data": "c"}, {"id": 4, "data": "d"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(second.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(second, -1), nil)
	require.NoError(t, err)

	pre := readRowIDsByID(t, ctx, tbl)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	// Simulate a mixed lineage group by forcing one task to look like a legacy
	// file (no inherited FirstRowID). The mixed-input behavior was previously a
	// silent drop for all surviving rows.
	legacyRows := readRowIDsByTasks(t, ctx, tbl, []table.FileScanTask{tasks[1]})
	tasks[1].FirstRowID = nil

	group := table.CompactionTaskGroup{
		PartitionKey:   "single",
		Tasks:          tasks,
		TotalSizeBytes: tasks[0].File.FileSizeBytes() + tasks[1].File.FileSizeBytes(),
	}

	gr, err := table.ExecuteCompactionGroup(ctx, tbl, group)
	require.NoError(t, err)
	require.Equal(t, 2, len(gr.OldDataFiles), "both source files should be replaced")

	tx := tbl.NewTransaction()
	rewrite := tx.NewRewrite(nil)
	rewrite.ApplyResult(gr)
	require.NoError(t, rewrite.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	post := readRowIDsByID(t, ctx, tbl)
	for id, before := range pre {
		if _, ok := legacyRows[id]; ok {
			assert.NotEqual(t, before, post[id],
				"legacy-task rows should be rewritten without legacy lineage")

			continue
		}
		assert.Equal(t, before, post[id], "lineage-capable file rows should keep their original _row_id")
	}
}
