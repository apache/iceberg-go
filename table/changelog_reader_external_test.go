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
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestIncrementalChangelogReadEmptyTable(t *testing.T) {
	schema, records, err := newV2RowLineageTestTable(t).NewIncrementalChangelogScan().ToArrowRecords(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{
		"id", "data",
		table.ChangelogChangeTypeColumn,
		table.ChangelogChangeOrdinalColumn,
		table.ChangelogCommitSnapshotIDColumn,
	}, schemaFieldNames(schema))

	require.Empty(t, collectChangelogRows(t, records))
}

func TestIncrementalChangelogReadFileChanges(t *testing.T) {
	ctx := t.Context()
	tbl := changelogOverwriteTable(t, ctx)

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 3)

	schema, records, err := tbl.NewIncrementalChangelogScan().Read(ctx, tasks)
	require.NoError(t, err)
	require.Equal(t, []string{
		"id", "data",
		table.ChangelogChangeTypeColumn,
		table.ChangelogChangeOrdinalColumn,
		table.ChangelogCommitSnapshotIDColumn,
	}, schemaFieldNames(schema))

	got := collectChangelogRows(t, records)
	require.Equal(t, []changelogRow{
		{id: 1, data: "a", op: string(table.ChangelogOpInsert), ordinal: int64(tasks[0].ChangeOrdinal()), snapshot: tasks[0].CommitSnapshotID()},
		{id: 1, data: "a", op: string(table.ChangelogOpDelete), ordinal: int64(tasks[1].ChangeOrdinal()), snapshot: tasks[1].CommitSnapshotID()},
		{id: 2, data: "b", op: string(table.ChangelogOpInsert), ordinal: int64(tasks[2].ChangeOrdinal()), snapshot: tasks[2].CommitSnapshotID()},
	}, got)
	require.NotEqual(t, tasks[0].CommitSnapshotID(), tasks[1].CommitSnapshotID())
	require.Equal(t, tasks[1].CommitSnapshotID(), tasks[2].CommitSnapshotID())
	require.Equal(t, 0, tasks[0].ChangeOrdinal())
	require.Equal(t, 1, tasks[1].ChangeOrdinal())
	require.Equal(t, 1, tasks[2].ChangeOrdinal())
}

func TestIncrementalChangelogReadHonorsProjectionFilterAndLimit(t *testing.T) {
	ctx := t.Context()
	tbl := changelogOverwriteTable(t, ctx)

	schema, records, err := tbl.NewIncrementalChangelogScan(
		table.WithSelectedFields("id"),
		table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int64(2))),
		table.WithLimit(1),
	).ToArrowRecords(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{
		"id",
		table.ChangelogChangeTypeColumn,
		table.ChangelogChangeOrdinalColumn,
		table.ChangelogCommitSnapshotIDColumn,
	}, schemaFieldNames(schema))

	got := collectChangelogRows(t, records)
	require.Equal(t, []changelogRow{
		{id: 2, op: string(table.ChangelogOpInsert), ordinal: 1},
	}, clearChangelogSnapshot(got))
}

func changelogOverwriteTable(t *testing.T, ctx context.Context) *table.Table {
	t.Helper()

	mem := memory.DefaultAllocator
	tbl := newV3RowLineageTestTable(t)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	initial, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 1, "data": "a"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(initial.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(initial, -1), nil)
	require.NoError(t, err)

	replacement, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 2, "data": "b"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(replacement.Release)
	tbl, err = tbl.Overwrite(ctx, array.NewTableReader(replacement, -1), nil, table.WithOverwriteConcurrency(1))
	require.NoError(t, err)

	return tbl
}

type changelogRow struct {
	id       int64
	data     string
	op       string
	ordinal  int64
	snapshot int64
}

func collectChangelogRows(t *testing.T, records iter.Seq2[arrow.RecordBatch, error]) []changelogRow {
	t.Helper()

	var got []changelogRow
	for rec, err := range records {
		require.NoError(t, err)

		got = append(got, changelogRowsFromBatch(rec)...)
		rec.Release()
	}

	return got
}

func changelogRowsFromBatch(rec arrow.RecordBatch) []changelogRow {
	ids := rec.Column(0).(*array.Int64)
	opIdx := 1
	var data *array.String
	if rec.NumCols() == 5 {
		data = rec.Column(1).(*array.String)
		opIdx = 2
	}
	ops := rec.Column(opIdx).(*array.String)
	ordinals := rec.Column(opIdx + 1).(*array.Int64)
	snapshots := rec.Column(opIdx + 2).(*array.Int64)

	rows := make([]changelogRow, 0, rec.NumRows())
	for i := range int(rec.NumRows()) {
		row := changelogRow{
			id:       ids.Value(i),
			op:       ops.Value(i),
			ordinal:  ordinals.Value(i),
			snapshot: snapshots.Value(i),
		}
		if data != nil && !data.IsNull(i) {
			row.data = data.Value(i)
		}
		rows = append(rows, row)
	}

	return rows
}

func schemaFieldNames(schema *arrow.Schema) []string {
	names := make([]string, schema.NumFields())
	for i := range names {
		names[i] = schema.Field(i).Name
	}

	return names
}

func TestIncrementalChangelogReadPositionDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := appendChangelogRows(t, ctx, newChangelogMORTable(t, 2))
	tbl = deleteChangelogID(t, ctx, tbl, 2)
	firstDelete := tbl.CurrentSnapshot().SnapshotID
	tbl = deleteChangelogID(t, ctx, tbl, 3)

	got := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan())
	require.Equal(t, []int64{1, 2, 3}, changelogIDs(got, table.ChangelogOpInsert))
	require.Equal(t, []int64{2, 3}, changelogIDs(got, table.ChangelogOpDelete))

	later := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan().FromSnapshotExclusive(firstDelete))
	require.Equal(t, []int64{3}, changelogIDs(later, table.ChangelogOpDelete))
	require.Empty(t, changelogIDs(later, table.ChangelogOpInsert))

	filtered := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan(
		table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int64(2))),
	))
	require.Equal(t, []int64{2}, changelogIDs(filtered, table.ChangelogOpInsert))
	require.Equal(t, []int64{2}, changelogIDs(filtered, table.ChangelogOpDelete))
}

func TestIncrementalChangelogReadDeletionVectors(t *testing.T) {
	ctx := t.Context()
	tbl := appendChangelogRows(t, ctx, newChangelogMORTable(t, 3))
	tbl = deleteChangelogID(t, ctx, tbl, 2)

	got := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan())
	require.Equal(t, []int64{1, 2, 3}, changelogIDs(got, table.ChangelogOpInsert))
	require.Equal(t, []int64{2}, changelogIDs(got, table.ChangelogOpDelete))
	require.Equal(t, "b", changelogData(got, table.ChangelogOpDelete)[0])
}

func TestIncrementalChangelogReadEqualityDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := newChangelogMORTable(t, 2)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSchema, `[
		{"id": 1, "data": "a"},
		{"id": 2, "data": "b"},
		{"id": 3, "data": "c"}
	]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, []string{dataPath}, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	eqPath := tbl.Location() + "/data/eq-del.parquet"
	eqSchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, eqPath, eqSchema, `[{"id": 2}]`)

	eqBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	eqBuilder.EqualityFieldIDs([]int{1})

	tx = tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(eqBuilder.Build())
	require.NoError(t, rd.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	got := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan())
	require.Equal(t, []int64{1, 2, 3}, changelogIDs(got, table.ChangelogOpInsert))
	require.Equal(t, []int64{2}, changelogIDs(got, table.ChangelogOpDelete))
}

func TestIncrementalChangelogReadSameSnapshotPositionDelete(t *testing.T) {
	ctx := t.Context()
	tbl := newChangelogMORTable(t, 2)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, true, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSchema, `[
		{"id": 1, "data": "a"},
		{"id": 2, "data": "b"},
		{"id": 3, "data": "c"}
	]`)
	posPath := tbl.Location() + "/data/pos-del.parquet"
	writeParquetFile(t, posPath, table.PositionalDeleteArrowSchema, fmt.Sprintf(`[{"file_path": %q, "pos": 1}]`, dataPath))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(changelogDataFile(t, dataPath, 3)).AddDeletes(changelogPosDeleteFile(t, posPath)).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	got := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan())
	require.Equal(t, []int64{1, 3}, changelogIDs(got, table.ChangelogOpInsert))
	require.Empty(t, changelogIDs(got, table.ChangelogOpDelete))
}

func TestIncrementalChangelogReadReplacedDeletionVector(t *testing.T) {
	ctx := t.Context()
	tbl := appendChangelogRows(t, ctx, newChangelogMORTable(t, 3))
	tbl = deleteChangelogID(t, ctx, tbl, 2)
	firstDelete := tbl.CurrentSnapshot().SnapshotID
	tbl = deleteChangelogID(t, ctx, tbl, 3)

	got := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan())
	require.Equal(t, []int64{1, 2, 3}, changelogIDs(got, table.ChangelogOpInsert))
	require.Equal(t, []int64{2, 3}, changelogIDs(got, table.ChangelogOpDelete))

	later := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan().FromSnapshotExclusive(firstDelete))
	require.Empty(t, changelogIDs(later, table.ChangelogOpInsert))
	require.Equal(t, []int64{3}, changelogIDs(later, table.ChangelogOpDelete))
}

func TestIncrementalChangelogReadSkipsReplaceBeforeRowDelete(t *testing.T) {
	ctx := t.Context()
	tbl := newChangelogMORTable(t, 3)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	for _, rows := range []string{
		`[{"id": 1, "data": "a"}]`,
		`[{"id": 2, "data": "b"}, {"id": 3, "data": "c"}]`,
	} {
		data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{rows})
		require.NoError(t, err)
		t.Cleanup(data.Release)
		tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)
	_, tbl = runRewriteWithCleanup(t, tbl, toTaskGroups(plan.Groups), false)
	replaceID := tbl.CurrentSnapshot().SnapshotID
	require.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)

	tbl = deleteChangelogID(t, ctx, tbl, 2)
	got := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan())
	require.Equal(t, []int64{1, 2, 3}, changelogIDs(got, table.ChangelogOpInsert))
	require.Equal(t, []int64{2}, changelogIDs(got, table.ChangelogOpDelete))
	for _, row := range got {
		require.NotEqual(t, replaceID, row.snapshot)
	}
}

func TestIncrementalChangelogReadPartitionedRowFilter(t *testing.T) {
	ctx := t.Context()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "part", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})
	tbl := newChangelogTable(t, 2, schema, &spec)

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id": 1, "data": "a", "part": 0}, {"id": 2, "data": "b", "part": 0}, {"id": 3, "data": "c", "part": 1}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	files, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, files, 2)

	tbl = deleteChangelogID(t, ctx, tbl, 2)
	filtered := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan(
		table.WithSelectedFields("id"),
		table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int64(2))),
	))
	require.Equal(t, []int64{2}, changelogIDs(filtered, table.ChangelogOpInsert))
	require.Equal(t, []int64{2}, changelogIDs(filtered, table.ChangelogOpDelete))

	partZero := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan(
		table.WithSelectedFields("id"),
		table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("part"), int64(0))),
	))
	require.Equal(t, []int64{1, 2}, changelogIDs(partZero, table.ChangelogOpInsert))
	require.Equal(t, []int64{2}, changelogIDs(partZero, table.ChangelogOpDelete))

	partOne := readChangelog(t, ctx, tbl.NewIncrementalChangelogScan(
		table.WithSelectedFields("id"),
		table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("part"), int64(1))),
	))
	require.Equal(t, []int64{3}, changelogIDs(partOne, table.ChangelogOpInsert))
	require.Empty(t, changelogIDs(partOne, table.ChangelogOpDelete))
}

func TestIncrementalChangelogReadDeletedRowLineage(t *testing.T) {
	ctx := t.Context()
	tbl := appendChangelogRows(t, ctx, newChangelogMORTable(t, 3))
	tbl = deleteChangelogID(t, ctx, tbl, 2)

	tasks, err := tbl.NewIncrementalChangelogScan(table.WithRowLineage()).PlanFiles(ctx)
	require.NoError(t, err)
	var deleted table.DeletedRowsScanTask
	var found bool
	for _, task := range tasks {
		rows, ok := task.(table.DeletedRowsScanTask)
		if !ok {
			continue
		}
		deleted = rows
		found = true

		break
	}
	require.True(t, found)
	require.NotNil(t, deleted.ScanTask().File.FirstRowID())
	require.NotNil(t, deleted.ScanTask().DataSequenceNumber)

	schema, records, err := tbl.NewIncrementalChangelogScan(table.WithRowLineage()).Read(ctx, []table.ChangelogScanTask{deleted})
	require.NoError(t, err)
	require.Contains(t, schemaFieldNames(schema), iceberg.RowIDColumnName)
	require.Contains(t, schemaFieldNames(schema), iceberg.LastUpdatedSequenceNumberColumnName)

	var gotRowID, gotSeq int64
	var n int
	for rec, err := range records {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		rowIdx := rec.Schema().FieldIndices(iceberg.RowIDColumnName)
		seqIdx := rec.Schema().FieldIndices(iceberg.LastUpdatedSequenceNumberColumnName)
		opIdx := rec.Schema().FieldIndices(table.ChangelogChangeTypeColumn)
		require.NotEmpty(t, idIdx)
		require.NotEmpty(t, rowIdx)
		require.NotEmpty(t, seqIdx)
		require.NotEmpty(t, opIdx)

		ids := rec.Column(idIdx[0]).(*array.Int64)
		rowIDs := rec.Column(rowIdx[0]).(*array.Int64)
		seqs := rec.Column(seqIdx[0]).(*array.Int64)
		ops := rec.Column(opIdx[0]).(*array.String)
		for i := range int(rec.NumRows()) {
			require.Equal(t, string(table.ChangelogOpDelete), ops.Value(i))
			require.Equal(t, int64(2), ids.Value(i))
			require.False(t, rowIDs.IsNull(i))
			require.False(t, seqs.IsNull(i))
			gotRowID = rowIDs.Value(i)
			gotSeq = seqs.Value(i)
			n++
		}
		rec.Release()
	}
	require.Equal(t, 1, n)
	require.Equal(t, *deleted.ScanTask().File.FirstRowID()+1, gotRowID)
	require.Equal(t, *deleted.ScanTask().DataSequenceNumber, gotSeq)
}

func TestIncrementalChangelogReadSchemaEvolution(t *testing.T) {
	ctx := t.Context()
	tbl := appendChangelogRows(t, ctx, newChangelogMORTable(t, 3))
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.String, "", false, iceberg.StringLiteral("x")).
		Commit())
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	tbl = deleteChangelogID(t, ctx, tbl, 2)

	schema, records, err := tbl.NewIncrementalChangelogScan(table.WithSelectedFields("id", "extra")).ToArrowRecords(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "extra", table.ChangelogChangeTypeColumn, table.ChangelogChangeOrdinalColumn, table.ChangelogCommitSnapshotIDColumn}, schemaFieldNames(schema))

	got := collectChangelogRows(t, records)
	require.Equal(t, []int64{2}, changelogIDs(got, table.ChangelogOpDelete))
	require.Equal(t, []string{"x"}, changelogData(got, table.ChangelogOpDelete))
}

func newChangelogMORTable(t *testing.T, version int) *table.Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	return newChangelogTable(t, version, schema, iceberg.UnpartitionedSpec)
}

func newChangelogTable(t *testing.T, version int, schema *iceberg.Schema, spec *iceberg.PartitionSpec) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(schema, spec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: strconv.Itoa(version),
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "changelog_read"}, meta, metaLoc, fsF, cat)
}

func appendChangelogRows(t *testing.T, ctx context.Context, tbl *table.Table) *table.Table {
	t.Helper()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}, {"id": 3, "data": "c"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	return tbl
}

func changelogDataFile(t *testing.T, path string, records int64) iceberg.DataFile {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, records, info.Size())
	require.NoError(t, err)

	return b.Build()
}

func changelogPosDeleteFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, info.Size())
	require.NoError(t, err)

	return b.Build()
}

func deleteChangelogID(t *testing.T, ctx context.Context, tbl *table.Table, id int64) *table.Table {
	t.Helper()

	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), id), nil)
	require.NoError(t, err)

	return tbl
}

func readChangelog(t *testing.T, ctx context.Context, scan *table.IncrementalChangelogScan) []changelogRow {
	t.Helper()

	_, records, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)

	return collectChangelogRows(t, records)
}

func changelogIDs(rows []changelogRow, op table.ChangelogOperation) []int64 {
	var ids []int64
	for _, row := range rows {
		if row.op == string(op) {
			ids = append(ids, row.id)
		}
	}

	return ids
}

func changelogData(rows []changelogRow, op table.ChangelogOperation) []string {
	var data []string
	for _, row := range rows {
		if row.op == string(op) {
			data = append(data, row.data)
		}
	}

	return data
}

func clearChangelogSnapshot(rows []changelogRow) []changelogRow {
	out := make([]changelogRow, len(rows))
	for i, row := range rows {
		row.snapshot = 0
		out[i] = row
	}

	return out
}
