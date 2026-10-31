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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
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

func TestIncrementalChangelogReadLimitCutsABatch(t *testing.T) {
	ctx := t.Context()
	tbl := newV3RowLineageTestTable(t)
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

	_, records, err := tbl.NewIncrementalChangelogScan(
		table.WithSelectedFields("id"),
		table.WithLimit(2),
	).ToArrowRecords(ctx)
	require.NoError(t, err)
	got := collectChangelogRows(t, records)
	require.Equal(t, []changelogRow{
		{id: 1, op: string(table.ChangelogOpInsert), ordinal: 0},
		{id: 2, op: string(table.ChangelogOpInsert), ordinal: 0},
	}, clearChangelogSnapshot(got))
}

func TestIncrementalChangelogReadReleasesCheckedAllocator(t *testing.T) {
	tbl := newV3RowLineageTestTable(t)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	for _, rows := range []string{
		`[{"id": 1, "data": "a"}]`,
		`[{"id": 2, "data": "b"}, {"id": 3, "data": "c"}]`,
	} {
		data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{rows})
		require.NoError(t, err)
		t.Cleanup(data.Release)
		tbl, err = tbl.Append(t.Context(), array.NewTableReader(data, -1), nil)
		require.NoError(t, err)
	}

	read := func(t *testing.T, stopEarly bool) {
		t.Helper()

		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		ctx := compute.WithAllocator(t.Context(), mem)
		defer mem.AssertSize(t, 0)

		_, records, err := tbl.NewIncrementalChangelogScan(
			table.WithSelectedFields("id"),
		).ToArrowRecords(ctx)
		require.NoError(t, err)
		for rec, err := range records {
			require.NoError(t, err)
			rec.Release()
			if stopEarly {
				break
			}
		}
	}

	t.Run("full drain", func(t *testing.T) { read(t, false) })
	t.Run("early break", func(t *testing.T) { read(t, true) })
}

func TestIncrementalChangelogReadAppliesFileDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := newV2RowLineageTestTable(t)
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
	require.NoError(t, tx.NewRowDelta(nil).
		AddRows(changelogDataFile(t, dataPath, 3)).
		AddDeletes(changelogPosDeleteFile(t, posPath)).
		Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	replacementSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	replacement, err := array.TableFromJSON(memory.DefaultAllocator, replacementSchema, []string{
		`[{"id": 4, "data": "d"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(replacement.Release)
	tbl, err = tbl.Overwrite(ctx, array.NewTableReader(replacement, -1), nil, table.WithOverwriteConcurrency(1))
	require.NoError(t, err)

	_, records, err := tbl.NewIncrementalChangelogScan().ToArrowRecords(ctx)
	require.NoError(t, err)
	got := collectChangelogRows(t, records)
	require.Equal(t, []int64{1, 3}, changelogIDs(got, table.ChangelogOpInsert, 0))
	require.Equal(t, []int64{1, 3}, changelogIDs(got, table.ChangelogOpDelete, 1))
	require.Equal(t, []int64{4}, changelogIDs(got, table.ChangelogOpInsert, 1))
}

func TestIncrementalChangelogReadListColumn(t *testing.T) {
	ctx := t.Context()
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "tags", Type: &iceberg.ListType{
			ElementID: 3, Element: iceberg.PrimitiveTypes.String,
		}},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	tbl := table.New(table.Identifier{"db", "changelog_list"}, meta, metaLoc, fsF,
		&concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF})

	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id": 1, "tags": ["red", "blue"]}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	out, records, err := tbl.NewIncrementalChangelogScan().ToArrowRecords(ctx)
	require.NoError(t, err)
	tags, ok := out.Field(out.FieldIndices("tags")[0]).Type.(*arrow.ListType)
	require.True(t, ok)
	require.Equal(t, arrow.STRING, tags.Elem().ID())

	var seen int
	for rec, err := range records {
		require.NoError(t, err)
		list := rec.Column(out.FieldIndices("tags")[0]).(*array.List)
		values := list.ListValues().(*array.String)
		require.Equal(t, []string{"red", "blue"}, []string{values.Value(0), values.Value(1)})
		seen += int(rec.NumRows())
		rec.Release()
	}
	require.Equal(t, 1, seen)
}

func TestIncrementalChangelogReadLargeTypes(t *testing.T) {
	ctx := t.Context()
	tbl := newV2RowLineageTestTable(t)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id": 1, "data": "a"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	out, records, err := tbl.NewIncrementalChangelogScan(table.WithOptions(iceberg.Properties{
		table.ScanOptionArrowUseLargeTypes: "true",
	})).ToArrowRecords(ctx)
	require.NoError(t, err)
	require.Equal(t, arrow.LARGE_STRING, out.Field(out.FieldIndices("data")[0]).Type.ID())
	require.Equal(t, arrow.LARGE_STRING, out.Field(out.FieldIndices(table.ChangelogChangeTypeColumn)[0]).Type.ID())

	for rec, err := range records {
		require.NoError(t, err)
		require.Equal(t, "a", rec.Column(out.FieldIndices("data")[0]).(*array.LargeString).Value(0))
		require.Equal(t, string(table.ChangelogOpInsert), rec.Column(out.FieldIndices(table.ChangelogChangeTypeColumn)[0]).(*array.LargeString).Value(0))
		rec.Release()
	}
}

func TestIncrementalChangelogReadPreRangeDeleteStaysOutOfFileDelete(t *testing.T) {
	ctx := t.Context()
	tbl := newV2RowLineageTestTable(t)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}, {"id": 3, "data": "c"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)
	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	files, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, files, 1)
	dataPath := files[0].File.FilePath()
	posPath := tbl.Location() + "/data/pos-del.parquet"
	writeParquetFile(t, posPath, table.PositionalDeleteArrowSchema, fmt.Sprintf(`[{"file_path": %q, "pos": 1}]`, dataPath))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(changelogPosDeleteFile(t, posPath)).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	deleted := tbl.CurrentSnapshot().SnapshotID

	replacement, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id": 4, "data": "d"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(replacement.Release)
	tbl, err = tbl.Overwrite(ctx, array.NewTableReader(replacement, -1), nil, table.WithOverwriteConcurrency(1))
	require.NoError(t, err)

	got := collectChangelogRows(t, mustReadChangelog(t, ctx, tbl.NewIncrementalChangelogScan().FromSnapshotExclusive(deleted)))
	require.Equal(t, []int64{4}, changelogIDs(got, table.ChangelogOpInsert, 0))
	require.Equal(t, []int64{1, 3}, changelogIDs(got, table.ChangelogOpDelete, 0))
}

func TestIncrementalChangelogReadPreRangeSharedDeletionVectorStaysOut(t *testing.T) {
	ctx := t.Context()
	tbl, location, pathA, _, dvA, _ := newTableWithSharedPuffinDVs(t)
	// Replace one deletion vector in the shared Puffin. The sibling stays.
	replacementDV := writeDV(t, location, "dv-a2.puffin", pathA, []int64{1})
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(replacementDV).RemoveDeletes(dvA).Commit(ctx))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	removed := tbl.CurrentSnapshot().SnapshotID

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	replacement, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id": 5, "data": "e"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(replacement.Release)
	tbl, err = tbl.Overwrite(ctx, array.NewTableReader(replacement, -1), nil, table.WithOverwriteConcurrency(1))
	require.NoError(t, err)

	got := collectChangelogRows(t, mustReadChangelog(t, ctx, tbl.NewIncrementalChangelogScan().FromSnapshotExclusive(removed)))
	require.Equal(t, []int64{5}, changelogIDs(got, table.ChangelogOpInsert, 0))
	// id 2 is hidden by the replacement and id 3 by the sibling that stayed in
	// the shared Puffin. Both were committed before the range.
	require.Equal(t, []int64{1, 4}, changelogIDs(got, table.ChangelogOpDelete, 0))
}

func mustReadChangelog(t *testing.T, ctx context.Context, scan *table.IncrementalChangelogScan) iter.Seq2[arrow.RecordBatch, error] {
	t.Helper()

	_, records, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)

	return records
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
	ordinals := rec.Column(opIdx + 1).(*array.Int32)
	snapshots := rec.Column(opIdx + 2).(*array.Int64)

	rows := make([]changelogRow, 0, rec.NumRows())
	for i := range int(rec.NumRows()) {
		row := changelogRow{
			id:       ids.Value(i),
			op:       ops.Value(i),
			ordinal:  int64(ordinals.Value(i)),
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

func clearChangelogSnapshot(rows []changelogRow) []changelogRow {
	out := make([]changelogRow, len(rows))
	for i, row := range rows {
		row.snapshot = 0
		out[i] = row
	}

	return out
}

func changelogIDs(rows []changelogRow, op table.ChangelogOperation, ordinal int64) []int64 {
	var ids []int64
	for _, row := range rows {
		if row.op == string(op) && row.ordinal == ordinal {
			ids = append(ids, row.id)
		}
	}

	return ids
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
