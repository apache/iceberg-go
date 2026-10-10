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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
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

func clearChangelogSnapshot(rows []changelogRow) []changelogRow {
	out := make([]changelogRow, len(rows))
	for i, row := range rows {
		row.snapshot = 0
		out[i] = row
	}

	return out
}
