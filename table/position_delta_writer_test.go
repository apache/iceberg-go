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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionDeltaWriter_ReinsertPreservesRowID(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	tbl := newV3RowLineageTestTable(t)

	// Append initial data: id=1, id=2 → _row_id 0, 1 in the initial file.
	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	initial, err := array.TableFromJSON(mem, dataSchema, []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}]`,
	})
	require.NoError(t, err)
	defer initial.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(initial, -1), nil)
	require.NoError(t, err)

	// Capture the initial data file so we can replace it via NewRewrite.
	initialFiles, err := planFilesAll(ctx, tbl)
	require.NoError(t, err)
	require.Len(t, initialFiles, 1)
	initialFile := initialFiles[0]

	// Create a PositionDeltaWriter and stage:
	//   - Reinsert(id=1, _row_id=0) — survivor with updated value.
	//   - Reinsert(id=2, _row_id=1) — unchanged survivor.
	//   - Insert(id=99) — fresh row, gets synthesized _row_id at read time.
	w, err := table.NewPositionDeltaWriter(tbl)
	require.NoError(t, err)

	// Use _row_id values far outside any plausible synthesized first_row_id
	// range so the assertions actually prove preservation: if the writer
	// silently dropped _row_id and the reader fell back to synthesizing IDs
	// from the new file's first_row_id, the survivors would *not* land on
	// these high values. Picking small IDs (e.g. 0, 1) would be
	// indistinguishable from a synthesized 0/1 in the rewritten file.
	const (
		preservedRowID1 = int64(1) << 40
		preservedRowID2 = preservedRowID1 + 1
	)

	reinsertBatch := buildReinsertBatch(mem,
		[]int64{1, 2},
		[]string{"a_updated", "b"},
		[]int64{preservedRowID1, preservedRowID2},
	)
	defer reinsertBatch.Release()
	require.NoError(t, w.Reinsert(reinsertBatch))

	insertBatch := buildInsertBatch(mem, []int64{99}, []string{"new"})
	defer insertBatch.Release()
	require.NoError(t, w.Insert(insertBatch))

	dataFiles, err := w.Close(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, dataFiles)

	// Atomically replace the initial file with the writer's output. This is
	// a library-internal rewrite (NewRewrite skips the explicit first_row_id
	// requirement that AddDataFiles enforces for externally-written files).
	tx := tbl.NewTransaction()
	rw := tx.NewRewrite(nil)
	rw.DeleteFile(initialFile)
	for _, df := range dataFiles {
		rw.AddDataFile(df)
	}
	require.NoError(t, rw.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// Scan with lineage — survivors must keep their original _row_ids; the
	// fresh row gets a new synthesized _row_id from the new file's first_row_id.
	scan := tbl.Scan(table.WithRowLineage())
	_, itr, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)

	rowsByID := map[int64]int64{}
	for rec, err := range itr {
		require.NoError(t, err)
		idCol := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		rowIDCol := rec.Column(rec.Schema().FieldIndices(iceberg.RowIDColumnName)[0]).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			rowsByID[idCol.Value(i)] = rowIDCol.Value(i)
		}
		rec.Release()
	}

	require.Contains(t, rowsByID, int64(1))
	require.Contains(t, rowsByID, int64(2))
	require.Contains(t, rowsByID, int64(99))

	assert.Equal(t, preservedRowID1, rowsByID[1], "reinserted id=1 must preserve original _row_id")
	assert.Equal(t, preservedRowID2, rowsByID[2], "reinserted id=2 must preserve original _row_id")
	// The fresh row's synthesized _row_id must not collide with the preserved
	// survivors. With first_row_id allocated from a fresh assign-counter, it
	// will sit in a low range and the high preserved values are unreachable.
	assert.NotEqual(t, preservedRowID1, rowsByID[99])
	assert.NotEqual(t, preservedRowID2, rowsByID[99])
}

// buildReinsertBatch creates an Arrow record batch with id, data, and _row_id
// columns suitable for PositionDeltaWriter.Reinsert.
func buildReinsertBatch(mem memory.Allocator, ids []int64, data []string, rowIDs []int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: iceberg.RowIDColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues(ids, nil)

	dataBldr := array.NewStringBuilder(mem)
	defer dataBldr.Release()
	for _, s := range data {
		dataBldr.Append(s)
	}

	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.AppendValues(rowIDs, nil)

	idArr := idBldr.NewArray()
	defer idArr.Release()
	dataArr := dataBldr.NewArray()
	defer dataArr.Release()
	rowIDArr := rowIDBldr.NewArray()
	defer rowIDArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr, dataArr, rowIDArr}, int64(len(ids)))
}

// buildInsertBatch creates an Arrow record batch with id and data columns.
func buildInsertBatch(mem memory.Allocator, ids []int64, data []string) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues(ids, nil)

	dataBldr := array.NewStringBuilder(mem)
	defer dataBldr.Release()
	for _, s := range data {
		dataBldr.Append(s)
	}

	idArr := idBldr.NewArray()
	defer idArr.Release()
	dataArr := dataBldr.NewArray()
	defer dataArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr, dataArr}, int64(len(ids)))
}

// planFilesAll returns every data file currently visible in the table's
// scan plan — used by tests that want to take a single existing data file
// and feed it back into NewRewrite.
func planFilesAll(ctx context.Context, tbl *table.Table) ([]iceberg.DataFile, error) {
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	files := make([]iceberg.DataFile, 0, len(tasks))
	for _, t := range tasks {
		files = append(files, t.File)
	}

	return files, nil
}

func TestPositionDeltaWriter_RequiresV3(t *testing.T) {
	tbl := newV3RowLineageTestTable(t)
	// newV3RowLineageTestTable already creates a v3 table, so this should succeed.
	w, err := table.NewPositionDeltaWriter(tbl)
	require.NoError(t, err)
	require.NotNil(t, w)
}

func TestPositionDeltaWriter_ReinsertRejectsNullRowID(t *testing.T) {
	mem := memory.DefaultAllocator
	tbl := newV3RowLineageTestTable(t)

	w, err := table.NewPositionDeltaWriter(tbl)
	require.NoError(t, err)

	// Build a batch with a null _row_id.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: iceberg.RowIDColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.Append(1)
	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.AppendNull()

	idArr := idBldr.NewArray()
	defer idArr.Release()
	rowIDArr := rowIDBldr.NewArray()
	defer rowIDArr.Release()

	batch := array.NewRecordBatch(schema, []arrow.Array{idArr, rowIDArr}, 1)
	defer batch.Release()

	err = w.Reinsert(batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not contain null")
}

func TestPositionDeltaWriter_ReinsertRejectsMissingColumn(t *testing.T) {
	mem := memory.DefaultAllocator
	tbl := newV3RowLineageTestTable(t)

	w, err := table.NewPositionDeltaWriter(tbl)
	require.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.Append(1)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	batch := array.NewRecordBatch(schema, []arrow.Array{idArr}, 1)
	defer batch.Release()

	err = w.Reinsert(batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must contain _row_id")
}
