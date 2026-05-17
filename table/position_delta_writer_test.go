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

	// Append initial data.
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

	// Create a PositionDeltaWriter.
	w, err := table.NewPositionDeltaWriter(tbl)
	require.NoError(t, err)

	// Reinsert row id=1 with preserved _row_id=0.
	reinsertSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: iceberg.RowIDColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.Append(1)

	dataBldr := array.NewStringBuilder(mem)
	defer dataBldr.Release()
	dataBldr.Append("a_updated")

	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.Append(0) // preserved _row_id

	idArr := idBldr.NewArray()
	defer idArr.Release()
	dataArr := dataBldr.NewArray()
	defer dataArr.Release()
	rowIDArr := rowIDBldr.NewArray()
	defer rowIDArr.Release()

	reinsertBatch := array.NewRecordBatch(reinsertSchema, []arrow.Array{idArr, dataArr, rowIDArr}, 1)
	defer reinsertBatch.Release()

	require.NoError(t, w.Reinsert(reinsertBatch))

	// Insert a fresh row (no _row_id).
	freshSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	freshData, err := array.TableFromJSON(mem, freshSchema, []string{
		`[{"id": 99, "data": "new"}]`,
	})
	require.NoError(t, err)
	defer freshData.Release()

	freshBatch := freshData.Column(0).Data().Chunk(0)
	_ = freshBatch

	idBldr2 := array.NewInt64Builder(mem)
	defer idBldr2.Release()
	idBldr2.Append(99)
	dataBldr2 := array.NewStringBuilder(mem)
	defer dataBldr2.Release()
	dataBldr2.Append("new")
	idArr2 := idBldr2.NewArray()
	defer idArr2.Release()
	dataArr2 := dataBldr2.NewArray()
	defer dataArr2.Release()
	insertBatch := array.NewRecordBatch(freshSchema, []arrow.Array{idArr2, dataArr2}, 1)
	defer insertBatch.Release()

	require.NoError(t, w.Insert(insertBatch))

	// Close and get data files.
	dataFiles, err := w.Close(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, dataFiles)

	// Read the output files back with row lineage to verify the _row_id values.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(ctx, dataFiles, nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// Scan with lineage — the reinserted row should keep _row_id=0,
	// the fresh row should have a synthesized _row_id from the new file's first_row_id.
	scan := tbl.Scan(table.WithRowLineage())
	_, itr, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)

	type row struct {
		id    int64
		rowID int64
	}
	var rows []row
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		rowIDIdx := rec.Schema().FieldIndices(iceberg.RowIDColumnName)
		require.NotEmpty(t, idIdx)
		require.NotEmpty(t, rowIDIdx)

		idCol := rec.Column(idIdx[0]).(*array.Int64)
		rowIDCol := rec.Column(rowIDIdx[0]).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			rows = append(rows, row{id: idCol.Value(i), rowID: rowIDCol.Value(i)})
		}
		rec.Release()
	}

	// Find the reinserted row and the fresh row.
	var reinsertedRow, freshRow *row
	for i := range rows {
		if rows[i].id == 1 {
			reinsertedRow = &rows[i]
		}
		if rows[i].id == 99 {
			freshRow = &rows[i]
		}
	}

	require.NotNil(t, reinsertedRow, "reinserted row (id=1) should be in output")
	assert.Equal(t, int64(0), reinsertedRow.rowID,
		"reinserted row must preserve original _row_id=0")

	require.NotNil(t, freshRow, "fresh row (id=99) should be in output")
	assert.NotEqual(t, int64(0), freshRow.rowID,
		"fresh row gets a new _row_id (synthesized from first_row_id + position)")
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
