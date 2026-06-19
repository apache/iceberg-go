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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScanRowLineagePreservedAcrossPrunedRowGroups: a filter that prunes a
// leading row group must not renumber survivors' _row_id (= first_row_id +
// ORIGINAL position). Two row groups (ids 1..5, 6..10); id > 5 prunes the first
// via stats, and the unfiltered baseline spans both, exercising multiple batches.
func TestScanRowLineagePreservedAcrossPrunedRowGroups(t *testing.T) {
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

	// Without two row groups there is nothing to prune and the test would pass
	// even with the bug (it was verified to fail without the fix).
	require.Equal(t, 2, parquetRowGroupCount(t, tbl), "file must have multiple row groups to prune")

	full := rowIDByID(t, tbl)
	require.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7, 9: 8, 10: 9}, full)

	got := rowIDByID(t, tbl,
		table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("id"), int64(5))))

	assert.Equal(t, map[int64]int64{6: 5, 7: 6, 8: 7, 9: 8, 10: 9}, got,
		"a pruned leading row group must not renumber surviving rows' _row_id")
}

// TestScanRowLineagePreservedThroughEqualityDeletes: equality deletes drop rows
// during the scan (a distinct pipeline step from DVs), so survivors must keep
// their original _row_id.
func TestScanRowLineagePreservedThroughEqualityDeletes(t *testing.T) {
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
	require.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}, rowIDByID(t, tbl))

	eqDelPath := tbl.Location() + "/data/eq-del.parquet"
	delSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, eqDelPath, delSc, `[{"id": 2}, {"id": 4}]`)

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqDelPath, iceberg.ParquetFile, nil, nil, nil, 2, 256)
	require.NoError(t, err)
	b.EqualityFieldIDs([]int{1})

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(b.Build())
	require.NoError(t, rd.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, map[int64]int64{1: 0, 3: 2, 5: 4}, rowIDByID(t, tbl),
		"equality-deleted rows must not renumber surviving rows' _row_id")
}

// TestScanRowLineagePreservedThroughPositionalDeletes: a Parquet position-delete
// file drops rows during the scan; survivors must keep their original _row_id.
func TestScanRowLineagePreservedThroughPositionalDeletes(t *testing.T) {
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
	require.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}, rowIDByID(t, tbl))

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataPath := tasks[0].File.FilePath()

	// Delete positions 1 and 3 (id=2, id=4) of the data file.
	posDelPath := tbl.Location() + "/data/pos-del.parquet"
	posSc, err := table.SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, posDelPath, posSc,
		fmt.Sprintf(`[{"file_path":%q,"pos":1},{"file_path":%q,"pos":3}]`, dataPath, dataPath))

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 2, 256)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(b.Build())
	require.NoError(t, rd.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	assert.Equal(t, map[int64]int64{1: 0, 3: 2, 5: 4}, rowIDByID(t, tbl),
		"position-deleted rows must not renumber surviving rows' _row_id")
}

// parquetRowGroupCount opens the table's single data file and returns its
// Parquet row-group count.
func parquetRowGroupCount(t *testing.T, tbl *table.Table) int {
	t.Helper()

	tasks, err := tbl.Scan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	f, err := iceio.LocalFS{}.Open(tasks[0].File.FilePath())
	require.NoError(t, err)
	rdr, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer rdr.Close()

	return rdr.NumRowGroups()
}
