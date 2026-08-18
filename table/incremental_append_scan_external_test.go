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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestIncrementalAppendTasksPreserveHistoricalSchemaBinding(t *testing.T) {
	ctx := context.Background()
	tbl, snapshotID := newIncrementalResidualTestTable(t, ctx)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		RenameColumn([]string{"old_name"}, "new_name").
		Commit())
	var err error
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	tasks, err := tbl.NewIncrementalAppendScan(table.WithRowFilter(
		iceberg.EqualTo(iceberg.Reference("old_name"), int64(2)),
	)).ToSnapshot(snapshotID).PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	got := readIncrementalTaskValues(t, ctx, tbl, tasks, "new_name")
	require.Equal(t, []int64{2}, got)
}

func TestIncrementalAppendTasksPreserveCaseInsensitiveBinding(t *testing.T) {
	ctx := context.Background()
	tbl, _ := newIncrementalResidualTestTable(t, ctx)

	tasks, err := tbl.NewIncrementalAppendScan(
		table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("OLD_NAME"), int64(2))),
		table.WithCaseSensitive(false),
	).PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	got := readIncrementalTaskValues(t, ctx, tbl, tasks, "old_name")
	require.Equal(t, []int64{2}, got)
}

func newIncrementalResidualTestTable(t *testing.T, ctx context.Context) (*table.Table, int64) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 7, Name: "old_name", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	metadataLocation := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metadataLocation, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "incremental_residual"}, meta, metadataLocation, fsF, cat)

	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "old_name", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
	}}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"old_name":1},{"old_name":2},{"old_name":3}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	require.NotNil(t, tbl.CurrentSnapshot())

	return tbl, tbl.CurrentSnapshot().SnapshotID
}

func readIncrementalTaskValues(
	t *testing.T,
	ctx context.Context,
	tbl *table.Table,
	tasks []table.FileScanTask,
	fieldName string,
) []int64 {
	t.Helper()

	_, records, err := tbl.Scan(table.WithSelectedFields(fieldName)).ReadTasks(ctx, tasks)
	require.NoError(t, err)

	var values []int64
	for record, err := range records {
		require.NoError(t, err)
		indices := record.Schema().FieldIndices(fieldName)
		require.Len(t, indices, 1)
		column := record.Column(indices[0]).(*array.Int64)
		for idx := range column.Len() {
			values = append(values, column.Value(idx))
		}
		record.Release()
	}

	return values
}
