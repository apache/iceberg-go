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
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestIncrementalChangelogScanPreservesV3RowLineageMetadata(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator
	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	initialData, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 1, "data": "a"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(initialData.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(initialData, -1), nil)
	require.NoError(t, err)

	replacementData, err := array.TableFromJSON(mem, arrowSchema, []string{
		`[{"id": 2, "data": "b"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(replacementData.Release)

	tbl, err = tbl.Overwrite(ctx, array.NewTableReader(replacementData, -1), nil,
		table.WithOverwriteConcurrency(1))
	require.NoError(t, err)

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 3)

	insertTask := tasks[0].ScanTask()
	deleteTask := tasks[1].ScanTask()
	laterInsertTask := tasks[2].ScanTask()
	require.Equal(t, table.ChangelogOpInsert, tasks[0].Operation())
	require.Equal(t, table.ChangelogOpDelete, tasks[1].Operation())
	require.Equal(t, table.ChangelogOpInsert, tasks[2].Operation())
	require.Equal(t, insertTask.File.FilePath(), deleteTask.File.FilePath())
	require.NotEqual(t, insertTask.File.FilePath(), laterInsertTask.File.FilePath())

	for _, task := range []table.FileScanTask{insertTask, deleteTask, laterInsertTask} {
		require.NotNil(t, task.FirstRowID)
		require.NotNil(t, task.DataSequenceNumber)
	}
	require.Equal(t, int64(0), *insertTask.FirstRowID)
	require.Equal(t, int64(1), *laterInsertTask.FirstRowID)
	require.Equal(t, int64(0), *deleteTask.FirstRowID)
	require.Equal(t, int64(1), *insertTask.DataSequenceNumber)
	require.Equal(t, int64(2), *laterInsertTask.DataSequenceNumber)
	require.Equal(t, int64(1), *deleteTask.DataSequenceNumber)
}
