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
	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestReadTasksUsesTaskResidual(t *testing.T) {
	ctx := context.Background()
	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	scan := tbl.Scan(table.WithRowFilter(iceberg.AlwaysTrue{}), table.WithSelectedFields("id"))
	tasks, err := scan.PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// This is the unbound residual a remote planner would return. It is
	// narrower than the original scan filter and must be applied by ReadTasks.
	tasks[0].Residual = iceberg.GreaterThan(iceberg.Reference("id"), int64(1))
	originalResidual := tasks[0].Residual
	_, records, err := scan.ReadTasks(ctx, tasks)
	require.NoError(t, err)
	require.Same(t, originalResidual, tasks[0].Residual,
		"ReadTasks must not replace the caller's task residual")

	var got []int64
	for record, err := range records {
		require.NoError(t, err)
		values := record.Column(0).(*array.Int64)
		for i := 0; i < values.Len(); i++ {
			got = append(got, values.Value(i))
		}
		record.Release()
	}

	require.Equal(t, []int64{2, 3}, got)
}

func TestReadTasksAppliesDifferentResidualsPerTask(t *testing.T) {
	ctx := context.Background()
	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	scan := tbl.Scan(table.WithRowFilter(iceberg.AlwaysTrue{}), table.WithSelectedFields("id"))
	tasks, err := scan.PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	first := tasks[0]
	second := tasks[0]
	first.Residual = iceberg.LessThan(iceberg.Reference("id"), int64(2))
	second.Residual = iceberg.GreaterThan(iceberg.Reference("id"), int64(2))
	readTasks := []table.FileScanTask{first, second}

	_, records, err := scan.ReadTasks(ctx, readTasks)
	require.NoError(t, err)

	var got []int64
	for record, err := range records {
		require.NoError(t, err)
		values := record.Column(0).(*array.Int64)
		for i := 0; i < values.Len(); i++ {
			got = append(got, values.Value(i))
		}
		record.Release()
	}

	require.Equal(t, []int64{1, 3}, got)
}

func TestReadTasksAlwaysFalseResidualCompletes(t *testing.T) {
	ctx := context.Background()
	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
	}}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1},{"id":2}]`,
	})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	scan := tbl.Scan(table.WithRowFilter(iceberg.AlwaysTrue{}), table.WithSelectedFields("id"))
	tasks, err := scan.PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	tasks[0].Residual = iceberg.AlwaysFalse{}

	_, records, err := scan.ReadTasks(ctx, tasks)
	require.NoError(t, err)
	count := 0
	for record, err := range records {
		require.NoError(t, err)
		count += int(record.NumRows())
		record.Release()
	}
	require.Zero(t, count)
}
