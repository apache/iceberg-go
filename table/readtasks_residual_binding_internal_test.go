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

package table

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func residualBindingTestScan(t *testing.T) (*Scan, *iceberg.Schema) {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	metadata, err := NewMetadata(
		schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://mixed-residuals", nil,
	)
	require.NoError(t, err)
	memFS := iceio.NewMemFS()
	tbl := New(
		Identifier{"db", "tbl"}, metadata, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memFS, nil }, nil,
	)

	return tbl.Scan(), schema
}

func writeResidualBindingParquetFile(t testing.TB, path string, schema *iceberg.Schema, jsonData string) iceberg.DataFile {
	t.Helper()

	// Real Iceberg field IDs are needed for the Parquet reader to reach residual filtering.
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(jsonData))
	require.NoError(t, err)
	defer record.Release()

	writer, err := (iceio.LocalFS{}).Create(path)
	require.NoError(t, err)
	defer writer.Close()

	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
	defer tbl.Release()

	props := parquet.NewWriterProperties(parquet.WithStats(true))
	require.NoError(t, pqarrow.WriteTable(
		tbl, writer, record.NumRows(), props, pqarrow.DefaultWriterProps(),
	))
	require.NoError(t, writer.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		nil, nil, nil,
		record.NumRows(),
		info.Size(),
	)
	require.NoError(t, err)

	return builder.Build()
}

func TestBindReadTasksResidualsCopyOnWrite(t *testing.T) {
	_, schema := residualBindingTestScan(t)
	unbound := iceberg.GreaterThan(iceberg.Reference("id"), int64(1))
	bound, err := iceberg.BindExpr(schema, unbound, true)
	require.NoError(t, err)

	tests := []struct {
		name      string
		residuals []iceberg.BooleanExpression
		wantAlias bool
		wantSame  []bool
	}{
		{
			name:      "all bound and nil",
			residuals: []iceberg.BooleanExpression{bound, nil, bound},
			wantAlias: true,
			wantSame:  []bool{true, false, true},
		},
		{
			name:      "all nil",
			residuals: []iceberg.BooleanExpression{nil, nil, nil},
			wantAlias: true,
			wantSame:  []bool{false, false, false},
		},
		{
			name:      "mixed",
			residuals: []iceberg.BooleanExpression{bound, nil, unbound, bound},
			wantSame:  []bool{true, false, false, true},
		},
		{
			name:      "first task unbound",
			residuals: []iceberg.BooleanExpression{unbound, bound},
			wantSame:  []bool{false, true},
		},
		{
			name:      "all unbound",
			residuals: []iceberg.BooleanExpression{unbound, unbound},
			wantSame:  []bool{false, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks := make([]FileScanTask, len(tt.residuals))
			for i, residual := range tt.residuals {
				tasks[i].Residual = residual
			}

			got, err := bindReadTasksResiduals(schema, tasks, true)
			require.NoError(t, err)
			require.Len(t, got, len(tasks))
			if len(tasks) > 0 {
				require.Equal(t, tt.wantAlias, &got[0] == &tasks[0])
			}

			for i, original := range tt.residuals {
				if original == nil {
					require.Nil(t, got[i].Residual)

					continue
				}

				// The input plan is never rewritten, even when the output needs binding.
				require.Same(t, original, tasks[i].Residual)
				if tt.wantSame[i] {
					require.Same(t, original, got[i].Residual)
				} else {
					require.NotSame(t, original, got[i].Residual)
				}
				state, visitErr := iceberg.VisitExpr(got[i].Residual, filterBindingVisitor{})
				require.NoError(t, visitErr)
				require.True(t, state.hasBound)
				require.False(t, state.hasUnbound)
			}
		})
	}
}

func TestReadTasksResidualPlanIsReusable(t *testing.T) {
	scan, schema := residualBindingTestScan(t)
	unbound := iceberg.GreaterThan(iceberg.Reference("id"), int64(1))
	bound, err := iceberg.BindExpr(schema, unbound, true)
	require.NoError(t, err)

	wrongSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 2, Name: "other", Type: iceberg.PrimitiveTypes.Int64},
	)
	wrongBound, err := iceberg.BindExpr(
		wrongSchema, iceberg.EqualTo(iceberg.Reference("other"), int64(1)), true,
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		tasks   []FileScanTask
		wantErr bool
	}{
		{
			name:  "mixed plan",
			tasks: []FileScanTask{{Residual: bound}, {}, {Residual: unbound}, {Residual: bound}},
		},
		{
			name:    "invalid bound residual",
			tasks:   []FileScanTask{{Residual: bound}, {Residual: unbound}, {Residual: wrongBound}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originals := make([]iceberg.BooleanExpression, len(tt.tasks))
			for i := range tt.tasks {
				originals[i] = tt.tasks[i].Residual
			}

			// Run twice to prove the caller-owned plan remains reusable.
			for range 2 {
				_, _, err := scan.ReadTasks(t.Context(), tt.tasks)
				if tt.wantErr {
					require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
					require.ErrorContains(t, err, "field ID 2")
				} else {
					require.NoError(t, err)
				}
				for i, original := range originals {
					if original == nil {
						require.Nil(t, tt.tasks[i].Residual)
					} else {
						require.Same(t, original, tt.tasks[i].Residual)
					}
				}
			}
		})
	}
}

func TestReadTasksConcurrentScansPreserveInputTasks(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	location := t.TempDir()
	metadata, err := NewMetadata(
		schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, location, nil,
	)
	require.NoError(t, err)
	tbl := New(
		Identifier{"db", "tbl"}, metadata, filepath.Join(location, "metadata.json"),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil,
	)

	bound, err := iceberg.BindExpr(schema, iceberg.GreaterThan(iceberg.Reference("id"), int64(1)), true)
	require.NoError(t, err)
	unbound := iceberg.LessThan(iceberg.Reference("id"), int64(3))
	files := []iceberg.DataFile{
		writeResidualBindingParquetFile(t, filepath.Join(location, "data-1.parquet"), schema, `[{"id":2}]`),
		writeResidualBindingParquetFile(t, filepath.Join(location, "data-2.parquet"), schema, `[{"id":3}]`),
	}

	tests := []struct {
		name      string
		residuals []iceberg.BooleanExpression
		wantRows  int64
	}{
		{name: "aliased bound plan", residuals: []iceberg.BooleanExpression{bound, bound}, wantRows: 2},
		{name: "mixed copy-on-write plan", residuals: []iceberg.BooleanExpression{bound, unbound}, wantRows: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks := []FileScanTask{
				{File: files[0], Residual: tt.residuals[0]},
				{File: files[1], Residual: tt.residuals[1]},
			}
			snapshot := append([]FileScanTask(nil), tasks...)

			type result struct {
				rows int64
				err  error
			}
			results := make(chan result, 2)
			for range 2 {
				go func() {
					scan := tbl.Scan(WithMaxConcurrency(4))
					_, records, readErr := scan.ReadTasks(t.Context(), tasks)
					if readErr != nil {
						results <- result{err: readErr}
						return
					}

					var rows int64
					for record, iterErr := range records {
						if iterErr != nil {
							results <- result{err: iterErr}
							return
						}
						rows += record.NumRows()
						record.Release()
					}
					results <- result{rows: rows}
				}()
			}

			for range 2 {
				got := <-results
				require.NoError(t, got.err)
				require.Equal(t, tt.wantRows, got.rows)
			}
			require.Equal(t, snapshot, tasks)
			for i := range tasks {
				require.Same(t, snapshot[i].Residual, tasks[i].Residual)
			}
		})
	}
}

func TestReadTasksPassesBoundResidualsToGetRecords(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	location := t.TempDir()
	metadata, err := NewMetadata(
		schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, location, nil,
	)
	require.NoError(t, err)
	tbl := New(
		Identifier{"db", "tbl"}, metadata, filepath.Join(location, "metadata.json"),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil,
	)

	unbound := iceberg.GreaterThan(iceberg.Reference("id"), int64(1))
	bound, err := iceberg.BindExpr(schema, iceberg.LessThan(iceberg.Reference("id"), int64(5)), true)
	require.NoError(t, err)

	// Distinct data and residuals expose both an unbound handoff and an
	// incorrect residual-to-task index, rather than just asserting row totals.
	files := []struct {
		name     string
		json     string
		residual iceberg.BooleanExpression
	}{
		{name: "unbound.parquet", json: `[{"id":1},{"id":2},{"id":3}]`, residual: unbound},
		{name: "bound.parquet", json: `[{"id":4},{"id":5},{"id":6}]`, residual: bound},
		{name: "nil.parquet", json: `[{"id":7},{"id":8},{"id":9}]`},
	}
	tasks := make([]FileScanTask, len(files))
	for i, file := range files {
		tasks[i] = FileScanTask{
			File:     writeResidualBindingParquetFile(t, filepath.Join(location, file.name), schema, file.json),
			Residual: file.residual,
		}
	}

	_, records, err := tbl.Scan().ReadTasks(t.Context(), tasks)
	require.NoError(t, err)

	var ids []int64
	for record, readErr := range records {
		require.NoError(t, readErr)
		values, ok := record.Column(0).(*array.Int64)
		require.True(t, ok)
		for i := 0; i < values.Len(); i++ {
			ids = append(ids, values.Value(i))
		}
		record.Release()
	}

	require.Equal(t, []int64{2, 3, 4, 7, 8, 9}, ids)
	require.Same(t, unbound, tasks[0].Residual)
	require.Same(t, bound, tasks[1].Residual)
	require.Nil(t, tasks[2].Residual)
}
