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
	"testing"

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

func TestBindReadTasksResidualsCopyOnWrite(t *testing.T) {
	_, schema := residualBindingTestScan(t)
	unbound := iceberg.GreaterThan(iceberg.Reference("id"), int64(1))
	bound, err := iceberg.BindExpr(schema, unbound, true)
	require.NoError(t, err)

	tests := []struct {
		name      string
		residuals []iceberg.BooleanExpression
		wantAlias bool
	}{
		{name: "all bound and nil", residuals: []iceberg.BooleanExpression{bound, nil, bound}, wantAlias: true},
		{name: "mixed", residuals: []iceberg.BooleanExpression{bound, nil, unbound, bound}},
		{name: "first task unbound", residuals: []iceberg.BooleanExpression{unbound, bound}},
		{name: "all unbound", residuals: []iceberg.BooleanExpression{unbound, unbound}},
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
				state, visitErr := iceberg.VisitExpr(got[i].Residual, filterBindingVisitor{})
				require.NoError(t, visitErr)
				require.True(t, state.hasBound)
				require.False(t, state.hasUnbound)
				if stateBefore, stateErr := iceberg.VisitExpr(original, filterBindingVisitor{}); stateErr == nil && !stateBefore.hasUnbound {
					require.Same(t, original, got[i].Residual)
				}
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

func TestReadTasksAlreadyBoundTasksRemainReadOnly(t *testing.T) {
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
	bound, err := iceberg.BindExpr(schema, unbound, true)
	require.NoError(t, err)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)

	paths := []string{
		filepath.Join(location, "data-1.parquet"),
		filepath.Join(location, "data-2.parquet"),
	}
	rows := []string{
		`[{"id":2}]`,
		`[{"id":3}]`,
	}
	tasks := make([]FileScanTask, len(paths))
	for i, path := range paths {
		writeParquetFile(t, path, arrowSchema, rows[i])
		info, statErr := os.Stat(path)
		require.NoError(t, statErr)

		builder, buildErr := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec,
			iceberg.EntryContentData,
			path,
			iceberg.ParquetFile,
			nil,
			nil,
			nil,
			1,
			info.Size(),
		)
		require.NoError(t, buildErr)
		tasks[i] = FileScanTask{File: builder.Build(), Residual: bound}
	}
	snapshot := append([]FileScanTask(nil), tasks...)

	errCh := make(chan error, 2)
	for range 2 {
		go func() {
			scan := tbl.Scan(WithMaxConcurrency(4))
			_, records, readErr := scan.ReadTasks(t.Context(), tasks)
			if readErr != nil {
				errCh <- readErr
				return
			}
			for record, iterErr := range records {
				if iterErr != nil {
					errCh <- iterErr
					return
				}
				record.Release()
			}
			errCh <- nil
		}()
	}

	for range 2 {
		require.NoError(t, <-errCh)
	}
	require.Equal(t, snapshot, tasks)
}
