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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestPositionDeleteAfterSpecEvolution(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		mixed, unpartitioned bool
	}{
		{name: "old-spec"},
		{name: "mixed-specs", mixed: true},
		{name: "evolve-to-unpartitioned", mixed: true, unpartitioned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			location := filepath.ToSlash(t.TempDir())
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "cat", Type: iceberg.PrimitiveTypes.String},
				iceberg.NestedField{ID: 3, Name: "region", Type: iceberg.PrimitiveTypes.String})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "cat"})
			meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, iceberg.Properties{table.PropertyFormatVersion: "2", table.WriteDeleteModeKey: table.WriteModeMergeOnRead})
			require.NoError(t, err)
			fs := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
			metaLoc := location + "/metadata/v1.metadata.json"
			cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fs}
			tbl := table.New(table.Identifier{"db", "spec_delete"}, meta, metaLoc, fs, cat)
			arrowSchema, err := table.SchemaToArrowSchema(schema, nil, false, false)
			require.NoError(t, err)
			appendJSON := func(rows string) {
				data, e := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{rows})
				require.NoError(t, e)
				defer data.Release()
				tbl, e = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
				require.NoError(t, e)
			}
			appendJSON(`[{"id":1,"cat":"a","region":"x"},{"id":2,"cat":"a","region":"x"}]`)
			evolve := tbl.NewTransaction()
			if tc.unpartitioned {
				require.NoError(t, evolve.UpdateSpec(true).RemoveField("cat").Commit())
			} else {
				require.NoError(t, evolve.UpdateSpec(true).AddIdentity("region").Commit())
			}
			tbl, err = evolve.Commit(ctx)
			require.NoError(t, err)
			if tc.mixed {
				appendJSON(`[{"id":1,"cat":"a","region":"x"},{"id":3,"cat":"a","region":"x"}]`)
			}
			tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
			require.NoError(t, err)
			want := []int64{2}
			if tc.mixed {
				want = []int64{2, 3}
			}
			require.Equal(t, want, idsInTable(t, tbl))
			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			expectedTasks := 1
			if tc.mixed {
				expectedTasks = 2
			}
			require.Len(t, tasks, expectedTasks)
			for _, task := range tasks {
				require.Len(t, task.DeleteFiles, 1)
				require.Equal(t, task.File.SpecID(), task.DeleteFiles[0].SpecID())
				require.Equal(t, task.File.Partition(), task.DeleteFiles[0].Partition())
			}
		})
	}
}
