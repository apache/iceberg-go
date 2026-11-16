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
	"iter"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDeleteTargetTestTable(t *testing.T, partitioned bool, props iceberg.Properties) *table.Table {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.UnpartitionedSpec
	if partitioned {
		partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "data", Transform: iceberg.IdentityTransform{},
		})
		spec = &partitionSpec
	}
	props[table.PropertyFormatVersion] = "2"
	meta, err := table.NewMetadata(sc, spec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)

	return table.New(table.Identifier{"db", "delete_target"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &rowDeltaCatalog{metadata: meta})
}

func deleteTargetRecords(t *testing.T, sc *arrow.Schema, count int) (iter.Seq2[arrow.RecordBatch, error], func()) {
	t.Helper()
	var rows []string
	for i := range count {
		rows = append(rows, fmt.Sprintf(`[{"id":%d,"data":"%c"}]`, i, 'a'+i%2))
	}
	// One-row input batches make both bin packing and streaming file rolling observable.
	data, err := array.TableFromJSON(memory.DefaultAllocator, sc, rows)
	require.NoError(t, err)
	reader := array.NewTableReader(data, 1)
	data.Release()
	records := func(yield func(arrow.RecordBatch, error) bool) {
		for reader.Next() {
			if !yield(reader.RecordBatch(), nil) {
				return
			}
		}
		if reader.Err() != nil {
			yield(nil, reader.Err())
		}
	}

	return records, reader.Release
}

func appendDeleteTargetData(t *testing.T, tbl *table.Table, wantFiles int) *table.Table {
	t.Helper()
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	records, release := deleteTargetRecords(t, sc, 8)
	defer release()
	var files []iceberg.DataFile
	// WriteRecords consumes its input, while the table reader owns the original batches.
	retained := func(yield func(arrow.RecordBatch, error) bool) {
		for rec, err := range records {
			require.NoError(t, err)
			rec.Retain()
			if !yield(rec, nil) {
				return
			}
		}
	}
	for df, err := range table.WriteRecords(t.Context(), tbl, sc, retained) {
		require.NoError(t, err)
		files = append(files, df)
	}
	require.Len(t, files, wantFiles, "delete target must not change data file rolling")
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(files...).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

func assertDeleteTargetSurvivors(t *testing.T, tbl *table.Table) {
	t.Helper()
	_, records, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)
	var ids []int64
	for rec, err := range records {
		require.NoError(t, err)
		ids = append(ids, rec.Column(0).(*array.Int64).Int64Values()...)
		rec.Release()
	}
	assert.ElementsMatch(t, []int64{4, 5, 6, 7}, ids)
}

func TestEqualityDeleteTargetFileSize(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		for _, tt := range []struct {
			name         string
			dataTarget   string
			deleteTarget string
			dataFiles    int
			deleteFiles  int
		}{
			{"small delete target", "1048576", "1", 1, 4},
			{"default delete target ignores data target", "1", "", 8, 1},
			{"large delete target", "1", "1048576", 8, 1},
		} {
			t.Run(fmt.Sprintf("%s/partitioned=%t", tt.name, partitioned), func(t *testing.T) {
				props := iceberg.Properties{table.WriteTargetFileSizeBytesKey: tt.dataTarget}
				if tt.deleteTarget != "" {
					props[table.WriteDeleteTargetFileSizeBytesKey] = tt.deleteTarget
				}
				dataFiles, deleteFiles := tt.dataFiles, tt.deleteFiles
				if partitioned {
					if dataFiles == 1 {
						dataFiles = 2
					}
					if deleteFiles == 1 {
						deleteFiles = 2
					}
				}
				tbl := appendDeleteTargetData(t, newDeleteTargetTestTable(t, partitioned, props), dataFiles)
				deleteSchema := iceberg.NewSchema(0, tbl.Schema().Field(0))
				if partitioned {
					deleteSchema = tbl.Schema()
				}
				arrowSchema, err := table.SchemaToArrowSchema(deleteSchema, nil, true, false)
				require.NoError(t, err)
				records, release := deleteTargetRecords(t, arrowSchema, 4)
				defer release()
				tx := tbl.NewTransaction()
				files, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
				require.NoError(t, err)
				require.Len(t, files, deleteFiles)
				var count int64
				paths := make(map[string]bool)
				for _, df := range files {
					assert.Equal(t, iceberg.EntryContentEqDeletes, df.ContentType())
					assert.Equal(t, []int{1}, df.EqualityFieldIDs())
					assert.Positive(t, df.Count())
					assert.False(t, paths[df.FilePath()])
					paths[df.FilePath()] = true
					count += df.Count()
				}
				assert.EqualValues(t, 4, count)
				require.NoError(t, tx.NewRowDelta(nil).AddDeletes(files...).Commit(t.Context()))
				tbl, err = tx.Commit(t.Context())
				require.NoError(t, err)
				assertDeleteTargetSurvivors(t, tbl)
			})
		}
	}
}

func TestPositionDeleteTargetFileSize(t *testing.T) {
	for _, partitioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("partitioned=%t", partitioned), func(t *testing.T) {
			props := iceberg.Properties{
				table.WriteTargetFileSizeBytesKey:       "1048576",
				table.WriteDeleteTargetFileSizeBytesKey: "1",
				table.WriteDeleteModeKey:                table.WriteModeMergeOnRead,
				table.ParquetBatchSizeKey:               "1",
			}
			dataFiles := 1
			if partitioned {
				dataFiles = 2
			}
			tbl := appendDeleteTargetData(t, newDeleteTargetTestTable(t, partitioned, props), dataFiles)
			// Small scan batches give the position-delete writer several batches to roll.
			ctx := tblutils.WithTableProperties(t.Context(), props)
			tbl, err := tbl.Delete(ctx, iceberg.LessThan(iceberg.Reference("id"), int64(4)), nil)
			require.NoError(t, err)
			assert.Equal(t, "4", tbl.CurrentSnapshot().Summary.Properties["added-position-delete-files"])
			tasks, err := tbl.Scan().PlanFiles(t.Context())
			require.NoError(t, err)
			files := make(map[string]iceberg.DataFile)
			for _, task := range tasks {
				for _, df := range task.DeleteFiles {
					assert.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType())
					assert.Positive(t, df.Count())
					files[df.FilePath()] = df
				}
			}
			assert.Len(t, files, 4)
			assertDeleteTargetSurvivors(t, tbl)
		})
	}
}
