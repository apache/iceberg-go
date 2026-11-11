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
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func newProjectedDeleteTable(t testing.TB, schema *iceberg.Schema, props iceberg.Properties) *table.Table {
	t.Helper()
	if props == nil {
		props = iceberg.Properties{}
	}
	if props[table.PropertyFormatVersion] == "" {
		props[table.PropertyFormatVersion] = "2"
	}
	props[table.WriteDeleteModeKey] = table.WriteModeMergeOnRead
	location := filepath.ToSlash(t.TempDir())
	metadata, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)
	fsF := func(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: metadata, location: location + "/metadata/v1.metadata.json", fsF: fsF}

	return table.New(table.Identifier{"db", "projected_delete"}, metadata, cat.location, fsF, cat)
}

func appendProjectedDeleteRows(t testing.TB, tbl *table.Table, data string) *table.Table {
	t.Helper()
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	rows, err := array.TableFromJSON(mem, schema, []string{data})
	require.NoError(t, err)
	defer rows.Release()
	reader := array.NewTableReader(rows, -1)
	defer reader.Release()
	tbl, err = tbl.Append(t.Context(), reader, nil)
	require.NoError(t, err)

	return tbl
}

type projectionBlockedFile struct {
	icebergio.File
	start, end int64
	reads      *atomic.Int32
}

func (f projectionBlockedFile) ReadAt(data []byte, offset int64) (int, error) {
	if offset < f.end && offset+int64(len(data)) > f.start {
		f.reads.Add(1)

		return 0, errors.New("unused column was read")
	}

	return f.File.ReadAt(data, offset)
}

type projectionBlockedFS struct {
	icebergio.LocalFS
	path       string
	start, end int64
	reads      atomic.Int32
}

func (f *projectionBlockedFS) Open(ctx context.Context, location string) (icebergio.File, error) {
	input, err := f.LocalFS.Open(ctx, location)
	if err == nil && location == f.path {
		return projectionBlockedFile{File: input, start: f.start, end: f.end, reads: &f.reads}, nil
	}

	return input, err
}

func blockDeleteProjectionColumn(t testing.TB, tbl *table.Table, location string, columnIndex int) (*table.Table, *projectionBlockedFS) {
	t.Helper()
	if location == "" {
		tasks, err := tbl.Scan().PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		location = tasks[0].File.FilePath()
	}
	input, err := (icebergio.LocalFS{}).Open(t.Context(), location)
	require.NoError(t, err)
	parquetReader, err := file.NewParquetReader(input)
	require.NoError(t, err)
	column, err := parquetReader.MetaData().RowGroup(0).ColumnChunk(columnIndex)
	require.NoError(t, err)
	start := column.DataPageOffset()
	if column.HasDictionaryPage() {
		start = min(start, column.DictionaryPageOffset())
	}
	blocked := &projectionBlockedFS{path: location, start: start, end: start + column.TotalCompressedSize()}
	require.NoError(t, parquetReader.Close())
	fsF := func(context.Context) (icebergio.IO, error) { return blocked, nil }
	cat := &concurrentTestCatalog{metadata: tbl.Metadata(), location: tbl.MetadataLocation(), fsF: fsF}

	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), fsF, cat), blocked
}

func TestMergeOnReadDeleteDoesNotReadUnusedColumn(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String})
	tbl := newProjectedDeleteTable(t, schema, nil)
	tbl = appendProjectedDeleteRows(t, tbl, `[{"id":1,"payload":"a"},{"id":2,"payload":"b"},{"id":3,"payload":"c"}]`)
	tbl, blocked := blockDeleteProjectionColumn(t, tbl, "", 1)
	tbl, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3}, projectedDeleteIDs(t, tbl))
	require.Zero(t, blocked.reads.Load())
}

func projectedDeleteIDs(t testing.TB, tbl *table.Table) []int64 {
	t.Helper()
	_, records, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)
	var result []int64
	for record, err := range records {
		require.NoError(t, err)
		result = append(result, record.Column(0).(*array.Int64).Int64Values()...)
		record.Release()
	}

	return result
}

func TestMergeOnReadDeleteProjectsNestedPredicate(t *testing.T) {
	for _, version := range []string{"2", "3"} {
		for _, deep := range []bool{false, true} {
			name, path := "second child", "details.target"
			var target iceberg.Type = iceberg.PrimitiveTypes.Int64
			data := `[{"id":1,"details":{"unused":"a","target":1}},{"id":2,"details":{"unused":"b","target":2}},{"id":3,"details":null}]`
			if deep {
				name, path = "deep second child", "details.target.value"
				target = &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 5, Name: "unused", Type: iceberg.PrimitiveTypes.String},
					{ID: 6, Name: "value", Type: iceberg.PrimitiveTypes.Int64},
				}}
				data = `[{"id":1,"details":{"unused":"a","target":{"unused":"x","value":1}}},{"id":2,"details":{"unused":"b","target":{"unused":"y","value":2}}},{"id":3,"details":null}]`
			}
			t.Run(version+"/"+name, func(t *testing.T) {
				schema := iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 2, Name: "details", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
						{ID: 3, Name: "unused", Type: iceberg.PrimitiveTypes.String},
						{ID: 4, Name: "target", Type: target},
					}}})
				tbl := newProjectedDeleteTable(t, schema, iceberg.Properties{table.PropertyFormatVersion: version})
				tbl = appendProjectedDeleteRows(t, tbl, data)
				tbl, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference(path), int64(2)), nil)
				require.NoError(t, err)
				require.Equal(t, []int64{1, 3}, projectedDeleteIDs(t, tbl))
			})
		}
	}
}

func TestMergeOnReadDeleteProjectsMissingPredicateFields(t *testing.T) {
	for _, dropOld := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained fields", true: "dropped fields"}[dropOld], func(t *testing.T) {
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String})
			tbl := newProjectedDeleteTable(t, schema, nil)
			tbl = appendProjectedDeleteRows(t, tbl, `[{"id":1,"payload":"a"},{"id":2,"payload":"b"}]`)
			txn := tbl.NewTransaction()
			require.NoError(t, txn.UpdateSchema(true, false).AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.String, "", false, nil).Commit())
			tbl, err := txn.Commit(t.Context())
			require.NoError(t, err)
			tbl = appendProjectedDeleteRows(t, tbl, `[{"id":3,"payload":"c","extra":"new"}]`)
			if dropOld {
				txn = tbl.NewTransaction()
				require.NoError(t, txn.UpdateSchema(true, false).DeleteColumn([]string{"id"}).DeleteColumn([]string{"payload"}).Commit())
				tbl, err = txn.Commit(t.Context())
				require.NoError(t, err)
			}
			tbl, err = tbl.Delete(t.Context(), iceberg.IsNull(iceberg.Reference("extra")), nil)
			require.NoError(t, err)
			planned, err := tbl.Scan().PlanFiles(t.Context())
			require.NoError(t, err)
			deleteFiles := map[string]iceberg.DataFile{}
			for _, task := range planned {
				for _, file := range task.DeleteFiles {
					deleteFiles[file.FilePath()] = file
				}
			}
			var deleted int64
			for _, file := range deleteFiles {
				deleted += file.Count()
			}
			require.Equal(t, int64(2), deleted)
			_, records, err := tbl.Scan(table.WithSelectedFields("extra"), table.WithRowFilter(iceberg.NotNull(iceberg.Reference("extra")))).ToArrowRecords(t.Context())
			require.NoError(t, err)
			var values []string
			for record, err := range records {
				require.NoError(t, err)
				column := record.Column(0).(*array.String)
				for i := range column.Len() {
					values = append(values, column.Value(i))
				}
				record.Release()
			}
			require.Equal(t, []string{"new"}, values)
		})
	}
}

func TestMergeOnReadDeleteDoesNotReadDroppedFirstColumn(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	tbl := newProjectedDeleteTable(t, schema, nil)
	tbl = appendProjectedDeleteRows(t, tbl, `[{"id":1,"payload":"a"},{"id":2,"payload":"b"}]`)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	oldPath := tasks[0].File.FilePath()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).DeleteColumn([]string{"payload"}).
		AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.String, "", false, nil).Commit())
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)
	tbl = appendProjectedDeleteRows(t, tbl, `[{"id":3,"extra":"new"}]`)
	tbl, blocked := blockDeleteProjectionColumn(t, tbl, oldPath, 0)
	tbl, err = tbl.Delete(t.Context(), iceberg.IsNull(iceberg.Reference("extra")), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{3}, projectedDeleteIDs(t, tbl))
	require.Zero(t, blocked.reads.Load())
}
