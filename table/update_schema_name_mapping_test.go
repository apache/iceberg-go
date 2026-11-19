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
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nameMappingCatalog struct {
	meta table.Metadata
}

func (c *nameMappingCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (c *nameMappingCatalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(c.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.meta = meta

	return meta, "", nil
}

func newNameMappingTestTable(t *testing.T, mapping string) (*table.Table, string) {
	t.Helper()

	dir := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)
	props := iceberg.Properties{}
	if mapping != "" {
		props[table.DefaultNameMappingKey] = mapping
	}
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, dir, props)
	require.NoError(t, err)

	tbl := table.New(table.Identifier{"default", "name_mapping"}, meta, dir+"/metadata/00000.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&nameMappingCatalog{meta: meta},
	)

	return tbl, dir
}

func mappedNames(t *testing.T, mapping iceberg.NameMapping, id int) []string {
	t.Helper()

	var names []string
	var visit func([]iceberg.MappedField)
	visit = func(fields []iceberg.MappedField) {
		for _, field := range fields {
			if field.FieldID != nil && *field.FieldID == id {
				names = append(names, field.Names...)
			}
			visit(field.Fields)
		}
	}
	visit(mapping)

	return names
}

// A schema update must keep the table's name mapping in step with the schema,
// as Java's SchemaUpdate and PyIceberg's UpdateSchema do, or files without
// field IDs cannot be read for added or renamed columns.
func TestUpdateSchemaUpdatesTableNameMapping(t *testing.T) {
	ctx := context.Background()
	tbl, _ := newNameMappingTestTable(t,
		`[{"field-id":1,"names":["id"]},{"field-id":2,"names":["data"]}]`)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn([]string{"category"}, iceberg.PrimitiveTypes.String, "", false, nil).
		RenameColumn([]string{"data"}, "payload").
		Commit())
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)

	mapping := tbl.Metadata().NameMapping()
	require.NotNil(t, mapping)
	assert.Equal(t, []string{"id"}, mappedNames(t, mapping, 1))
	assert.Equal(t, []string{"data", "payload"}, mappedNames(t, mapping, 2))
	assert.Equal(t, []string{"category"}, mappedNames(t, mapping, 3))
}

// Without a name mapping property the update must not add one.
func TestUpdateSchemaWithoutNameMappingLeavesPropertyUnset(t *testing.T) {
	ctx := context.Background()
	tbl, _ := newNameMappingTestTable(t, "")

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn([]string{"category"}, iceberg.PrimitiveTypes.String, "", false, nil).
		Commit())
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)

	assert.NotContains(t, tbl.Metadata().Properties(), table.DefaultNameMappingKey)
}

// End to end: AddDataFilesByPath sets the name mapping, a column is added, and
// a later file without field IDs that carries the new column is added. Adding
// that file must succeed and the scan must read the new column from it.
func TestAddFilesAfterAddColumnReadsNewColumnByName(t *testing.T) {
	ctx := context.Background()
	tbl, dir := newNameMappingTestTable(t, "")
	fs := iceio.LocalFS{}

	writeParquet := func(path string, fields []arrow.Field, cols ...arrow.Array) {
		sc := arrow.NewSchema(fields, nil)
		rec := array.NewRecordBatch(sc, cols, int64(cols[0].Len()))
		defer rec.Release()
		arrTbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
		defer arrTbl.Release()

		fo, err := fs.Create(ctx, path)
		require.NoError(t, err)
		require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(), nil, pqarrow.DefaultWriterProps()))
	}
	int32s := func(vals ...int32) arrow.Array {
		b := array.NewInt32Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(vals, nil)

		return b.NewArray()
	}
	strs := func(vals ...string) arrow.Array {
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(vals, nil)

		return b.NewArray()
	}

	idField := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}
	dataField := arrow.Field{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true}
	categoryField := arrow.Field{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true}

	first := dir + "/data/first.parquet"
	writeParquet(first, []arrow.Field{idField, dataField}, int32s(1), strs("a"))
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFilesByPath(ctx, []string{first}, nil, false))
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)
	require.Contains(t, tbl.Metadata().Properties(), table.DefaultNameMappingKey)

	txn = tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn([]string{"category"}, iceberg.PrimitiveTypes.String, "", false, nil).
		Commit())
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	second := dir + "/data/second.parquet"
	writeParquet(second, []arrow.Field{idField, dataField, categoryField}, int32s(2), strs("b"), strs("books"))
	txn = tbl.NewTransaction()
	require.NoError(t, txn.AddDataFilesByPath(ctx, []string{second}, nil, false))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	result, err := tbl.Scan(table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int32(2)))).ToArrowTable(ctx)
	require.NoError(t, err)
	defer result.Release()

	require.EqualValues(t, 1, result.NumRows())
	category := result.Column(result.Schema().FieldIndices("category")[0]).Data().Chunk(0).(*array.String)
	require.False(t, category.IsNull(0), "category was read as null")
	assert.Equal(t, "books", category.Value(0))
}
