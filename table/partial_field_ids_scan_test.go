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
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func parquetFieldID(id string) arrow.Metadata {
	return arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{id})
}

// newPartialFieldIDsTable builds the shape of the iceberg-verification table
// v2/partial-field-ids-parquet-data, plus one top-level column without an ID.
//
// The data file has field IDs on c1, c2, c3 and its fields, c4 and c5. It has
// none on the list element, the map key, the map value or the column "extra".
// The name mapping lists "element", "key", "value" and "extra", and none of
// the names of the columns that carry an ID.
func newPartialFieldIDsTable(t *testing.T) *table.Table {
	t.Helper()

	ctx := t.Context()
	cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "location", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 8, Name: "lat", Type: iceberg.PrimitiveTypes.Float64},
			iceberg.NestedField{ID: 9, Name: "lon", Type: iceberg.PrimitiveTypes.Float64},
		)},
		iceberg.NestedField{ID: 4, Name: "tags", Type: iceberg.NewListType(10, iceberg.PrimitiveTypes.String, false)},
		iceberg.NestedField{ID: 5, Name: "props", Type: iceberg.NewMapType(
			11, iceberg.PrimitiveTypes.String, 12, iceberg.PrimitiveTypes.Int32, false)},
		iceberg.NestedField{ID: 6, Name: "added", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 7, Name: "extra", Type: iceberg.PrimitiveTypes.String},
	)
	mapping := iceberg.NameMapping{
		{FieldID: new(1), Names: []string{"id"}},
		{FieldID: new(2), Names: []string{"name"}},
		{FieldID: new(3), Names: []string{"location"}, Fields: []iceberg.MappedField{
			{FieldID: new(8), Names: []string{"lat"}},
			{FieldID: new(9), Names: []string{"lon"}},
		}},
		{FieldID: new(4), Names: []string{"tags", "c4"}, Fields: []iceberg.MappedField{
			{FieldID: new(10), Names: []string{"element"}},
		}},
		{FieldID: new(5), Names: []string{"props", "c5"}, Fields: []iceberg.MappedField{
			{FieldID: new(11), Names: []string{"key"}},
			{FieldID: new(12), Names: []string{"value"}},
		}},
		{FieldID: new(6), Names: []string{"added"}},
		{FieldID: new(7), Names: []string{"extra"}},
	}
	mappingJSON, err := json.Marshal(mapping)
	require.NoError(t, err)
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "partial"), schema,
		catalog.WithProperties(iceberg.Properties{
			table.DefaultNameMappingKey: string(mappingJSON),
		}))
	require.NoError(t, err)
	// The catalog assigns fresh IDs; the file below is written against these.
	require.True(t, schema.Equals(tbl.Schema()), "table schema IDs: %s", tbl.Schema())

	// The physical column order differs from the table schema's.
	fileSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "c5", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
			Nullable: true, Metadata: parquetFieldID("5"),
		},
		{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "c2", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: parquetFieldID("2")},
		{Name: "c4", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true, Metadata: parquetFieldID("4")},
		{Name: "c3", Type: arrow.StructOf(
			arrow.Field{Name: "c9", Type: arrow.PrimitiveTypes.Float64, Nullable: true, Metadata: parquetFieldID("9")},
			arrow.Field{Name: "c8", Type: arrow.PrimitiveTypes.Float64, Nullable: true, Metadata: parquetFieldID("8")},
		), Nullable: true, Metadata: parquetFieldID("3")},
		{Name: "c1", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: parquetFieldID("1")},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, fileSchema, []string{`[
		{"c5":[{"key":"k","value":1}],"extra":"x1","c2":"alpha","c4":["a","b"],"c3":{"c9":-74.006,"c8":40.7128},"c1":1},
		{"c5":null,"extra":"x2","c2":null,"c4":null,"c3":null,"c1":2},
		{"c5":[],"extra":"x3","c2":"charlie","c4":[],"c3":{"c9":-0.1278,"c8":51.5074},"c1":3}
	]`})
	require.NoError(t, err)
	defer data.Release()

	filePath := tbl.Location() + "/data/partial.parquet"
	writer, err := (iceio.LocalFS{}).Create(ctx, filePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(data, writer, 2,
		parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()))

	dataFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, filePath, iceberg.ParquetFile,
		nil, nil, nil, 3, 1024,
	)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(ctx,
		[]iceberg.DataFile{dataFileBuilder.Build()}, nil, table.WithoutAutoNameMapping()))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	return tbl
}

func scanToJSON(t *testing.T, scan *table.Scan) string {
	t.Helper()

	result, err := scan.ToArrowTable(t.Context())
	require.NoError(t, err)
	defer result.Release()

	rdr := array.NewTableReader(result, -1)
	defer rdr.Release()

	rows := []json.RawMessage{}
	for rdr.Next() {
		batch, err := json.Marshal(rdr.RecordBatch())
		require.NoError(t, err)

		var batchRows []json.RawMessage
		require.NoError(t, json.Unmarshal(batch, &batchRows))
		rows = append(rows, batchRows...)
	}
	require.NoError(t, rdr.Err())

	out, err := json.Marshal(rows)
	require.NoError(t, err)

	return string(out)
}

// A data file that has a field ID at any depth is read by ID only. The name
// mapping is not consulted, and a field without an ID, a list without an
// element ID and a map without a key or value ID are absent, so they are null.
func TestScanDataFileWithPartialFieldIDs(t *testing.T) {
	tbl := newPartialFieldIDsTable(t)

	tests := []struct {
		name     string
		fields   []string
		expected string
	}{
		{"all fields", []string{"*"}, `[
			{"id":1,"name":"alpha","location":{"lat":40.7128,"lon":-74.006},"tags":null,"props":null,"added":null,"extra":null},
			{"id":2,"name":null,"location":null,"tags":null,"props":null,"added":null,"extra":null},
			{"id":3,"name":"charlie","location":{"lat":51.5074,"lon":-0.1278},"tags":null,"props":null,"added":null,"extra":null}
		]`},
		{"only fields with an id", []string{"id", "name"}, `[
			{"id":1,"name":"alpha"},{"id":2,"name":null},{"id":3,"name":"charlie"}
		]`},
		{"nested field with an id", []string{"location.lon"}, `[
			{"location":{"lon":-74.006}},{"location":null},{"location":{"lon":-0.1278}}
		]`},
		{"list without an element id", []string{"tags"}, `[{"tags":null},{"tags":null},{"tags":null}]`},
		{"map without key and value ids", []string{"props"}, `[{"props":null},{"props":null},{"props":null}]`},
		{
			"column without an id that the mapping names",
			[]string{"extra"},
			`[{"extra":null},{"extra":null},{"extra":null}]`,
		},
		{"absent fields beside a field with an id", []string{"tags", "id", "extra"}, `[
			{"tags":null,"id":1,"extra":null},{"tags":null,"id":2,"extra":null},{"tags":null,"id":3,"extra":null}
		]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scanToJSON(t, tbl.Scan(table.WithSelectedFields(tt.fields...)))
			require.JSONEq(t, tt.expected, got)
		})
	}
}

// Filters see an absent field as null too, so the result does not depend on
// whether the filter column is also projected.
func TestScanDataFileWithPartialFieldIDsFilters(t *testing.T) {
	tbl := newPartialFieldIDsTable(t)

	tests := []struct {
		name     string
		filter   iceberg.BooleanExpression
		expected string
	}{
		{"absent column is null", iceberg.IsNull(iceberg.Reference("extra")), `[{"id":1},{"id":2},{"id":3}]`},
		{"absent column is not null", iceberg.NotNull(iceberg.Reference("extra")), `[]`},
		{"absent column equals its file value", iceberg.EqualTo(iceberg.Reference("extra"), "x1"), `[]`},
		{"column with an id", iceberg.EqualTo(iceberg.Reference("name"), "charlie"), `[{"id":3}]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scanToJSON(t, tbl.Scan(
				table.WithSelectedFields("id"), table.WithRowFilter(tt.filter)))
			require.JSONEq(t, tt.expected, got)
		})
	}
}
