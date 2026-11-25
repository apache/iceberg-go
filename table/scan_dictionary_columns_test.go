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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var dictUTF8 = &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}

func newDictionaryColumnsTable(t *testing.T, schema *iceberg.Schema, props iceberg.Properties) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

	return table.New(table.Identifier{"db", "dictionary_columns"}, meta, metaLoc, fsF,
		&concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF})
}

// addDictionaryColumnsFile writes rows to a Parquet file with the given Arrow
// schema and writer settings and adds it to tbl without rewriting it.
func addDictionaryColumnsFile(t *testing.T, tbl *table.Table, name string, sc *arrow.Schema, rows string,
	props []parquet.WriterProperty, arrowProps ...pqarrow.WriterOption,
) *table.Table {
	t.Helper()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(rows))
	require.NoError(t, err)
	defer rec.Release()

	path := tbl.Location() + "/data/" + name + ".parquet"
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)
	w, err := pqarrow.NewFileWriter(sc, f, parquet.NewWriterProperties(props...),
		pqarrow.NewArrowWriterProperties(arrowProps...))
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

func fieldWithID(name string, id int, typ arrow.DataType) arrow.Field {
	return arrow.Field{
		Name: name, Type: typ, Nullable: true,
		Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{strconv.Itoa(id)}),
	}
}

// rowValue returns the value at row i, decoding dictionaries and
// rendering binary as string so expectations can be literals.
func rowValue(t *testing.T, col arrow.Array, i int) any {
	t.Helper()

	if col.IsNull(i) {
		return nil
	}
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(i)
	case *array.Dictionary:
		return rowValue(t, c.Dictionary(), c.GetValueIndex(i))
	case *array.String:
		return c.Value(i)
	case *array.Binary:
		return string(c.Value(i))
	case *array.Struct:
		return rowValue(t, c.Field(0), i)
	}
	t.Fatalf("unexpected column type %s", col.DataType())

	return nil
}

func tableRows(t *testing.T, tbl arrow.Table) [][]any {
	t.Helper()

	var rows [][]any
	rdr := array.NewTableReader(tbl, -1)
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]any, rec.NumCols())
			for j, col := range rec.Columns() {
				row[j] = rowValue(t, col, i)
			}
			rows = append(rows, row)
		}
	}

	return rows
}

func TestWithDictionaryColumnsReturnsDictionaries(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String, InitialDefault: "z"},
		iceberg.NestedField{ID: 3, Name: "b", Type: iceberg.PrimitiveTypes.Binary},
	)
	tbl := newDictionaryColumnsTable(t, schema, iceberg.Properties{table.PropertyFormatVersion: "3"})

	id := arrow.Field{
		Name: "id", Type: arrow.PrimitiveTypes.Int64,
		Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"1"}),
	}
	fileSchema := func(str, bin arrow.DataType) *arrow.Schema {
		return arrow.NewSchema([]arrow.Field{id, fieldWithID("s", 2, str), fieldWithID("b", 3, bin)}, nil)
	}
	plain := fileSchema(arrow.BinaryTypes.String, arrow.BinaryTypes.Binary)

	// b holds base64 in JSON: eA== x, eQ== y, dw== w, dg== v, dQ== u, dA== t.
	// Compression turns off the writer's cost-based fallback to plain pages, so
	// files written with it keep their dictionary pages.
	tbl = addDictionaryColumnsFile(t, tbl, "dictionary-pages", plain,
		`[{"id":1,"s":"a","b":null},{"id":2,"s":"b","b":"eQ=="},{"id":3,"s":"a","b":"eA=="}]`,
		[]parquet.WriterProperty{parquet.WithMaxRowGroupLength(2), parquet.WithCompression(compress.Codecs.Snappy)})
	tbl = addDictionaryColumnsFile(t, tbl, "plain-pages", plain,
		`[{"id":4,"s":"c","b":"dw=="}]`,
		[]parquet.WriterProperty{parquet.WithDictionaryDefault(false)})
	tbl = addDictionaryColumnsFile(t, tbl, "dictionary-fallback", plain,
		`[{"id":5,"s":null,"b":"dg=="},{"id":6,"s":"e","b":"dQ=="}]`,
		// One dictionary-encoded page, then plain pages once the dictionary
		// outgrows its limit.
		[]parquet.WriterProperty{
			parquet.WithCompression(compress.Codecs.Snappy), parquet.WithDictionaryPageSizeLimit(6),
			parquet.WithDataPageSize(1), parquet.WithBatchSize(1),
		})
	tbl = addDictionaryColumnsFile(t, tbl, "stored-large-types",
		fileSchema(arrow.BinaryTypes.LargeString, arrow.BinaryTypes.LargeBinary),
		`[{"id":7,"s":"a","b":"eA=="}]`, nil, pqarrow.WithStoreSchema())
	tbl = addDictionaryColumnsFile(t, tbl, "stored-ordered-int8-dictionary",
		fileSchema(
			&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String, Ordered: true},
			&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.Binary, Ordered: true}),
		`[{"id":8,"s":"f","b":"dA=="}]`, nil, pqarrow.WithStoreSchema())
	tbl = addDictionaryColumnsFile(t, tbl, "missing-columns", arrow.NewSchema([]arrow.Field{id}, nil),
		`[{"id":9}]`, nil)

	// Columns added after the files were written: every file lacks the
	// parents of the requested nested leaves.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"st"}, iceberg.NewStructType(
			iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.String},
		), "", false, nil).
		AddColumn([]string{"l"}, iceberg.NewListType(1, iceberg.PrimitiveTypes.String, false), "", false, nil).
		AddColumn([]string{"m"}, iceberg.NewMapType(
			1, iceberg.PrimitiveTypes.String, 2, iceberg.PrimitiveTypes.String, false,
		), "", false, nil).
		Commit())
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	for _, largeTypes := range []string{"false", "true"} {
		t.Run("use_large_types="+largeTypes, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			arrTbl, err := tbl.Scan(
				table.WithDictionaryColumns("s", "b", "st.x", "l.element", "m.key", "m.value"),
				table.WithOptions(iceberg.Properties{table.ScanOptionArrowUseLargeTypes: largeTypes}),
			).ToArrowTable(ctx)
			require.NoError(t, err)
			defer arrTbl.Release()

			list := "list"
			if largeTypes == "true" {
				list = "large_list"
			}
			assert.Equal(t, []string{
				"int64",
				"dictionary<values=utf8, indices=int32, ordered=false>",
				"dictionary<values=binary, indices=int32, ordered=false>",
				"struct<x: dictionary<values=utf8, indices=int32, ordered=false> nullable>",
				list + "<element: dictionary<values=utf8, indices=int32, ordered=false>, nullable>",
				"map<dictionary<values=utf8, indices=int32, ordered=false>, dictionary<values=utf8, indices=int32, ordered=false>, items_nullable>",
			}, arrowTypeStrings(arrTbl.Schema()))

			rows := tableRows(t, arrTbl)
			slices.SortFunc(rows, func(a, b []any) int { return int(a[0].(int64) - b[0].(int64)) })
			assert.Equal(t, [][]any{
				{int64(1), "a", nil, nil, nil, nil},
				{int64(2), "b", "y", nil, nil, nil},
				{int64(3), "a", "x", nil, nil, nil},
				{int64(4), "c", "w", nil, nil, nil},
				{int64(5), nil, "v", nil, nil, nil},
				{int64(6), "e", "u", nil, nil, nil},
				{int64(7), "a", "x", nil, nil, nil},
				{int64(8), "f", "t", nil, nil, nil},
				{int64(9), "z", nil, nil, nil, nil},
			}, rows)
		})
	}

	t.Run("incompatible file type", func(t *testing.T) {
		path := tbl.Location() + "/data/int32-s.parquet"
		writeParquetFile(t, path,
			arrow.NewSchema([]arrow.Field{id, fieldWithID("s", 2, arrow.PrimitiveTypes.Int32)}, nil), `[{"id":10,"s":1}]`)
		info, err := os.Stat(path)
		require.NoError(t, err)
		df, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
			path, iceberg.ParquetFile, nil, nil, nil, 1, info.Size())
		require.NoError(t, err)

		_, records, err := tbl.Scan(table.WithDictionaryColumns("s")).
			ReadTasks(t.Context(), []table.FileScanTask{{File: df.Build()}})
		require.NoError(t, err)
		var rec arrow.RecordBatch
		for rec, err = range records {
			if err != nil {
				break
			}
			rec.Release()
		}
		require.ErrorIs(t, err, iceberg.ErrResolve)
	})
}

func arrowTypeStrings(sc *arrow.Schema) []string {
	types := make([]string, sc.NumFields())
	for i, f := range sc.Fields() {
		types[i] = f.Type.String()
	}

	return types
}

// Re-encoding a decoded batch gives a dictionary of only that batch's values,
// while a Parquet dictionary read accumulates the row group's dictionary
// across batches, so dictionary lengths show which path produced a batch.
func TestWithDictionaryColumnsReadsParquetDictionaries(t *testing.T) {
	dictionaryLengths := func(t *testing.T, scan *table.Scan) []int {
		t.Helper()

		_, records, err := scan.ToArrowRecords(t.Context())
		require.NoError(t, err)
		var lengths []int
		var walk func(col arrow.Array)
		walk = func(col arrow.Array) {
			switch c := col.(type) {
			case *array.Dictionary:
				lengths = append(lengths, c.Dictionary().Len())
			case *array.Struct:
				walk(c.Field(0))
			case *array.Map:
				walk(c.Keys())
				walk(c.Items())
			case *array.List:
				walk(c.ListValues())
			default:
				t.Fatalf("unexpected column type %s", col.DataType())
			}
		}
		for rec, err := range records {
			require.NoError(t, err)
			for _, col := range rec.Columns()[1:] {
				walk(col)
			}
			rec.Release()
		}

		return lengths
	}

	t.Run("split tasks", func(t *testing.T) {
		schema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String},
			iceberg.NestedField{ID: 3, Name: "st", Type: iceberg.NewStructType(
				iceberg.NestedField{ID: 5, Name: "x", Type: iceberg.PrimitiveTypes.String},
			)},
			iceberg.NestedField{ID: 4, Name: "l", Type: iceberg.NewListType(6, iceberg.PrimitiveTypes.String, false)},
			iceberg.NestedField{ID: 7, Name: "m", Type: iceberg.NewMapType(
				8, iceberg.PrimitiveTypes.String, 9, iceberg.PrimitiveTypes.String, false,
			)},
		)
		tbl := newDictionaryColumnsTable(t, schema, iceberg.Properties{
			table.ParquetRowGroupLimitKey: "2",
			table.ReadSplitTargetSizeKey:  "1",
		})
		tbl = appendRows(t, tbl, `[
			{"id":1,"s":"a","st":{"x":"a"},"l":["a"],"m":[{"key":"a","value":"a"}]},
			{"id":2,"s":"b","st":{"x":"b"},"l":["b"],"m":[{"key":"b","value":"b"}]},
			{"id":3,"s":"c","st":{"x":"c"},"l":["c"],"m":[{"key":"c","value":"c"}]},
			{"id":4,"s":"d","st":{"x":"d"},"l":["d"],"m":[{"key":"d","value":"d"}]}]`)

		scan := tbl.Scan(table.WithDictionaryColumns("s", "st.x", "l.element", "m.key", "m.value"),
			table.WithArrowBatchSize(1))
		tasks, err := scan.PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 2)
		require.Equal(t, tasks[0].File.FilePath(), tasks[1].File.FilePath())

		assert.Equal(t, []int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, dictionaryLengths(t, scan))
	})

	t.Run("name mapping", func(t *testing.T) {
		schema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String},
		)
		tbl := newDictionaryColumnsTable(t, schema, nameMappingProperties(t, schema.NameMapping()))
		tbl = addDictionaryColumnsFile(t, tbl, "without-field-ids", arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil), `[{"id":1,"s":"a"},{"id":2,"s":"b"}]`, nil)

		scan := tbl.Scan(table.WithDictionaryColumns("s"), table.WithArrowBatchSize(1))
		assert.Equal(t, []int{1, 2}, dictionaryLengths(t, scan))
	})
}

func TestWithDictionaryColumnsValidation(t *testing.T) {
	// The table has no snapshot, so these scans have no tasks.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "u", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 4, Name: "f", Type: iceberg.MustFixedTypeOf(4)},
		iceberg.NestedField{ID: 5, Name: "g", Type: iceberg.GeometryType{}},
	)
	tbl := newDictionaryColumnsTable(t, schema, iceberg.Properties{table.PropertyFormatVersion: "3"})

	for _, tc := range []struct {
		name string
		opts []table.ScanOption
		err  error
	}{
		{"unknown column", []table.ScanOption{table.WithDictionaryColumns("missing")}, iceberg.ErrInvalidSchema},
		{"case sensitive", []table.ScanOption{table.WithDictionaryColumns("S")}, iceberg.ErrInvalidSchema},
		{"uuid", []table.ScanOption{table.WithDictionaryColumns("u")}, iceberg.ErrInvalidArgument},
		{"fixed", []table.ScanOption{table.WithDictionaryColumns("f")}, iceberg.ErrInvalidArgument},
		{"geometry", []table.ScanOption{table.WithDictionaryColumns("g")}, iceberg.ErrInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tbl.Scan(tc.opts...).ToArrowRecords(t.Context())
			require.ErrorIs(t, err, tc.err)
		})
	}

	for _, tc := range []struct {
		name   string
		opts   []table.ScanOption
		fields []arrow.Field
	}{
		{"case insensitive", []table.ScanOption{
			table.WithSelectedFields("id", "s"), table.WithCaseSensitive(false), table.WithDictionaryColumns("S"),
		}, []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "s", Type: dictUTF8, Nullable: true},
		}},
		{"unprojected column", []table.ScanOption{
			table.WithSelectedFields("id"), table.WithDictionaryColumns("s"),
		}, []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, records, err := tbl.Scan(tc.opts...).ToArrowRecords(t.Context())
			require.NoError(t, err)
			for _, err := range records {
				require.NoError(t, err)
			}
			assert.True(t, arrow.NewSchema(tc.fields, nil).Equal(sc), sc.String())
		})
	}

	t.Run("renamed column with time travel", func(t *testing.T) {
		tbl := newDictionaryColumnsTable(t, iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String},
		), nil)
		tbl = appendRows(t, tbl, `[{"id":1,"s":"a"}]`)
		before := tbl.CurrentSnapshot().SnapshotID
		tx := tbl.NewTransaction()
		require.NoError(t, tx.UpdateSchema(true, false).RenameColumn([]string{"s"}, "renamed").Commit())
		tbl, err := tx.Commit(t.Context())
		require.NoError(t, err)

		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)
		ctx := compute.WithAllocator(t.Context(), mem)
		sc, records, err := tbl.Scan(table.WithSnapshotID(before), table.WithDictionaryColumns("s")).ToArrowRecords(ctx)
		require.NoError(t, err)
		for rec, err := range records {
			require.NoError(t, err)
			rec.Release()
		}
		assert.Equal(t, []string{"int64", "dictionary<values=utf8, indices=int32, ordered=false>"}, arrowTypeStrings(sc))

		_, _, err = tbl.Scan(table.WithSnapshotID(before), table.WithDictionaryColumns("renamed")).ToArrowRecords(t.Context())
		require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	})
}

func TestWithDictionaryColumnsDeletes(t *testing.T) {
	st := iceberg.NestedField{ID: 4, Name: "st", Type: iceberg.NewStructType(
		iceberg.NestedField{ID: 5, Name: "x", Type: iceberg.PrimitiveTypes.String},
	)}
	s := iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String}
	b := iceberg.NestedField{ID: 3, Name: "b", Type: iceberg.PrimitiveTypes.Binary}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}, s, b, st)
	tbl := newDictionaryColumnsTable(t, schema, iceberg.Properties{
		table.PropertyFormatVersion: "2",
		table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
	})
	// b holds base64 in JSON: eA== x, eQ== y, eg== z.
	tbl = appendRows(t, tbl, `[
		{"id":1,"s":"a","b":"eA==","st":{"x":"p"}},
		{"id":2,"s":"b","b":"eQ==","st":{"x":"p"}},
		{"id":3,"s":"a","b":"eQ==","st":{"x":"p"}},
		{"id":4,"s":"c","b":"eA==","st":{"x":"p"}},
		{"id":5,"s":"b","b":"eA==","st":{"x":"p"}},
		{"id":6,"s":"d","b":"eg==","st":{"x":"q"}},
		{"id":7,"s":"a","b":"eg==","st":{"x":"p"}}]`)

	tbl, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	for _, key := range []struct {
		id    int
		field iceberg.NestedField
		rows  string
	}{
		{2, s, `[{"s":"b"}]`},
		{3, b, `[{"b":"eQ=="}]`},
		{5, st, `[{"st":{"x":"q"}}]`},
	} {
		sc, err := table.SchemaToArrowSchema(iceberg.NewSchema(0, key.field), nil, true, false)
		require.NoError(t, err)
		records, release := makeEqDeleteRecords(t, sc, key.rows)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.CommitEqualityDeletes(t.Context(), []int{key.id}, records, nil))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
		release()
	}

	arrTbl, err := tbl.Scan(
		table.WithDictionaryColumns("s", "b", "st.x"),
	).ToArrowTable(t.Context())
	require.NoError(t, err)
	defer arrTbl.Release()

	assert.Equal(t, [][]any{{int64(4), "c", "x", "p"}, {int64(7), "a", "z", "p"}}, tableRows(t, arrTbl))
}

func TestWithDictionaryColumnsChangelog(t *testing.T) {
	ctx := t.Context()
	tbl := newChangelogMORTable(t, 2)
	tbl = appendChangelogRows(t, ctx, tbl)
	tbl = deleteChangelogID(t, ctx, tbl, 2)

	sc, records, err := tbl.NewIncrementalChangelogScan(table.WithDictionaryColumns("data")).ToArrowRecords(ctx)
	require.NoError(t, err)
	assert.True(t, arrow.TypeEqual(dictUTF8, sc.Field(1).Type), sc.Field(1).Type.String())

	var rows [][]any
	for rec, err := range records {
		require.NoError(t, err)
		require.True(t, sc.Equal(rec.Schema()), rec.Schema().String())
		for i := range int(rec.NumRows()) {
			rows = append(rows, []any{
				rowValue(t, rec.Column(0), i), rowValue(t, rec.Column(1), i), rec.Column(2).ValueStr(i),
			})
		}
		rec.Release()
	}
	assert.ElementsMatch(t, [][]any{
		{int64(1), "a", "INSERT"},
		{int64(2), "b", "INSERT"},
		{int64(3), "c", "INSERT"},
		{int64(2), "b", "DELETE"},
	}, rows)

	t.Run("normalized", func(t *testing.T) {
		normTbl, start := normalizationTable(t, 2)
		for _, largeTypes := range []string{"false", "true"} {
			t.Run("use_large_types="+largeTypes, func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
				defer mem.AssertSize(t, 0)
				ctx := compute.WithAllocator(t.Context(), mem)

				sc, records, err := normTbl.NewIncrementalChangelogScan(
					table.WithDictionaryColumns("data"),
					table.WithOptions(iceberg.Properties{table.ScanOptionArrowUseLargeTypes: largeTypes}),
				).FromSnapshotExclusive(start).ToNormalizedArrowRecords(ctx, table.ChangelogNormalizationOptions{
					ComputeUpdates: true,
				})
				require.NoError(t, err)
				assert.True(t, arrow.TypeEqual(dictUTF8, sc.Field(1).Type), sc.Field(1).Type.String())

				var got [][]any
				for rec, err := range records {
					require.NoError(t, err)
					require.True(t, sc.Equal(rec.Schema()), rec.Schema().String())
					assert.True(t, arrow.TypeEqual(dictUTF8, rec.Column(1).DataType()), rec.Column(1).DataType().String())
					for i := range int(rec.NumRows()) {
						got = append(got, []any{
							rowValue(t, rec.Column(0), i),
							rowValue(t, rec.Column(1), i),
							rec.Column(2).ValueStr(i),
						})
					}
					rec.Release()
				}
				assert.Equal(t, [][]any{
					{int64(2), "old", string(table.ChangelogOpUpdateBefore)},
					{int64(2), "new", string(table.ChangelogOpUpdateAfter)},
					{int64(3), "removed", string(table.ChangelogOpDelete)},
					{int64(4), "added", string(table.ChangelogOpInsert)},
				}, got)
			})
		}
	})
}

func TestWithDictionaryColumnsRowFilters(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "b", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 4, Name: "st", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 5, Name: "x", Type: iceberg.PrimitiveTypes.String},
		)},
	)
	tbl := newDictionaryColumnsTable(t, schema, nil)
	// b holds base64 in JSON: eA== x, eQ== y.
	tbl = appendRows(t, tbl, `[
		{"id":1,"s":"alpha","b":"eA==","st":{"x":"p"}},
		{"id":2,"s":"alpine","b":"eQ==","st":{"x":"q"}},
		{"id":3,"s":"beta","b":"eA==","st":{"x":"p"}},
		{"id":4,"s":null,"b":null,"st":{"x":"q"}}]`)

	wantTypes := []string{
		"int64",
		"dictionary<values=utf8, indices=int32, ordered=false>",
		"dictionary<values=binary, indices=int32, ordered=false>",
		"struct<x: dictionary<values=utf8, indices=int32, ordered=false> nullable>",
	}

	for _, tc := range []struct {
		name   string
		filter iceberg.BooleanExpression
		want   [][]any
	}{
		{
			name:   "string equal",
			filter: iceberg.EqualTo(iceberg.Reference("s"), "alpha"),
			want:   [][]any{{int64(1), "alpha", "x", "p"}},
		},
		{
			name:   "string in",
			filter: iceberg.IsIn(iceberg.Reference("s"), "alpha", "beta"),
			want: [][]any{
				{int64(1), "alpha", "x", "p"},
				{int64(3), "beta", "x", "p"},
			},
		},
		{
			name:   "string starts_with",
			filter: iceberg.StartsWith(iceberg.Reference("s"), "alp"),
			want: [][]any{
				{int64(1), "alpha", "x", "p"},
				{int64(2), "alpine", "y", "q"},
			},
		},
		{
			name:   "string is_null",
			filter: iceberg.IsNull(iceberg.Reference("s")),
			want:   [][]any{{int64(4), nil, nil, "q"}},
		},
		{
			name:   "string not_null",
			filter: iceberg.NotNull(iceberg.Reference("s")),
			want: [][]any{
				{int64(1), "alpha", "x", "p"},
				{int64(2), "alpine", "y", "q"},
				{int64(3), "beta", "x", "p"},
			},
		},
		{
			name:   "binary equal",
			filter: iceberg.EqualTo(iceberg.Reference("b"), []byte("x")),
			want: [][]any{
				{int64(1), "alpha", "x", "p"},
				{int64(3), "beta", "x", "p"},
			},
		},
		{
			name:   "nested struct string equal",
			filter: iceberg.EqualTo(iceberg.Reference("st.x"), "q"),
			want: [][]any{
				{int64(2), "alpine", "y", "q"},
				{int64(4), nil, nil, "q"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			arrTbl, err := tbl.Scan(
				table.WithDictionaryColumns("s", "b", "st.x"),
				table.WithRowFilter(tc.filter),
			).ToArrowTable(ctx)
			require.NoError(t, err)
			defer arrTbl.Release()

			assert.Equal(t, wantTypes, arrowTypeStrings(arrTbl.Schema()))
			assert.Equal(t, tc.want, tableRows(t, arrTbl))
		})
	}
}
