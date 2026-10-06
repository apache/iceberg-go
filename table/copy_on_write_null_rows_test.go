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
	"math"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// A copy-on-write delete must remove only the rows the filter is true for.
// Rows where the filter is NULL (because a referenced column is NULL) don't
// match it and must survive the rewrite.
func TestCopyOnWriteDeleteKeepsRowsWhereFilterIsNull(t *testing.T) {
	age := iceberg.Reference("age")
	name := iceberg.Reference("name")

	// rows: id=1 {25, "a"}, id=2 {30, NULL}, id=3 {NULL, "c"}, id=4 {40, "d"}
	cases := []struct {
		name   string
		filter iceberg.BooleanExpression
		want   []int64
	}{
		{"equal", iceberg.EqualTo(age, int64(30)), []int64{1, 3, 4}},
		{"greater than", iceberg.GreaterThan(age, int64(35)), []int64{1, 2, 3}},
		{"in", iceberg.IsIn(age, int64(30), int64(40)), []int64{1, 3}},
		// binding reduces the set to one value and the predicate to Equal
		{"in with duplicate values", iceberg.IsIn(age, int64(30), int64(30)), []int64{1, 3, 4}},
		{"not in", iceberg.NotIn(age, int64(30), int64(40)), []int64{2, 3, 4}},
		{"not in with duplicate values", iceberg.NotIn(age, int64(30), int64(30)), []int64{2, 3}},
		{"not", iceberg.NewNot(iceberg.EqualTo(age, int64(30))), []int64{2, 3}},
		{"is null", iceberg.IsNull(age), []int64{1, 2, 4}},
		{"and", iceberg.NewAnd(iceberg.EqualTo(age, int64(30)), iceberg.EqualTo(name, "c")), []int64{1, 2, 3, 4}},
		{"or", iceberg.NewOr(iceberg.EqualTo(age, int64(25)), iceberg.EqualTo(name, "c")), []int64{2, 4}},
		{"not and", iceberg.NewNot(iceberg.NewAnd(iceberg.EqualTo(age, int64(30)), iceberg.EqualTo(name, "c"))), []int64{2, 3}},
	}

	for _, version := range []string{"2", "3"} {
		for _, c := range cases {
			t.Run("v"+version+"/"+c.name, func(t *testing.T) {
				ctx := context.Background()
				tbl := newCopyOnWriteNullRowsTable(t, version)

				txn := tbl.NewTransaction()
				require.NoError(t, txn.Delete(ctx, c.filter, nil))
				tbl, err := txn.Commit(ctx)
				require.NoError(t, err)

				ids := scanIDs(t, tbl)
				slices.Sort(ids)
				require.Equal(t, c.want, ids)
			})
		}
	}
}

// A filtered overwrite rewrites matched files through the same path.
func TestCopyOnWriteOverwriteKeepsRowsWhereFilterIsNull(t *testing.T) {
	ctx := context.Background()
	tbl := newCopyOnWriteNullRowsTable(t, "2")

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":5,"age":30,"name":"e"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	txn := tbl.NewTransaction()
	require.NoError(t, txn.OverwriteTable(ctx, data, 1024, nil,
		table.WithOverwriteFilter(iceberg.EqualTo(iceberg.Reference("age"), int64(30)))))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	ids := scanIDs(t, tbl)
	slices.Sort(ids)
	require.Equal(t, []int64{1, 3, 4, 5}, ids)
}

// For floating-point columns, NaN compares false rather than NULL, so a delete
// must remove exactly the rows a scan with the same filter returns.
func TestCopyOnWriteDeleteMatchesScanWithNaNAndNull(t *testing.T) {
	x := iceberg.Reference("x")

	cases := []struct {
		name   string
		filter iceberg.BooleanExpression
	}{
		{"less than", iceberg.LessThan(x, 5.0)},
		{"greater than or equal", iceberg.GreaterThanEqual(x, 5.0)},
		{"not less than", iceberg.NewNot(iceberg.LessThan(x, 5.0))},
		{"not equal", iceberg.NotEqualTo(x, 1.0)},
		{"is nan", iceberg.IsNaN(x)},
		{"not nan", iceberg.NotNaN(x)},
		{"not is nan", iceberg.NewNot(iceberg.IsNaN(x))},
	}

	for _, version := range []string{"2", "3"} {
		for _, c := range cases {
			t.Run("v"+version+"/"+c.name, func(t *testing.T) {
				tbl := newCopyOnWriteNaNTable(t, version,
					[]float64{1, math.NaN(), 10, 0}, []bool{true, true, true, false})
				requireDeleteMatchesScan(t, tbl, c.filter)
			})
		}
	}
}

// Row-group pruning on the rows to keep must not skip a NaN row. Parquet
// min/max leave NaN out, so with every other value on one side of 5 a pruning
// filter of x >= 5 for NOT(x < 5) would drop the whole row group.
func TestCopyOnWriteDeleteKeepsNaNRowsInPrunedRowGroups(t *testing.T) {
	x := iceberg.Reference("x")

	cases := []struct {
		name   string
		values []float64
		filter iceberg.BooleanExpression
	}{
		{"less than", []float64{1, math.NaN(), 2}, iceberg.LessThan(x, 5.0)},
		{"less than or equal", []float64{1, math.NaN(), 2}, iceberg.LessThanEqual(x, 5.0)},
		{"greater than", []float64{10, math.NaN(), 20}, iceberg.GreaterThan(x, 5.0)},
		{"greater than or equal", []float64{10, math.NaN(), 20}, iceberg.GreaterThanEqual(x, 5.0)},
		{"or", []float64{1, math.NaN(), 20}, iceberg.NewOr(iceberg.LessThan(x, 5.0), iceberg.GreaterThan(x, 10.0))},
		{"and", []float64{1, math.NaN(), 2}, iceberg.NewAnd(iceberg.GreaterThan(x, 0.0), iceberg.LessThan(x, 5.0))},
	}

	for _, version := range []string{"2", "3"} {
		for _, c := range cases {
			t.Run("v"+version+"/"+c.name, func(t *testing.T) {
				tbl := newCopyOnWriteNaNTable(t, version, c.values, nil)
				requireDeleteMatchesScan(t, tbl, c.filter)
			})
		}
	}
}

// requireDeleteMatchesScan deletes filter from tbl and checks that exactly the
// rows a scan with the same filter returns were removed.
func requireDeleteMatchesScan(t *testing.T, tbl *table.Table, filter iceberg.BooleanExpression) {
	t.Helper()
	ctx := context.Background()

	before := scanIDs(t, tbl)
	require.NotEmpty(t, before)
	matched := scanIDsMatching(t, tbl, filter)
	want := slices.DeleteFunc(before, func(id int64) bool {
		return slices.Contains(matched, id)
	})
	slices.Sort(want)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.Delete(ctx, filter, nil))
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)

	ids := scanIDs(t, tbl)
	slices.Sort(ids)
	require.Equal(t, want, ids, "scan with the filter matched %v", matched)
}

func newCopyOnWriteNullRowsTable(t *testing.T, version string) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "age", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: version,
			table.WriteDeleteModeKey:    table.WriteModeCopyOnWrite,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "cow_null_rows"}, meta, metaLoc, fsF, cat)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"age":25,"name":"a"},{"id":2,"age":30,"name":null},` +
			`{"id":3,"age":null,"name":"c"},{"id":4,"age":40,"name":"d"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(context.Background(), array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	return tbl
}

func newCopyOnWriteNaNTable(t *testing.T, version string, values []float64, valid []bool) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "x", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: version,
			table.WriteDeleteModeKey:    table.WriteModeCopyOnWrite,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "cow_nan_rows"}, meta, metaLoc, fsF, cat)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil))
	defer bldr.Release()
	for i := range values {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i + 1))
	}
	bldr.Field(1).(*array.Float64Builder).AppendValues(values, valid)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()

	tbl, err = tbl.Append(context.Background(), rdr, nil)
	require.NoError(t, err)

	return tbl
}

func scanIDsMatching(t *testing.T, tbl *table.Table, filter iceberg.BooleanExpression) []int64 {
	t.Helper()

	out, err := tbl.Scan(table.WithRowFilter(filter)).ToArrowTable(t.Context())
	require.NoError(t, err)
	defer out.Release()

	ids := []int64{}
	for _, chunk := range out.Column(0).Data().Chunks() {
		ids = append(ids, chunk.(*array.Int64).Int64Values()...)
	}

	return ids
}
