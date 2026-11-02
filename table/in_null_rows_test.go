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

// In and NotIn are NULL for a NULL value, as Equal and NotEqual are, so a
// NULL row matches neither, also under NOT and whatever the number of values.
// Scans and both delete modes must agree.
// https://github.com/apache/iceberg-go/issues/2132
func TestInAndNotInNeverMatchNull(t *testing.T) {
	age := iceberg.Reference("age")

	// ids 1 to 4
	ages := []int64{25, 30, 0, 40}
	valid := []bool{true, true, false, true}

	cases := []struct {
		name   string
		filter iceberg.BooleanExpression
		want   []int64
	}{
		{"not in", iceberg.NotIn(age, int64(30), int64(40)), []int64{1}},
		// binding reduces the set to one value and the predicate to NotEqual
		{"not in with duplicate values", iceberg.NotIn(age, int64(30), int64(30)), []int64{1, 4}},
		{"not equal", iceberg.NotEqualTo(age, int64(30)), []int64{1, 4}},
		{"in", iceberg.IsIn(age, int64(30), int64(40)), []int64{2, 4}},
		{"not of in", iceberg.NewNot(iceberg.IsIn(age, int64(30), int64(40))), []int64{1}},
		{"not of not in", iceberg.NewNot(iceberg.NotIn(age, int64(30), int64(40))), []int64{2, 4}},
		{"not of and", iceberg.NewNot(iceberg.NewAnd(iceberg.IsIn(age, int64(30), int64(40)), iceberg.GreaterThan(iceberg.Reference("id"), int64(0)))), []int64{1}},
		{"is null or not in", iceberg.NewOr(iceberg.IsNull(age), iceberg.NotIn(age, int64(30), int64(40))), []int64{1, 3}},
	}

	for _, partitioned := range []bool{false, true} {
		for _, mode := range []string{table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead} {
			for _, c := range cases {
				name := mode + "/" + c.name
				if partitioned {
					name = "partitioned/" + name
				}
				t.Run(name, func(t *testing.T) {
					tbl := newNullAgeTable(t, mode, partitioned, ages, valid)
					requireNullRowsScanAndDelete(t, tbl, c.filter, c.want, len(ages))
				})
			}
		}
	}
}

// A column added after a file was written reads as NULL for that file's rows,
// and a filter must treat them like a stored NULL, also under NOT. Ids 1 to 4
// are in a file without x, id 5 has a stored NULL and id 6 has x = 5.
func TestMissingColumnMatchesLikeStoredNull(t *testing.T) {
	x, id := iceberg.Reference("x"), iceberg.Reference("id")

	cases := []struct {
		name   string
		filter iceberg.BooleanExpression
		want   []int64
	}{
		{"not of in", iceberg.NewNot(iceberg.IsIn(x, int64(5), int64(6))), []int64{}},
		{"not of not in", iceberg.NewNot(iceberg.NotIn(x, int64(5), int64(6))), []int64{6}},
		{"not in", iceberg.NotIn(x, int64(7), int64(8)), []int64{6}},
		{"not of equal", iceberg.NewNot(iceberg.EqualTo(x, int64(7))), []int64{6}},
		{"not of less than", iceberg.NewNot(iceberg.LessThan(x, int64(1))), []int64{6}},
		{"not of and", iceberg.NewNot(iceberg.NewAnd(iceberg.IsIn(x, int64(5), int64(6)), iceberg.GreaterThan(id, int64(0)))), []int64{}},
		{"not of or", iceberg.NewNot(iceberg.NewOr(iceberg.EqualTo(x, int64(7)), iceberg.LessThan(id, int64(3)))), []int64{6}},
		{"is null", iceberg.IsNull(x), []int64{1, 2, 3, 4, 5}},
		{"not of is null", iceberg.NewNot(iceberg.IsNull(x)), []int64{6}},
		{"not of not null", iceberg.NewNot(iceberg.NotNull(x)), []int64{1, 2, 3, 4, 5}},
	}

	for _, mode := range []string{table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead} {
		for _, c := range cases {
			t.Run(mode+"/"+c.name, func(t *testing.T) {
				ctx := context.Background()
				// ids 1 to 4, written before x exists
				tbl := newCopyOnWriteNullRowsTable(t, "2")

				txn := tbl.NewTransaction()
				require.NoError(t, txn.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: mode}))
				require.NoError(t, txn.UpdateSchema(true, false).
					AddColumn([]string{"x"}, iceberg.PrimitiveTypes.Int64, "", false, nil).Commit())
				tbl, err := txn.Commit(ctx)
				require.NoError(t, err)

				arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
				require.NoError(t, err)
				bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
				defer bldr.Release()
				bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{5, 6}, nil)
				bldr.Field(1).(*array.Int64Builder).AppendValues([]int64{50, 60}, nil)
				bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"e", "f"}, nil)
				bldr.Field(3).(*array.Int64Builder).AppendValues([]int64{0, 5}, []bool{false, true})
				rec := bldr.NewRecordBatch()
				defer rec.Release()

				rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
				require.NoError(t, err)
				defer rdr.Release()

				tbl, err = tbl.Append(ctx, rdr, nil)
				require.NoError(t, err)

				requireNullRowsScanAndDelete(t, tbl, c.filter, c.want, 6)
			})
		}
	}
}

// newNullAgeTable writes ages with ids from 1, NULL where valid is false. A
// partitioned table has an identity partition on age, so one file per value.
func newNullAgeTable(t *testing.T, mode string, partitioned bool, ages []int64, valid []bool) *table.Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "age", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)
	spec := iceberg.UnpartitionedSpec
	if partitioned {
		identity := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "age", Transform: iceberg.IdentityTransform{},
		})
		spec = &identity
	}

	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(schema, spec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: "2",
			table.WriteDeleteModeKey:    mode,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "null_age"}, meta, metaLoc, fsF, cat)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
	defer bldr.Release()
	for i := range ages {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i + 1))
	}
	bldr.Field(1).(*array.Int64Builder).AppendValues(ages, valid)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()

	tbl, err = tbl.Append(context.Background(), rdr, nil)
	require.NoError(t, err)

	return tbl
}

// requireNullRowsScanAndDelete checks that a scan with filter returns want, and
// that a delete with it removes exactly those rows.
func requireNullRowsScanAndDelete(t *testing.T, tbl *table.Table, filter iceberg.BooleanExpression, want []int64, total int) {
	t.Helper()
	ctx := context.Background()

	matched := scanIDsMatching(t, tbl, filter)
	slices.Sort(matched)
	require.Equal(t, want, matched)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.Delete(ctx, filter, nil))
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)

	remaining := scanIDs(t, tbl)
	for _, id := range remaining {
		require.NotContains(t, want, id)
	}
	require.Len(t, remaining, total-len(want))
}
