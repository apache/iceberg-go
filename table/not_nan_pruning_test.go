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

// NOT(x < 5) is true for NaN, so the NaN row must match whatever the other
// values in its file are. Min/max leave NaN out, and pruning on x >= 5 alone
// skipped a file whose other values are all below 5. Scans and both delete
// modes must agree.
// https://github.com/apache/iceberg-go/issues/2131
func TestNotComparisonMatchesNaNRegardlessOfFileBounds(t *testing.T) {
	x := iceberg.Reference("x")
	notLessThan5 := iceberg.NewNot(iceberg.LessThan(x, 5.0))

	cases := []struct {
		name        string
		isFloat32   bool
		partitioned bool
		values      []float64
		filter      iceberg.BooleanExpression
		want        []int64
	}{
		{name: "bounds above", values: []float64{10, math.NaN(), 20}, filter: notLessThan5, want: []int64{1, 2, 3}},
		{name: "bounds below", values: []float64{1, math.NaN(), 2}, filter: notLessThan5, want: []int64{2}},
		{
			name: "not and", values: []float64{1, math.NaN(), 2},
			filter: iceberg.NewNot(iceberg.NewAnd(iceberg.GreaterThan(x, 0.0), iceberg.LessThan(x, 5.0))),
			want:   []int64{2},
		},
		// each file is pruned by evaluating the projected filter on its
		// partition value, NaN for the second row
		{name: "identity partition", partitioned: true, values: []float64{1, math.NaN(), 2}, filter: notLessThan5, want: []int64{2}},
		// binding folds a literal outside the float32 range, and NaN still
		// fails the comparison
		{name: "float32 out of range", isFloat32: true, values: []float64{1, math.NaN(), 2}, filter: iceberg.NewNot(iceberg.LessThan(x, 1e100)), want: []int64{2}},
		{name: "float32 out of range without not", isFloat32: true, values: []float64{1, math.NaN(), 2}, filter: iceberg.LessThan(x, 1e100), want: []int64{1, 3}},
	}

	for _, mode := range []string{table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead} {
		for _, c := range cases {
			t.Run(mode+"/"+c.name, func(t *testing.T) {
				tbl := newNaNTable(t, mode, c.isFloat32, c.partitioned, c.values, nil)

				requireScanAndDeleteMatch(t, tbl, c.filter, c.want, len(c.values))
			})
		}
	}
}

// An ordering comparison against a literal beyond the float32 range binds to
// one against the largest finite float32, so NaN, the infinities and NULL
// compare as they do against the literal itself, also under NOT.
func TestFloat32OutOfRangeComparisonMatchesIEEE(t *testing.T) {
	x := iceberg.Reference("x")
	above, below, inf := 1e100, -1e100, math.Inf(1)

	// ids 1 to 6
	values := []float64{1, math.NaN(), inf, -inf, 0, 2}
	valid := []bool{true, true, true, true, false, true}

	cases := []struct {
		filter iceberg.BooleanExpression
		want   []int64
	}{
		{iceberg.LessThan(x, above), []int64{1, 4, 6}},
		{iceberg.NewNot(iceberg.LessThan(x, above)), []int64{2, 3}},
		{iceberg.LessThanEqual(x, above), []int64{1, 4, 6}},
		{iceberg.NewNot(iceberg.LessThanEqual(x, above)), []int64{2, 3}},
		{iceberg.GreaterThan(x, above), []int64{3}},
		{iceberg.NewNot(iceberg.GreaterThan(x, above)), []int64{1, 2, 4, 6}},
		{iceberg.GreaterThanEqual(x, above), []int64{3}},
		{iceberg.GreaterThan(x, below), []int64{1, 3, 6}},
		{iceberg.NewNot(iceberg.GreaterThan(x, below)), []int64{2, 4}},
		{iceberg.LessThan(x, below), []int64{4}},
		{iceberg.NewNot(iceberg.LessThan(x, below)), []int64{1, 2, 3, 6}},
		{iceberg.NewNot(iceberg.NewAnd(iceberg.LessThan(x, above), iceberg.GreaterThan(x, below))), []int64{2, 3, 4}},
		{iceberg.NewNot(iceberg.NewOr(iceberg.GreaterThan(x, above), iceberg.LessThan(x, below))), []int64{1, 2, 6}},
		{iceberg.LessThan(x, inf), []int64{1, 4, 6}},
		{iceberg.LessThanEqual(x, inf), []int64{1, 3, 4, 6}},
		{iceberg.GreaterThan(x, inf), []int64{}},
		{iceberg.EqualTo(x, inf), []int64{3}},
		{iceberg.NewNot(iceberg.LessThanEqual(x, inf)), []int64{2}},
	}

	for _, mode := range []string{table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead} {
		for _, c := range cases {
			t.Run(mode+"/"+c.filter.String(), func(t *testing.T) {
				tbl := newNaNTable(t, mode, true, false, values, valid)
				requireScanAndDeleteMatch(t, tbl, c.filter, c.want, len(values))
			})
		}
	}
}

// requireScanAndDeleteMatch checks that a scan with filter returns want, and
// that a delete with it removes exactly those rows.
func requireScanAndDeleteMatch(t *testing.T, tbl *table.Table, filter iceberg.BooleanExpression, want []int64, total int) {
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

// newNaNTable writes values to one data file per partition, with ids from 1. A
// value is NULL where valid is false; a nil valid has no NULL.
func newNaNTable(t *testing.T, mode string, isFloat32, partitioned bool, values []float64, valid []bool) *table.Table {
	t.Helper()

	icebergType, arrowType := iceberg.Type(iceberg.PrimitiveTypes.Float64), arrow.DataType(arrow.PrimitiveTypes.Float64)
	if isFloat32 {
		icebergType, arrowType = iceberg.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float32
	}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "x", Type: icebergType, Required: false},
	)
	spec := iceberg.UnpartitionedSpec
	if partitioned {
		identity := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "x", Transform: iceberg.IdentityTransform{},
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
	tbl := table.New(table.Identifier{"db", "nan_rows"}, meta, metaLoc, fsF, cat)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "x", Type: arrowType, Nullable: true},
	}, nil))
	defer bldr.Release()
	for i, v := range values {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i + 1))
		if valid != nil && !valid[i] {
			bldr.Field(1).AppendNull()

			continue
		}
		if isFloat32 {
			bldr.Field(1).(*array.Float32Builder).Append(float32(v))
		} else {
			bldr.Field(1).(*array.Float64Builder).Append(v)
		}
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()

	tbl, err = tbl.Append(context.Background(), rdr, nil)
	require.NoError(t, err)

	return tbl
}
