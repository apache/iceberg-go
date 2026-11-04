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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A delete drops a file whole when the strict evaluator says every row
// matches. That must agree with a scan's row filter for files of only NaN,
// only NULL or only +Inf, also when binding moves a literal beyond the float32
// range. iceberg-go doesn't write NaN counts, but other engines write complete
// ones.
func TestDeleteWholeFileDecisionMatchesNaNRows(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Float32},
	)
	x := iceberg.Reference("x")
	above, below := 1e100, -1e100

	posInf, err := iceberg.Float32Literal(float32(math.Inf(1))).MarshalBinary()
	require.NoError(t, err)

	newFile := func(nulls, nans int64, bound []byte) iceberg.DataFile {
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentData, "file.parquet", iceberg.ParquetFile,
			nil, nil, nil, 2, 100,
		)
		require.NoError(t, err)
		builder.
			ValueCounts(map[int]int64{1: 2}).
			NullValueCounts(map[int]int64{1: nulls}).
			NaNValueCounts(map[int]int64{1: nans})
		if bound != nil {
			builder.
				LowerBoundValues(map[int][]byte{1: bound}).
				UpperBoundValues(map[int][]byte{1: bound})
		}

		return builder.Build()
	}
	allNaN, allNull, allPosInf := newFile(0, 2, nil), newFile(2, 0, nil), newFile(0, 0, posInf)

	// whether a scan's row filter matches a NaN row and a +Inf row; it never
	// matches a NULL row, since every filter here is NULL for it
	cases := []struct {
		filter        iceberg.BooleanExpression
		nan, infinity bool
	}{
		{iceberg.LessThan(x, above), false, false},
		{iceberg.NewNot(iceberg.LessThan(x, above)), true, true},
		{iceberg.GreaterThan(x, above), false, true},
		{iceberg.NewNot(iceberg.GreaterThan(x, above)), true, false},
		{iceberg.GreaterThan(x, below), false, true},
		{iceberg.NewNot(iceberg.GreaterThan(x, below)), true, false},
		{iceberg.NewNot(iceberg.NewAnd(iceberg.LessThan(x, above), iceberg.GreaterThan(x, below))), true, true},
		{iceberg.NewNot(iceberg.NewOr(iceberg.GreaterThan(x, above), iceberg.LessThan(x, below))), true, false},
		{iceberg.LessThan(x, 5.0), false, false},
		{iceberg.NewNot(iceberg.LessThan(x, 5.0)), true, true},
		{iceberg.IsNaN(x), true, false},
	}

	for _, c := range cases {
		t.Run(c.filter.String(), func(t *testing.T) {
			notTrue, err := isNotTrueExpr(c.filter)
			require.NoError(t, err)
			strict, err := newStrictMetricsEvaluator(schema, iceberg.NewNot(notTrue), true, false)
			require.NoError(t, err)

			for _, f := range []struct {
				name    string
				file    iceberg.DataFile
				matches bool
			}{
				{"all NaN", allNaN, c.nan},
				{"all NULL", allNull, false},
				{"all +Inf", allPosInf, c.infinity},
			} {
				deleteWhole, err := strict(f.file)
				require.NoError(t, err)
				assert.Equal(t, f.matches, deleteWhole, f.name)
			}
		})
	}
}
