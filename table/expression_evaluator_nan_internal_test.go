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
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExpressionEvaluatorMatchesRowFilterForNaN checks that the exported
// ExpressionEvaluator selects the same rows as the Arrow row filter a scan
// applies when the value is NaN. NULL rows are left out: the evaluator is
// two-valued and orders NULL below every value, while the row filter treats a
// comparison with NULL as unknown.
func TestExpressionEvaluatorMatchesRowFilterForNaN(t *testing.T) {
	sc := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "row", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "d", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 3, Name: "f", Type: iceberg.PrimitiveTypes.Float32, Required: true},
	)
	values := []float64{math.Inf(-1), 1, 5, 10, math.Inf(1), math.NaN()}

	arrowSchema, err := SchemaToArrowSchema(sc, nil, false, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()
	rows := make([]partitionRecord, len(values))
	for i, v := range values {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		bldr.Field(1).(*array.Float64Builder).Append(v)
		bldr.Field(2).(*array.Float32Builder).Append(float32(v))
		rows[i] = partitionRecord{int32(i), v, float32(v)}
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	var exprs []iceberg.BooleanExpression
	for _, col := range []string{"d", "f"} {
		ref := iceberg.Reference(col)
		for _, pred := range []iceberg.BooleanExpression{
			iceberg.LessThan(ref, 5.0),
			iceberg.LessThanEqual(ref, 5.0),
			iceberg.GreaterThan(ref, 5.0),
			iceberg.GreaterThanEqual(ref, 5.0),
			iceberg.EqualTo(ref, 5.0),
			iceberg.NotEqualTo(ref, 5.0),
		} {
			exprs = append(exprs, pred, iceberg.NewNot(pred))
		}
	}

	for _, expr := range exprs {
		t.Run(expr.String(), func(t *testing.T) {
			filter, err := prepareBatchFilter(expr, sc, true)
			require.NoError(t, err)
			rec.Retain()
			filtered, err := filter(context.Background(), rec)
			require.NoError(t, err)
			defer filtered.Release()
			want := filteredRowIDs(t, filtered)

			eval, err := iceberg.ExpressionEvaluator(sc, expr, true)
			require.NoError(t, err)
			got := []int32{}
			for _, row := range rows {
				ok, err := eval(row)
				require.NoError(t, err)
				if ok {
					got = append(got, row[0].(int32))
				}
			}

			assert.Equal(t, want, got)
		})
	}
}

func filteredRowIDs(t *testing.T, rec arrow.RecordBatch) []int32 {
	t.Helper()

	ids := rec.Column(0).(*array.Int32)
	out := make([]int32, ids.Len())
	for i := range out {
		out[i] = ids.Value(i)
	}

	return out
}
