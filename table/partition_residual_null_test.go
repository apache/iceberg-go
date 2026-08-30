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
	"fmt"
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionResidualPreservesArrowNullFiltering(t *testing.T) {
	checkPartitionResidualArrowFiltering(t, iceberg.PrimitiveTypes.Int32, nil, "null",
		iceberg.Int32Literal(5), []iceberg.Transform{iceberg.IdentityTransform{}, iceberg.TruncateTransform{Width: 10}, iceberg.BucketTransform{NumBuckets: 4}})
}

func TestPartitionResidualPreservesArrowNaNFiltering(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   any
		json    string
		literal iceberg.Literal
	}{
		{"null", nil, "null", iceberg.Float64Literal(5)},
		{"nan partition", math.NaN(), `"NaN"`, iceberg.Float64Literal(5)},
		{"nan literal", float64(5), "5", iceberg.Float64Literal(math.NaN())},
		{"nan partition and literal", math.NaN(), `"NaN"`, iceberg.Float64Literal(math.NaN())},
	} {
		t.Run(tt.name, func(t *testing.T) {
			checkPartitionResidualArrowFiltering(t, iceberg.PrimitiveTypes.Float64, tt.value, tt.json,
				tt.literal, []iceberg.Transform{iceberg.IdentityTransform{}})
		})
	}
}

func checkPartitionResidualArrowFiltering(t *testing.T, typ iceberg.Type, value any, jsonValue string,
	literal iceberg.Literal, transforms []iceberg.Transform,
) {
	t.Helper()
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: typ},
		iceberg.NestedField{ID: 2, Name: "flag", Type: iceberg.PrimitiveTypes.Bool, Required: true},
	)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	countRows := func(t *testing.T, filter iceberg.BooleanExpression) int64 {
		t.Helper()
		plan, err := compileFileFilterPlan(schema, filter, true, true, false)
		require.NoError(t, err)
		if plan.dropFile {
			return 0
		}
		record := mustLoadRecordBatchFromJSON(arrowSchema,
			fmt.Sprintf(`[{"id":%s,"flag":true},{"id":%s,"flag":false}]`, jsonValue, jsonValue))
		if process := plan.recordProcessor(t.Context()); process != nil {
			record, err = process(record)
			require.NoError(t, err)
		}
		defer record.Release()

		return record.NumRows()
	}
	ref := iceberg.Reference("id")
	otherLiteral, err := iceberg.Int32Literal(1).To(typ)
	require.NoError(t, err)
	predicates := []iceberg.BooleanExpression{
		iceberg.IsNull(ref), iceberg.NotNull(ref),
		iceberg.SetPredicate(iceberg.OpIn, ref, []iceberg.Literal{literal, otherLiteral}),
		iceberg.SetPredicate(iceberg.OpNotIn, ref, []iceberg.Literal{literal, otherLiteral}),
	}
	if typ.Equals(iceberg.PrimitiveTypes.Float64) {
		predicates = append(predicates, iceberg.IsNaN(ref), iceberg.NotNaN(ref))
	}
	for _, op := range []iceberg.Operation{
		iceberg.OpEQ, iceberg.OpNEQ, iceberg.OpLT, iceberg.OpLTEQ, iceberg.OpGT, iceberg.OpGTEQ,
	} {
		predicates = append(predicates, iceberg.LiteralPredicate(op, ref, literal))
	}
	flag := iceberg.EqualTo(iceberg.Reference("flag"), true)
	for _, transform := range transforms {
		t.Run(transform.String(), func(t *testing.T) {
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: transform,
			})
			for _, predicate := range predicates {
				for _, filter := range []iceberg.BooleanExpression{
					predicate, iceberg.NewNot(predicate),
					iceberg.NewAnd(predicate, flag), iceberg.NewOr(predicate, flag),
					iceberg.NewNot(iceberg.NewAnd(predicate, flag)),
					iceberg.NewNot(iceberg.NewOr(predicate, flag)),
				} {
					t.Run(filter.String(), func(t *testing.T) {
						bound, err := iceberg.BindExpr(schema, filter, true)
						require.NoError(t, err)
						residualEvaluator, err := newPartitionResidualEvaluator(schema, &spec, bound, true)
						require.NoError(t, err)
						residual := bound
						if residualEvaluator != nil {
							candidate, changed, err := residualEvaluator.residual(map[int]any{1000: value})
							require.NoError(t, err)
							if changed {
								residual = candidate
							}
						}
						assert.Equal(t, countRows(t, bound), countRows(t, residual), "residual=%s", residual)
					})
				}
			}
		})
	}
}

func TestPartitionResidualEvaluatorKeepsMissingPartitionValues(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: iceberg.IdentityTransform{},
	})
	for _, filter := range []iceberg.BooleanExpression{
		iceberg.IsNull(iceberg.Reference("id")),
		iceberg.NotNull(iceberg.Reference("id")),
		iceberg.EqualTo(iceberg.Reference("id"), int32(5)),
	} {
		t.Run(filter.String(), func(t *testing.T) {
			evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)
			residual, changed, err := evaluator.residual(nil)
			require.NoError(t, err)
			assert.False(t, changed)
			assert.Nil(t, residual)
		})
	}
}
