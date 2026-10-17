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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPartitionResidualEvaluatorReusesUnchangedSubtrees(t *testing.T) {
	schema := partitionResidualTestSchema()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})

	tenant := iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme")
	amount := iceberg.Reference("amount")
	opaque := iceberg.NewAnd(
		iceberg.NewNot(iceberg.GreaterThan(amount, int64(100))),
		iceberg.NewOr(
			iceberg.LessThan(amount, int64(20)),
			iceberg.GreaterThan(amount, int64(200)),
		),
	)

	tests := []struct {
		name           string
		filter         iceberg.BooleanExpression
		partitionValue string
		want           iceberg.BooleanExpression
	}{
		{name: "and", filter: iceberg.NewAnd(tenant, opaque), partitionValue: "acme", want: opaque},
		{name: "and-right", filter: iceberg.NewAnd(opaque, tenant), partitionValue: "acme", want: opaque},
		{name: "or", filter: iceberg.NewOr(tenant, opaque), partitionValue: "other", want: opaque},
		{name: "or-right", filter: iceberg.NewOr(opaque, tenant), partitionValue: "other", want: opaque},
		{name: "not", filter: iceberg.NewNot(iceberg.NewOr(tenant, opaque)), partitionValue: "other", want: iceberg.NewNot(opaque)},
		{name: "not-right", filter: iceberg.NewNot(iceberg.NewOr(opaque, tenant)), partitionValue: "other", want: iceberg.NewNot(opaque)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator := boundPartitionResidualEvaluator(t, schema, spec, tt.filter)
			want, err := iceberg.BindExpr(schema, tt.want, true)
			require.NoError(t, err)

			got, changed, err := evaluator.residual(map[int]any{1000: tt.partitionValue})
			require.NoError(t, err)
			require.True(t, changed)
			require.NotNil(t, got)
			require.True(t, got.Equals(want), "expected %s, got %s", want, got)

			for _, partition := range []map[int]any{{}, {1000: nil}} {
				got, changed, err = evaluator.residual(partition)
				require.NoError(t, err)
				require.False(t, changed)
				require.Nil(t, got)
			}
			got, changed, err = evaluator.residual(map[int]any{1000: tt.partitionValue})
			require.NoError(t, err)
			require.True(t, changed)
			require.True(t, got.Equals(want), "interleaved unchanged files must not affect later residuals")
		})
	}
}

func TestPartitionResidualEvaluatorUnchangedSubtreeAllocations(t *testing.T) {
	schema := partitionResidualTestSchema()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})
	tenant := iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme")
	opaque := iceberg.NewOr(
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
		iceberg.NewNot(iceberg.LessThan(iceberg.Reference("amount"), int64(20))),
	)

	for _, tt := range []struct {
		name   string
		filter iceberg.BooleanExpression
	}{
		{name: "and", filter: iceberg.NewAnd(tenant, opaque)},
		{name: "or", filter: iceberg.NewOr(tenant, opaque)},
		{name: "not", filter: iceberg.NewNot(iceberg.NewOr(tenant, opaque))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			evaluator := boundPartitionResidualEvaluator(t, schema, spec, tt.filter)
			for _, tc := range []struct {
				name      string
				partition map[int]any
			}{
				{name: "missing", partition: map[int]any{}},
				{name: "null", partition: map[int]any{1000: nil}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					// The node must retain the expression it built at
					// evaluator creation instead of allocating a new tree.
					nodeExpr, changed, err := evaluator.root.residual(
						borrowedPartitionRecord{partition: tc.partition, partitionType: evaluator.partitionType})
					require.NoError(t, err)
					require.False(t, changed)
					require.True(t, nodeExpr.Equals(evaluator.root.expr))

					allocs := testing.AllocsPerRun(100, func() {
						residual, changed, err := evaluator.residual(tc.partition)
						if err != nil || changed || residual != nil {
							panic("unchanged partition residual must be nil without an error")
						}
					})
					require.Zero(t, allocs)
				})
			}
		})
	}
}
