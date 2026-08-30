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
	"bytes"
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func partitionResidualTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "tenant_id", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "amount", Type: iceberg.PrimitiveTypes.Int64},
	)
}

func boundPartitionResidualEvaluator(
	t *testing.T,
	schema *iceberg.Schema,
	spec iceberg.PartitionSpec,
	filter iceberg.BooleanExpression,
) *partitionResidualEvaluator {
	t.Helper()

	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)

	evaluator, err := newPartitionResidualEvaluator(schema, &spec, bound, true)
	require.NoError(t, err)
	require.NotNil(t, evaluator)

	return evaluator
}

func TestPartitionResidualEvaluatorElidesSatisfiedIdentityPredicate(t *testing.T) {
	schema := partitionResidualTestSchema()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})
	filter := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
	)
	evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)

	residual, simplified, err := evaluator.residual(map[int]any{1000: "acme"})
	require.NoError(t, err)
	require.True(t, simplified)

	want, err := iceberg.BindExpr(schema,
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)), true)
	require.NoError(t, err)
	assert.True(t, residual.Equals(want), "expected %s, got %s", want, residual)

	residual, simplified, err = evaluator.residual(map[int]any{1000: "other"})
	require.NoError(t, err)
	require.True(t, simplified)
	assert.Equal(t, iceberg.AlwaysFalse{}, residual)
}

func TestPartitionResidualEvaluatorSimplifiesBooleanCombinations(t *testing.T) {
	schema := partitionResidualTestSchema()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})

	t.Run("or", func(t *testing.T) {
		filter := iceberg.NewOr(
			iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
			iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
		)
		evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)

		residual, simplified, err := evaluator.residual(map[int]any{1000: "acme"})
		require.NoError(t, err)
		require.True(t, simplified)
		assert.Equal(t, iceberg.AlwaysTrue{}, residual)

		residual, simplified, err = evaluator.residual(map[int]any{1000: "other"})
		require.NoError(t, err)
		require.True(t, simplified)
		want, err := iceberg.BindExpr(schema,
			iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)), true)
		require.NoError(t, err)
		assert.True(t, residual.Equals(want))
	})

	t.Run("not", func(t *testing.T) {
		filter := iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"))
		evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)

		residual, simplified, err := evaluator.residual(map[int]any{1000: "acme"})
		require.NoError(t, err)
		require.True(t, simplified)
		assert.Equal(t, iceberg.AlwaysFalse{}, residual)

		residual, simplified, err = evaluator.residual(map[int]any{1000: "other"})
		require.NoError(t, err)
		require.True(t, simplified)
		assert.Equal(t, iceberg.AlwaysTrue{}, residual)
	})
}

func TestPartitionResidualEvaluatorHandlesNullAndSetPredicates(t *testing.T) {
	schema := partitionResidualTestSchema()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})

	t.Run("is null", func(t *testing.T) {
		evaluator := boundPartitionResidualEvaluator(t, schema, spec,
			iceberg.IsNull(iceberg.Reference("tenant_id")))

		residual, simplified, err := evaluator.residual(map[int]any{1000: nil})
		require.NoError(t, err)
		require.True(t, simplified)
		assert.Equal(t, iceberg.AlwaysTrue{}, residual)

		residual, simplified, err = evaluator.residual(map[int]any{1000: "acme"})
		require.NoError(t, err)
		require.True(t, simplified)
		assert.Equal(t, iceberg.AlwaysFalse{}, residual)
	})

	t.Run("in", func(t *testing.T) {
		evaluator := boundPartitionResidualEvaluator(t, schema, spec,
			iceberg.IsIn(iceberg.Reference("tenant_id"), "acme", "iceberg"))

		residual, simplified, err := evaluator.residual(map[int]any{1000: "acme"})
		require.NoError(t, err)
		require.True(t, simplified)
		assert.Equal(t, iceberg.AlwaysTrue{}, residual)

		residual, simplified, err = evaluator.residual(map[int]any{1000: "other"})
		require.NoError(t, err)
		require.True(t, simplified)
		assert.Equal(t, iceberg.AlwaysFalse{}, residual)
	})
}

func TestPartitionResidualEvaluatorComputesDayBoundaries(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "event_ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "event_day", Transform: iceberg.DayTransform{},
	})
	filter := iceberg.NewAnd(
		iceberg.GreaterThanEqual(iceberg.Reference("event_ts"), "2022-11-27T10:00:00"),
		iceberg.LessThan(iceberg.Reference("event_ts"), "2022-11-30T10:00:00"),
	)
	evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)

	lower, err := iceberg.BindExpr(schema,
		iceberg.GreaterThanEqual(iceberg.Reference("event_ts"), "2022-11-27T10:00:00"), true)
	require.NoError(t, err)
	upper, err := iceberg.BindExpr(schema,
		iceberg.LessThan(iceberg.Reference("event_ts"), "2022-11-30T10:00:00"), true)
	require.NoError(t, err)

	tests := []struct {
		name      string
		partition iceberg.Date
		want      iceberg.BooleanExpression
	}{
		{name: "lower boundary", partition: iceberg.Date(19323), want: lower},
		{name: "interior", partition: iceberg.Date(19324), want: iceberg.AlwaysTrue{}},
		{name: "upper boundary", partition: iceberg.Date(19326), want: upper},
		{name: "outside", partition: iceberg.Date(19327), want: iceberg.AlwaysFalse{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			residual, simplified, err := evaluator.residual(map[int]any{1000: tt.partition})
			require.NoError(t, err)
			require.True(t, simplified)
			assert.True(t, residual.Equals(tt.want), "expected %s, got %s", tt.want, residual)
		})
	}
}

func TestPartitionResidualEvaluatorComputesTruncateBoundaries(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id_truncated", Transform: iceberg.TruncateTransform{Width: 10},
	})
	filter := iceberg.NewAnd(
		iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(25)),
		iceberg.LessThan(iceberg.Reference("id"), int32(65)),
	)
	evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)

	lower, err := iceberg.BindExpr(schema,
		iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(25)), true)
	require.NoError(t, err)
	upper, err := iceberg.BindExpr(schema,
		iceberg.LessThan(iceberg.Reference("id"), int32(65)), true)
	require.NoError(t, err)

	tests := []struct {
		name      string
		partition int32
		want      iceberg.BooleanExpression
	}{
		{name: "before range", partition: 10, want: iceberg.AlwaysFalse{}},
		{name: "lower boundary", partition: 20, want: lower},
		{name: "interior", partition: 40, want: iceberg.AlwaysTrue{}},
		{name: "upper boundary", partition: 60, want: upper},
		{name: "after range", partition: 70, want: iceberg.AlwaysFalse{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			residual, simplified, err := evaluator.residual(map[int]any{1000: tt.partition})
			require.NoError(t, err)
			require.True(t, simplified)
			assert.True(t, residual.Equals(tt.want), "expected %s, got %s", tt.want, residual)
		})
	}
}

func TestPartitionResidualEvaluatorHandlesTransformedFilterTerms(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "event_ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "event_day", Transform: iceberg.DayTransform{},
	})
	filter := iceberg.EqualTo(
		iceberg.NewUnboundTransform(iceberg.DayTransform{}, iceberg.Reference("event_ts")),
		iceberg.Date(19323),
	)
	evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)

	residual, simplified, err := evaluator.residual(map[int]any{1000: iceberg.Date(19323)})
	require.NoError(t, err)
	require.True(t, simplified)
	assert.Equal(t, iceberg.AlwaysTrue{}, residual)

	residual, simplified, err = evaluator.residual(map[int]any{1000: iceberg.Date(19324)})
	require.NoError(t, err)
	require.True(t, simplified)
	assert.Equal(t, iceberg.AlwaysFalse{}, residual)
}

func TestPartitionResidualEvaluatorKeepsBucketPredicateConservative(t *testing.T) {
	schema := partitionResidualTestSchema()
	transform := iceberg.BucketTransform{NumBuckets: 16}
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_bucket", Transform: transform,
	})
	filter := iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme")
	evaluator := boundPartitionResidualEvaluator(t, schema, spec, filter)

	partition := transform.Apply(iceberg.Optional[iceberg.Literal]{
		Valid: true, Val: iceberg.StringLiteral("acme"),
	})
	require.True(t, partition.Valid)
	residual, simplified, err := evaluator.residual(map[int]any{1000: partition.Val.Any()})
	require.NoError(t, err)
	assert.False(t, simplified, "a bucket match does not prove equality because buckets can collide")
	assert.Nil(t, residual)
}

func TestPartitionResidualEvaluatorLeavesUnpartitionedFiltersUnchanged(t *testing.T) {
	schema := partitionResidualTestSchema()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_bucket",
		Transform: iceberg.BucketTransform{NumBuckets: 16},
	})
	filter := iceberg.GreaterThan(iceberg.Reference("amount"), int64(100))

	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
	evaluator, err := newPartitionResidualEvaluator(schema, &spec, bound, true)
	require.NoError(t, err)
	assert.Nil(t, evaluator)
}

func TestPlanFilesLocalPopulatesPartitionResidual(t *testing.T) {
	const (
		manifestPath     = "mem://default/table/metadata/manifest.avro"
		manifestListPath = "mem://default/table/metadata/snap-7.avro"
		dataPath         = "mem://default/table/data/file.parquet"
	)

	spec := partitionedSpec()
	fs := iceio.NewMemFS()
	scan, schema, snapshotID := newSchemaEvolutionScanWithSnapshot(
		t, &spec, fs, manifestListPath, nil)
	dataFile := newTestDataFile(t, spec, dataPath, map[int]any{1000: int32(5)})
	entry := iceberg.NewManifestEntryBuilder(
		iceberg.EntryStatusADDED, &snapshotID, dataFile,
	).SequenceNum(1).Build()

	var manifestBuffer bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath, &manifestBuffer, 2, spec, schema, snapshotID,
		[]iceberg.ManifestEntry{entry},
	)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(manifestPath, manifestBuffer.Bytes()))

	var listBuffer bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(
		2, &listBuffer, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest},
	))
	require.NoError(t, fs.WriteFile(manifestListPath, listBuffer.Bytes()))

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, iceberg.AlwaysTrue{}, tasks[0].Residual,
		"the identity partition already proves id == 5")
}

func TestPartitionResidualEvaluatorPreservesRowsAtTransformBoundaries(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
	)
	ref := iceberg.Reference("id")
	filters := []iceberg.BooleanExpression{iceberg.IsNull(ref), iceberg.NotNull(ref)}
	for _, boundary := range []int32{-10, -1, 0, 1, 10} {
		for _, op := range []iceberg.Operation{
			iceberg.OpEQ, iceberg.OpNEQ, iceberg.OpLT,
			iceberg.OpLTEQ, iceberg.OpGT, iceberg.OpGTEQ,
		} {
			filters = append(filters, iceberg.LiteralPredicate(op, ref, iceberg.Int32Literal(boundary)))
		}
	}
	filters = append(filters,
		iceberg.IsIn(ref, int32(-10), int32(0), int32(10)),
		iceberg.NotIn(ref, int32(-10), int32(0), int32(10)),
	)
	values := []any{
		nil, int32(-21), int32(-20), int32(-11), int32(-10), int32(-1),
		int32(0), int32(1), int32(9), int32(10), int32(11), int32(20), int32(21),
	}
	for _, transform := range []iceberg.Transform{
		iceberg.IdentityTransform{},
		iceberg.TruncateTransform{Width: 10},
		iceberg.BucketTransform{NumBuckets: 4},
	} {
		t.Run(transform.String(), func(t *testing.T) {
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: transform,
			})
			for _, filter := range filters {
				t.Run(filter.String(), func(t *testing.T) {
					bound, err := iceberg.BindExpr(schema, filter, true)
					require.NoError(t, err)
					residualEvaluator, err := newPartitionResidualEvaluator(schema, &spec, bound, true)
					require.NoError(t, err)
					if residualEvaluator == nil {
						return
					}
					evaluate, err := iceberg.ExpressionEvaluator(schema, filter, true)
					require.NoError(t, err)
					for _, value := range values {
						var literal iceberg.Optional[iceberg.Literal]
						if value != nil {
							literal = iceberg.Optional[iceberg.Literal]{Valid: true, Val: iceberg.Int32Literal(value.(int32))}
						}
						partitionLiteral := transform.Apply(literal)
						var partition any
						if partitionLiteral.Valid {
							partition = partitionLiteral.Val.Any()
						}
						residual, changed, err := residualEvaluator.residual(map[int]any{1000: partition})
						require.NoError(t, err)
						if !changed {
							continue
						}
						unbound, err := iceberg.TranslateColumnNames(residual, schema)
						require.NoError(t, err)
						evaluateResidual, err := iceberg.ExpressionEvaluator(schema, unbound, true)
						require.NoError(t, err)
						want, err := evaluate(partitionRecord{value})
						require.NoError(t, err)
						got, err := evaluateResidual(partitionRecord{value})
						require.NoError(t, err)
						assert.Equal(t, want, got, "value=%v partition=%v residual=%s", value, partition, residual)
					}
				})
			}
		})
	}
}
