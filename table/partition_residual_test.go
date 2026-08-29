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

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func partitionResidualTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{
			ID: 1, Name: "tenant_id", Type: iceberg.PrimitiveTypes.String,
		},
		iceberg.NestedField{
			ID: 2, Name: "amount", Type: iceberg.PrimitiveTypes.Int64,
		},
	)
}

func partitionResidualTestSpec(transform iceberg.Transform) iceberg.PartitionSpec {
	return iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "tenant_id",
		Transform: transform,
	})
}

func boundPartitionResidualPlan(
	t *testing.T,
	filter iceberg.BooleanExpression,
	transform iceberg.Transform,
) *partitionResidualPlan {
	t.Helper()

	schema := partitionResidualTestSchema()
	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
	spec := partitionResidualTestSpec(transform)

	return newPartitionResidualPlan(schema, &spec, bound, true)
}

func TestPartitionResidualPlanElidesSatisfiedIdentityPredicate(t *testing.T) {
	filter := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
	)
	plan := boundPartitionResidualPlan(t, filter, iceberg.IdentityTransform{})
	require.NotNil(t, plan)

	residual, changed := plan.residual(map[int]any{1000: "acme"})
	require.True(t, changed)

	want, err := iceberg.BindExpr(partitionResidualTestSchema(),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)), true)
	require.NoError(t, err)
	assert.True(t, residual.Equals(want), "expected %s, got %s", want, residual)
}

func TestPartitionResidualPlanElidesIdentityPredicateToAlwaysTrue(t *testing.T) {
	plan := boundPartitionResidualPlan(t,
		iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
		iceberg.IdentityTransform{})
	require.NotNil(t, plan)

	residual, changed := plan.residual(map[int]any{1000: "acme"})
	require.True(t, changed)
	assert.Equal(t, iceberg.AlwaysTrue{}, residual)
}

func TestPartitionResidualPlanUsesNullIdentityValues(t *testing.T) {
	schema := partitionResidualTestSchema()
	filter := iceberg.IsNull(iceberg.Reference("tenant_id"))
	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
	spec := partitionResidualTestSpec(iceberg.IdentityTransform{})
	plan := newPartitionResidualPlan(schema, &spec, bound, true)
	require.NotNil(t, plan)

	residual, changed := plan.residual(map[int]any{1000: nil})
	require.True(t, changed)
	assert.Equal(t, iceberg.AlwaysTrue{}, residual)
}

func TestPartitionResidualPlanNormalizesDecodedLiteralValues(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "price", Type: iceberg.DecimalTypeOf(10, 2),
	})
	value := iceberg.Decimal{Val: decimal128.FromI64(123), Scale: 2}
	filter := iceberg.EqualTo(iceberg.Reference("price"), value)
	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "price", Transform: iceberg.IdentityTransform{},
	})
	plan := newPartitionResidualPlan(schema, &spec, bound, true)
	require.NotNil(t, plan)

	residual, changed := plan.residual(map[int]any{1000: iceberg.DecimalLiteral(value)})
	require.True(t, changed)
	assert.Equal(t, iceberg.AlwaysTrue{}, residual)
}

func TestPartitionResidualPlanPreservesUnknownAndNonIdentityPredicates(t *testing.T) {
	filter := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
	)

	identityPlan := boundPartitionResidualPlan(t, filter, iceberg.IdentityTransform{})
	residual, changed := identityPlan.residual(nil)
	assert.False(t, changed)
	assert.Nil(t, residual)

	nonIdentityPlan := boundPartitionResidualPlan(t, filter, iceberg.BucketTransform{NumBuckets: 16})
	assert.Nil(t, nonIdentityPlan)
}

func TestPartitionResidualPlanDoesNotEvaluateTransformedPredicates(t *testing.T) {
	filter := iceberg.EqualTo(
		iceberg.NewUnboundTransform(iceberg.BucketTransform{NumBuckets: 16}, iceberg.Reference("tenant_id")),
		int32(1),
	)
	plan := boundPartitionResidualPlan(t, filter, iceberg.IdentityTransform{})
	assert.Nil(t, plan)
}

func TestPartitionResidualPlanHandlesOrResiduals(t *testing.T) {
	filter := iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
	)
	plan := boundPartitionResidualPlan(t, filter, iceberg.IdentityTransform{})
	require.NotNil(t, plan)

	residual, changed := plan.residual(map[int]any{1000: "acme"})
	require.True(t, changed)
	assert.Equal(t, iceberg.AlwaysTrue{}, residual)

	residual, changed = plan.residual(map[int]any{1000: "other"})
	require.True(t, changed)
	want, err := iceberg.BindExpr(partitionResidualTestSchema(),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)), true)
	require.NoError(t, err)
	assert.True(t, residual.Equals(want), "expected %s, got %s", want, residual)
}

func TestPartitionResidualPlanHandlesNestedIdentityFields(t *testing.T) {
	nested := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 2, Name: "tenant_id", Type: iceberg.PrimitiveTypes.String},
		{ID: 3, Name: "amount", Type: iceberg.PrimitiveTypes.Int64},
	}}
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "details", Type: nested,
	})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})
	filter := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("details.tenant_id"), "acme"),
		iceberg.GreaterThan(iceberg.Reference("details.amount"), int64(100)),
	)
	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
	plan := newPartitionResidualPlan(schema, &spec, bound, true)
	require.NotNil(t, plan)

	residual, changed := plan.residual(map[int]any{1000: "acme"})
	require.True(t, changed)
	want, err := iceberg.BindExpr(schema,
		iceberg.GreaterThan(iceberg.Reference("details.amount"), int64(100)), true)
	require.NoError(t, err)
	assert.True(t, residual.Equals(want), "expected %s, got %s", want, residual)
}

func TestPlanFilesLocalSetsIdentityPartitionResidual(t *testing.T) {
	ctx := context.Background()
	const tableLocation = "mem://identity-residual"

	schema := partitionResidualTestSchema()
	spec := partitionResidualTestSpec(iceberg.IdentityTransform{})
	metadata, err := NewMetadata(schema, &spec, UnsortedSortOrder, tableLocation, nil)
	require.NoError(t, err)

	dataFileBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		tableLocation+"/data/file.parquet",
		iceberg.ParquetFile,
		map[int]any{1000: "acme"},
		nil,
		nil,
		1,
		1024,
	)
	require.NoError(t, err)

	snapshotID := int64(1)
	entry := iceberg.NewManifestEntryBuilder(
		iceberg.EntryStatusADDED,
		&snapshotID,
		dataFileBuilder.Build(),
	).SequenceNum(1).Build()
	manifestPath := tableLocation + "/metadata/manifest.avro"
	var manifestBuffer bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath, &manifestBuffer, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry},
	)
	require.NoError(t, err)

	manifestListPath := tableLocation + "/metadata/snap-1.avro"
	var manifestListBuffer bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(
		2, &manifestListBuffer, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest},
	))

	fs := iceio.NewMemFS()
	require.NoError(t, fs.WriteFile(manifestPath, manifestBuffer.Bytes()))
	require.NoError(t, fs.WriteFile(manifestListPath, manifestListBuffer.Bytes()))

	metadataBuilder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	schemaID := schema.ID
	require.NoError(t, metadataBuilder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    metadata.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       &schemaID,
	}))
	require.NoError(t, metadataBuilder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	built, err := metadataBuilder.Build()
	require.NoError(t, err)

	tbl := New(
		Identifier{"db", "identity-residual"},
		built,
		tableLocation+"/metadata/metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil },
		nil,
	)
	filter := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
	)
	tasks, err := tbl.Scan(WithRowFilter(filter)).PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].Residual)

	want, err := iceberg.BindExpr(schema,
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)), true)
	require.NoError(t, err)
	assert.True(t, tasks[0].Residual.Equals(want),
		"expected %s, got %s", want, tasks[0].Residual)
}

var partitionResidualBenchmarkSink iceberg.BooleanExpression

func BenchmarkPartitionResidualPlanning(b *testing.B) {
	schema := partitionResidualTestSchema()
	spec := partitionResidualTestSpec(iceberg.IdentityTransform{})
	filter := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
	)
	bound, err := iceberg.BindExpr(schema, filter, true)
	if err != nil {
		b.Fatal(err)
	}
	plan := newPartitionResidualPlan(schema, &spec, bound, true)
	if plan == nil {
		b.Fatal("expected an identity partition residual plan")
	}

	partitions := make([]map[int]any, 4096)
	for i := range partitions {
		value := "acme"
		if i%2 == 0 {
			value = "other"
		}
		partitions[i] = map[int]any{1000: value}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, partition := range partitions {
			partitionResidualBenchmarkSink, _ = plan.residual(partition)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(partitions)), "files/op")
}
