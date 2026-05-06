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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func partitionedConflictMeta(t *testing.T, schema *iceberg.Schema, partColID int, branchHead *int64) Metadata {
	t.Helper()

	field, ok := schema.FindFieldByID(partColID)
	require.True(t, ok)

	partSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			FieldID:   1000,
			SourceIDs: []int{partColID},
			Name:      field.Name,
			Transform: iceberg.IdentityTransform{},
		},
	)

	props := iceberg.Properties{PropertyFormatVersion: "2"}
	meta, err := NewMetadata(schema, &partSpec, UnsortedSortOrder, "file:///tmp/conflict-part-test", props)
	require.NoError(t, err)

	if branchHead == nil {
		return meta
	}

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	snap := Snapshot{
		SnapshotID:     *branchHead,
		SequenceNumber: 1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&snap))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, *branchHead, BranchRef))
	out, err := builder.Build()
	require.NoError(t, err)

	return out
}

func writeTestManifest(
	t *testing.T,
	dir string,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	snapshotID int64,
	partitionValues map[int]any,
	dataFilePath string,
) iceberg.ManifestFile {
	t.Helper()

	df, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		dataFilePath,
		iceberg.ParquetFile,
		partitionValues,
		nil,
		nil,
		1,
		1024,
	)
	require.NoError(t, err)

	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df.Build()).
		SequenceNum(1).
		Build()

	manifestPath := filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("manifest-%d.avro", snapshotID)))
	var buf bytes.Buffer
	mf, err := iceberg.WriteManifest(manifestPath, &buf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)

	fs := iceio.LocalFS{}
	f, err := fs.Create(manifestPath)
	require.NoError(t, err)
	_, err = f.Write(buf.Bytes())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return mf
}

func writeTestManifestList(
	t *testing.T,
	dir string,
	snapshotID int64,
	manifests []iceberg.ManifestFile,
) string {
	t.Helper()

	listPath := filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("snap-%d-manifests.avro", snapshotID)))
	fs := iceio.LocalFS{}
	f, err := fs.Create(listPath)
	require.NoError(t, err)

	seqNum := int64(1)
	err = iceberg.WriteManifestList(2, f, snapshotID, nil, &seqNum, 0, manifests)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return listPath
}

// buildPartitionedContext creates a conflictContext where the writer's base
// metadata has a snapshot at writerBaseID, and a concurrent commit at
// concSnapshotID (pointing at the given manifest list) happened afterward.
//
// The conflictContext will have ctx.concurrent == [concSnapshot].
func buildPartitionedContext(
	t *testing.T,
	writerBaseMeta Metadata,
	manifestListPath string,
	writerBaseID int64,
	concSnapshotID int64,
) *conflictContext {
	t.Helper()

	// currentMeta: writerBaseMeta + concurrent snapshot with manifest list.
	builder, err := MetadataBuilderFromBase(writerBaseMeta, "")
	require.NoError(t, err)

	concSnap := Snapshot{
		SnapshotID:       concSnapshotID,
		ParentSnapshotID: &writerBaseID,
		SequenceNumber:   2,
		TimestampMs:      writerBaseMeta.LastUpdatedMillis() + 2,
		ManifestList:     manifestListPath,
		Summary:          &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&concSnap))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, concSnapshotID, BranchRef))
	current, err := builder.Build()
	require.NoError(t, err)

	ctx, err := newConflictContext(writerBaseMeta, current, MainBranch, iceio.LocalFS{}, true)
	require.NoError(t, err)

	return ctx
}

// ---------------------------------------------------------------------------
// anyToLiteral
// ---------------------------------------------------------------------------

func TestAnyToLiteral_SupportedTypes(t *testing.T) {
	cases := []struct {
		name string
		v    any
	}{
		{"bool", true},
		{"int32", int32(42)},
		{"int64", int64(42)},
		{"float32", float32(1.5)},
		{"float64", float64(1.5)},
		{"string", "hello"},
		{"bytes", []byte{0x01}},
		{"Date", iceberg.Date(100)},
		{"Time", iceberg.Time(1000)},
		{"Timestamp", iceberg.Timestamp(9999)},
		// TimestampNano: arm in anyToLiteral was unreachable from the manifest read path
		// because convertAvroValueToIcebergType had no timestamp-nanos case; now fixed.
		{"TimestampNano", iceberg.TimestampNano(9999)},
		{"UUID", uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
		// DecimalLiteral is the named type returned by convertAvroValueToIcebergType
		// (type DecimalLiteral Decimal). It must be accepted without falling through
		// to the default error branch.
		{"DecimalLiteral", iceberg.DecimalLiteral{Scale: 2}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lit, err := anyToLiteral(tc.v)
			require.NoError(t, err)
			assert.NotNil(t, lit)
		})
	}
}

func TestAnyToLiteral_UnsupportedType(t *testing.T) {
	_, err := anyToLiteral(struct{}{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported partition value type")
}

// ---------------------------------------------------------------------------
// validateNoConflictingDataFilesInPartitions short-circuit paths
// ---------------------------------------------------------------------------

func TestValidateNoConflictingDataFilesInPartitions_SnapshotIsolationIsNoOp(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	head := int64(1)
	meta := partitionedConflictMeta(t, schema, 1, &head)
	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	spec := meta.PartitionSpec()
	partVals := map[int]any{}
	for _, pf := range spec.Fields() {
		partVals[pf.FieldID] = "us-east-1"
	}
	df, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		"s3://bucket/eq-del.parquet", iceberg.ParquetFile,
		partVals, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	require.NoError(t, validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{df.Build()}, IsolationSnapshot))
}

func TestValidateNoConflictingDataFilesInPartitions_EmptyInputsNoOp(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	head := int64(1)
	meta := partitionedConflictMeta(t, schema, 1, &head)
	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	require.NoError(t, validateNoConflictingDataFilesInPartitions(ctx, nil, IsolationSerializable))
	require.NoError(t, validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{}, IsolationSerializable))
}

// ---------------------------------------------------------------------------
// Regression #978: different-partition concurrent append must NOT be rejected
// ---------------------------------------------------------------------------

func TestRowDeltaValidate_DifferentPartitionAllowed(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	writerBaseID := int64(99)
	baseMeta := partitionedConflictMeta(t, schema, 2, &writerBaseID)
	concSnapshotID := int64(100)

	spec := baseMeta.PartitionSpec()

	euPartition := map[int]any{}
	for _, pf := range spec.Fields() {
		euPartition[pf.FieldID] = "eu-west-1"
	}
	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID, euPartition, dir+"/eu-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	usPartition := map[int]any{}
	for _, pf := range spec.Fields() {
		usPartition[pf.FieldID] = "us-east-1"
	}
	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/us-eq-del.parquet", iceberg.ParquetFile,
		usPartition, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	assert.NoError(t, err, "different-partition concurrent append must not be rejected")
}

// ---------------------------------------------------------------------------
// Same-partition concurrent append MUST be rejected
// ---------------------------------------------------------------------------

func TestRowDeltaValidate_SamePartitionRejected(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	writerBaseID := int64(199)
	baseMeta := partitionedConflictMeta(t, schema, 2, &writerBaseID)
	concSnapshotID := int64(200)

	spec := baseMeta.PartitionSpec()

	usPartition := map[int]any{}
	for _, pf := range spec.Fields() {
		usPartition[pf.FieldID] = "us-east-1"
	}
	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID, usPartition, dir+"/us-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/us-eq-del.parquet", iceberg.ParquetFile,
		usPartition, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}

// ---------------------------------------------------------------------------
// UUID partition column: same UUID rejected, different UUID allowed
// ---------------------------------------------------------------------------

func TestRowDeltaValidate_UUIDPartitionSameRejected(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "tenant", Type: iceberg.PrimitiveTypes.UUID, Required: true},
	)

	writerBaseID := int64(299)
	baseMeta := partitionedConflictMeta(t, schema, 2, &writerBaseID)
	concSnapshotID := int64(300)

	spec := baseMeta.PartitionSpec()
	tenantA := uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")

	partA := map[int]any{}
	for _, pf := range spec.Fields() {
		partA[pf.FieldID] = tenantA
	}

	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID, partA, dir+"/tenant-a-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)

	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/tenant-a-eq-del.parquet", iceberg.ParquetFile,
		partA, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	require.Error(t, err, "same UUID partition must conflict")
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}

func TestRowDeltaValidate_UUIDPartitionDifferentAllowed(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "tenant", Type: iceberg.PrimitiveTypes.UUID, Required: true},
	)

	writerBaseID := int64(300)
	baseMeta := partitionedConflictMeta(t, schema, 2, &writerBaseID)
	concSnapshotID := int64(301)

	spec := baseMeta.PartitionSpec()
	tenantA := uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")
	tenantB := uuid.MustParse("bbbbbbbb-0000-0000-0000-000000000002")

	concPartition := map[int]any{}
	eqPartition := map[int]any{}
	for _, pf := range spec.Fields() {
		concPartition[pf.FieldID] = tenantA
		eqPartition[pf.FieldID] = tenantB
	}

	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID, concPartition, dir+"/tenant-a-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)

	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/tenant-b-eq-del.parquet", iceberg.ParquetFile,
		eqPartition, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	assert.NoError(t, err, "different UUID partition must not conflict")
}

// ---------------------------------------------------------------------------
// Unpartitioned table: empty partition map triggers AlwaysTrue fallback
// ---------------------------------------------------------------------------

func TestRowDeltaValidate_UnpartitionedTableFallsBackToAlwaysTrue(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	props := iceberg.Properties{PropertyFormatVersion: "2"}
	emptyMeta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/unpart-test", props)
	require.NoError(t, err)

	// Writer's base: an initial snapshot already committed.
	writerBaseID := int64(399)
	builderBase, err := MetadataBuilderFromBase(emptyMeta, "")
	require.NoError(t, err)
	baseSnap := Snapshot{
		SnapshotID:     writerBaseID,
		SequenceNumber: 1,
		TimestampMs:    emptyMeta.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}
	require.NoError(t, builderBase.AddSnapshot(&baseSnap))
	require.NoError(t, builderBase.SetSnapshotRef(MainBranch, writerBaseID, BranchRef))
	baseMeta, err := builderBase.Build()
	require.NoError(t, err)

	concSnapshotID := int64(400)
	spec := baseMeta.PartitionSpec()

	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID, nil, dir+"/unpart-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/unpart-eq-del.parquet", iceberg.ParquetFile,
		nil, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	require.Error(t, err, "unpartitioned table with concurrent append must conflict under AlwaysTrue fallback")
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}

// ---------------------------------------------------------------------------
// Spec evolution: eq-delete under spec A, concurrent data under spec B
// (renamed partition field, same source field) — conflict must still be detected
// ---------------------------------------------------------------------------

func TestRowDeltaValidate_SpecEvolutionConflictDetected(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	// Spec A: identity(region) — the original partition spec.
	// partitionedConflictMeta assigns specID=0 and partitionFieldID=1000.
	initialMeta := partitionedConflictMeta(t, schema, 2, nil) // no snapshot yet
	specAID := initialMeta.DefaultPartitionSpec()             // = 0

	// Add spec B to the metadata: same source field, renamed partition field
	// ("region_v2") — simulating a partition-spec evolution.  The builder
	// assigns it a new specID (1) and a new partitionFieldID (1001).
	builder, err := MetadataBuilderFromBase(initialMeta, "")
	require.NoError(t, err)

	specBInput := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			// FieldID intentionally left 0 (unassigned) so the builder assigns a new one.
			SourceIDs: []int{2},
			Name:      "region_v2",
			Transform: iceberg.IdentityTransform{},
		},
	)
	require.NoError(t, builder.AddPartitionSpec(&specBInput, false))

	// spec B ID = specAID + 1 (reuseOrCreateNewPartitionSpecID increments from max existing).
	specBID := specAID + 1
	require.NoError(t, builder.SetDefaultSpecID(specBID))

	// Writer's base snapshot: committed before the spec evolution was applied.
	writerBaseID := int64(499)
	baseSnap := Snapshot{
		SnapshotID:     writerBaseID,
		SequenceNumber: 1,
		TimestampMs:    initialMeta.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&baseSnap))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, writerBaseID, BranchRef))
	writerBaseMeta, err := builder.Build()
	require.NoError(t, err)

	// Resolve both specs from the built metadata (both must be present).
	resolvedSpecA := writerBaseMeta.PartitionSpecByID(specAID)
	require.NotNil(t, resolvedSpecA, "spec A not found in metadata")
	resolvedSpecB := writerBaseMeta.PartitionSpecByID(specBID)
	require.NotNil(t, resolvedSpecB, "spec B not found in metadata")

	// Retrieve actual partition field IDs assigned by the builder.
	var specAFieldID, specBFieldID int
	for _, pf := range resolvedSpecA.Fields() {
		specAFieldID = pf.FieldID
	}
	for _, pf := range resolvedSpecB.Fields() {
		specBFieldID = pf.FieldID
	}
	require.NotEqual(t, specAFieldID, specBFieldID,
		"spec A and spec B must have different partition field IDs")

	// Concurrent commit: data file written under spec B, region_v2 = "us-east-1".
	concSnapshotID := int64(500)
	specBPartition := map[int]any{specBFieldID: "us-east-1"}
	mf := writeTestManifest(t, dir, *resolvedSpecB, schema, concSnapshotID, specBPartition, dir+"/spec-b-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, writerBaseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	// Eq-delete file written under spec A, region = "us-east-1".
	// Same logical partition value, different spec — cross-spec conflict.
	specAPartition := map[int]any{specAFieldID: "us-east-1"}
	eqDf, err := iceberg.NewDataFileBuilder(
		*resolvedSpecA, iceberg.EntryContentEqDeletes,
		dir+"/spec-a-eq-del.parquet", iceberg.ParquetFile,
		specAPartition, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	// eqDeletePartitionsToFilter builds Reference("region") == "us-east-1" (row space, spec A).
	// validateAddedDataFilesMatchingFilter projects this against spec B's "region_v2"
	// (also identity on source "region"), correctly matching the concurrent file.
	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	require.Error(t, err, "cross-spec same-partition conflict must be detected after spec evolution")
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}

// ---------------------------------------------------------------------------
// Non-identity transforms: bucket and day fall back to AlwaysTrue
// ---------------------------------------------------------------------------

// TestRowDeltaValidate_BucketTransformFallsBackToAlwaysTrue verifies that a
// bucket[N]-partitioned eq-delete triggers the AlwaysTrue conservative fallback.
// Bucket partition values stored in DataFile.Partition() are post-transform
// (bucket indices); building a row-space predicate from them would cause
// double-transformation downstream. The safe path is to treat the eq-delete as
// table-wide (AlwaysTrue), which rejects any concurrent data file.
func TestRowDeltaValidate_BucketTransformFallsBackToAlwaysTrue(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "user_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	partSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			FieldID:   1000,
			SourceIDs: []int{2},
			Name:      "user_id_bucket",
			Transform: iceberg.BucketTransform{NumBuckets: 16},
		},
	)
	props := iceberg.Properties{PropertyFormatVersion: "2"}
	meta, err := NewMetadata(schema, &partSpec, UnsortedSortOrder, "file:///tmp/bucket-test", props)
	require.NoError(t, err)

	writerBaseID := int64(600)
	b, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, b.AddSnapshot(&Snapshot{
		SnapshotID: writerBaseID, SequenceNumber: 1,
		TimestampMs: meta.LastUpdatedMillis() + 1,
		Summary:     &Summary{Operation: OpAppend},
	}))
	require.NoError(t, b.SetSnapshotRef(MainBranch, writerBaseID, BranchRef))
	baseMeta, err := b.Build()
	require.NoError(t, err)

	spec := baseMeta.PartitionSpec()
	var bucketFieldID int
	for _, pf := range spec.Fields() {
		bucketFieldID = pf.FieldID
	}

	// Concurrent commit: data file in bucket 1.
	concSnapshotID := int64(601)
	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID,
		map[int]any{bucketFieldID: int32(1)}, dir+"/bucket1-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})
	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	// Eq-delete in bucket 1 — same bucket, but non-identity transform forces
	// AlwaysTrue fallback; concurrent file must still be rejected.
	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/bucket1-eq-del.parquet", iceberg.ParquetFile,
		map[int]any{bucketFieldID: int32(1)}, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	require.Error(t, err, "bucket-partitioned eq-delete must trigger conservative AlwaysTrue fallback")
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}

// TestRowDeltaValidate_DayTransformFallsBackToAlwaysTrue verifies that a
// day(ts)-partitioned eq-delete also triggers the AlwaysTrue fallback — even
// when the concurrent data file is in a different day. The code cannot safely
// reverse a day transform to obtain a row-space bound, so it must be conservative.
func TestRowDeltaValidate_DayTransformFallsBackToAlwaysTrue(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "event_ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)

	partSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			FieldID:   1000,
			SourceIDs: []int{2},
			Name:      "event_day",
			Transform: iceberg.DayTransform{},
		},
	)
	props := iceberg.Properties{PropertyFormatVersion: "2"}
	meta, err := NewMetadata(schema, &partSpec, UnsortedSortOrder, "file:///tmp/day-test", props)
	require.NoError(t, err)

	writerBaseID := int64(700)
	b, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, b.AddSnapshot(&Snapshot{
		SnapshotID: writerBaseID, SequenceNumber: 1,
		TimestampMs: meta.LastUpdatedMillis() + 1,
		Summary:     &Summary{Operation: OpAppend},
	}))
	require.NoError(t, b.SetSnapshotRef(MainBranch, writerBaseID, BranchRef))
	baseMeta, err := b.Build()
	require.NoError(t, err)

	spec := baseMeta.PartitionSpec()
	var dayFieldID int
	for _, pf := range spec.Fields() {
		dayFieldID = pf.FieldID
	}

	// Concurrent commit: data file in day 100 (days since epoch).
	concSnapshotID := int64(701)
	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID,
		map[int]any{dayFieldID: int32(100)}, dir+"/day100-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})
	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	// Eq-delete in day 200 — a different day, but non-identity transform means
	// the code falls back to AlwaysTrue and cannot distinguish the days;
	// concurrent file must still be rejected (conservative correctness).
	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/day200-eq-del.parquet", iceberg.ParquetFile,
		map[int]any{dayFieldID: int32(200)}, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	require.Error(t, err, "day-partitioned eq-delete must trigger conservative AlwaysTrue fallback even for a different day")
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}
