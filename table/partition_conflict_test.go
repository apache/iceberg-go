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
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
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
		DataSequenceNumber(1).
		Build()

	manifestPath := filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("manifest-%d.avro", snapshotID)))
	var buf bytes.Buffer
	mf, err := iceberg.WriteManifest(manifestPath, &buf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)

	fs := iceio.LocalFS{}
	f, err := fs.Create(context.Background(), manifestPath)
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
	f, err := fs.Create(context.Background(), listPath)
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

	ctx, err := newConflictContext(t.Context(), writerBaseMeta, current, MainBranch, iceio.LocalFS{}, true)
	require.NoError(t, err)

	return ctx
}

type countingPartitionConflictMetadata struct {
	Metadata
	specOverrides        map[int]*iceberg.PartitionSpec
	currentSchemaCalls   int
	partitionSpecIDCalls int
}

func (m *countingPartitionConflictMetadata) CurrentSchema() *iceberg.Schema {
	m.currentSchemaCalls++

	return m.Metadata.CurrentSchema()
}

func (m *countingPartitionConflictMetadata) PartitionSpecByID(id int) *iceberg.PartitionSpec {
	m.partitionSpecIDCalls++
	if spec, ok := m.specOverrides[id]; ok {
		return spec
	}

	return m.Metadata.PartitionSpecByID(id)
}

func equalityDeleteTestFile(
	t *testing.T,
	spec iceberg.PartitionSpec,
	path string,
	partition map[int]any,
) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentEqDeletes,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		1,
		1024,
	)
	require.NoError(t, err)

	return builder.Build()
}

func TestEqDeletePartitionsToFilterDeduplicatesPartitions(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	meta := partitionedConflictMeta(t, schema, 2, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID
	countingMeta := &countingPartitionConflictMetadata{Metadata: meta}

	files := []iceberg.DataFile{
		equalityDeleteTestFile(t, spec, "eq-us-1.parquet", map[int]any{fieldID: "us-east-1"}),
		equalityDeleteTestFile(t, spec, "eq-us-2.parquet", map[int]any{fieldID: "us-east-1"}),
		equalityDeleteTestFile(t, spec, "eq-eu.parquet", map[int]any{fieldID: "eu-west-1"}),
		equalityDeleteTestFile(t, spec, "eq-us-3.parquet", map[int]any{fieldID: "us-east-1"}),
	}

	filter, err := eqDeletePartitionsToFilter(files, countingMeta)
	require.NoError(t, err)

	expected := iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("region"), []iceberg.Literal{
		iceberg.StringLiteral("us-east-1"),
		iceberg.StringLiteral("eu-west-1"),
	})
	assert.True(t, filter.Equals(expected), "duplicate equality-delete partitions should produce one filter term")
	assert.Equal(t, 1, countingMeta.currentSchemaCalls,
		"the current schema should be cloned once per filter")
	assert.Equal(t, 1, countingMeta.partitionSpecIDCalls,
		"each partition spec should be resolved once per filter")
}

func TestEqDeletePartitionsToFilterKeepsSpecIDsDistinct(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	meta := partitionedConflictMeta(t, schema, 1, nil)
	specA := meta.PartitionSpec()
	specB := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		FieldID:   specA.Field(0).FieldID,
		SourceIDs: []int{1},
		Name:      "region_v2",
		Transform: iceberg.IdentityTransform{},
	})
	countingMeta := &countingPartitionConflictMetadata{
		Metadata:      meta,
		specOverrides: map[int]*iceberg.PartitionSpec{specB.ID(): &specB},
	}

	files := []iceberg.DataFile{
		equalityDeleteTestFile(t, specA, "eq-a.parquet", map[int]any{specA.Field(0).FieldID: "us-east-1"}),
		equalityDeleteTestFile(t, specB, "eq-b.parquet", map[int]any{specB.Field(0).FieldID: "us-east-1"}),
	}

	filter, err := eqDeletePartitionsToFilter(files, countingMeta)
	require.NoError(t, err)

	leaf := iceberg.EqualTo(iceberg.Reference("region"), "us-east-1")
	expected := iceberg.NewOr(leaf, leaf)
	assert.True(t, filter.Equals(expected),
		"identical partition values under different specs must remain separate terms")
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
	ctx, err := newConflictContext(t.Context(), meta, meta, MainBranch, nil, true)
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
	ctx, err := newConflictContext(t.Context(), meta, meta, MainBranch, nil, true)
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

func TestRowDeltaValidate_NestedPartitionSources(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "device", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "root_id", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{FieldID: 1001, SourceIDs: []int{3}, Name: "device_id", Transform: iceberg.IdentityTransform{}},
	)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "file:///tmp/nested-conflict",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	baseID := int64(199)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: baseID, SequenceNumber: 1, TimestampMs: meta.LastUpdatedMillis() + 1,
		Summary: &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, baseID, BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)
	spec = meta.PartitionSpec()
	rootID, nestedID := spec.Field(0).FieldID, spec.Field(1).FieldID

	for _, tc := range []struct {
		name          string
		deleteValue   any
		appendedValue any
		wantConflict  bool
	}{
		{name: "matching nested value", deleteValue: int64(7), appendedValue: int64(7), wantConflict: true},
		{name: "different nested value", deleteValue: int64(7), appendedValue: int64(8)},
		{name: "matching null", wantConflict: true},
		{name: "null and non-null", appendedValue: int64(7)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.ToSlash(t.TempDir())
			concurrentID := int64(200)
			dataPartition := map[int]any{rootID: int64(9), nestedID: tc.appendedValue}
			manifest := writeTestManifest(t, dir, spec, meta.CurrentSchema(), concurrentID, dataPartition, dir+"/data.parquet")
			list := writeTestManifestList(t, dir, concurrentID, []iceberg.ManifestFile{manifest})
			cc := buildPartitionedContext(t, meta, list, baseID, concurrentID)
			deleteFile := equalityDeleteTestFile(t, spec, dir+"/delete.parquet",
				map[int]any{rootID: int64(9), nestedID: tc.deleteValue})

			err := validateNoConflictingDataFilesInPartitions(cc, []iceberg.DataFile{deleteFile}, IsolationSerializable)
			if tc.wantConflict {
				require.ErrorIs(t, err, ErrConflictingDataFiles)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEqDeleteConflictGroupsNestedSourceValues(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "device", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		FieldID: 1000, SourceIDs: []int{3}, Name: "device_id", Transform: iceberg.IdentityTransform{},
	})
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "file:///tmp/nested-conflict",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	spec = meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID
	files := []iceberg.DataFile{
		equalityDeleteTestFile(t, spec, "first.parquet", map[int]any{fieldID: int64(7)}),
		equalityDeleteTestFile(t, spec, "second.parquet", map[int]any{fieldID: int64(8)}),
		equalityDeleteTestFile(t, spec, "null.parquet", map[int]any{fieldID: nil}),
	}
	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)
	want := iceberg.NewOr(
		iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("device.id"),
			[]iceberg.Literal{iceberg.Int64Literal(7), iceberg.Int64Literal(8)}),
		iceberg.IsNull(iceberg.Reference("device.id")),
	)
	require.True(t, filter.Equals(want), "got %s", filter)
	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, true)
	require.NoError(t, err)
	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, false)
	require.NoError(t, err)
}

type conflictSchemaOverride struct {
	Metadata
	schema *iceberg.Schema
}

func (m conflictSchemaOverride) CurrentSchema() *iceberg.Schema { return m.schema }

func TestEqDeleteConflictAmbiguousSourceNameFallsBack(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "other", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "device", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
	)
	meta := partitionedConflictMeta(t, schema, 3, nil)
	spec := meta.PartitionSpec()
	deleteFile := equalityDeleteTestFile(t, spec, "delete.parquet", map[int]any{spec.Field(0).FieldID: int64(7)})

	// These two field IDs have the same dotted path, so a name predicate
	// cannot safely distinguish the nested source from the top-level field.
	ambiguous := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "device.id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "device", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
	)
	filter, err := eqDeletePartitionsToFilter([]iceberg.DataFile{deleteFile}, conflictSchemaOverride{Metadata: meta, schema: ambiguous})
	require.NoError(t, err)
	require.True(t, filter.Equals(iceberg.AlwaysTrue{}), "got %s", filter)
}

func TestEqDeleteConflictUsesCurrentNestedName(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "device", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
	)
	meta := partitionedConflictMeta(t, schema, 2, nil)
	spec := meta.PartitionSpec()
	deleteFile := equalityDeleteTestFile(t, spec, "delete.parquet", map[int]any{spec.Field(0).FieldID: int64(7)})

	renamed := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "sensor", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
	)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSchema(renamed))
	require.NoError(t, builder.SetCurrentSchemaID(1))
	meta, err = builder.Build()
	require.NoError(t, err)

	filter, err := eqDeletePartitionsToFilter([]iceberg.DataFile{deleteFile}, meta)
	require.NoError(t, err)
	want := iceberg.EqualTo(iceberg.Reference("sensor.id"), int64(7))
	require.True(t, filter.Equals(want), "got %s", filter)
}

func TestEqDeleteConflictCaseInsensitiveAmbiguityFailsClosed(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "device", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
		iceberg.NestedField{ID: 3, Name: "DEVICE", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{ID: 4, Name: "id", Type: iceberg.PrimitiveTypes.Int64}},
		}},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		FieldID: 1000, SourceIDs: []int{2}, Name: "device_id", Transform: iceberg.IdentityTransform{},
	})
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "file:///tmp/nested-conflict",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	spec = meta.PartitionSpec()
	deleteFile := equalityDeleteTestFile(t, spec, "delete.parquet", map[int]any{spec.Field(0).FieldID: int64(7)})
	filter, err := eqDeletePartitionsToFilter([]iceberg.DataFile{deleteFile}, meta)
	require.NoError(t, err)

	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, false)
	require.Error(t, err, "case-insensitive ambiguity must not bind to the wrong field")
}

func TestRowDeltaValidate_NullIdentityPartitionRejected(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "dt", Type: iceberg.PrimitiveTypes.Date},
	)

	writerBaseID := int64(249)
	baseMeta := partitionedConflictMeta(t, schema, 2, &writerBaseID)
	concSnapshotID := int64(250)

	spec := baseMeta.PartitionSpec()

	nullPartition := map[int]any{}
	for _, pf := range spec.Fields() {
		nullPartition[pf.FieldID] = nil
	}
	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID, nullPartition, dir+"/null-dt-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/null-dt-eq-del.parquet", iceberg.ParquetFile,
		nullPartition, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	err = validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}

// ---------------------------------------------------------------------------
// NaN identity partition: Bind rejects x = NaN, so the filter uses IsNaN
// ---------------------------------------------------------------------------

func TestEqDeletePartitionsToFilterUsesIsNaNForNaNPartition(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
	)
	meta := partitionedConflictMeta(t, schema, 2, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID

	filter, err := eqDeletePartitionsToFilter([]iceberg.DataFile{
		equalityDeleteTestFile(t, spec, "eq-nan.parquet", map[int]any{fieldID: math.NaN()}),
	}, meta)
	require.NoError(t, err)

	expected := iceberg.IsNaN(iceberg.Reference("score"))
	assert.True(t, filter.Equals(expected), "got %s, want %s", filter, expected)
	_, err = iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
}

func checkNaNPartitionEqDeleteConflict(t *testing.T, concurrentValue float64) error {
	t.Helper()
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
	)

	writerBaseID := int64(259)
	baseMeta := partitionedConflictMeta(t, schema, 2, &writerBaseID)
	concSnapshotID := int64(260)

	spec := baseMeta.PartitionSpec()
	fieldID := spec.Field(0).FieldID

	mf := writeTestManifest(t, dir, spec, schema, concSnapshotID,
		map[int]any{fieldID: concurrentValue}, dir+"/score-data.parquet")
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, baseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	eqDf, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes,
		dir+"/nan-score-eq-del.parquet", iceberg.ParquetFile,
		map[int]any{fieldID: math.NaN()}, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	return validateNoConflictingDataFilesInPartitions(ctx, []iceberg.DataFile{eqDf.Build()}, IsolationSerializable)
}

func TestRowDeltaValidate_NaNIdentityPartitionRejected(t *testing.T) {
	err := checkNaNPartitionEqDeleteConflict(t, math.NaN())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
}

func TestRowDeltaValidate_NaNIdentityPartitionDifferentAllowed(t *testing.T) {
	require.NoError(t, checkNaNPartitionEqDeleteConflict(t, 1.5))
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

func TestEqDeleteConflictSingleFieldWithNullAndSeparatedValues(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String},
	)
	meta := partitionedConflictMeta(t, schema, 1, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID

	var files []iceberg.DataFile
	values := []any{"west", "east", nil, "central"}
	for i := range 32 {
		value := values[i%len(values)]
		files = append(files, equalityDeleteTestFile(t, spec,
			fmt.Sprintf("delete-%d.parquet", i), map[int]any{fieldID: value}))
	}
	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)

	want := iceberg.NewOr(
		iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("region"),
			[]iceberg.Literal{
				iceberg.StringLiteral("west"),
				iceberg.StringLiteral("east"),
				iceberg.StringLiteral("central"),
			}),
		iceberg.IsNull(iceberg.Reference("region")),
	)
	require.True(t, filter.Equals(want), "single-field values must be grouped without dropping nulls")
	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, true)
	require.NoError(t, err)
}

func TestEqDeleteConflictLargeMultiFieldPredicateBinds(t *testing.T) {
	const fieldCount = 3
	fields := make([]iceberg.NestedField, fieldCount)
	partFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fields {
		fields[i] = iceberg.NestedField{
			ID: i + 1, Name: fmt.Sprintf("field_%d", i), Type: iceberg.PrimitiveTypes.String,
		}
		partFields[i] = iceberg.PartitionField{
			FieldID: 1000 + i, SourceIDs: []int{i + 1},
			Name: fmt.Sprintf("field_%d", i), Transform: iceberg.IdentityTransform{},
		}
	}
	schema := iceberg.NewSchema(0, fields...)
	spec := iceberg.NewPartitionSpec(partFields...)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "file:///tmp/conflict-balanced",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	files := make([]iceberg.DataFile, 256)
	for i := range files {
		p := make(map[int]any, fieldCount)
		for field := range fieldCount {
			p[1000+field] = fmt.Sprintf("v-%d-%d", i, field)
		}
		files[i] = equalityDeleteTestFile(t, spec, fmt.Sprintf("del-%d.parquet", i), p)
	}

	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)
	require.Equal(t, iceberg.OpOr, filter.Op())
	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, true)
	require.NoError(t, err)
}

func TestEqDeleteConflictLargeSparseSetPreservesMetricsPruning(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32})
	meta := partitionedConflictMeta(t, schema, 1, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID
	files := make([]iceberg.DataFile, 401)
	for i := range files {
		files[i] = equalityDeleteTestFile(t, spec, fmt.Sprintf("delete-%d.parquet", i), map[int]any{fieldID: int32(i * 2)})
	}
	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)
	evaluate, err := newInclusiveMetricsEvaluator(schema, filter, true, true)
	require.NoError(t, err)
	for _, tc := range []struct {
		value int32
		want  bool
	}{{0, true}, {1, false}, {199, false}, {200, true}, {399, false}, {400, true}, {799, false}, {800, true}, {801, false}} {
		raw, err := iceberg.NewLiteral(tc.value).MarshalBinary()
		require.NoError(t, err)
		got, err := evaluate(inclusiveMetricsInTestFile(t, raw, raw))
		require.NoError(t, err)
		require.Equalf(t, tc.want, got, "value=%d", tc.value)
	}
}

// A NaN partition must stay out of the IN set, because Go map lookup cannot
// find a NaN key, and match through IsNaN, because Bind rejects x = NaN.
func TestEqDeleteConflictNaNPartition(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Float64})
	meta := partitionedConflictMeta(t, schema, 1, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID
	values := []float64{1, 2, 3, 4, 5, 6, 7, math.NaN()}
	files := make([]iceberg.DataFile, 32)
	for i := range files {
		value := values[i%len(values)]
		files[i] = equalityDeleteTestFile(t, spec, fmt.Sprintf("delete-%d.parquet", i), map[int]any{fieldID: value})
	}
	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)
	filters := newKeyDefaultMapWrapErr(func(id int) (iceberg.BooleanExpression, error) {
		return buildPartitionProjection(id, meta, schema, filter, true)
	})
	evaluate, err := buildPartitionEvaluator(spec.ID(), meta, schema, filters, true)
	require.NoError(t, err)
	for _, i := range []int{0, 6, 7} {
		got, err := evaluate(files[i])
		require.NoError(t, err)
		require.True(t, got, "partition %d must match", i)
	}
}

func TestEqDeleteConflictDecimalPrecisionPreservesGrouping(t *testing.T) {
	// DecimalLiteral.Type reports decimal(9, scale), even when the source
	// column has higher precision. Grouping must use the actual source scale.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.MustDecimalTypeOf(18, 2)},
	)
	meta := partitionedConflictMeta(t, schema, 1, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID

	values := []iceberg.Decimal{
		{Val: decimal128.FromI64(123), Scale: 2},
		{Val: decimal128.FromI64(456), Scale: 2},
		{Val: decimal128.FromI64(789), Scale: 2},
	}
	files := make([]iceberg.DataFile, len(values))
	literals := make([]iceberg.Literal, len(values))
	for i, value := range values {
		files[i] = equalityDeleteTestFile(t, spec, fmt.Sprintf("decimal-%d.parquet", i),
			map[int]any{fieldID: value})
		literals[i] = iceberg.NewLiteral(value)
	}

	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)
	want := iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("amount"), literals)
	require.True(t, filter.Equals(want), "decimal precision must not disable safe IN grouping")

	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, true)
	require.NoError(t, err)
}

func TestEqDeleteConflictCastValuePreservesEqualityPredicate(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Float64},
	)
	meta := partitionedConflictMeta(t, schema, 1, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID

	files := []iceberg.DataFile{
		equalityDeleteTestFile(t, spec, "float-1.parquet", map[int]any{fieldID: float64(1)}),
		equalityDeleteTestFile(t, spec, "cast-nan.parquet", map[int]any{fieldID: "NaN"}),
		equalityDeleteTestFile(t, spec, "float-2.parquet", map[int]any{fieldID: float64(2)}),
	}

	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)
	want := iceberg.NewOr(
		iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("value"), []iceberg.Literal{
			iceberg.Float64Literal(1), iceberg.Float64Literal(2),
		}),
		iceberg.IsNaN(iceberg.Reference("value")),
	)
	require.True(t, filter.Equals(want), "got %s: cast values must stay out of the IN set, and a cast to NaN must use IsNaN", filter)
	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, true)
	require.NoError(t, err)
}
