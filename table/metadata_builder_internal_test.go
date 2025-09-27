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
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

func schema() iceberg.Schema {
	return *iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
}

func sortOrder() SortOrder {
	// TODO: rust has a constructor for SortOrder which checks for compat to schema
	newSortOrder, err := NewSortOrder(
		1,
		[]SortField{
			{
				SourceID:  3,
				Direction: SortDESC,
				NullOrder: NullsFirst,
				Transform: iceberg.BucketTransform{NumBuckets: 4},
			},
		},
	)
	if err != nil {
		panic(err)
	}

	return newSortOrder
}

func partitionSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpecID(0,
		iceberg.PartitionField{
			SourceID:  2,
			Name:      "y",
			Transform: iceberg.IdentityTransform{},
		},
	)
}

func builderWithoutChanges(formatVersion int) MetadataBuilder {
	tableSchema := schema()
	partitionSpec := partitionSpec()
	sortOrder := sortOrder()

	builder, err := NewMetadataBuilder(formatVersion)
	if err != nil {
		panic(err)
	}
	if err = builder.SetLoc("s3://bucket/test/location"); err != nil {
	}
	if err = builder.AddSchema(&tableSchema); err != nil {
		panic(err)
	}
	if err = builder.SetCurrentSchemaID(-1); err != nil {
		panic(err)
	}
	if err = builder.AddSortOrder(&sortOrder); err != nil {
		panic(err)
	}
	if err = builder.SetDefaultSortOrderID(-1); err != nil {
		panic(err)
	}
	if err = builder.AddPartitionSpec(&partitionSpec, true); err != nil {
		panic(err)
	}
	if err = builder.SetDefaultSpecID(-1); err != nil {
		panic(err)
	}

	meta, err := builder.Build()
	if err != nil {
		panic(err)
	}
	builder, err = MetadataBuilderFromBase(meta, "s3://bucket/test/location/metadata/metadata1.json")
	if err != nil {
		panic(err)
	}

	return *builder
}

func TestBuildUnpartitionedUnsorted(t *testing.T) {
	TestLocation := "file:///tmp/iceberg-test"
	tableSchema := schema()
	partitionSpec := iceberg.NewPartitionSpecID(0)

	builder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	require.NoError(t, builder.SetFormatVersion(2))
	require.NoError(t, builder.AddSchema(&tableSchema))
	require.NoError(t, builder.SetCurrentSchemaID(-1))
	require.NoError(t, builder.AddSortOrder(&UnsortedSortOrder))
	require.NoError(t, builder.SetDefaultSortOrderID(-1))
	require.NoError(t, builder.AddPartitionSpec(&partitionSpec, true))
	require.NoError(t, builder.SetDefaultSpecID(-1))
	require.NoError(t, builder.SetLoc(TestLocation))

	meta, err := builder.Build()

	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, 2, meta.Version())
	require.Equal(t, TestLocation, meta.Location())
	require.Equal(t, 0, meta.CurrentSchema().ID)
	require.Equal(t, 0, meta.DefaultPartitionSpec())
	require.Equal(t, 0, meta.DefaultSortOrder())
	require.Equal(t, 999, *meta.LastPartitionSpecID())
	require.Equal(t, 3, meta.LastColumnID())
	require.Equal(t, 0, len(meta.Snapshots()))
	require.Nil(t, meta.CurrentSnapshot())
	for range meta.Refs() {
		t.Fatalf("refs should be empty.")
	}
	require.Equal(t, len(meta.Properties()), 0)
	for range meta.PreviousFiles() {
		t.Fatalf("metadata log should be empty.")
	}
	require.Equal(t, meta.LastSequenceNumber(), int64(0))
	require.Equal(t, meta.LastColumnID(), 3)
}

func TestReassignIds(t *testing.T) {
	TestLocation := "file:///tmp/iceberg-test"
	schema := iceberg.NewSchema(10, iceberg.NestedField{
		ID:       11,
		Name:     "a",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:       12,
		Name:     "b",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:   13,
		Name: "struct",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{
					Type:     iceberg.PrimitiveTypes.Int64,
					ID:       14,
					Name:     "nested",
					Required: true,
				},
			},
		},
		Required: true,
	},
		iceberg.NestedField{
			ID:       15,
			Name:     "c",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		})

	spec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(20),
		iceberg.AddPartitionFieldByName("a", "a", iceberg.IdentityTransform{}, schema, nil),
		iceberg.AddPartitionFieldByName("struct.nested", "nested_partition", iceberg.IdentityTransform{}, schema, nil))

	require.NoError(t, err)

	sortOrder, err := NewSortOrder(10, []SortField{
		{
			SourceID:  11,
			Transform: iceberg.IdentityTransform{},
			Direction: SortASC,
			NullOrder: NullsFirst,
		},
	})
	require.NoError(t, err)
	meta, err := NewMetadata(
		schema,
		&spec,
		sortOrder,
		TestLocation,
		map[string]string{})

	require.NoError(t, err)
	require.NotNil(t, meta)

	expectedSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "a",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:       2,
		Name:     "b",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	}, iceberg.NestedField{
		ID:   3,
		Name: "struct",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{
					Type: iceberg.PrimitiveTypes.Int64,
					// TODO: this is discrepancy with rust impl, is 5 over there
					ID:       4,
					Name:     "nested",
					Required: true,
				},
			},
		},
		Required: true,
	},
		iceberg.NestedField{
			// TODO: this is discrepancy with rust impl, is 4 over there
			ID:       5,
			Name:     "c",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		})
	id := 1000
	fieldID := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(0),
		iceberg.AddPartitionFieldByName("a", "a", iceberg.IdentityTransform{}, expectedSchema, &id),
		iceberg.AddPartitionFieldByName("struct.nested", "nested_partition", iceberg.IdentityTransform{}, expectedSchema, &fieldID))

	require.NoError(t, err)

	expectedSortOrder, err := NewSortOrder(1, []SortField{
		{
			SourceID:  1,
			Transform: iceberg.IdentityTransform{},
			Direction: SortASC,
			NullOrder: NullsFirst,
		},
	})
	require.NoError(t, err)

	require.True(t, expectedSchema.Equals(meta.Schemas()[0]))
	require.True(t, expectedSpec.Equals(meta.PartitionSpecs()[0]))
	require.True(t, expectedSortOrder.Equals(meta.SortOrders()[0]))
}

func TestAddRemovePartitionSpec(t *testing.T) {
	builder := builderWithoutChanges(2)
	builderRef := &builder
	i := 1000
	addedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.schemaList[0], &i),
		iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.schemaList[0], nil))
	require.NoError(t, err)

	err = builderRef.AddPartitionSpec(&addedSpec, false)
	require.NoError(t, err)
	metadata, err := builderRef.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)

	i2 := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(1),
		iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.schemaList[0], &i),
		iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.schemaList[0], &i2))
	require.NoError(t, err)
	require.Equal(t, metadata.DefaultPartitionSpec(), 0)
	require.Equal(t, *metadata.LastPartitionSpecID(), i2)
	found := false
	for _, part := range metadata.PartitionSpecs() {
		if part.ID() == 1 {
			found = true
			require.True(t, part.Equals(expectedSpec), "expected partition spec to match added spec")
		}
	}
	require.True(t, found, "expected partition spec to be added")

	newBuilder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	// Remove the spec
	require.NoError(t, newBuilder.RemovePartitionSpecs([]int{1}))
	newBuild, err := newBuilder.Build()
	require.NoError(t, err)
	require.NotNil(t, newBuild)
	require.Len(t, newBuilder.updates, 1)
	require.Len(t, newBuild.PartitionSpecs(), 1)
	_, err = newBuilder.GetSpecByID(1)
	require.ErrorContains(t, err, "partition spec with id 1 not found")
}

func TestSetDefaultPartitionSpec(t *testing.T) {
	builder := builderWithoutChanges(2)
	curSchema, err := builder.GetSchemaByID(builder.currentSchemaID)
	require.NoError(t, err)
	// Add a partition spec
	addedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, nil))
	require.NoError(t, err)
	require.NoError(t, builder.AddPartitionSpec(&addedSpec, false))
	require.NoError(t, builder.SetDefaultSpecID(-1))

	id := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(1),
		iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, &id))
	require.NoError(t, err)
	require.True(t, builder.HasChanges())
	require.Equal(t, len(builder.updates), 2)
	require.True(t, builder.updates[0].(*addPartitionSpecUpdate).Spec.Equals(expectedSpec))
	require.Equal(t, -1, builder.updates[1].(*setDefaultSpecUpdate).SpecID)
	metadata, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)

	require.Equal(t, metadata.DefaultPartitionSpec(), 1)
	require.True(t, expectedSpec.Equals(metadata.PartitionSpec()), fmt.Sprintf("expected partition spec to match added spec %s, %s", spew.Sdump(expectedSpec), spew.Sdump(metadata.PartitionSpec())))
	require.Equal(t, *metadata.LastPartitionSpecID(), 1001)
}

func TestSetExistingDefaultPartitionSpec(t *testing.T) {
	builder := builderWithoutChanges(2)
	curSchema, err := builder.GetSchemaByID(builder.currentSchemaID)
	require.NoError(t, err)

	addedSpec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, nil))
	require.NoError(t, err)

	id := 1001
	expectedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(1), iceberg.AddPartitionFieldBySourceID(1, "y_bucket[2]", iceberg.BucketTransform{NumBuckets: 2}, curSchema, &id))
	require.NoError(t, err)

	require.NoError(t, builder.AddPartitionSpec(&addedSpec, false))

	require.NoError(t, builder.SetDefaultSpecID(-1))

	require.True(t, builder.HasChanges())
	require.Len(t, builder.updates, 2)
	require.True(t, builder.updates[0].(*addPartitionSpecUpdate).Spec.Equals(expectedSpec))
	require.Equal(t, -1, builder.updates[1].(*setDefaultSpecUpdate).SpecID)

	metadata, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Equal(t, 1, metadata.DefaultPartitionSpec())

	require.True(t, expectedSpec.Equals(metadata.PartitionSpec()), "expected partition spec to match added spec")

	newBuilder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.NotNil(t, newBuilder)

	require.NoError(t, newBuilder.SetDefaultSpecID(0))

	require.True(t, newBuilder.HasChanges(), "expected changes after setting default spec")
	require.Len(t, newBuilder.updates, 1, "expected one update")
	require.Equal(t, 0, newBuilder.updates[0].(*setDefaultSpecUpdate).SpecID, "expected default partition spec to be set to 0")

	newBuild, err := newBuilder.Build()
	require.NoError(t, err)
	require.NotNil(t, newBuild)
	require.Equal(t, 0, newBuild.DefaultPartitionSpec(), "expected default partition spec to be set to 0")

	newWithoutChanges := builderWithoutChanges(2)
	require.True(t, newWithoutChanges.specs[0].Equals(newBuild.PartitionSpec()), "expected partition spec to match added spec")
}

func TestSetSortOrder(t *testing.T) {
	builder := builderWithoutChanges(2)
	added, err := NewSortOrder(10, []SortField{
		{
			SourceID:  1,
			Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst,
		},
	})
	require.NoError(t, err)
	schema := schema()
	require.NoError(t, added.CheckCompatibility(&schema))
	require.NoError(t, builder.AddSortOrder(&added))
	expected, err := NewSortOrder(2, []SortField{
		{
			SourceID:  1,
			Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst,
		},
	})
	require.NoError(t, err)
	require.Len(t, builder.updates, 1)
	require.Equal(t, maxBy(builder.sortOrderList, func(e SortOrder) int {
		return e.OrderID()
	}), 2)
	order, err := builder.GetSortOrderByID(2)
	require.NoError(t, err)
	require.True(t, (*order).Equals(expected), "expected sort order to match added sort order")
	require.True(t, builder.updates[0].(*addSortOrderUpdate).SortOrder.Equals(expected), "expected sort order to match added sort order")
}

func TestSetRef(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.base.LastUpdatedMillis() + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}

	require.NoError(t, builder.AddSnapshot(&snapshot))
	err := builder.SetSnapshotRef(MainBranch, 10, BranchRef, WithMinSnapshotsToKeep(10))
	require.ErrorContains(t, err, "can't set snapshot ref main to unknown snapshot 10: snapshot with id 10 not found")
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 1, BranchRef, WithMinSnapshotsToKeep(10)))
	require.Len(t, builder.snapshotList, 1)
	snap, err := builder.SnapshotByID(1)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, snap.SnapshotID, int64(1))
	require.True(t, snap.Equals(snapshot), "expected snapshot to match added snapshot")
	require.Len(t, builder.snapshotLog, 1)
}

func TestAddPartitionSpecForV1RequiresSequentialIDs(t *testing.T) {
	builder := builderWithoutChanges(1)

	// Add a partition spec with non-sequential IDs
	id := 1000
	id2 := 1002
	addedSpec, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(10),
		iceberg.AddPartitionFieldBySourceID(2, "y", iceberg.IdentityTransform{}, builder.CurrentSchema(), &id),
		iceberg.AddPartitionFieldBySourceID(3, "z", iceberg.IdentityTransform{}, builder.CurrentSchema(), &id2))
	require.NoError(t, err)

	require.ErrorContains(t, builder.AddPartitionSpec(&addedSpec, false), "v1 constraint: partition field IDs are not sequential: expected 1001, got 1002")
}

func TestSnapshotLogSkipsIntermediate(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot1 := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.base.LastUpdatedMillis() + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}

	snapshot2 := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.base.LastUpdatedMillis() + 2,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}
	err := builder.AddSnapshot(&snapshot1)
	require.NoError(t, err)
	err = builder.SetSnapshotRef(MainBranch, 1, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)

	err = builder.AddSnapshot(&snapshot2)
	require.NoError(t, err)
	err = builder.SetSnapshotRef(MainBranch, 2, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)

	require.NoError(t, err)

	res, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Len(t, res.(*metadataV2).SnapshotLog, 1)
	snap := res.(*metadataV2).SnapshotLog[0]
	require.NotNil(t, snap)
	require.Equal(t, snap, SnapshotLogEntry{
		SnapshotID:  2,
		TimestampMs: snapshot2.TimestampMs,
	}, "expected snapshot to match added snapshot")
	require.True(t, res.CurrentSnapshot().Equals(snapshot2))
}

func TestSetBranchSnapshotCreatesBranchIfNotExists(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.base.LastUpdatedMillis(),
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}

	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.NoError(t, builder.SetSnapshotRef("new_branch", 2, BranchRef))

	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)
	// Check that the branch was created
	ref := meta.(*metadataV2).SnapshotRefs["new_branch"]
	require.Len(t, meta.(*metadataV2).SnapshotRefs, 1)
	require.Equal(t, int64(2), ref.SnapshotID)
	require.Equal(t, BranchRef, ref.SnapshotRefType)
	require.True(t, builder.updates[0].(*addSnapshotUpdate).Snapshot.Equals(snapshot))
	require.Equal(t, "new_branch", builder.updates[1].(*setSnapshotRefUpdate).RefName)
	require.Equal(t, BranchRef, builder.updates[1].(*setSnapshotRefUpdate).RefType)
	require.Equal(t, int64(2), builder.updates[1].(*setSnapshotRefUpdate).SnapshotID)
}

func TestRemoveSnapshotRemovesBranch(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.base.LastUpdatedMillis() + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}

	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.NoError(t, builder.SetSnapshotRef("new_branch", 2, BranchRef))

	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)
	// Check that the branch was created
	ref := meta.(*metadataV2).SnapshotRefs["new_branch"]
	require.Len(t, meta.(*metadataV2).SnapshotRefs, 1)
	require.Equal(t, int64(2), ref.SnapshotID)
	require.Equal(t, BranchRef, ref.SnapshotRefType)
	require.True(t, builder.updates[0].(*addSnapshotUpdate).Snapshot.Equals(snapshot))
	require.Equal(t, "new_branch", builder.updates[1].(*setSnapshotRefUpdate).RefName)
	require.Equal(t, BranchRef, builder.updates[1].(*setSnapshotRefUpdate).RefType)
	require.Equal(t, int64(2), builder.updates[1].(*setSnapshotRefUpdate).SnapshotID)

	newBuilder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, newBuilder.RemoveSnapshots([]int64{snapshot.SnapshotID}))
	newMeta, err := newBuilder.Build()
	require.NoError(t, err)
	require.NotNil(t, newMeta)
	require.Len(t, newMeta.(*metadataV2).SnapshotRefs, 0)
	require.Len(t, newBuilder.updates, 1)
	require.Equal(t, newBuilder.updates[0].(*removeSnapshotsUpdate).SnapshotIDs[0], snapshot.SnapshotID)
	for k, r := range newMeta.Refs() {
		require.NotEqual(t, r.SnapshotID, snapshot.SnapshotID)
		require.NotEqual(t, k, "new_branch")
	}
}

func TestExpireMetadataLog(t *testing.T) {
	builder1 := builderWithoutChanges(2)
	meta, err := builder1.Build()
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "s3://bla")
	require.NoError(t, err)
	err = builder.SetProperties(map[string]string{
		MetadataPreviousVersionsMaxKey: "2",
	})
	require.NoError(t, err)
	meta, err = builder.Build()
	require.NoError(t, err)
	require.Len(t, meta.(*metadataV2).MetadataLog, 1)

	location := "p"
	newBuilder, err := MetadataBuilderFromBase(meta, location)
	require.NoError(t, err)
	err = newBuilder.SetProperties(map[string]string{
		"change_nr": "1",
	})
	require.NoError(t, err)
	meta, err = newBuilder.Build()
	require.NoError(t, err)
	require.Len(t, meta.(*metadataV2).MetadataLog, 2)

	newBuilder, err = MetadataBuilderFromBase(meta, location)
	require.NoError(t, err)
	err = newBuilder.SetProperties(map[string]string{
		"change_nr": "2",
	})
	require.NoError(t, err)
	meta, err = newBuilder.Build()
	require.NoError(t, err)
	require.Len(t, meta.(*metadataV2).MetadataLog, 2)
}

func TestV2SequenceNumberCannotDecrease(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot1 := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   1,
		TimestampMs:      builder.base.LastUpdatedMillis() + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation:  OpAppend,
			Properties: map[string]string{},
		},
		SchemaID: &schemaID,
	}

	err := builder.AddSnapshot(&snapshot1)
	require.NoError(t, err)

	err = builder.SetSnapshotRef(MainBranch, 1, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)

	parentSnapshotID := int64(1)
	snapshot2 := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: &parentSnapshotID,
		SequenceNumber:   0, // Lower sequence number than previous
		TimestampMs:      builder.lastUpdatedMS + 1,
		ManifestList:     "/snap-0.avro",
		Summary: &Summary{
			Operation:  OpAppend,
			Properties: map[string]string{},
		},
		SchemaID: &schemaID,
	}
	err = builder.AddSnapshot(&snapshot2)
	require.ErrorContains(t, err, "can't add snapshot with sequence number 0, must be > than last sequence number 1")
}

func TestCannotAddDuplicateSnapshotID(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.base.LastUpdatedMillis() + 1,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation: OpAppend,
			Properties: map[string]string{
				"spark.app.id":     "local-1662532784305",
				"added-data-files": "4",
				"added-records":    "4",
				"added-files-size": "6001",
			},
		},
		SchemaID: &schemaID,
	}
	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.ErrorContains(t, builder.AddSnapshot(&snapshot), "can't add snapshot with id 2, already exists")
}

func TestLastUpdateIncreasedForPropertyOnlyUpdate(t *testing.T) {
	builder := builderWithoutChanges(2)
	meta, err := builder.Build()
	require.NoError(t, err)
	lastUpdatedMS := builder.lastUpdatedMS
	time.Sleep(5 * time.Millisecond)
	// Set a property

	location := "some-location"

	newBuilder, err := MetadataBuilderFromBase(meta, location)
	require.NoError(t, err)

	err = newBuilder.SetProperties(map[string]string{
		"foo": "bar",
	})
	require.NoError(t, err)
	newMeta, err := newBuilder.Build()
	require.NoError(t, err)
	require.NotNil(t, newMeta)

	// Check that the last updated timestamp has increased
	require.Greater(t, newMeta.LastUpdatedMillis(), lastUpdatedMS, "expected last updated timestamp to increase after property update")
}

func TestAddSnapshotRejectsInvalidTimestamp(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.base.LastUpdatedMillis() - 61000,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation:  OpAppend,
			Properties: map[string]string{},
		},
		SchemaID: &schemaID,
	}
	err := builder.AddSnapshot(&snapshot)
	require.ErrorContains(t, err, "before last updated timestamp")

	snapshot.TimestampMs = builder.base.LastUpdatedMillis() + (60000 * 2)

	err = builder.AddSnapshot(&snapshot)
	require.NoError(t, err)

	// cause an entry to snapshot log by setting the main branch ref
	err = builder.SetSnapshotRef(MainBranch, snapshot.SnapshotID, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)

	snapshot2 := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   1,
		TimestampMs:      snapshot.TimestampMs - 61000,
		ManifestList:     "/snap-1.avro",
		Summary: &Summary{
			Operation:  OpAppend,
			Properties: map[string]string{},
		},
		SchemaID: &schemaID,
	}

	err = builder.AddSnapshot(&snapshot2)
	require.ErrorContains(t, err, "before last snapshot timestamp")

	snapshot2.TimestampMs = snapshot.TimestampMs + (60000 * 2) + 1
	err = builder.AddSnapshot(&snapshot2)
	require.NoError(t, err)
}

func TestConstructDefaultMainBranch(t *testing.T) {
	// TODO: Not sure what this test is supposed to do Rust: `test_construct_default_main_branch`
	meta, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NotNil(t, meta)

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	meta, err = builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)

	require.Equal(t, meta.(*metadataV2).SnapshotRefs[MainBranch].SnapshotID, meta.CurrentSnapshot().SnapshotID)
}

func TestAddIncompatibleCurrentSchemaFails(t *testing.T) {
	builder := builderWithoutChanges(2)
	addedSchema := iceberg.NewSchema(1)
	err := builder.AddSchema(addedSchema)
	require.NoError(t, err)
	err = builder.SetCurrentSchemaID(1)
	require.NoError(t, err)
	_, err = builder.Build()
	require.ErrorContains(t, err, "with source id 3 not found in schema")
}

func TestRemoveMainSnapshotRef(t *testing.T) {
	meta, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.NotNil(t, meta.CurrentSnapshot())
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NotNil(t, builder.currentSnapshotID)
	if _, ok := builder.refs[MainBranch]; !ok {
		t.Fatal("expected main branch to exist")
	}
	err = builder.RemoveSnapshotRef(MainBranch)
	require.NoError(t, err)
	require.Nil(t, builder.currentSnapshotID)
	meta, err = builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestActiveSchemaCannotBeRemoved(t *testing.T) {
	builder := builderWithoutChanges(2)
	// Try to remove the current schema
	require.ErrorContains(t, builder.RemoveSchemas([]int{0}), "can't remove current schema with id 0")
}

func TestRemoveSchemas(t *testing.T) {
	meta, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.Len(t, meta.Schemas(), 2, "expected 2 schemas in the metadata")
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	err = builder.RemoveSchemas([]int{0})
	require.NoError(t, err, "expected to remove schema with ID 1")
	newMeta, err := builder.Build()
	require.NoError(t, err)
	require.Len(t, newMeta.Schemas(), 1, "expected 1 schema in the metadata after removal")
	require.Equal(t, 1, newMeta.CurrentSchema().ID, "expected current schema to be 1")
	require.Equal(t, 1, newMeta.(*metadataV2).CurrentSchemaID)
	require.Len(t, builder.updates, 1, "expected one update for schema removal")
	require.Equal(t, builder.updates[0].Action(), UpdateRemoveSchemas)
	require.Equal(t, builder.updates[0].(*removeSchemasUpdate).SchemaIDs, []int{0}, "expected schema ID 0 to be removed")
}

// Java: TestTableMetadata.testUpdateSchema
func TestUpdateSchema(t *testing.T) {
	// Test schema updates and evolution
	schema1 := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
	)

	meta, err := NewMetadata(
		schema1,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://bucket/test/location",
		map[string]string{},
	)
	require.NoError(t, err)

	require.Equal(t, 0, meta.CurrentSchema().ID)
	require.Len(t, meta.Schemas(), 1)
	require.Equal(t, 1, meta.LastColumnID())

	// Update schema by adding a field
	schema2 := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 2, Name: "x", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	err = builder.AddSchema(schema2)
	require.NoError(t, err)

	err = builder.SetCurrentSchemaID(-1) // Use last added
	require.NoError(t, err)

	updatedMeta, err := builder.Build()
	require.NoError(t, err)

	require.Equal(t, 1, updatedMeta.CurrentSchema().ID)
	require.Len(t, updatedMeta.Schemas(), 2)
	require.Equal(t, 2, updatedMeta.LastColumnID())

	// Verify both schemas are preserved
	schemas := updatedMeta.Schemas()
	require.True(t, schemas[0].Equals(schema1))
	require.True(t, schemas[1].Equals(schema2))
}

func TestDefaultSpecCannotBeRemoved(t *testing.T) {
	builder := builderWithoutChanges(2)

	require.ErrorContains(t, builder.RemovePartitionSpecs([]int{0}), "can't remove default partition spec with id 0")
}

func TestSetReservedPropertiesFails(t *testing.T) {
	builder := builderWithoutChanges(2)

	// Test that setting non-reserved properties works
	err := builder.SetProperties(iceberg.Properties{
		"custom-property":         "value1",
		"another-custom-property": "value2",
	})
	require.NoError(t, err)
	require.True(t, builder.HasChanges())

	// Test setting each reserved property individually
	for _, reserved := range ReservedProperties {
		err := builder.SetProperties(iceberg.Properties{reserved: "some-value"})
		require.ErrorContains(t, err, "can't set reserved property "+reserved)
	}

	// Test setting multiple properties where one is reserved
	err = builder.SetProperties(iceberg.Properties{
		"custom-property":         "allowed",
		PropertyCurrentSnapshotId: "12345",
		"another-custom-property": "also-allowed",
	})
	require.ErrorContains(t, err, "can't set reserved property "+PropertyCurrentSnapshotId)
}

func TestRemoveReservedPropertiesFails(t *testing.T) {
	builder := builderWithoutChanges(2)

	// Test removing each reserved property individually
	for _, reserved := range ReservedProperties {
		err := builder.RemoveProperties([]string{reserved})
		require.ErrorContains(t, err, "can't remove reserved property "+reserved)
	}

	// Test removing multiple properties where one is reserved
	err := builder.RemoveProperties([]string{
		"custom-property",
		PropertyUuid,
		"another-custom-property",
	})
	require.ErrorContains(t, err, "can't remove reserved property "+PropertyUuid)

	// Add some custom properties first, then test that removing non-reserved properties works
	require.NoError(t, builder.SetProperties(iceberg.Properties{
		"custom-property":         "value1",
		"another-custom-property": "value2",
	}))

	err = builder.RemoveProperties([]string{"custom-property"})
	require.NoError(t, err)
	require.True(t, builder.HasChanges())
}

func TestIdsAreReassignedForNewMetadata(t *testing.T) {
	// Create schema with ID 10 (should be reassigned to 0)
	tableSchema := iceberg.NewSchema(
		10,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	partitionSpec := partitionSpec()
	sortOrder := sortOrder()

	metadata, err := NewMetadata(
		tableSchema,
		&partitionSpec,
		sortOrder,
		"file:///tmp/iceberg-test",
		map[string]string{},
	)

	require.NoError(t, err)
	require.NotNil(t, metadata)

	require.Equal(t, 0, metadata.CurrentSchema().ID)
	require.Equal(t, 0, metadata.(*metadataV2).CurrentSchemaID)
}

func TestNewMetadataChanges(t *testing.T) {
	tableSchema := schema()
	partitionSpec := partitionSpec()
	sortOrder := sortOrder()
	properties := map[string]string{
		"property 1": "value 1",
	}

	builder, err := NewMetadataBuilder(1)
	require.NoError(t, err)
	require.NoError(t, builder.SetLoc("file:///tmp/iceberg-test"))
	require.NoError(t, builder.AddSchema(&tableSchema))
	require.NoError(t, builder.SetCurrentSchemaID(-1))
	require.NoError(t, builder.AddPartitionSpec(&partitionSpec, true))
	require.NoError(t, builder.SetDefaultSpecID(-1))
	require.NoError(t, builder.AddSortOrder(&sortOrder))
	require.NoError(t, builder.SetDefaultSortOrderID(-1))
	require.NoError(t, builder.SetProperties(properties))

	_, err = builder.Build()
	require.NoError(t, err)

	require.Len(t, builder.updates, 8)

	require.IsType(t, &setLocationUpdate{}, builder.updates[0])
	require.Equal(t, "file:///tmp/iceberg-test", builder.updates[0].(*setLocationUpdate).Location)

	require.IsType(t, &addSchemaUpdate{}, builder.updates[1])
	require.True(t, builder.updates[1].(*addSchemaUpdate).Schema.Equals(&tableSchema))

	require.IsType(t, &setCurrentSchemaUpdate{}, builder.updates[2])
	require.Equal(t, -1, builder.updates[2].(*setCurrentSchemaUpdate).SchemaID)

	require.IsType(t, &addPartitionSpecUpdate{}, builder.updates[3])
	// For new tables, field IDs should be assigned (1000 for first partition field)
	addedSpec := builder.updates[3].(*addPartitionSpecUpdate).Spec
	require.Equal(t, 0, addedSpec.ID())
	require.Equal(t, 1, addedSpec.Len())
	require.Equal(t, 1000, addedSpec.Field(0).FieldID)

	require.IsType(t, &setDefaultSpecUpdate{}, builder.updates[4])
	require.Equal(t, -1, builder.updates[4].(*setDefaultSpecUpdate).SpecID)

	require.IsType(t, &addSortOrderUpdate{}, builder.updates[5])
	require.True(t, builder.updates[5].(*addSortOrderUpdate).SortOrder.Equals(sortOrder))

	require.IsType(t, &setDefaultSortOrderUpdate{}, builder.updates[6])
	require.Equal(t, -1, builder.updates[6].(*setDefaultSortOrderUpdate).SortOrderID)

	require.IsType(t, &setPropertiesUpdate{}, builder.updates[7])
	require.Equal(t, iceberg.Properties{"property 1": "value 1"}, builder.updates[7].(*setPropertiesUpdate).Updates)
}

func TestNewMetadataChangesUnpartitionedUnsorted(t *testing.T) {
	tableSchema := *iceberg.NewSchema(0)
	partitionSpec := *iceberg.UnpartitionedSpec
	sortOrder := UnsortedSortOrder

	builder, err := NewMetadataBuilder(1)
	require.NoError(t, err)
	require.NoError(t, builder.SetLoc("file:///tmp/iceberg-test"))
	require.NoError(t, builder.AddSchema(&tableSchema))
	require.NoError(t, builder.SetCurrentSchemaID(-1))
	require.NoError(t, builder.AddPartitionSpec(&partitionSpec, true))
	require.NoError(t, builder.SetDefaultSpecID(-1))
	require.NoError(t, builder.AddSortOrder(&sortOrder))
	require.NoError(t, builder.SetDefaultSortOrderID(-1))

	_, err = builder.Build()
	require.NoError(t, err)

	// Verify the expected updates were created (7 updates, no properties)
	require.Len(t, builder.updates, 7)

	// Check each update type in order
	require.IsType(t, &setLocationUpdate{}, builder.updates[0])
	require.Equal(t, "file:///tmp/iceberg-test", builder.updates[0].(*setLocationUpdate).Location)

	require.IsType(t, &addSchemaUpdate{}, builder.updates[1])
	require.True(t, builder.updates[1].(*addSchemaUpdate).Schema.Equals(&tableSchema))

	require.IsType(t, &setCurrentSchemaUpdate{}, builder.updates[2])
	require.Equal(t, -1, builder.updates[2].(*setCurrentSchemaUpdate).SchemaID)

	require.IsType(t, &addPartitionSpecUpdate{}, builder.updates[3])

	addedSpec := builder.updates[3].(*addPartitionSpecUpdate).Spec
	require.Equal(t, 0, addedSpec.ID())
	require.Equal(t, 0, addedSpec.Len()) // Unpartitioned = no fields

	require.IsType(t, &setDefaultSpecUpdate{}, builder.updates[4])
	require.Equal(t, -1, builder.updates[4].(*setDefaultSpecUpdate).SpecID)

	require.IsType(t, &addSortOrderUpdate{}, builder.updates[5])
	require.True(t, builder.updates[5].(*addSortOrderUpdate).SortOrder.Equals(sortOrder))

	require.IsType(t, &setDefaultSortOrderUpdate{}, builder.updates[6])
	require.Equal(t, -1, builder.updates[6].(*setDefaultSortOrderUpdate).SortOrderID)
}

func TestSetCurrentSchemaChangeIsMinusOneIfSchemaWasAddedInThisChange(t *testing.T) {
	builder := builderWithoutChanges(2)

	addedSchema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 4, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	err := builder.AddSchema(addedSchema)
	require.NoError(t, err)

	err = builder.SetCurrentSchemaID(1)
	require.NoError(t, err)

	_, err = builder.Build()
	require.NoError(t, err)

	// Should have 2 updates
	require.Len(t, builder.updates, 2)

	// First update should be AddSchema
	require.IsType(t, &addSchemaUpdate{}, builder.updates[0])
	require.True(t, builder.updates[0].(*addSchemaUpdate).Schema.Equals(addedSchema))

	// Second update should be SetCurrentSchema with schema_id = -1 (indicates last added)
	require.IsType(t, &setCurrentSchemaUpdate{}, builder.updates[1])
	require.Equal(t, -1, builder.updates[1].(*setCurrentSchemaUpdate).SchemaID)
}

func TestNoMetadataLogForCreateTable(t *testing.T) {
	tableSchema := schema()
	partitionSpec := partitionSpec()
	sortOrder := sortOrder()

	metadata, err := NewMetadata(
		&tableSchema,
		&partitionSpec,
		sortOrder,
		"file:///tmp/iceberg-test",
		map[string]string{},
	)

	require.NoError(t, err)
	require.NotNil(t, metadata)

	require.Len(t, metadata.(*metadataV2).MetadataLog, 0)
}

func TestNoMetadataLogEntryForNoPreviousLocation(t *testing.T) {
	builder := builderWithoutChanges(2)
	require.NoError(t, builder.SetLoc("file:///tmp/iceberg-test"))
	metadata, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Len(t, metadata.(*metadataV2).MetadataLog, 1)

	newBuilder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)

	err = newBuilder.SetProperties(map[string]string{
		"foo": "bar",
	})
	require.NoError(t, err)

	newMetadata, err := newBuilder.Build()
	require.NoError(t, err)

	require.Len(t, newMetadata.(*metadataV2).MetadataLog, 1)
}

func TestFromMetadataGeneratesMetadataLog(t *testing.T) {
	metadataPath := "s3://bucket/test/location/metadata/metadata1.json"

	tableSchema := schema()
	partitionSpec := partitionSpec()
	sortOrder := sortOrder()

	metadata, err := NewMetadata(
		&tableSchema,
		&partitionSpec,
		sortOrder,
		"file:///tmp/iceberg-test",
		map[string]string{},
	)
	require.NoError(t, err)
	require.NotNil(t, metadata)

	builder, err := MetadataBuilderFromBase(metadata, metadataPath)
	require.NoError(t, err)

	err = builder.AddSortOrder(&UnsortedSortOrder)
	require.NoError(t, err)

	newMetadata, err := builder.Build()
	require.NoError(t, err)

	require.Len(t, newMetadata.(*metadataV2).MetadataLog, 1)
	require.Equal(t, metadataPath, newMetadata.(*metadataV2).MetadataLog[0].MetadataFile)
}
