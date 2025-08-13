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
	return SortOrder{
		OrderID: 1,
		Fields: []SortField{
			{
				SourceID:  3,
				Direction: SortDESC,
				NullOrder: NullsFirst,
			},
		},
	}
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

	builder, err := NewMetadataBuilder()
	if err != nil {
		panic(err)
	}
	_, err = builder.SetFormatVersion(formatVersion)
	if err != nil {
		panic(err)
	}
	_, err = builder.AddSortOrder(&sortOrder, true)
	if err != nil {
		panic(err)
	}
	_, err = builder.AddPartitionSpec(&partitionSpec, true)
	if err != nil {
		panic(err)
	}
	_, err = builder.AddSchema(&tableSchema)
	if err != nil {
		panic(err)
	}
	meta, err := builder.Build()
	if err != nil {
		panic(err)
	}
	builder, err = MetadataBuilderFromBase(meta)
	if err != nil {
		panic(err)
	}

	return *builder
}

func TestSetRef(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       1,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
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
	_, err := builder.AddSnapshot(&snapshot)
	require.NoError(t, err)
	_, err = builder.SetSnapshotRef(MainBranch, 10, BranchRef, WithMinSnapshotsToKeep(10))
	require.ErrorContains(t, err, "can't set snapshot ref main to unknown snapshot 10: snapshot with id 10 not found")
	_, err = builder.SetSnapshotRef(MainBranch, 1, BranchRef, WithMinSnapshotsToKeep(10))
	require.NoError(t, err)
	require.Len(t, builder.snapshotList, 1)
	snap, err := builder.SnapshotByID(1)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, snap.SnapshotID, int64(1))
	require.True(t, snap.Equals(snapshot), "expected snapshot to match added snapshot")
	require.Len(t, builder.snapshotLog, 1)
}

func TestSetBranchSnapshotCreatesBranchIfNotExists(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
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

	_, err := builder.AddSnapshot(&snapshot)
	require.NoError(t, err)
	_, err = builder.SetSnapshotRef("new_branch", 2, BranchRef)
	require.NoError(t, err)

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

func TestCannotAddDuplicateSnapshotID(t *testing.T) {
	builder := builderWithoutChanges(2)
	schemaID := 0
	snapshot := Snapshot{
		SnapshotID:       2,
		ParentSnapshotID: nil,
		SequenceNumber:   0,
		TimestampMs:      builder.lastUpdatedMS + 1,
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
	_, err := builder.AddSnapshot(&snapshot)
	require.NoError(t, err)
	_, err = builder.AddSnapshot(&snapshot)
	require.ErrorContains(t, err, "can't add snapshot with id 2, already exists")
}

func TestRemoveMainSnapshotRef(t *testing.T) {
	meta, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.NotNil(t, meta.CurrentSnapshot())
	builder, err := MetadataBuilderFromBase(meta)
	require.NoError(t, err)
	require.NotNil(t, builder.currentSnapshotID)
	if _, ok := builder.refs[MainBranch]; !ok {
		t.Fatal("expected main branch to exist")
	}
	_, err = builder.RemoveSnapshotRef(MainBranch)
	require.NoError(t, err)
	require.Nil(t, builder.currentSnapshotID)
	meta, err = builder.Build()
	require.NoError(t, err)
	require.NotNil(t, meta)
}
