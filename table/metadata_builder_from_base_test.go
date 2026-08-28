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
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMetadataBuilderFromBaseCopiesBuiltinMetadata(t *testing.T) {
	tableSchema := iceberg.NewSchemaWithIdentifiers(
		0,
		[]int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1},
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1},
		Transform: iceberg.IdentityTransform{},
		Direction: SortASC,
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)

	currentSnapshotID := int64(2)
	parentSnapshotID := int64(1)
	schemaID := 0
	lastPartitionID := 1
	minSnapshotsToKeep := 2
	maxSnapshotAgeMs := int64(3)
	maxRefAgeMs := int64(4)
	statisticsKeyMetadata := "statistics-key"
	encryptedByID := "kms-key"

	metadata := &metadataV3{
		LastSeqNum:     2,
		NextRowIDValue: 3,
		commonMetadata: commonMetadata{
			FormatVersion:   3,
			UUID:            uuid.New(),
			Loc:             "s3://test/table",
			LastUpdatedMS:   1000,
			LastColumnId:    1,
			SchemaList:      []*iceberg.Schema{tableSchema},
			CurrentSchemaID: 0,
			Specs:           []iceberg.PartitionSpec{partitionSpec},
			DefaultSpecID:   0,
			LastPartitionID: &lastPartitionID,
			Props:           iceberg.Properties{"property": "value"},
			SnapshotList: []Snapshot{{
				SnapshotID:        currentSnapshotID,
				ParentSnapshotID:  &parentSnapshotID,
				ManifestLocations: []string{"manifest.avro"},
				Summary:           &Summary{Operation: OpAppend, Properties: map[string]string{"summary": "value"}},
				SchemaID:          &schemaID,
			}},
			CurrentSnapshotID:  &currentSnapshotID,
			SnapshotLog:        []SnapshotLogEntry{{SnapshotID: currentSnapshotID, TimestampMs: 1000}},
			MetadataLog:        []MetadataLogEntry{{MetadataFile: "metadata.json", TimestampMs: 900}},
			SortOrderList:      []SortOrder{sortOrder},
			DefaultSortOrderID: 1,
			SnapshotRefs: map[string]SnapshotRef{
				MainBranch: {
					SnapshotID:         currentSnapshotID,
					SnapshotRefType:    BranchRef,
					MinSnapshotsToKeep: &minSnapshotsToKeep,
					MaxSnapshotAgeMs:   &maxSnapshotAgeMs,
					MaxRefAgeMs:        &maxRefAgeMs,
				},
			},
			StatisticsList: []StatisticsFile{{
				SnapshotID:   currentSnapshotID,
				KeyMetadata:  &statisticsKeyMetadata,
				BlobMetadata: []BlobMetadata{{Fields: []int32{1}, Properties: map[string]string{"blob": "value"}}},
			}},
			PartitionStatsList: []PartitionStatisticsFile{{
				SnapshotID:     currentSnapshotID,
				StatisticsPath: "partition-stats.parquet",
			}},
			EncryptionKeyList: []EncryptionKey{{
				KeyID:                "key",
				EncryptedKeyMetadata: "metadata",
				EncryptedByID:        &encryptedByID,
				Properties:           map[string]string{"encryption": "value"},
			}},
		},
	}

	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)

	require.NotSame(t, &metadata.SchemaList[0], &builder.schemaList[0])
	require.NotSame(t, metadata.SchemaList[0], builder.schemaList[0])
	builder.schemaList[0].IdentifierFieldIDs[0] = 99
	require.Equal(t, []int{1}, metadata.SchemaList[0].IdentifierFieldIDs)

	require.NotSame(t, &metadata.Specs[0], &builder.specs[0])
	require.NotSame(t, &metadata.SnapshotList[0], &builder.snapshotList[0])
	builder.snapshotList[0].ManifestLocations[0] = "changed.avro"
	*builder.snapshotList[0].ParentSnapshotID = 99
	builder.snapshotList[0].Summary.Properties["summary"] = "changed"
	*builder.snapshotList[0].SchemaID = 99
	require.Equal(t, []string{"manifest.avro"}, metadata.SnapshotList[0].ManifestLocations)
	require.Equal(t, int64(1), *metadata.SnapshotList[0].ParentSnapshotID)
	require.Equal(t, "value", metadata.SnapshotList[0].Summary.Properties["summary"])
	require.Equal(t, 0, *metadata.SnapshotList[0].SchemaID)

	require.NotSame(t, &metadata.SortOrderList[0], &builder.sortOrderList[0])
	builder.sortOrderList[0].fields[0].SourceIDs[0] = 99
	require.Equal(t, []int{1}, metadata.SortOrderList[0].fields[0].SourceIDs)

	builder.props["property"] = "changed"
	require.Equal(t, "value", metadata.Props["property"])
	*builder.lastPartitionID = 99
	require.Equal(t, 1, *metadata.LastPartitionID)
	*builder.currentSnapshotID = 99
	require.Equal(t, currentSnapshotID, *metadata.CurrentSnapshotID)

	ref := builder.refs[MainBranch]
	ref.MinSnapshotsToKeep = clonePtr(ref.MinSnapshotsToKeep)
	*ref.MinSnapshotsToKeep = 99
	builder.refs[MainBranch] = ref
	require.Equal(t, 2, *metadata.SnapshotRefs[MainBranch].MinSnapshotsToKeep)
	delete(builder.refs, MainBranch)
	require.Contains(t, metadata.SnapshotRefs, MainBranch)

	*builder.statisticsList[0].KeyMetadata = "changed"
	builder.statisticsList[0].BlobMetadata[0].Fields[0] = 99
	builder.statisticsList[0].BlobMetadata[0].Properties["blob"] = "changed"
	require.Equal(t, "statistics-key", *metadata.StatisticsList[0].KeyMetadata)
	require.Equal(t, int32(1), metadata.StatisticsList[0].BlobMetadata[0].Fields[0])
	require.Equal(t, "value", metadata.StatisticsList[0].BlobMetadata[0].Properties["blob"])

	*builder.encryptionKeyList[0].EncryptedByID = "changed"
	builder.encryptionKeyList[0].Properties["encryption"] = "changed"
	require.Equal(t, "kms-key", *metadata.EncryptionKeyList[0].EncryptedByID)
	require.Equal(t, "value", metadata.EncryptionKeyList[0].Properties["encryption"])
}
