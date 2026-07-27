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
	"encoding/json"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestMetadataGettersReturnDefensiveCopies(t *testing.T) {
	parentID := int64(1)
	refMinSnapshots := 2
	refMaxSnapshotAge := int64(3)
	refMaxAge := int64(4)
	keyMetadata := "key-metadata"
	encryptedByID := "root-key"
	metadata := commonMetadata{
		CurrentSchemaID: 1,
		DefaultSpecID:   1,
		SchemaList: []*iceberg.Schema{iceberg.NewSchemaWithIdentifiers(1, []int{1}, iceberg.NestedField{
			ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
			InitialDefault: iceberg.BinaryLiteral{1, 2},
			WriteDefault:   iceberg.FixedLiteral{3, 4},
		})},
		Specs: []iceberg.PartitionSpec{iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
		})},
		SnapshotList: []Snapshot{{
			SnapshotID:       2,
			ParentSnapshotID: &parentID,
			Summary: &Summary{
				Operation:  OpAppend,
				Properties: map[string]string{"source": "test"},
			},
		}},
		CurrentSnapshotID: &[]int64{2}[0],
		SnapshotRefs: map[string]SnapshotRef{
			MainBranch: {
				SnapshotID:         2,
				SnapshotRefType:    BranchRef,
				MinSnapshotsToKeep: &refMinSnapshots,
				MaxSnapshotAgeMs:   &refMaxSnapshotAge,
				MaxRefAgeMs:        &refMaxAge,
			},
		},
		Props: map[string]string{"owner": "iceberg"},
		StatisticsList: []StatisticsFile{{
			SnapshotID:     2,
			StatisticsPath: "stats.puffin",
			KeyMetadata:    &keyMetadata,
			BlobMetadata: []BlobMetadata{{
				Type:       BlobTypeApacheDatasketchesThetaV1,
				Fields:     []int32{1},
				Properties: map[string]string{"source": "test"},
			}},
		}},
		PartitionStatsList: []PartitionStatisticsFile{{
			SnapshotID:     2,
			StatisticsPath: "partition-stats.puffin",
		}},
		EncryptionKeyList: []EncryptionKey{{
			KeyID:                "key-1",
			EncryptedKeyMetadata: "encrypted",
			EncryptedByID:        &encryptedByID,
			Properties:           map[string]string{"scope": "table"},
		}},
		SortOrderList: []SortOrder{{
			orderID: 1,
			fields: []SortField{{
				SourceIDs: []int{10},
				Transform: iceberg.IdentityTransform{},
			}},
		}},
	}

	original, err := json.Marshal(metadata)
	require.NoError(t, err)

	properties := metadata.Properties()
	properties["owner"] = "mutated"

	schemas := metadata.Schemas()
	schemas[0].ID = 99
	schemas[0].IdentifierFieldIDs[0] = 99
	nestedFields := schemas[0].Fields()
	nestedFields[0].Name = "mutated"
	nestedFields[0].Type = iceberg.PrimitiveTypes.String
	nestedFields[0].InitialDefault.(iceberg.BinaryLiteral)[0] = 99
	nestedFields[0].WriteDefault.(iceberg.FixedLiteral)[0] = 99

	partitionSpecs := metadata.PartitionSpecs()
	partitionField := partitionSpecs[0].Field(0)
	partitionField.SourceIDs[0] = 99
	partitionField.Name = "mutated"
	require.Equal(t, []int{1}, metadata.Specs[0].Field(0).SourceIDs)

	currentSchema := metadata.CurrentSchema()
	currentSchema.ID = 100
	defaultSpec := metadata.PartitionSpec()
	defaultSpecField := defaultSpec.Field(0)
	defaultSpecField.SourceIDs[0] = 100
	byIDSpec := metadata.PartitionSpecByID(1)
	require.NotNil(t, byIDSpec)
	byIDField := byIDSpec.Field(0)
	byIDField.SourceIDs[0] = 101

	snapshots := metadata.Snapshots()
	snapshots[0].ParentSnapshotID = new(int64)
	snapshots[0].Summary.Properties["source"] = "mutated"

	byID := metadata.SnapshotByID(2)
	require.NotNil(t, byID)
	*byID.ParentSnapshotID = 99
	byID.Summary.Properties["source"] = "mutated"

	current := metadata.CurrentSnapshot()
	require.NotNil(t, current)
	current.Summary.Properties["source"] = "mutated"

	statistics := slices.Collect(metadata.Statistics())
	statistics[0].BlobMetadata[0].Fields[0] = 99
	statistics[0].BlobMetadata[0].Properties["source"] = "mutated"
	*statistics[0].KeyMetadata = "mutated"

	partitionStatistics := slices.Collect(metadata.PartitionStatistics())
	partitionStatistics[0].StatisticsPath = "mutated"

	encryptionKeys := slices.Collect(metadata.EncryptionKeys())
	*encryptionKeys[0].EncryptedByID = "mutated"
	encryptionKeys[0].Properties["scope"] = "mutated"

	refs := metadata.Ref()
	*refs.MinSnapshotsToKeep = 99
	for _, ref := range metadata.Refs() {
		*ref.MaxSnapshotAgeMs = 99
	}

	sortOrders := metadata.SortOrders()
	fields := sortOrders[0].Fields()
	for _, field := range fields {
		field.SourceIDs[0] = 99
	}
	require.Equal(t, []int{10}, metadata.SortOrderList[0].fields[0].SourceIDs)

	got, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.JSONEq(t, string(original), string(got))
}
