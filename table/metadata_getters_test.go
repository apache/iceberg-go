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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestMetadataGettersReturnDefensiveCopies(t *testing.T) {
	parentID := int64(1)
	refMinSnapshots := 2
	refMaxSnapshotAge := int64(3)
	refMaxAge := int64(4)
	metadata := commonMetadata{
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

	got, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.JSONEq(t, string(original), string(got))
}
