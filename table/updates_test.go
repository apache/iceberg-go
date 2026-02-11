// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveSnapshotsPostCommitSkipped(t *testing.T) {
	update := NewRemoveSnapshotsUpdate([]int64{1, 2, 3})
	update.postCommit = false

	// PostCommit should return nil immediately when postCommit is false,
	// without accessing the table arguments (which are nil here)
	err := update.PostCommit(context.Background(), nil, nil)
	assert.NoError(t, err)
}

func TestUnmarshalUpdates(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket",
		},
	)
	sortOrder, err := NewSortOrder(
		22,
		[]SortField{
			{SourceID: 19, Transform: iceberg.IdentityTransform{}, NullOrder: NullsFirst, Direction: SortASC},
			{SourceID: 25, Transform: iceberg.BucketTransform{NumBuckets: 4}, NullOrder: NullsFirst, Direction: SortDESC},
			{SourceID: 22, Transform: iceberg.VoidTransform{}, NullOrder: NullsFirst, Direction: SortASC},
		},
	)
	require.NoError(t, err)

	testCases := []struct {
		name        string
		data        []byte
		expected    Updates
		expectedErr bool
		errContains string
	}{
		{
			name: "should unmarshal a list of updates",
			data: []byte(`[
				{"action": "assign-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"},
				{"action": "upgrade-format-version", "format-version": 2},
				{"action": "set-location", "location": "s3://bucket/new-location"},
				{"action": "set-properties", "updates": {"key1": "value1"}},
				{"action": "remove-properties", "removals": ["key2"]},
				{"action": "remove-schemas", "schema-ids": [1,2,3,4]},
				{"action": "remove-partition-specs", "schema-ids": [1,2,3]},
				{"action": "remove-snapshots", "snapshot-ids": [1,2]},
				{"action": "remove-snapshot-ref", "ref-name": "main"},
				{"action": "set-default-sort-order", "order-id": 1},
				{"action": "set-default-spec", "spec-id": 1},
				{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 1}
			]`),
			expected: Updates{
				NewAssignUUIDUpdate(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")),
				NewUpgradeFormatVersionUpdate(2),
				NewSetLocationUpdate("s3://bucket/new-location"),
				NewSetPropertiesUpdate(iceberg.Properties{"key1": "value1"}),
				NewRemovePropertiesUpdate([]string{"key2"}),
				NewRemoveSchemasUpdate([]int{1, 2, 3, 4}),
				NewRemoveSpecUpdate([]int{1, 2, 3}),
				NewRemoveSnapshotsUpdate([]int64{1, 2}),
				NewRemoveSnapshotRefUpdate("main"),
				NewSetDefaultSortOrderUpdate(1),
				NewSetDefaultSpecUpdate(1),
				NewSetSnapshotRefUpdate("main", 1, "branch", 0, 0, 0),
			},
			expectedErr: false,
		},
		{
			name: "should unmarshal a complex list of updates",
			data: []byte(`[
  {
    "action": "add-schema",
    "schema": {
      "type": "struct",
      "schema-id": 1,
      "fields": [
        {
          "id": 1,
          "name": "foo",
          "required": true,
          "type": "string"
        }
      ]
    },
    "last-column-id": 1
  },
  {
    "action": "add-spec",
    "spec": {
      "spec-id": 3,
      "fields": [
        {
          "source-id": 1,
          "field-id": 1000,
          "transform": "truncate[19]",
          "name": "str_truncate"
        },
        {
          "source-id": 2,
          "field-id": 1001,
          "transform": "bucket[25]",
          "name": "int_bucket"
        }
      ]
    }
  },
  {
    "action": "add-sort-order",
    "sort-order": {
      "order-id": 22,
      "fields": [
        {
          "source-id": 19,
          "transform": "identity",
          "direction": "asc",
          "null-order": "nulls-first"
        },
        {
          "source-id": 25,
          "transform": "bucket[4]",
          "direction": "desc",
          "null-order": "nulls-last"
        },
        {
          "source-id": 22,
          "transform": "void",
          "direction": "asc",
          "null-order": "nulls-first"
        }
      ]
    }
  },
  {
    "action": "set-current-schema",
    "schema-id": 1
  }
]`),
			expected: Updates{
				NewAddSchemaUpdate(iceberg.NewSchema(1,
					iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.StringType{}, Required: true},
				)),
				NewAddPartitionSpecUpdate(
					&spec, false),
				NewAddSortOrderUpdate(&sortOrder),
				NewSetCurrentSchemaUpdate(1),
			},
			expectedErr: false,
		},
		{
			name:        "should handle an empty list",
			data:        []byte(`[]`),
			expected:    nil,
			expectedErr: false,
		},
		{
			name:        "should error on unknown action",
			data:        []byte(`[{"action": "unknown-action"}]`),
			expected:    nil,
			expectedErr: true,
			errContains: "unknown update action: unknown-action",
		},
		{
			name:        "should error on invalid json value",
			data:        []byte(`[{"action": "assign-uuid", "uuid": "not-a-uuid"}]`),
			expected:    nil,
			expectedErr: true,
		},
		{
			name:        "should error on invalid json array syntax",
			data:        []byte(`[{"action": "set-location", "location": "loc1"},]`),
			expected:    nil,
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var actual Updates
			err := json.Unmarshal(tc.data, &actual)

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tc.expected), len(actual))
				for idx, u := range actual {
					switch u.Action() {
					case "add-schema":
						expectedAddSchema := u.(*addSchemaUpdate)
						actualAddSchema := actual[idx].(*addSchemaUpdate)
						assert.True(t, expectedAddSchema.Schema.Equals(actualAddSchema.Schema))
						assert.Equal(t, actualAddSchema.initial, expectedAddSchema.initial)
					case "add-partition-spec":
						expectedAddPartitionSpec := u.(*addPartitionSpecUpdate)
						actualAddPartitionSpec := actual[idx].(*addPartitionSpecUpdate)
						assert.True(t, expectedAddPartitionSpec.Spec.Equals(*actualAddPartitionSpec.Spec))
						assert.Equal(t, actualAddPartitionSpec.initial, expectedAddPartitionSpec.initial)
					case "add-sort-order":
						expectedAddSortOrder := u.(*addSortOrderUpdate)
						actualAddSortOrder := actual[idx].(*addSortOrderUpdate)
						assert.True(t, expectedAddSortOrder.SortOrder.Equals(*actualAddSortOrder.SortOrder))
						assert.Equal(t, actualAddSortOrder.initial, expectedAddSortOrder.initial)
					default:
						assert.Equal(t, u, actual[idx])
					}
				}
			}
		})
	}
}

// buildMetadataWithSnapshots creates table metadata with the given snapshots
// and refs for use in FilterReferencedSnapshots tests.
func buildMetadataWithSnapshots(t *testing.T, snapshots []Snapshot, refs map[string]SnapshotRef) Metadata {
	t.Helper()
	builder := builderWithoutChanges(2)
	baseTs := builder.base.LastUpdatedMillis()
	for i := range snapshots {
		snapshots[i].TimestampMs = baseTs + int64(i) + 1
		require.NoError(t, builder.AddSnapshot(&snapshots[i]))
	}
	for name, ref := range refs {
		require.NoError(t, builder.SetSnapshotRef(name, ref.SnapshotID, ref.SnapshotRefType))
	}
	meta, err := builder.Build()
	require.NoError(t, err)
	return meta
}

func TestFilterReferencedSnapshots(t *testing.T) {
	schemaID := 0
	snap := func(id int64) Snapshot {
		return Snapshot{
			SnapshotID:   id,
			ManifestList: "/snap.avro",
			Summary:      &Summary{Operation: OpAppend},
			SchemaID:     &schemaID,
		}
	}

	t.Run("skips referenced snapshots", func(t *testing.T) {
		// snap-1 referenced by feature-branch, snap-2 unreferenced, snap-3 referenced by main
		meta := buildMetadataWithSnapshots(t,
			[]Snapshot{snap(1), snap(2), snap(3)},
			map[string]SnapshotRef{
				"feature-branch": {SnapshotID: 1, SnapshotRefType: BranchRef},
				MainBranch:       {SnapshotID: 3, SnapshotRefType: BranchRef},
			},
		)

		updates := []Update{
			NewRemoveSnapshotsUpdate([]int64{1, 2}),
		}

		filtered := FilterReferencedSnapshots(updates, meta)
		require.Len(t, filtered, 1)
		rs := filtered[0].(*removeSnapshotsUpdate)
		require.Equal(t, []int64{2}, rs.SnapshotIDs)
	})

	t.Run("allows removal when ref is also removed in same batch", func(t *testing.T) {
		// tag-1 → snap-1, main → snap-2
		meta := buildMetadataWithSnapshots(t,
			[]Snapshot{snap(1), snap(2)},
			map[string]SnapshotRef{
				"tag-1":    {SnapshotID: 1, SnapshotRefType: TagRef},
				MainBranch: {SnapshotID: 2, SnapshotRefType: BranchRef},
			},
		)

		updates := []Update{
			NewRemoveSnapshotRefUpdate("tag-1"),
			NewRemoveSnapshotsUpdate([]int64{1}),
		}

		filtered := FilterReferencedSnapshots(updates, meta)
		// RemoveSnapshotRef passes through, RemoveSnapshots keeps snap-1
		require.Len(t, filtered, 2)
		require.Equal(t, UpdateRemoveSnapshotRef, filtered[0].Action())
		rs := filtered[1].(*removeSnapshotsUpdate)
		require.Equal(t, []int64{1}, rs.SnapshotIDs)
	})

	t.Run("drops update entirely when all snapshots are referenced", func(t *testing.T) {
		meta := buildMetadataWithSnapshots(t,
			[]Snapshot{snap(1), snap(2)},
			map[string]SnapshotRef{
				"branch-a": {SnapshotID: 1, SnapshotRefType: BranchRef},
				"branch-b": {SnapshotID: 2, SnapshotRefType: BranchRef},
			},
		)

		updates := []Update{
			NewRemoveSnapshotsUpdate([]int64{1, 2}),
		}

		filtered := FilterReferencedSnapshots(updates, meta)
		require.Len(t, filtered, 0)
	})

	t.Run("passes through non-RemoveSnapshots updates unchanged", func(t *testing.T) {
		meta := buildMetadataWithSnapshots(t,
			[]Snapshot{snap(1)},
			map[string]SnapshotRef{
				MainBranch: {SnapshotID: 1, SnapshotRefType: BranchRef},
			},
		)

		updates := []Update{
			NewSetPropertiesUpdate(iceberg.Properties{"key": "value"}),
			NewRemovePropertiesUpdate([]string{"old-key"}),
		}

		filtered := FilterReferencedSnapshots(updates, meta)
		require.Len(t, filtered, 2)
		require.Equal(t, UpdateSetProperties, filtered[0].Action())
		require.Equal(t, UpdateRemoveProperties, filtered[1].Action())
	})
}
