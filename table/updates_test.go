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
	update := NewRemoveSnapshotsUpdate([]int64{1, 2, 3}, false)

	// PostCommit should return nil immediately when postCommit is false,
	// without accessing the table arguments (which are nil here)
	err := update.PostCommit(context.Background(), nil, nil)
	assert.NoError(t, err)
}

func TestRemoveSnapshotsPostCommitDeletesStatisticsFiles(t *testing.T) {
	// preTable has snapshot 1 with associated statistics and partition statistics files.
	// postTable has no snapshots and no statistics (snapshot 1 has been expired).
	// PostCommit must delete the statistics paths that belonged to the expired snapshot.
	const preMeta = `{
	  "format-version": 2,
	  "table-uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	  "location": "s3://bucket/table",
	  "last-sequence-number": 1,
	  "last-updated-ms": 1000,
	  "last-column-id": 1,
	  "current-schema-id": 0,
	  "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],
	  "default-spec-id": 0,
	  "partition-specs": [{"spec-id":0,"fields":[]}],
	  "last-partition-id": 0,
	  "default-sort-order-id": 0,
	  "sort-orders": [{"order-id":0,"fields":[]}],
	  "current-snapshot-id": 1,
	  "snapshots": [{"snapshot-id":1,"timestamp-ms":1000,"sequence-number":1,"schema-id":0}],
	  "snapshot-log": [],
	  "metadata-log": [],
	  "statistics": [{"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]}],
	  "partition-statistics": [{"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1-part.puffin","file-size-in-bytes":50}]
	}`
	const postMeta = `{
	  "format-version": 2,
	  "table-uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	  "location": "s3://bucket/table",
	  "last-sequence-number": 1,
	  "last-updated-ms": 2000,
	  "last-column-id": 1,
	  "current-schema-id": 0,
	  "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],
	  "default-spec-id": 0,
	  "partition-specs": [{"spec-id":0,"fields":[]}],
	  "last-partition-id": 0,
	  "default-sort-order-id": 0,
	  "sort-orders": [{"order-id":0,"fields":[]}],
	  "snapshot-log": [],
	  "metadata-log": []
	}`

	pre, err := ParseMetadataString(preMeta)
	require.NoError(t, err)
	post, err := ParseMetadataString(postMeta)
	require.NoError(t, err)

	// Pass *trackingIO to createTestTransaction — Go converts it to iceio.IO implicitly,
	tio := newTrackingIO()
	tio.files["s3://bucket/stats/snap1.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap1-part.puffin"] = []byte("puffin")

	txn := createTestTransaction(t, tio, iceberg.NewPartitionSpec())
	fsF := txn.tbl.fsF
	preTable := New(Identifier{"ns", "tbl"}, pre, "metadata.json", fsF, nil)
	postTable := New(Identifier{"ns", "tbl"}, post, "metadata.json", fsF, nil)

	update := NewRemoveSnapshotsUpdate([]int64{1}, true)
	err = update.PostCommit(context.Background(), preTable, postTable)
	require.NoError(t, err)

	assert.NotContains(t, tio.files, "s3://bucket/stats/snap1.puffin")
	assert.NotContains(t, tio.files, "s3://bucket/stats/snap1-part.puffin")
}

func TestRemoveSnapshotsPostCommitPreservesStatisticsOfSurvivingSnapshots(t *testing.T) {
	// pre:  snapshots 1 and 2, statistics for both.
	// post: snapshot 1 expired, snapshot 2 kept with its statistics still present.
	// PostCommit must delete only snap1's statistics files; snap2's must survive.
	const preMeta = `{
	  "format-version": 2,
	  "table-uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	  "location": "s3://bucket/table",
	  "last-sequence-number": 2,
	  "last-updated-ms": 1000,
	  "last-column-id": 1,
	  "current-schema-id": 0,
	  "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],
	  "default-spec-id": 0,
	  "partition-specs": [{"spec-id":0,"fields":[]}],
	  "last-partition-id": 0,
	  "default-sort-order-id": 0,
	  "sort-orders": [{"order-id":0,"fields":[]}],
	  "current-snapshot-id": 2,
	  "snapshots": [
	    {"snapshot-id":1,"timestamp-ms":1000,"sequence-number":1,"schema-id":0},
	    {"snapshot-id":2,"timestamp-ms":2000,"sequence-number":2,"schema-id":0}
	  ],
	  "snapshot-log": [],
	  "metadata-log": [],
	  "statistics": [
	    {"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]},
	    {"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]}
	  ],
	  "partition-statistics": [
	    {"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1-part.puffin","file-size-in-bytes":50},
	    {"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2-part.puffin","file-size-in-bytes":50}
	  ]
	}`
	const postMeta = `{
	  "format-version": 2,
	  "table-uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	  "location": "s3://bucket/table",
	  "last-sequence-number": 2,
	  "last-updated-ms": 3000,
	  "last-column-id": 1,
	  "current-schema-id": 0,
	  "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],
	  "default-spec-id": 0,
	  "partition-specs": [{"spec-id":0,"fields":[]}],
	  "last-partition-id": 0,
	  "default-sort-order-id": 0,
	  "sort-orders": [{"order-id":0,"fields":[]}],
	  "current-snapshot-id": 2,
	  "snapshots": [
	    {"snapshot-id":2,"timestamp-ms":2000,"sequence-number":2,"schema-id":0}
	  ],
	  "snapshot-log": [],
	  "metadata-log": [],
	  "statistics": [
	    {"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]}
	  ],
	  "partition-statistics": [
	    {"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2-part.puffin","file-size-in-bytes":50}
	  ]
	}`

	pre, err := ParseMetadataString(preMeta)
	require.NoError(t, err)
	post, err := ParseMetadataString(postMeta)
	require.NoError(t, err)

	tio := newTrackingIO()
	tio.files["s3://bucket/stats/snap1.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap1-part.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap2.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap2-part.puffin"] = []byte("puffin")

	txn := createTestTransaction(t, tio, iceberg.NewPartitionSpec())
	fsF := txn.tbl.fsF
	preTable := New(Identifier{"ns", "tbl"}, pre, "metadata.json", fsF, nil)
	postTable := New(Identifier{"ns", "tbl"}, post, "metadata.json", fsF, nil)

	update := NewRemoveSnapshotsUpdate([]int64{1}, true)
	err = update.PostCommit(context.Background(), preTable, postTable)
	require.NoError(t, err)

	// snap1's statistics must be deleted
	assert.NotContains(t, tio.files, "s3://bucket/stats/snap1.puffin")
	assert.NotContains(t, tio.files, "s3://bucket/stats/snap1-part.puffin")

	// snap2's statistics must survive
	assert.Contains(t, tio.files, "s3://bucket/stats/snap2.puffin")
	assert.Contains(t, tio.files, "s3://bucket/stats/snap2-part.puffin")
}

func TestUnmarshalUpdates(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket",
		},
	)
	sortOrder, err := NewSortOrder(
		22,
		[]SortField{
			{SourceIDs: []int{19}, Transform: iceberg.IdentityTransform{}, NullOrder: NullsFirst, Direction: SortASC},
			{SourceIDs: []int{25}, Transform: iceberg.BucketTransform{NumBuckets: 4}, NullOrder: NullsFirst, Direction: SortDESC},
			{SourceIDs: []int{22}, Transform: iceberg.VoidTransform{}, NullOrder: NullsFirst, Direction: SortASC},
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
				NewRemoveSnapshotsUpdate([]int64{1, 2}, false),
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

// baseMetaJSON is a minimal valid V2 metadata document used by the Apply tests below.
const baseMetaJSON = `{
  "format-version": 2,
  "table-uuid": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
  "location": "s3://bucket/table",
  "last-sequence-number": 1,
  "last-updated-ms": 1000,
  "last-column-id": 1,
  "current-schema-id": 0,
  "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id":0,"fields":[]}],
  "last-partition-id": 0,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id":0,"fields":[]}],
  "snapshot-log": [],
  "metadata-log": []
}`

func buildFromBase(t *testing.T) *MetadataBuilder {
	t.Helper()
	meta, err := ParseMetadataString(baseMetaJSON)
	require.NoError(t, err)
	b, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	return b
}

func TestSetStatisticsUpdate_Unmarshal(t *testing.T) {
	data := []byte(`[{
		"action": "set-statistics",
		"snapshot-id": 42,
		"statistics": {
			"snapshot-id": 42,
			"statistics-path": "s3://bucket/stats.puffin",
			"file-size-in-bytes": 100,
			"file-footer-size-in-bytes": 10,
			"blob-metadata": [{"type":"apache-datasketches-theta-v1","snapshot-id":42,"sequence-number":1,"fields":[1]}]
		}
	}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	u, ok := updates[0].(*setStatisticsUpdate)
	require.True(t, ok)
	assert.Equal(t, int64(42), u.SnapshotID)
	assert.Equal(t, "s3://bucket/stats.puffin", u.Statistics.StatisticsPath)
}

func TestSetStatisticsUpdate_Apply(t *testing.T) {
	b := buildFromBase(t)
	stats := StatisticsFile{
		SnapshotID:            1,
		StatisticsPath:        "s3://bucket/stats.puffin",
		FileSizeInBytes:       200,
		FileFooterSizeInBytes: 20,
		BlobMetadata:          []BlobMetadata{},
	}

	upd := NewSetStatisticsUpdate(stats)
	require.NoError(t, upd.Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	var found *StatisticsFile
	for s := range meta.Statistics() {
		sc := s
		found = &sc
	}
	require.NotNil(t, found)
	assert.Equal(t, stats.StatisticsPath, found.StatisticsPath)
}

func TestSetStatisticsUpdate_Apply_Replaces(t *testing.T) {
	// Applying set-statistics twice for the same snapshot ID should replace, not append.
	b := buildFromBase(t)
	first := StatisticsFile{SnapshotID: 5, StatisticsPath: "s3://first.puffin", FileSizeInBytes: 10, BlobMetadata: []BlobMetadata{}}
	second := StatisticsFile{SnapshotID: 5, StatisticsPath: "s3://second.puffin", FileSizeInBytes: 20, BlobMetadata: []BlobMetadata{}}

	require.NoError(t, NewSetStatisticsUpdate(first).Apply(b))
	require.NoError(t, NewSetStatisticsUpdate(second).Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	count := 0
	var got StatisticsFile
	for s := range meta.Statistics() {
		count++
		got = s
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, "s3://second.puffin", got.StatisticsPath)
}

func TestRemoveStatisticsUpdate_Unmarshal(t *testing.T) {
	data := []byte(`[{"action":"remove-statistics","snapshot-id":7}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	u, ok := updates[0].(*removeStatisticsUpdate)
	require.True(t, ok)
	assert.Equal(t, int64(7), u.SnapshotID)
}

func TestRemoveStatisticsUpdate_Apply(t *testing.T) {
	b := buildFromBase(t)
	stats := StatisticsFile{SnapshotID: 3, StatisticsPath: "s3://bucket/stats.puffin", BlobMetadata: []BlobMetadata{}}
	require.NoError(t, NewSetStatisticsUpdate(stats).Apply(b))

	require.NoError(t, NewRemoveStatisticsUpdate(3).Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	count := 0
	for range meta.Statistics() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestRemoveStatisticsUpdate_Apply_NoOp(t *testing.T) {
	// Removing a statistics file that does not exist should not error.
	b := buildFromBase(t)
	require.NoError(t, NewRemoveStatisticsUpdate(999).Apply(b))
}

func TestAddEncryptionKeyUpdate_Unmarshal(t *testing.T) {
	data := []byte(`[{
		"action": "add-encryption-key",
		"encryption-key": {"key-id": "key-1", "key-metadata": "c2VjcmV0"}
	}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	u, ok := updates[0].(*addEncryptionKeyUpdate)
	require.True(t, ok)
	assert.Equal(t, "key-1", u.EncryptionKey.KeyID)
	require.NotNil(t, u.EncryptionKey.KeyMetadata)
	assert.Equal(t, "c2VjcmV0", *u.EncryptionKey.KeyMetadata)
}

func TestAddEncryptionKeyUpdate_Apply(t *testing.T) {
	b := buildFromBase(t)
	key := EncryptionKey{KeyID: "my-key", KeyMetadata: nil}

	require.NoError(t, NewAddEncryptionKeyUpdate(key).Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	found := false
	for id, k := range meta.EncryptionKeys() {
		if id == "my-key" {
			found = true
			assert.Equal(t, key, k)
		}
	}
	assert.True(t, found)
}

func TestRemoveEncryptionKeyUpdate_Unmarshal(t *testing.T) {
	data := []byte(`[{"action":"remove-encryption-key","key-id":"key-42"}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	u, ok := updates[0].(*removeEncryptionKeyUpdate)
	require.True(t, ok)
	assert.Equal(t, "key-42", u.KeyID)
}

func TestRemoveEncryptionKeyUpdate_Apply(t *testing.T) {
	b := buildFromBase(t)
	key := EncryptionKey{KeyID: "key-to-remove"}
	require.NoError(t, NewAddEncryptionKeyUpdate(key).Apply(b))

	require.NoError(t, NewRemoveEncryptionKeyUpdate("key-to-remove").Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	for id := range meta.EncryptionKeys() {
		assert.NotEqual(t, "key-to-remove", id)
	}
}

func TestRemoveEncryptionKeyUpdate_Apply_NoOp(t *testing.T) {
	// Removing a key that doesn't exist should not error.
	b := buildFromBase(t)
	require.NoError(t, NewRemoveEncryptionKeyUpdate("nonexistent").Apply(b))
}
