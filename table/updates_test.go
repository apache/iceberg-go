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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFSF(io iceio.IO) FSysF {
	return func(context.Context) (iceio.IO, error) { return io, nil }
}

// trackingCallsIO wraps trackingIO to count Open and Remove calls per path.
type trackingCallsIO struct {
	*trackingIO
	openCount   map[string]int
	removeCount map[string]int
}

func newTrackingCallsIO() *trackingCallsIO {
	return &trackingCallsIO{
		trackingIO:  newTrackingIO(),
		openCount:   make(map[string]int),
		removeCount: make(map[string]int),
	}
}

func (c *trackingCallsIO) Open(name string) (iceio.File, error) {
	c.openCount[name]++

	return c.trackingIO.Open(name)
}

func (c *trackingCallsIO) Remove(name string) error {
	c.removeCount[name]++

	return c.trackingIO.Remove(name)
}

// writeManifest writes a v2 data manifest with a single ADDED
// entry pointing at dataPath into tio.files at manifestPath, and returns a
// ManifestFile descriptor with seqNum pre-assigned so the same descriptor
// can be referenced from multiple manifest lists.
func writeManifest(t *testing.T, tio *trackingIO, snapshotID, seqNum int64, manifestPath, dataPath string) iceberg.ManifestFile {
	t.Helper()

	dataSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec()

	df, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
		nil, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df.Build()).
		SequenceNum(seqNum).
		Build()

	var buf bytes.Buffer
	_, err = iceberg.WriteManifest(manifestPath, &buf, 2, spec, dataSchema, snapshotID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	require.NoError(t, tio.WriteFile(manifestPath, buf.Bytes()))

	return iceberg.NewManifestFile(2, manifestPath, int64(buf.Len()), 0, snapshotID).
		SequenceNum(seqNum, seqNum).
		AddedFiles(1).
		AddedRows(1).
		Build()
}

// writeManifestList writes a v2 manifest list referencing the
// given manifests into tio.files at listPath.
func writeManifestList(t *testing.T, tio *trackingIO, snapshotID int64, listPath string, manifests []iceberg.ManifestFile) {
	t.Helper()

	var buf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil, &seqNum, 0, manifests))
	require.NoError(t, tio.WriteFile(listPath, buf.Bytes()))
}

// metaJSONOpts configures the metadata document built by buildMetaJSON.
type metaJSONOpts struct {
	snapshots           string
	statistics          string
	partitionStatistics string
}

// buildMetaJSON returns the minimal v2 metadata document that ParseMetadataString will accept
func buildMetaJSON(o metaJSONOpts) string {
	return fmt.Sprintf(`{
	  "format-version": 2,
	  "table-uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	  "location": "s3://bucket/table",
	  "last-sequence-number": 0,
	  "last-updated-ms": 1000,
	  "last-column-id": 1,
	  "current-schema-id": 0,
	  "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],
	  "default-spec-id": 0,
	  "partition-specs": [{"spec-id":0,"fields":[]}],
	  "last-partition-id": 0,
	  "default-sort-order-id": 0,
	  "sort-orders": [{"order-id":0,"fields":[]}],
	  "snapshots": [%s],
	  "statistics": [%s],
	  "partition-statistics": [%s]
	}`, o.snapshots, o.statistics, o.partitionStatistics)
}

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
	preMeta := buildMetaJSON(metaJSONOpts{
		snapshots:           `{"snapshot-id":1,"timestamp-ms":1000}`,
		statistics:          `{"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]}`,
		partitionStatistics: `{"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1-part.puffin","file-size-in-bytes":50}`,
	})
	postMeta := buildMetaJSON(metaJSONOpts{})

	pre, err := ParseMetadataString(preMeta)
	require.NoError(t, err)
	post, err := ParseMetadataString(postMeta)
	require.NoError(t, err)

	tio := newTrackingIO()
	tio.files["s3://bucket/stats/snap1.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap1-part.puffin"] = []byte("puffin")

	fsF := testFSF(tio)
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
	preMeta := buildMetaJSON(metaJSONOpts{
		snapshots: `{"snapshot-id":1,"timestamp-ms":1000},{"snapshot-id":2,"timestamp-ms":2000}`,
		statistics: `{"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]},` +
			`{"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]}`,
		partitionStatistics: `{"snapshot-id":1,"statistics-path":"s3://bucket/stats/snap1-part.puffin","file-size-in-bytes":50},` +
			`{"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2-part.puffin","file-size-in-bytes":50}`,
	})
	postMeta := buildMetaJSON(metaJSONOpts{
		snapshots:           `{"snapshot-id":2,"timestamp-ms":2000}`,
		statistics:          `{"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2.puffin","file-size-in-bytes":100,"file-footer-size-in-bytes":10,"blob-metadata":[]}`,
		partitionStatistics: `{"snapshot-id":2,"statistics-path":"s3://bucket/stats/snap2-part.puffin","file-size-in-bytes":50}`,
	})

	pre, err := ParseMetadataString(preMeta)
	require.NoError(t, err)
	post, err := ParseMetadataString(postMeta)
	require.NoError(t, err)

	tio := newTrackingIO()
	tio.files["s3://bucket/stats/snap1.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap1-part.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap2.puffin"] = []byte("puffin")
	tio.files["s3://bucket/stats/snap2-part.puffin"] = []byte("puffin")

	fsF := testFSF(tio)
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

func TestRemoveSnapshotsPostCommitSharedManifestRetained(t *testing.T) {
	// Manifest M is referenced by both an expired snapshot (1) and a
	// retained snapshot (2). PostCommit must keep M and its data file
	// because a retained snapshot still references them; only snap1's
	// manifest list should be deleted.
	const (
		dataPath      = "s3://bucket/data/file-1.parquet"
		manifestPath  = "s3://bucket/meta/manifest-shared.avro"
		manifestList1 = "s3://bucket/meta/snap-1.avro"
		manifestList2 = "s3://bucket/meta/snap-2.avro"
	)

	tio := newTrackingIO()
	mf := writeManifest(t, tio, 1, 1, manifestPath, dataPath)
	writeManifestList(t, tio, 1, manifestList1, []iceberg.ManifestFile{mf})
	writeManifestList(t, tio, 2, manifestList2, []iceberg.ManifestFile{mf})
	tio.files[dataPath] = []byte("data")

	preMeta := buildMetaJSON(metaJSONOpts{
		snapshots: fmt.Sprintf(
			`{"snapshot-id":1,"timestamp-ms":1000,"manifest-list":%q},`+
				`{"snapshot-id":2,"timestamp-ms":2000,"manifest-list":%q}`,
			manifestList1, manifestList2),
	})
	postMeta := buildMetaJSON(metaJSONOpts{
		snapshots: fmt.Sprintf(`{"snapshot-id":2,"timestamp-ms":2000,"manifest-list":%q}`, manifestList2),
	})

	pre, err := ParseMetadataString(preMeta)
	require.NoError(t, err)
	post, err := ParseMetadataString(postMeta)
	require.NoError(t, err)

	fsF := testFSF(tio)
	preTable := New(Identifier{"ns", "tbl"}, pre, "metadata.json", fsF, nil)
	postTable := New(Identifier{"ns", "tbl"}, post, "metadata.json", fsF, nil)

	update := NewRemoveSnapshotsUpdate([]int64{1}, true)
	err = update.PostCommit(context.Background(), preTable, postTable)
	require.NoError(t, err)

	// snap1's manifest list must be deleted.
	assert.NotContains(t, tio.files, manifestList1)

	// The shared manifest, its data file, and the retained snapshot's
	// manifest list must all survive because snap2 still references them.
	assert.Contains(t, tio.files, manifestPath)
	assert.Contains(t, tio.files, dataPath)
	assert.Contains(t, tio.files, manifestList2)
}

func TestRemoveSnapshotsPostCommitSharedManifestExpiredOnce(t *testing.T) {
	// Manifest M is referenced by two expired snapshots (1 and 2) and no
	// retained snapshot. PostCommit must delete M and its data file, and
	// must open M exactly once across the two expired snapshots — the
	// dedup invariant the PR is built on. Open count on the shared
	// manifest is the perf signal: a regression that drops the dedup
	// would open it twice.
	const (
		dataPath      = "s3://bucket/data/file-1.parquet"
		manifestPath  = "s3://bucket/meta/manifest-shared.avro"
		manifestList1 = "s3://bucket/meta/snap-1.avro"
		manifestList2 = "s3://bucket/meta/snap-2.avro"
	)

	tio := newTrackingCallsIO()
	mf := writeManifest(t, tio.trackingIO, 1, 1, manifestPath, dataPath)
	writeManifestList(t, tio.trackingIO, 1, manifestList1, []iceberg.ManifestFile{mf})
	writeManifestList(t, tio.trackingIO, 2, manifestList2, []iceberg.ManifestFile{mf})
	tio.files[dataPath] = []byte("data")

	preMeta := buildMetaJSON(metaJSONOpts{
		snapshots: fmt.Sprintf(
			`{"snapshot-id":1,"timestamp-ms":1000,"manifest-list":%q},`+
				`{"snapshot-id":2,"timestamp-ms":2000,"manifest-list":%q}`,
			manifestList1, manifestList2),
	})
	postMeta := buildMetaJSON(metaJSONOpts{})

	pre, err := ParseMetadataString(preMeta)
	require.NoError(t, err)
	post, err := ParseMetadataString(postMeta)
	require.NoError(t, err)

	fsF := testFSF(tio)
	preTable := New(Identifier{"ns", "tbl"}, pre, "metadata.json", fsF, nil)
	postTable := New(Identifier{"ns", "tbl"}, post, "metadata.json", fsF, nil)

	update := NewRemoveSnapshotsUpdate([]int64{1, 2}, true)
	err = update.PostCommit(context.Background(), preTable, postTable)
	require.NoError(t, err)

	// Both manifest lists, the shared manifest, and its data file must
	// all be deleted.
	assert.NotContains(t, tio.files, manifestList1)
	assert.NotContains(t, tio.files, manifestList2)
	assert.NotContains(t, tio.files, manifestPath)
	assert.NotContains(t, tio.files, dataPath)

	// The shared manifest must be opened exactly once: snap1's pass
	// records it in visitedManifests, snap2's pass must skip it.
	assert.Equal(t, 1, tio.openCount[manifestPath],
		"shared manifest must be read only once across expired snapshots")

	// Each file must be deleted exactly once.
	assert.Equal(t, 1, tio.removeCount[manifestPath],
		"shared manifest must be deleted exactly once")
	assert.Equal(t, 1, tio.removeCount[dataPath],
		"data file must be deleted exactly once")
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

func TestUnmarshalUpdatesReplacesExistingSlice(t *testing.T) {
	var updates Updates
	require.NoError(t, json.Unmarshal([]byte(`[
		{"action": "set-location", "location": "s3://bucket/old-location"}
	]`), &updates))

	require.NoError(t, json.Unmarshal([]byte(`[
		{"action": "set-properties", "updates": {"key": "value"}}
	]`), &updates))
	require.Len(t, updates, 1)
	assert.Equal(t, UpdateSetProperties, updates[0].Action())

	previous := append(Updates(nil), updates...)
	err := json.Unmarshal([]byte(`[
		{"action": "set-location", "location": "s3://bucket/new-location"},
		{"action": "unknown-action"}
	]`), &updates)
	require.Error(t, err)
	assert.Equal(t, previous, updates)

	require.NoError(t, json.Unmarshal([]byte(`[]`), &updates))
	assert.Empty(t, updates)
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

func TestAssignUUIDUpdate_ApplyRejectsNilUUID(t *testing.T) {
	b := buildFromBase(t)
	originalUUID := b.uuid

	err := NewAssignUUIDUpdate(uuid.Nil).Apply(b)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.False(t, b.HasChanges())

	meta, err := b.Build()
	require.NoError(t, err)
	require.Equal(t, originalUUID, meta.TableUUID())
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

func TestSetPartitionStatisticsUpdate_Unmarshal(t *testing.T) {
	data := []byte(`[{
		"action": "set-partition-statistics",
		"partition-statistics": {
			"snapshot-id": 42,
			"statistics-path": "s3://bucket/partition-stats.parquet",
			"file-size-in-bytes": 100
		}
	}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	u, ok := updates[0].(*setPartitionStatisticsUpdate)
	require.True(t, ok)
	assert.Equal(t, int64(42), u.PartitionStatistics.SnapshotID)
	assert.Equal(t, "s3://bucket/partition-stats.parquet", u.PartitionStatistics.StatisticsPath)
}

func TestSetPartitionStatisticsUpdate_Apply(t *testing.T) {
	b := buildFromBase(t)
	stats := PartitionStatisticsFile{
		SnapshotID:      1,
		StatisticsPath:  "s3://bucket/partition-stats.parquet",
		FileSizeInBytes: 200,
	}

	upd := NewSetPartitionStatisticsUpdate(stats)
	require.NoError(t, upd.Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	var found *PartitionStatisticsFile
	for s := range meta.PartitionStatistics() {
		sc := s
		found = &sc
	}
	require.NotNil(t, found)
	assert.Equal(t, stats.StatisticsPath, found.StatisticsPath)
}

func TestSetPartitionStatisticsUpdate_Apply_Replaces(t *testing.T) {
	b := buildFromBase(t)
	first := PartitionStatisticsFile{SnapshotID: 5, StatisticsPath: "s3://first.parquet", FileSizeInBytes: 10}
	second := PartitionStatisticsFile{SnapshotID: 5, StatisticsPath: "s3://second.parquet", FileSizeInBytes: 20}

	require.NoError(t, NewSetPartitionStatisticsUpdate(first).Apply(b))
	require.NoError(t, NewSetPartitionStatisticsUpdate(second).Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	count := 0
	var got PartitionStatisticsFile
	for s := range meta.PartitionStatistics() {
		count++
		got = s
	}
	assert.Equal(t, 1, count)
	assert.Equal(t, "s3://second.parquet", got.StatisticsPath)
}

func TestRemovePartitionStatisticsUpdate_Unmarshal(t *testing.T) {
	data := []byte(`[{"action":"remove-partition-statistics","snapshot-id":7}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	u, ok := updates[0].(*removePartitionStatisticsUpdate)
	require.True(t, ok)
	assert.Equal(t, int64(7), u.SnapshotID)
}

func TestRemovePartitionStatisticsUpdate_Apply(t *testing.T) {
	b := buildFromBase(t)
	stats := PartitionStatisticsFile{SnapshotID: 3, StatisticsPath: "s3://bucket/partition-stats.parquet", FileSizeInBytes: 50}
	require.NoError(t, NewSetPartitionStatisticsUpdate(stats).Apply(b))

	require.NoError(t, NewRemovePartitionStatisticsUpdate(3).Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	count := 0
	for range meta.PartitionStatistics() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestRemovePartitionStatisticsUpdate_Apply_NoOp(t *testing.T) {
	// Removing a partition statistics file that does not exist should not error.
	b := buildFromBase(t)
	require.NoError(t, NewRemovePartitionStatisticsUpdate(999).Apply(b))
}

func TestAddEncryptionKeyUpdate_Unmarshal(t *testing.T) {
	data := []byte(`[{
		"action": "add-encryption-key",
		"encryption-key": {"key-id": "key-1", "encrypted-key-metadata": "c2VjcmV0"}
	}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	u, ok := updates[0].(*addEncryptionKeyUpdate)
	require.True(t, ok)
	assert.Equal(t, "key-1", u.EncryptionKey.KeyID)
	assert.Equal(t, "c2VjcmV0", u.EncryptionKey.EncryptedKeyMetadata)
}

// baseMetaV3JSON is a minimal valid V3 metadata document used by encryption key tests.
const baseMetaV3JSON = `{
  "format-version": 3,
  "table-uuid": "cccccccc-cccc-cccc-cccc-cccccccccccc",
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
  "metadata-log": [],
  "next-row-id": 0
}`

func buildFromBaseV3(t *testing.T) *MetadataBuilder {
	t.Helper()
	meta, err := ParseMetadataString(baseMetaV3JSON)
	require.NoError(t, err)
	b, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	return b
}

func TestAddEncryptionKeyUpdate_Apply(t *testing.T) {
	b := buildFromBaseV3(t)
	key := EncryptionKey{KeyID: "my-key", EncryptedKeyMetadata: "dGVzdA=="}

	require.NoError(t, NewAddEncryptionKeyUpdate(key).Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	found := false
	for k := range meta.EncryptionKeys() {
		if k.KeyID == "my-key" {
			found = true
			assert.True(t, key.Equals(k))
		}
	}
	assert.True(t, found)
}

func TestAddEncryptionKeyUpdate_Apply_RejectsV2(t *testing.T) {
	b := buildFromBase(t) // V2 base
	key := EncryptionKey{KeyID: "my-key", EncryptedKeyMetadata: "dGVzdA=="}

	err := NewAddEncryptionKeyUpdate(key).Apply(b)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format version 3")
}

func TestAddEncryptionKeyUpdate_Apply_RejectsMissingKeyID(t *testing.T) {
	b := buildFromBaseV3(t)
	key := EncryptionKey{KeyID: "", EncryptedKeyMetadata: "dGVzdA=="}

	err := NewAddEncryptionKeyUpdate(key).Apply(b)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Contains(t, err.Error(), "key-id")
}

func TestAddEncryptionKeyUpdate_Apply_RejectsMissingEncryptedKeyMetadata(t *testing.T) {
	b := buildFromBaseV3(t)
	key := EncryptionKey{KeyID: "my-key", EncryptedKeyMetadata: ""}

	err := NewAddEncryptionKeyUpdate(key).Apply(b)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Contains(t, err.Error(), "metadata")
}

func TestMetadataBuilderAddEncryptionKeyRejectsMissingKeyID(t *testing.T) {
	b := buildFromBaseV3(t)
	err := b.AddEncryptionKey(EncryptionKey{EncryptedKeyMetadata: "dGVzdA=="})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Contains(t, err.Error(), "key-id")
}

func TestAddEncryptionKeyUpdate_Apply_RejectsInvalidEncryptedKeyMetadata(t *testing.T) {
	b := buildFromBaseV3(t)
	key := EncryptionKey{KeyID: "my-key", EncryptedKeyMetadata: "not-base64"}

	err := NewAddEncryptionKeyUpdate(key).Apply(b)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Contains(t, err.Error(), "base64")
}

func TestAddEncryptionKeyUpdate_Apply_RejectsPaddedKeyID(t *testing.T) {
	b := buildFromBaseV3(t)
	key := EncryptionKey{KeyID: " my-key ", EncryptedKeyMetadata: "dGVzdA=="}

	err := NewAddEncryptionKeyUpdate(key).Apply(b)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Contains(t, err.Error(), "key-id")
}

func TestAddEncryptionKeyUpdate_UnmarshalMissingFields_ApplyRejects(t *testing.T) {
	data := []byte(`[{"action":"add-encryption-key","encryption-key":{"key-id":"my-key"}}]`)

	var updates Updates
	require.NoError(t, json.Unmarshal(data, &updates))
	require.Len(t, updates, 1)

	err := updates[0].Apply(buildFromBaseV3(t))
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Contains(t, err.Error(), "metadata")
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
	b := buildFromBaseV3(t)
	key := EncryptionKey{KeyID: "key-to-remove", EncryptedKeyMetadata: "dGVzdA=="}
	require.NoError(t, NewAddEncryptionKeyUpdate(key).Apply(b))

	require.NoError(t, NewRemoveEncryptionKeyUpdate("key-to-remove").Apply(b))

	meta, err := b.Build()
	require.NoError(t, err)

	for k := range meta.EncryptionKeys() {
		assert.NotEqual(t, "key-to-remove", k.KeyID)
	}
}

func TestRemoveEncryptionKeyUpdate_Apply_NoOp(t *testing.T) {
	// Removing a key that doesn't exist should not error.
	b := buildFromBase(t)
	require.NoError(t, NewRemoveEncryptionKeyUpdate("nonexistent").Apply(b))
}
