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

package table_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Snapshot() table.Snapshot {
	parentID := int64(19)
	manifest, schemaid := "s3:/a/b/c.avro", 3

	return table.Snapshot{
		SnapshotID:       25,
		ParentSnapshotID: &parentID,
		SequenceNumber:   200,
		TimestampMs:      1602638573590,
		ManifestList:     manifest,
		SchemaID:         &schemaid,
		Summary: &table.Summary{
			Operation: table.OpAppend,
		},
	}
}

func SnapshotWithProperties() table.Snapshot {
	parentID := int64(19)
	manifest, schemaid := "s3:/a/b/c.avro", 3

	return table.Snapshot{
		SnapshotID:       25,
		ParentSnapshotID: &parentID,
		SequenceNumber:   200,
		TimestampMs:      1602638573590,
		ManifestList:     manifest,
		SchemaID:         &schemaid,
		Summary: &table.Summary{
			Operation:  table.OpAppend,
			Properties: map[string]string{"foo": "bar"},
		},
	}
}

func TestSerializeSnapshot(t *testing.T) {
	snapshot := Snapshot()
	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"snapshot-id": 25, 
		"parent-snapshot-id": 19,
		"sequence-number": 200,
		"timestamp-ms": 1602638573590,
		"manifest-list": "s3:/a/b/c.avro",
		"summary": {"operation": "append"},
		"schema-id": 3
	}`, string(data))
}

func TestSerializeSnapshotWithProps(t *testing.T) {
	snapshot := SnapshotWithProperties()
	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"snapshot-id": 25, 
		"parent-snapshot-id": 19,
		"sequence-number": 200,
		"timestamp-ms": 1602638573590,
		"manifest-list": "s3:/a/b/c.avro",
		"summary": {"operation": "append", "foo": "bar"},
		"schema-id": 3
	}`, string(data))
}

func TestSerializeSnapshotWithEmbeddedManifestLocations(t *testing.T) {
	snapshot := table.Snapshot{
		SnapshotID:        25,
		TimestampMs:       1602638573590,
		ManifestLocations: []string{"s3:/a/b/manifest-1.avro", "s3:/a/b/manifest-2.avro"},
	}

	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"snapshot-id": 25,
		"timestamp-ms": 1602638573590,
		"manifests": ["s3:/a/b/manifest-1.avro", "s3:/a/b/manifest-2.avro"]
	}`, string(data))
}

func TestSerializeSnapshotWithEmptyEmbeddedManifestLocations(t *testing.T) {
	var snapshot table.Snapshot
	require.NoError(t, json.Unmarshal([]byte(`{
		"snapshot-id": 25,
		"timestamp-ms": 1602638573590,
		"manifests": []
	}`), &snapshot))
	require.NotNil(t, snapshot.ManifestLocations)

	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"snapshot-id": 25,
		"timestamp-ms": 1602638573590,
		"manifests": []
	}`, string(data))
}

func TestDeserializeSnapshotWithEmbeddedManifestLocations(t *testing.T) {
	paths := []string{"mem://bucket/manifest-1.avro", "mem://bucket/manifest-2.avro"}
	var snapshot table.Snapshot
	err := json.Unmarshal([]byte(`{
		"snapshot-id": 25,
		"timestamp-ms": 1602638573590,
		"manifests": ["mem://bucket/manifest-1.avro", "mem://bucket/manifest-2.avro"]
	}`), &snapshot)
	require.NoError(t, err)

	assert.Equal(t, paths, snapshot.ManifestLocations)
	fs := iceio.NewMemFS()
	require.NoError(t, fs.WriteFile(paths[0], []byte("manifest-one")))
	require.NoError(t, fs.WriteFile(paths[1], []byte("manifest-two-longer")))

	manifests, err := snapshot.Manifests(fs)
	require.NoError(t, err)
	require.Len(t, manifests, 2)
	assert.Equal(t, paths[0], manifests[0].FilePath())
	assert.Equal(t, paths[1], manifests[1].FilePath())
	assert.Equal(t, 1, manifests[0].Version())
	assert.Equal(t, int32(0), manifests[0].PartitionSpecID())
	assert.Equal(t, int64(25), manifests[0].SnapshotID())
	assert.Equal(t, int32(-1), manifests[0].AddedDataFiles())
	assert.Equal(t, int64(len("manifest-one")), manifests[0].Length())
	assert.Equal(t, int64(len("manifest-two-longer")), manifests[1].Length())

	var manifestList bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(1, &manifestList, snapshot.SnapshotID, nil, nil, 0, manifests))
	writtenManifests, err := iceberg.ReadManifestList(bytes.NewReader(manifestList.Bytes()))
	require.NoError(t, err)
	require.Len(t, writtenManifests, 2)
	assert.Equal(t, manifests[0].Length(), writtenManifests[0].Length())
	assert.Equal(t, manifests[1].Length(), writtenManifests[1].Length())
}

func TestSnapshotUnmarshalEmbeddedManifestsReplacesManifestList(t *testing.T) {
	snapshot := table.Snapshot{ManifestList: "old-manifest-list.avro"}

	err := json.Unmarshal([]byte(`{
		"snapshot-id": 25,
		"timestamp-ms": 1602638573590,
		"manifests": ["new-manifest.avro"]
	}`), &snapshot)
	require.NoError(t, err)
	assert.Empty(t, snapshot.ManifestList)
	assert.Equal(t, []string{"new-manifest.avro"}, snapshot.ManifestLocations)
}

func TestSnapshotUnmarshalManifestListReplacesEmbeddedManifests(t *testing.T) {
	snapshot := table.Snapshot{ManifestLocations: []string{"old-manifest.avro"}}

	err := json.Unmarshal([]byte(`{
		"snapshot-id": 25,
		"sequence-number": 1,
		"timestamp-ms": 1602638573590,
		"manifest-list": "new-manifest-list.avro"
	}`), &snapshot)
	require.NoError(t, err)
	assert.Equal(t, "new-manifest-list.avro", snapshot.ManifestList)
	assert.Nil(t, snapshot.ManifestLocations)
}

func TestSnapshotUnmarshalPrefersManifestListOverEmbeddedManifests(t *testing.T) {
	var snapshot table.Snapshot

	err := json.Unmarshal([]byte(`{
		"snapshot-id": 25,
		"timestamp-ms": 1602638573590,
		"manifest-list": "new-manifest-list.avro",
		"manifests": ["old-manifest.avro"]
	}`), &snapshot)
	require.NoError(t, err)
	assert.Equal(t, "new-manifest-list.avro", snapshot.ManifestList)
	assert.Nil(t, snapshot.ManifestLocations)
}

func TestSerializeSnapshotPrefersManifestListOverEmbeddedManifestLocations(t *testing.T) {
	snapshot := table.Snapshot{
		SnapshotID:        25,
		TimestampMs:       1602638573590,
		ManifestList:      "s3:/a/b/manifest-list.avro",
		ManifestLocations: []string{"s3:/a/b/manifest.avro"},
	}

	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"snapshot-id": 25,
		"sequence-number": 0,
		"timestamp-ms": 1602638573590,
		"manifest-list": "s3:/a/b/manifest-list.avro"
	}`, string(data))
}

func TestMissingOperationDefaultsToOverwrite(t *testing.T) {
	var summary table.Summary
	err := json.Unmarshal([]byte(`{"foo": "bar"}`), &summary)
	require.NoError(t, err)
	assert.Equal(t, table.OpOverwrite, summary.Operation)
	assert.Equal(t, iceberg.Properties{"foo": "bar"}, summary.Properties)
}

func TestEmptySummary(t *testing.T) {
	var summary table.Summary
	require.NoError(t, json.Unmarshal([]byte(`{}`), &summary))
	assert.Empty(t, summary.Operation)
	assert.Empty(t, summary.Properties)
}

func TestUnknownOperationIsPreserved(t *testing.T) {
	var summary table.Summary
	require.NoError(t, json.Unmarshal([]byte(`{"operation":"merge","foo":"bar"}`), &summary))
	assert.Equal(t, table.Operation("merge"), summary.Operation)
	assert.Equal(t, iceberg.Properties{"foo": "bar"}, summary.Properties)

	encoded, err := json.Marshal(&summary)
	require.NoError(t, err)
	assert.JSONEq(t, `{"operation":"merge","foo":"bar"}`, string(encoded))
}

func TestEmptyOperationIsInvalid(t *testing.T) {
	var summary table.Summary
	err := json.Unmarshal([]byte(`{"operation":""}`), &summary)
	assert.ErrorIs(t, err, table.ErrInvalidOperation)
}

func TestNullOperationIsInvalid(t *testing.T) {
	var summary table.Summary
	err := json.Unmarshal([]byte(`{"operation":null}`), &summary)
	assert.ErrorIs(t, err, table.ErrInvalidOperation)
}

func TestSummaryEqualsHandlesNil(t *testing.T) {
	var nilSummary *table.Summary
	summary := &table.Summary{Operation: table.OpAppend}

	assert.True(t, nilSummary.Equals(nil))
	assert.False(t, nilSummary.Equals(summary))
	assert.False(t, summary.Equals(nilSummary))
}

func TestSnapshotEqualsHandlesMissingSummary(t *testing.T) {
	withSummary := Snapshot()
	withoutSummary := withSummary
	withoutSummary.Summary = nil

	assert.False(t, withoutSummary.Equals(withSummary))
	assert.False(t, withSummary.Equals(withoutSummary))
}

func TestSnapshotString(t *testing.T) {
	snapshot := Snapshot()
	assert.Equal(t, `append: id=25, parent_id=19, schema_id=3, sequence_number=200, timestamp_ms=1602638573590, manifest_list=s3:/a/b/c.avro`,
		snapshot.String())

	snapshot = SnapshotWithProperties()
	assert.Equal(t, `append, {"foo":"bar"}: id=25, parent_id=19, schema_id=3, sequence_number=200, timestamp_ms=1602638573590, manifest_list=s3:/a/b/c.avro`,
		snapshot.String())
}

func TestSerializeSnapshotWithRowLineage(t *testing.T) {
	parentID := int64(19)
	manifest, schemaid := "s3:/a/b/c.avro", 3
	firstRowID := int64(0)
	addedRows := int64(100)

	snapshot := table.Snapshot{
		SnapshotID:       25,
		ParentSnapshotID: &parentID,
		SequenceNumber:   200,
		TimestampMs:      1602638573590,
		ManifestList:     manifest,
		SchemaID:         &schemaid,
		FirstRowID:       &firstRowID,
		AddedRows:        &addedRows,
		Summary: &table.Summary{
			Operation: table.OpAppend,
		},
	}

	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"snapshot-id": 25,
		"parent-snapshot-id": 19,
		"sequence-number": 200,
		"timestamp-ms": 1602638573590,
		"manifest-list": "s3:/a/b/c.avro",
		"summary": {"operation": "append"},
		"schema-id": 3,
		"first-row-id": 0,
		"added-rows": 100
	}`, string(data))
}

func TestDeserializeSnapshotWithRowLineage(t *testing.T) {
	jsonData := `{
		"snapshot-id": 25,
		"parent-snapshot-id": 19,
		"sequence-number": 200,
		"timestamp-ms": 1602638573590,
		"manifest-list": "s3:/a/b/c.avro",
		"summary": {"operation": "append"},
		"schema-id": 3,
		"first-row-id": 0,
		"added-rows": 100
	}`

	var snapshot table.Snapshot
	err := json.Unmarshal([]byte(jsonData), &snapshot)
	require.NoError(t, err)

	assert.Equal(t, int64(25), snapshot.SnapshotID)
	require.NotNil(t, snapshot.FirstRowID)
	assert.Equal(t, int64(0), *snapshot.FirstRowID)
	require.NotNil(t, snapshot.AddedRows)
	assert.Equal(t, int64(100), *snapshot.AddedRows)
}

func TestValidateRowLineage(t *testing.T) {
	tests := []struct {
		name       string
		firstRowID *int64
		addedRows  *int64
		wantErr    string
	}{
		{
			name:       "valid: both nil",
			firstRowID: nil,
			addedRows:  nil,
			wantErr:    "",
		},
		{
			name:       "valid: both set",
			firstRowID: ptr(int64(0)),
			addedRows:  ptr(int64(100)),
			wantErr:    "",
		},
		{
			name:       "valid: zero added rows",
			firstRowID: ptr(int64(30)),
			addedRows:  ptr(int64(0)),
			wantErr:    "",
		},
		{
			name:       "invalid: first-row-id set but added-rows nil",
			firstRowID: ptr(int64(0)),
			addedRows:  nil,
			wantErr:    "added-rows is required when first-row-id is set",
		},
		{
			name:       "invalid: negative added-rows",
			firstRowID: ptr(int64(0)),
			addedRows:  ptr(int64(-1)),
			wantErr:    "added-rows cannot be negative: -1",
		},
		{
			name:       "invalid: negative first-row-id",
			firstRowID: ptr(int64(-1)),
			addedRows:  ptr(int64(100)),
			wantErr:    "first-row-id cannot be negative: -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := table.Snapshot{
				SnapshotID: 1,
				FirstRowID: tt.firstRowID,
				AddedRows:  tt.addedRows,
			}

			err := snapshot.ValidateRowLineage()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, table.ErrInvalidRowLineage)
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}

func TestSnapshotUnmarshalRequiresSnapshotIDAndTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantErr string
	}{
		{
			name:    "missing snapshot-id",
			data:    `{"timestamp-ms": 1602638573590, "manifests": []}`,
			wantErr: "snapshot-id is absent or null",
		},
		{
			name:    "null snapshot-id",
			data:    `{"snapshot-id": null, "timestamp-ms": 1602638573590, "manifests": []}`,
			wantErr: "snapshot-id is absent or null",
		},
		{
			name:    "missing timestamp-ms",
			data:    `{"snapshot-id": 25, "manifests": []}`,
			wantErr: "timestamp-ms is absent or null",
		},
		{
			name:    "null timestamp-ms",
			data:    `{"snapshot-id": 25, "timestamp-ms": null, "manifests": []}`,
			wantErr: "timestamp-ms is absent or null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var snapshot table.Snapshot
			err := json.Unmarshal([]byte(tt.data), &snapshot)
			require.ErrorIs(t, err, table.ErrInvalidMetadata)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// Zero is a legal value for both fields, so presence must be tracked
// separately from the decoded value.
func TestSnapshotUnmarshalAcceptsExplicitZeroValues(t *testing.T) {
	var snapshot table.Snapshot
	require.NoError(t, json.Unmarshal([]byte(`{
		"snapshot-id": 0,
		"timestamp-ms": 0,
		"manifests": []
	}`), &snapshot))

	assert.Zero(t, snapshot.SnapshotID)
	assert.Zero(t, snapshot.TimestampMs)
	assert.NotNil(t, snapshot.ManifestLocations)
}

func TestSnapshotUnmarshalFailureLeavesSnapshotUnchanged(t *testing.T) {
	snapshot := Snapshot()
	original := Snapshot()

	err := json.Unmarshal([]byte(`{
		"timestamp-ms": 1602638573590,
		"manifest-list": "s3:/a/b/new.avro"
	}`), &snapshot)
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
	assert.True(t, snapshot.Equals(original), "expected snapshot to be untouched, got %s", snapshot)
}

// A null snapshot carries neither identity nor timestamp, so it is rejected
// rather than decoded into a zero-value snapshot. Java's SnapshotParser
// likewise refuses a null node.
func TestSnapshotUnmarshalRejectsNullDocument(t *testing.T) {
	var snapshot table.Snapshot
	err := json.Unmarshal([]byte(`null`), &snapshot)
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
	assert.ErrorContains(t, err, "snapshot-id is absent or null")
}

func TestParseMetadataRejectsSnapshotMissingRequiredFields(t *testing.T) {
	raw, err := os.ReadFile("testdata/TableMetadataV2Valid.json")
	require.NoError(t, err)

	tests := []struct {
		name    string
		mutate  func(snapshots []any)
		wantErr string
	}{
		{
			name:    "missing snapshot-id",
			mutate:  func(snapshots []any) { delete(snapshots[0].(map[string]any), "snapshot-id") },
			wantErr: "snapshot-id is absent or null",
		},
		{
			name:    "missing timestamp-ms",
			mutate:  func(snapshots []any) { delete(snapshots[0].(map[string]any), "timestamp-ms") },
			wantErr: "timestamp-ms is absent or null",
		},
		{
			name:    "null snapshot",
			mutate:  func(snapshots []any) { snapshots[0] = nil },
			wantErr: "snapshot-id is absent or null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var metadata map[string]any
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber() // Preserve snapshot IDs larger than float64 can represent exactly.
			require.NoError(t, decoder.Decode(&metadata))

			snapshots, ok := metadata["snapshots"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, snapshots)
			tt.mutate(snapshots)

			mutated, err := json.Marshal(metadata)
			require.NoError(t, err)

			_, err = table.ParseMetadataBytes(mutated)
			require.ErrorIs(t, err, table.ErrInvalidMetadata)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
