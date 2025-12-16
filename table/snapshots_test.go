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
	"encoding/json"
	"testing"

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

func TestMissingOperation(t *testing.T) {
	var summary table.Summary
	err := json.Unmarshal([]byte(`{"foo": "bar"}`), &summary)
	assert.ErrorIs(t, err, table.ErrMissingOperation)
}

func TestInvalidOperation(t *testing.T) {
	var summary table.Summary
	err := json.Unmarshal([]byte(`{"operation": "foobar"}`), &summary)
	assert.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.ErrorContains(t, err, "found 'foobar'")
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
