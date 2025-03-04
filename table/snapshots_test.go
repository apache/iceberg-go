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
