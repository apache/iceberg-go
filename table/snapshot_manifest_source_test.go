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
	"os"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotUnmarshalRequiresManifestSource(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"missing both", `{"snapshot-id": 25, "timestamp-ms": 1602638573590}`},
		{"null manifest list", `{"snapshot-id": 25, "timestamp-ms": 1602638573590, "manifest-list": null}`},
		{"empty manifest list", `{"snapshot-id": 25, "timestamp-ms": 1602638573590, "manifest-list": ""}`},
		{"null embedded manifests", `{"snapshot-id": 25, "timestamp-ms": 1602638573590, "manifests": null}`},
		{"both null", `{"snapshot-id": 25, "timestamp-ms": 1602638573590, "manifest-list": null, "manifests": null}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := table.Snapshot{
				SnapshotID:   9,
				TimestampMs:  10,
				ManifestList: "s3://bucket/existing.avro",
			}
			snapshot := original

			err := json.Unmarshal([]byte(tt.data), &snapshot)
			require.ErrorIs(t, err, table.ErrInvalidMetadata)
			assert.ErrorContains(t, err, "manifest-list or manifests")
			assert.Equal(t, original, snapshot, "failed decode must preserve the receiver")
		})
	}
}

func TestParseMetadataRejectsSnapshotWithoutManifestSource(t *testing.T) {
	raw, err := os.ReadFile("testdata/TableMetadataV2Valid.json")
	require.NoError(t, err)

	var metadata map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &metadata))

	var snapshots []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadata["snapshots"], &snapshots))
	require.NotEmpty(t, snapshots)
	delete(snapshots[0], "manifest-list")
	delete(snapshots[0], "manifests")

	metadata["snapshots"], err = json.Marshal(snapshots)
	require.NoError(t, err)
	mutated, err := json.Marshal(metadata)
	require.NoError(t, err)

	_, err = table.ParseMetadataBytes(mutated)
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
	assert.ErrorContains(t, err, "manifest-list or manifests")
}
