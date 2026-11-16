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
	"github.com/stretchr/testify/require"
)

func TestMetadataLogEntryRequiredFields(t *testing.T) {
	for _, tc := range []struct {
		name, json, missing string
	}{
		{"missing file", `{"timestamp-ms":123}`, "metadata-file"},
		{"null file", `{"metadata-file":null,"timestamp-ms":123}`, "metadata-file"},
		{"missing timestamp", `{"metadata-file":"metadata.json"}`, "timestamp-ms"},
		{"null timestamp", `{"metadata-file":"metadata.json","timestamp-ms":null}`, "timestamp-ms"},
		{"null entry", "null", "metadata-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := table.MetadataLogEntry{MetadataFile: "previous.json", TimestampMs: 42}
			entry := original
			err := json.Unmarshal([]byte(tc.json), &entry)
			require.ErrorIs(t, err, table.ErrInvalidMetadata)
			require.ErrorContains(t, err, tc.missing)
			require.Equal(t, original, entry)
		})
	}
}

func TestSnapshotLogEntryRequiredFields(t *testing.T) {
	for _, tc := range []struct {
		name, json, missing string
	}{
		{"missing snapshot", `{"timestamp-ms":123}`, "snapshot-id"},
		{"null snapshot", `{"snapshot-id":null,"timestamp-ms":123}`, "snapshot-id"},
		{"missing timestamp", `{"snapshot-id":12}`, "timestamp-ms"},
		{"null timestamp", `{"snapshot-id":12,"timestamp-ms":null}`, "timestamp-ms"},
		{"null entry", "null", "snapshot-id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := table.SnapshotLogEntry{SnapshotID: 20, TimestampMs: 42}
			entry := original
			err := json.Unmarshal([]byte(tc.json), &entry)
			require.ErrorIs(t, err, table.ErrInvalidMetadata)
			require.ErrorContains(t, err, tc.missing)
			require.Equal(t, original, entry)
		})
	}
}

func TestTableLogEntriesAcceptExplicitZeroValues(t *testing.T) {
	var metadataEntry table.MetadataLogEntry
	require.NoError(t, json.Unmarshal([]byte(`{"metadata-file":"","timestamp-ms":0}`), &metadataEntry))
	require.Equal(t, table.MetadataLogEntry{MetadataFile: "", TimestampMs: 0}, metadataEntry)

	var snapshotEntry table.SnapshotLogEntry
	require.NoError(t, json.Unmarshal([]byte(`{"snapshot-id":0,"timestamp-ms":0}`), &snapshotEntry))
	require.Equal(t, table.SnapshotLogEntry{SnapshotID: 0, TimestampMs: 0}, snapshotEntry)
}

func TestTableLogEntriesRoundTrip(t *testing.T) {
	for _, entry := range []any{
		table.MetadataLogEntry{MetadataFile: "s3://bucket/metadata.json", TimestampMs: 123},
		table.SnapshotLogEntry{SnapshotID: 9, TimestampMs: 456},
	} {
		raw, err := json.Marshal(entry)
		require.NoError(t, err)
		switch entry := entry.(type) {
		case table.MetadataLogEntry:
			var decoded table.MetadataLogEntry
			require.NoError(t, json.Unmarshal(raw, &decoded))
			require.Equal(t, entry, decoded)
		case table.SnapshotLogEntry:
			var decoded table.SnapshotLogEntry
			require.NoError(t, json.Unmarshal(raw, &decoded))
			require.Equal(t, entry, decoded)
		}
	}
}
