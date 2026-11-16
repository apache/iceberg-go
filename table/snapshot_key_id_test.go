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

	"github.com/stretchr/testify/require"
)

func metadataWithSnapshotKeyIDs(t *testing.T) []byte {
	t.Helper()

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV3), &fields))
	fields["refs"] = json.RawMessage("{}")

	var snapshots []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(fields["snapshots"], &snapshots))
	require.Len(t, snapshots, 2)
	snapshots[0]["key-id"] = json.RawMessage("\"historical-key\"")
	snapshots[1]["key-id"] = json.RawMessage("\"current-key\"")

	var err error
	fields["snapshots"], err = json.Marshal(snapshots)
	require.NoError(t, err)
	data, err := json.Marshal(fields)
	require.NoError(t, err)

	return data
}

func TestSnapshotKeyIDJSONRoundTrip(t *testing.T) {
	keyID := "manifest-key-123"
	snapshot := Snapshot{
		SnapshotID:     42,
		SequenceNumber: 5,
		TimestampMs:    1234,
		ManifestList:   "s3://bucket/manifest-list.avro",
		KeyID:          &keyID,
	}

	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	require.Equal(t, "\"manifest-key-123\"", string(fields["key-id"]))

	var decoded Snapshot
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.NotNil(t, decoded.KeyID)
	require.Equal(t, keyID, *decoded.KeyID)
	require.True(t, decoded.Equals(snapshot))

	withoutKeyID := decoded
	withoutKeyID.KeyID = nil
	require.False(t, decoded.Equals(withoutKeyID))
	require.False(t, withoutKeyID.Equals(decoded))

	encoded, err = json.Marshal(withoutKeyID)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "\"key-id\"")
	var legacy Snapshot
	require.NoError(t, json.Unmarshal(encoded, &legacy))
	require.Nil(t, legacy.KeyID)
}

func TestSnapshotKeyIDIsDefensivelyCloned(t *testing.T) {
	keyID := "original-key"
	source := Snapshot{SnapshotID: 1, TimestampMs: 2, KeyID: &keyID}
	clone := cloneSnapshot(source)
	require.NotNil(t, clone.KeyID)
	require.Equal(t, keyID, *clone.KeyID)
	*clone.KeyID = "changed-key"

	require.Equal(t, "original-key", *source.KeyID)
	require.False(t, source.Equals(clone))
}

func TestSnapshotKeyIDTableMetadataRoundTrip(t *testing.T) {
	const historicalID int64 = 3051729675574597004
	data := metadataWithSnapshotKeyIDs(t)
	decoders := []struct {
		name   string
		decode func([]byte) (Metadata, error)
	}{
		{"eager", ParseMetadataBytes},
		{"deferred", ParseMetadataBytesDeferredSnapshots},
	}

	for _, decoder := range decoders {
		t.Run(decoder.name, func(t *testing.T) {
			metadata, err := decoder.decode(data)
			require.NoError(t, err)

			current := metadata.CurrentSnapshot()
			require.NotNil(t, current)
			require.NotNil(t, current.KeyID)
			require.Equal(t, "current-key", *current.KeyID)

			historical := metadata.SnapshotByID(historicalID)
			require.NotNil(t, historical)
			require.NotNil(t, historical.KeyID)
			require.Equal(t, "historical-key", *historical.KeyID)

			encoded, err := json.Marshal(metadata)
			require.NoError(t, err)
			roundTrip, err := ParseMetadataBytes(encoded)
			require.NoError(t, err)
			require.Equal(t, "current-key", *roundTrip.CurrentSnapshot().KeyID)
			require.Equal(t, "historical-key", *roundTrip.SnapshotByID(historicalID).KeyID)
		})
	}
}

func TestSnapshotKeyIDInvalidTypeRejectedByDeferredDecoder(t *testing.T) {
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadataWithSnapshotKeyIDs(t), &fields))

	var snapshots []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(fields["snapshots"], &snapshots))
	snapshots[0]["key-id"] = json.RawMessage("123")

	var err error
	fields["snapshots"], err = json.Marshal(snapshots)
	require.NoError(t, err)
	data, err := json.Marshal(fields)
	require.NoError(t, err)

	_, err = ParseMetadataBytes(data)
	require.Error(t, err)
	_, err = ParseMetadataBytesDeferredSnapshots(data)
	require.Error(t, err)
}
