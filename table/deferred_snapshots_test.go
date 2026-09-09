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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func metadataWithUnreferencedSnapshot(t testing.TB) []byte {
	t.Helper()

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV2), &fields))
	fields["refs"] = json.RawMessage(`{}`)

	data, err := json.Marshal(fields)
	require.NoError(t, err)

	return data
}

func metadataWithHistoricalSnapshotFields(
	t testing.TB,
	summary, manifests json.RawMessage,
) []byte {
	t.Helper()

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadataWithUnreferencedSnapshot(t), &fields))

	var snapshots []json.RawMessage
	require.NoError(t, json.Unmarshal(fields["snapshots"], &snapshots))
	var historical map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snapshots[0], &historical))
	if summary != nil {
		historical["summary"] = summary
	}
	if manifests != nil {
		historical["manifests"] = manifests
	}

	var err error
	snapshots[0], err = json.Marshal(historical)
	require.NoError(t, err)
	fields["snapshots"], err = json.Marshal(snapshots)
	require.NoError(t, err)
	data, err := json.Marshal(fields)
	require.NoError(t, err)

	return data
}

func TestParseMetadataBytesDeferredSnapshots(t *testing.T) {
	data := metadataWithUnreferencedSnapshot(t)
	eager, err := ParseMetadataBytes(data)
	require.NoError(t, err)
	require.Len(t, eager.Snapshots(), 2)
	historicalID := eager.Snapshots()[0].SnapshotID

	meta, err := ParseMetadataBytesDeferredSnapshots(data)
	require.NoError(t, err)
	common := commonMetadataOf(meta)
	require.NotNil(t, common.deferredSnapshots)
	require.Len(t, common.SnapshotList, 1)
	assert.Nil(t, common.deferredSnapshots.snapshots)

	assert.Equal(t, eager.CurrentSnapshot(), meta.CurrentSnapshot())
	assert.Equal(t, eager.SnapshotByName(MainBranch), meta.SnapshotByName(MainBranch))
	assert.Nil(t, common.deferredSnapshots.snapshots, "referenced snapshot access must remain eager")

	assert.Equal(t, eager.SnapshotByID(historicalID), meta.SnapshotByID(historicalID))
	assert.Nil(t, common.deferredSnapshots.snapshots, "historical lookup must decode only the requested snapshot")
	entry := &common.deferredSnapshots.entries[common.deferredSnapshots.byID[historicalID]]
	assert.Equal(t, historicalID, entry.snapshot.SnapshotID)

	assert.Len(t, meta.Snapshots(), 2)
	assert.Len(t, common.deferredSnapshots.snapshots, 2)
	assert.True(t, eager.Equals(meta))
}

func TestDeferredSnapshotsMaterializeForCollectionAndSerialization(t *testing.T) {
	data := metadataWithUnreferencedSnapshot(t)
	meta, err := ParseMetadataBytesDeferredSnapshots(data)
	require.NoError(t, err)
	deferredState := commonMetadataOf(meta).deferredSnapshots
	require.NotEmpty(t, deferredState.raw)

	serialized, err := json.Marshal(meta)
	require.NoError(t, err)
	assert.Nil(t, deferredState.raw)
	assert.Nil(t, deferredState.entries)
	assert.Nil(t, deferredState.byID)
	reparsed, err := ParseMetadataBytes(serialized)
	require.NoError(t, err)
	assert.Len(t, reparsed.Snapshots(), 2)
	assert.Len(t, meta.Snapshots(), 2)

	eager, err := ParseMetadataBytes(data)
	require.NoError(t, err)
	eagerBuilder, err := MetadataBuilderFromBase(eager, "")
	require.NoError(t, err)
	eagerRebuilt, err := eagerBuilder.Build()
	require.NoError(t, err)

	deferredForBuilder, err := ParseMetadataBytesDeferredSnapshots(data)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(deferredForBuilder, "")
	require.NoError(t, err)
	rebuilt, err := builder.Build()
	require.NoError(t, err)
	assert.Equal(t, eagerRebuilt.Snapshots(), rebuilt.Snapshots())
}

func TestDeferredSnapshotsV1ToV2MaterializesHistory(t *testing.T) {
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV1), &fields))
	fields["refs"] = json.RawMessage(`{}`)
	data, err := json.Marshal(fields)
	require.NoError(t, err)

	deferred, err := ParseMetadataBytesDeferredSnapshots(data)
	require.NoError(t, err)
	eager, err := ParseMetadataBytes(data)
	require.NoError(t, err)

	converted := deferred.(*metadataV1).ToV2()
	assert.Equal(t, eager.Snapshots(), converted.Snapshots())
	assert.Nil(t, converted.deferredSnapshots)
}

func TestDeferredSnapshotGettersReturnDefensiveCopies(t *testing.T) {
	meta, err := ParseMetadataBytesDeferredSnapshots(metadataWithUnreferencedSnapshot(t))
	require.NoError(t, err)
	historicalID := int64(3051729675574597004)

	historical := meta.SnapshotByID(historicalID)
	require.NotNil(t, historical)
	historical.ManifestList = "mutated"
	historical.Summary.Properties["mutated"] = "true"

	all := meta.Snapshots()
	require.Len(t, all, 2)
	all[0].ManifestList = "also-mutated"

	unchanged := meta.SnapshotByID(historicalID)
	require.NotNil(t, unchanged)
	assert.NotEqual(t, "mutated", unchanged.ManifestList)
	assert.NotEqual(t, "also-mutated", unchanged.ManifestList)
	assert.NotContains(t, unchanged.Summary.Properties, "mutated")
}

func TestDeferredSnapshotsRoundTripAllMetadataVersions(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "v1", data: ExampleTableMetadataV1},
		{name: "v2", data: ExampleTableMetadataV2},
		{name: "v3", data: ExampleTableMetadataV3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tc.data), &fields))
			fields["refs"] = json.RawMessage(`{}`)
			data, err := json.Marshal(fields)
			require.NoError(t, err)

			eager, err := ParseMetadataBytes(data)
			require.NoError(t, err)
			deferred, err := ParseMetadataBytesDeferredSnapshots(data)
			require.NoError(t, err)
			assert.True(t, eager.Equals(deferred))

			serialized, err := json.Marshal(deferred)
			require.NoError(t, err)
			reparsed, err := ParseMetadataBytes(serialized)
			require.NoError(t, err)
			assert.True(t, eager.Equals(reparsed))
		})
	}
}

func TestDeferredSnapshotsValidationMatchesEagerJSONSemantics(t *testing.T) {
	tests := []struct {
		name      string
		summary   json.RawMessage
		manifests json.RawMessage
		wantErr   error
	}{
		{
			name:    "null summary property",
			summary: json.RawMessage(`{"operation":"append","extra":null}`),
		},
		{
			name:      "null manifest location",
			manifests: json.RawMessage(`["a.avro",null]`),
		},
		{
			name:    "duplicate operation uses last value",
			summary: json.RawMessage(`{"operation":"","operation":"append"}`),
		},
		{
			name:    "null final operation remains invalid",
			summary: json.RawMessage(`{"operation":"append","operation":null}`),
			wantErr: ErrInvalidOperation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := metadataWithHistoricalSnapshotFields(t, tt.summary, tt.manifests)
			eager, eagerErr := ParseMetadataBytes(data)
			deferred, deferredErr := ParseMetadataBytesDeferredSnapshots(data)

			if tt.wantErr != nil {
				require.ErrorIs(t, eagerErr, tt.wantErr)
				require.ErrorIs(t, deferredErr, tt.wantErr)

				return
			}

			require.NoError(t, eagerErr)
			require.NoError(t, deferredErr)
			assert.Equal(t, eager.SnapshotByID(3051729675574597004), deferred.SnapshotByID(3051729675574597004))
		})
	}
}

func TestDeferredSnapshotsMissingLastSequenceNumberErrorParity(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "v2", data: ExampleTableMetadataV2},
		{name: "v3", data: ExampleTableMetadataV3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tc.data), &fields))
			delete(fields, "last-sequence-number")
			data, err := json.Marshal(fields)
			require.NoError(t, err)

			_, eagerErr := ParseMetadataBytes(data)
			_, deferredErr := ParseMetadataBytesDeferredSnapshots(data)
			require.ErrorIs(t, eagerErr, ErrInvalidMetadata)
			require.ErrorIs(t, deferredErr, ErrInvalidMetadata)
			assert.EqualError(t, deferredErr, eagerErr.Error())
			assert.ErrorContains(t, deferredErr,
				"last-sequence-number is required for format versions greater than 1")
		})
	}
}

func TestSplitJSONArrayOffsetsReferToOriginalInput(t *testing.T) {
	raw := []byte(" \n [ {\"id\":1}, [\"comma,brace}\"] ] \t")
	spans, err := splitJSONArray(raw)
	require.NoError(t, err)
	require.Len(t, spans, 2)
	assert.Equal(t, `{"id":1}`, string(raw[spans[0].start:spans[0].end]))
	assert.Equal(t, `["comma,brace}"]`, string(raw[spans[1].start:spans[1].end]))
}

func TestSplitJSONArrayPreservesMissingNullAndEmptyDistinction(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte(" \n\t"), []byte(" \n null\t")} {
		spans, err := splitJSONArray(raw)
		require.NoError(t, err)
		assert.Nil(t, spans)
	}

	spans, err := splitJSONArray([]byte(" \n []\t"))
	require.NoError(t, err)
	assert.NotNil(t, spans)
	assert.Empty(t, spans)
}

func TestDeferredSnapshotsMarshalMetadataValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "v1", data: ExampleTableMetadataV1},
		{name: "v2", data: ExampleTableMetadataV2},
		{name: "v3", data: ExampleTableMetadataV3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tc.data), &fields))
			fields["refs"] = json.RawMessage(`{}`)
			data, err := json.Marshal(fields)
			require.NoError(t, err)

			meta, err := ParseMetadataBytesDeferredSnapshots(data)
			require.NoError(t, err)

			var serialized []byte
			switch typed := meta.(type) {
			case *metadataV1:
				serialized, err = json.Marshal(*typed)
			case *metadataV2:
				serialized, err = json.Marshal(*typed)
			case *metadataV3:
				serialized, err = json.Marshal(*typed)
			default:
				t.Fatalf("unexpected metadata type %T", meta)
			}
			require.NoError(t, err)

			reparsed, err := ParseMetadataBytes(serialized)
			require.NoError(t, err)
			assert.Len(t, reparsed.Snapshots(), len(meta.Snapshots()))
		})
	}
}

func TestDeferredSnapshotsValidateUnreferencedHistory(t *testing.T) {
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadataWithUnreferencedSnapshot(t), &fields))

	var snapshots []map[string]any
	require.NoError(t, json.Unmarshal(fields["snapshots"], &snapshots))
	snapshots[0]["summary"] = map[string]any{"operation": "append", "invalid": 1}
	fields["snapshots"], _ = json.Marshal(snapshots)
	data, err := json.Marshal(fields)
	require.NoError(t, err)

	_, err = ParseMetadataBytesDeferredSnapshots(data)
	require.ErrorIs(t, err, ErrInvalidMetadata)
}

func TestDeferredSnapshotsConcurrentMaterialization(t *testing.T) {
	meta, err := ParseMetadataBytesDeferredSnapshots(metadataWithUnreferencedSnapshot(t))
	require.NoError(t, err)
	historicalID := int64(3051729675574597004)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NotNil(t, meta.CurrentSnapshot())
			require.NotNil(t, meta.SnapshotByID(historicalID))
			require.Len(t, meta.Snapshots(), 2)
		}()
	}
	wg.Wait()
}

func TestDeferredSnapshotsConcurrentLookupAndIndexRelease(t *testing.T) {
	const (
		attempts = 100
		lookups  = 32
	)
	historicalID := int64(3051729675574597004)

	for range attempts {
		meta, err := ParseMetadataBytesDeferredSnapshots(metadataWithUnreferencedSnapshot(t))
		require.NoError(t, err)

		start := make(chan struct{})
		results := make(chan bool, lookups+1)
		for range lookups {
			go func() {
				<-start
				snapshot := meta.SnapshotByID(historicalID)
				results <- snapshot != nil && snapshot.SnapshotID == historicalID
			}()
		}
		go func() {
			<-start
			results <- len(meta.Snapshots()) == 2
		}()
		close(start)

		for range lookups + 1 {
			assert.True(t, <-results)
		}

		state := commonMetadataOf(meta).deferredSnapshots
		assert.Nil(t, state.raw)
		assert.Nil(t, state.entries)
		assert.Nil(t, state.byID)
	}
}

func TestDeferredSnapshotsConcurrentSingleLookupDoesNotMaterializeHistory(t *testing.T) {
	meta, err := ParseMetadataBytesDeferredSnapshots(metadataWithUnreferencedSnapshot(t))
	require.NoError(t, err)
	historicalID := int64(3051729675574597004)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Equal(t, historicalID, meta.SnapshotByID(historicalID).SnapshotID)
		}()
	}
	wg.Wait()

	common := commonMetadataOf(meta)
	assert.Nil(t, common.deferredSnapshots.snapshots)
	entry := &common.deferredSnapshots.entries[common.deferredSnapshots.byID[historicalID]]
	assert.Equal(t, historicalID, entry.snapshot.SnapshotID)
}
