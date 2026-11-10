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

package puffin_test

import (
	"bytes"
	"encoding/json"
	"maps"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/require"
)

func TestReaderRejectsMissingRequiredPuffinFields(t *testing.T) {
	t.Parallel()

	validBlob := map[string]any{
		"type":            "vendor-stats-v1",
		"fields":          []int{1},
		"snapshot-id":     int64(10),
		"sequence-number": int64(3),
		"offset":          int64(4),
		"length":          int64(0),
	}

	for _, field := range []string{"type", "fields", "snapshot-id", "sequence-number", "offset", "length"} {
		for _, invalidValue := range []string{"missing", "null"} {
			t.Run(invalidValue+"/"+field, func(t *testing.T) {
				t.Parallel()

				blob := make(map[string]any, len(validBlob))
				maps.Copy(blob, validBlob)
				if invalidValue == "missing" {
					delete(blob, field)
				} else {
					blob[field] = nil
				}
				payload, err := json.Marshal(map[string]any{"blobs": []any{blob}})
				require.NoError(t, err)

				_, err = puffin.NewReader(bytes.NewReader(fileWithFooterPayload(payload)))
				require.ErrorContains(t, err, field)
			})
		}
	}

	for _, tc := range []struct {
		name    string
		payload string
	}{
		{name: "missing blobs", payload: `{}`},
		{name: "null blobs", payload: `{"blobs":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := puffin.NewReader(bytes.NewReader(fileWithFooterPayload([]byte(tc.payload))))
			require.ErrorContains(t, err, "blobs")
		})
	}
}

func TestReaderAcceptsExplicitZeroAndEmptyPuffinFields(t *testing.T) {
	payload := []byte(`{"blobs":[{"type":"vendor-stats-v1","fields":[],"snapshot-id":0,"sequence-number":0,"offset":4,"length":0}],"properties":{"source":"test"}}`)
	reader, err := puffin.NewReader(bytes.NewReader(fileWithFooterPayload(payload)))
	require.NoError(t, err)

	blobs := reader.Blobs()
	require.Len(t, blobs, 1)
	require.NotNil(t, blobs[0].Fields)
	require.Empty(t, blobs[0].Fields)
	require.Zero(t, blobs[0].SnapshotID)
	require.Zero(t, blobs[0].SequenceNumber)
	require.Zero(t, blobs[0].Length)
	require.Equal(t, "test", reader.Properties()["source"])
}
