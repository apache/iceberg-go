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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMetadataBytesAssignsMissingPartitionFieldIDs(t *testing.T) {
	for _, tt := range []struct {
		name              string
		metadata          string
		partitionSpecsKey string
		wantFieldID       int
		wantLastFieldID   int
		hasLastFieldID    bool
	}{
		{
			name:              "v1 legacy partition spec",
			metadata:          ExampleTableMetadataV1,
			partitionSpecsKey: "partition-spec",
			wantFieldID:       1000,
			wantLastFieldID:   1000,
			hasLastFieldID:    true,
		},
		{
			name:              "v2 partition spec list",
			metadata:          ExampleTableMetadataV2,
			partitionSpecsKey: "partition-specs",
			wantFieldID:       1001,
			wantLastFieldID:   1001,
			hasLastFieldID:    true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var metadata map[string]any
			decoder := json.NewDecoder(strings.NewReader(tt.metadata))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&metadata))

			var partitionFields []any
			if tt.partitionSpecsKey == "partition-spec" {
				partitionFields = metadata[tt.partitionSpecsKey].([]any)
			} else {
				partitionSpecs := metadata[tt.partitionSpecsKey].([]any)
				partitionSpec := partitionSpecs[0].(map[string]any)
				partitionFields = partitionSpec["fields"].([]any)
			}
			delete(partitionFields[0].(map[string]any), "field-id")

			data, err := json.Marshal(metadata)
			require.NoError(t, err)

			parsed, err := ParseMetadataBytes(data)
			require.NoError(t, err)
			parsedSpec := parsed.PartitionSpec()
			assert.Equal(t, tt.wantFieldID, parsedSpec.Field(0).FieldID)
			if !tt.hasLastFieldID {
				assert.Nil(t, parsed.LastPartitionSpecID())
			} else {
				require.NotNil(t, parsed.LastPartitionSpecID())
				assert.Equal(t, tt.wantLastFieldID, *parsed.LastPartitionSpecID())
			}
		})
	}
}

func TestParseMetadataBytesNormalizesStaleLastPartitionID(t *testing.T) {
	data := strings.Replace(ExampleTableMetadataV2,
		`"last-partition-id": 1000`, `"last-partition-id": 999`, 1)

	parsed, err := ParseMetadataBytes([]byte(data))
	require.NoError(t, err)
	require.NotNil(t, parsed.LastPartitionSpecID())
	assert.Equal(t, 1000, *parsed.LastPartitionSpecID())
}

func TestParseMetadataBytesRejectsCaseFoldedFormatVersionCollision(t *testing.T) {
	data := strings.Replace(
		ExampleTableMetadataV2,
		`"format-version": 2,`,
		`"format-version": 1, "FORMAT-VERSION": 2,`,
		1,
	)
	data = strings.Replace(data, `"last-sequence-number": 34,`, "", 1)

	_, err := ParseMetadataBytes([]byte(data))
	require.ErrorIs(t, err, ErrInvalidMetadataFormatVersion)
}
