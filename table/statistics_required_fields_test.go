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

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatisticsJSONRequiresFields(t *testing.T) {
	tests := []struct {
		name     string
		valid    string
		required []string
		newValue func() any
	}{
		{
			name:     "statistics file",
			valid:    "{\"snapshot-id\":0,\"statistics-path\":\"\",\"file-size-in-bytes\":0,\"file-footer-size-in-bytes\":0,\"blob-metadata\":[]}",
			required: []string{"snapshot-id", "statistics-path", "file-size-in-bytes", "file-footer-size-in-bytes", "blob-metadata"},
			newValue: func() any { return new(StatisticsFile) },
		},
		{
			name:     "blob metadata",
			valid:    "{\"type\":\"vendor-sketch-v1\",\"snapshot-id\":0,\"sequence-number\":0,\"fields\":[]}",
			required: []string{"type", "snapshot-id", "sequence-number", "fields"},
			newValue: func() any { return new(BlobMetadata) },
		},
		{
			name:     "partition statistics file",
			valid:    "{\"snapshot-id\":0,\"statistics-path\":\"\",\"file-size-in-bytes\":0}",
			required: []string{"snapshot-id", "statistics-path", "file-size-in-bytes"},
			newValue: func() any { return new(PartitionStatisticsFile) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, json.Unmarshal([]byte(tt.valid), tt.newValue()))

			for _, field := range tt.required {
				for _, mode := range []string{"missing", "null"} {
					t.Run(field+"/"+mode, func(t *testing.T) {
						var obj map[string]json.RawMessage
						require.NoError(t, json.Unmarshal([]byte(tt.valid), &obj))
						if mode == "missing" {
							delete(obj, field)
						} else {
							obj[field] = json.RawMessage("null")
						}

						data, err := json.Marshal(obj)
						require.NoError(t, err)
						err = json.Unmarshal(data, tt.newValue())
						require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
						assert.ErrorContains(t, err, field)
					})
				}
			}
		})
	}
}

func TestParseMetadataRejectsMissingStatisticsFields(t *testing.T) {
	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV2), &obj))
	obj["statistics"] = json.RawMessage("[{\"snapshot-id\":0,\"statistics-path\":\"s3://bucket/stats.puffin\",\"file-size-in-bytes\":100,\"file-footer-size-in-bytes\":20}]")

	data, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = ParseMetadataBytes(data)
	require.ErrorIs(t, err, ErrInvalidMetadata)
	assert.ErrorContains(t, err, "blob-metadata")
}
