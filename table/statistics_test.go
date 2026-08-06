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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataPreservesUnknownStatisticsBlobType(t *testing.T) {
	meta, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)

	meta.(*metadataV2).StatisticsList = []StatisticsFile{{
		SnapshotID:     3055729675574597004,
		StatisticsPath: "s3://bucket/stats/vendor.puffin",
		BlobMetadata: []BlobMetadata{{
			Type:           BlobType("vendor-sketch-v1"),
			SnapshotID:     3055729675574597004,
			SequenceNumber: 2,
			Fields:         []int32{1},
			Properties:     map[string]string{"source": "vendor"},
		}},
	}}

	data, err := json.Marshal(meta)
	require.NoError(t, err)
	decoded, err := ParseMetadataBytes(data)
	require.NoError(t, err)

	stats := make([]StatisticsFile, 0, 1)
	for stat := range decoded.Statistics() {
		stats = append(stats, stat)
	}
	require.Len(t, stats, 1)
	require.Len(t, stats[0].BlobMetadata, 1)
	assert.Equal(t, BlobType("vendor-sketch-v1"), stats[0].BlobMetadata[0].Type)
	assert.False(t, stats[0].BlobMetadata[0].Type.IsKnown())
	assert.False(t, stats[0].BlobMetadata[0].Type.IsValid())
}

func TestBlobTypeRejectsNullAndEmptyValues(t *testing.T) {
	for _, value := range []string{"null", `""`} {
		t.Run(value, func(t *testing.T) {
			var blob BlobMetadata
			err := json.Unmarshal([]byte(`{"type":`+value+`}`), &blob)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid blob type")
		})
	}
}
