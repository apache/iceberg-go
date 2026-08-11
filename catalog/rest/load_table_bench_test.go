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

package rest

import (
	stdjson "encoding/json"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// BenchmarkDecodeTableMetadata benchmarks the json decoding of LoadTable responses, comparing encoding/json with
// goccy/go-json. Because responses can get big with tables with a long snapshot history, the impact of the json
// decoding performance can be significant in higher-throughput workloads.
func BenchmarkDecodeTableMetadata(b *testing.B) {
	snapshotCounts := []struct {
		name          string
		snapshotCount int64
		decode        func(body []byte, v interface{}) error
	}{
		{
			name:          "1 snapshot, encoding/json",
			snapshotCount: 1,
			decode: func(body []byte, v interface{}) error {
				return stdjson.Unmarshal(body, v)
			},
		},
		{
			name:          "1 snapshot, goccy/go-json",
			snapshotCount: 1,
			decode: func(body []byte, v interface{}) error {
				return json.Unmarshal(body, v)
			},
		},
		{
			name:          "10 snapshots, encoding/json",
			snapshotCount: 10,
			decode: func(body []byte, v interface{}) error {
				return stdjson.Unmarshal(body, v)
			},
		},
		{
			name:          "10 snapshots, goccy/go-json",
			snapshotCount: 10,
			decode: func(body []byte, v interface{}) error {
				return json.Unmarshal(body, v)
			},
		},
		{
			name:          "100 snapshots, encoding/json",
			snapshotCount: 100,
			decode: func(body []byte, v interface{}) error {
				return stdjson.Unmarshal(body, v)
			},
		},
		{
			name:          "100 snapshots, goccy/go-json",
			snapshotCount: 100,
			decode: func(body []byte, v interface{}) error {
				return json.Unmarshal(body, v)
			},
		},
		{
			name:          "1000 snapshots, encoding/json",
			snapshotCount: 1000,
			decode: func(body []byte, v interface{}) error {
				return stdjson.Unmarshal(body, v)
			},
		},
		{
			name:          "1000 snapshots, goccy/go-json",
			snapshotCount: 1000,
			decode: func(body []byte, v interface{}) error {
				return json.Unmarshal(body, v)
			},
		},
		{
			name:          "10000 snapshots, encoding/json",
			snapshotCount: 10000,
			decode: func(body []byte, v interface{}) error {
				return stdjson.Unmarshal(body, v)
			},
		},
		{
			name:          "10000 snapshots, goccy/go-json",
			snapshotCount: 10000,
			decode: func(body []byte, v interface{}) error {
				return json.Unmarshal(body, v)
			},
		},
	}

	for _, tc := range snapshotCounts {
		b.Run(tc.name, func(b *testing.B) {
			body := makeTableResponseWithSnapshots(tc.snapshotCount)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var resp loadTableResponse
				err := tc.decode(body, &resp)
				require.NoError(b, err)
			}
		})
	}
}

// makeTableResponseWithSnapshots formats a LoadTable json body with a specified number of snapshots. Snapshots are used
// here to exercise different response size profiles as it's one of the common sources for large responses.
func makeTableResponseWithSnapshots(snapshotCount int64) []byte {
	snapshots := make([]table.Snapshot, 0, snapshotCount)
	snapshotLogEntries := make([]table.SnapshotLogEntry, 0, snapshotCount)
	schemaID := 0
	var snapshotTimestamp int64
	var snapshotID int64
	for i := int64(0); i < snapshotCount; i++ {
		var parentID *int64
		if i > 0 {
			parentID = &i
		}
		snapshotTimestamp = 1785448901408 + i
		snapshotID = i

		snapshots = append(snapshots, table.Snapshot{
			SnapshotID:       snapshotID,
			ParentSnapshotID: parentID,
			SequenceNumber:   i,
			TimestampMs:      snapshotTimestamp,
			ManifestList:     fmt.Sprintf("s3://warehouse/database/table/metadata/snap-%s.avro", uuid.NewString()),
			Summary: &table.Summary{
				Operation: "append",
				Properties: map[string]string{
					"spark.app.id":            "local-1646787004168",
					"added-data-files":        "1",
					"added-records":           "1",
					"added-files-size":        "697",
					"changed-partition-count": "1",
					"total-records":           "1",
					"total-files-size":        "697",
					"total-data-files":        "1",
					"total-delete-files":      "0",
					"total-position-deletes":  "0",
					"total-equality-deletes":  "0",
				},
			},
			SchemaID: &schemaID,
		})
		snapshotLogEntries = append(snapshotLogEntries, table.SnapshotLogEntry{
			SnapshotID:  i,
			TimestampMs: snapshotTimestamp,
		})
	}
	snapshotsJson, err := json.Marshal(snapshots)
	if err != nil {
		panic(fmt.Errorf("failed to generate load table response: %w", err))
	}

	snapshotsLogEntriesJson, err := json.Marshal(snapshotLogEntries)
	if err != nil {
		panic(fmt.Errorf("failed to generate load table response: %w", err))
	}

	return []byte(fmt.Sprintf(`{
			"metadata-location": "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
			"metadata": {
				"format-version": 1,
				"table-uuid": "b55d9dda-6561-423a-8bfc-787980ce421f",
				"location": "s3://warehouse/database/table",
				"last-updated-ms": %d,
				"last-column-id": 2,
				"schema": {
					"type": "struct",
					"schema-id": 0,
					"fields": [
						{"id": 1, "name": "id", "required": false, "type": "int"},
						{"id": 2, "name": "data", "required": false, "type": "string"}
					]
				},
				"current-schema-id": 0,
				"schemas": [
					{
						"type": "struct",
						"schema-id": 0,
						"fields": [
							{"id": 1, "name": "id", "required": false, "type": "int"},
							{"id": 2, "name": "data", "required": false, "type": "string"}
						]
					}
				],
				"partition-spec": [],
				"default-spec-id": 0,
				"partition-specs": [{"spec-id": 0, "fields": []}],
				"last-partition-id": 999,
				"default-sort-order-id": 0,
				"sort-orders": [{"order-id": 0, "fields": []}],
				"properties": {"owner": "bryan", "write.metadata.compression-codec": "gzip"},
				"current-snapshot-id": %d,
				"refs": {"main": {"snapshot-id": %d, "type": "branch"}},
				"snapshots": %s,
				"snapshot-log": %s,
				"metadata-log": [
					{
						"timestamp-ms": 1646787031514,
						"metadata-file": "s3://warehouse/database/table/metadata/00000-88484a1c-00e5-4a07-a787-c0e7aeffa805.gz.metadata.json"
					}
				]
			}
		}`, snapshotTimestamp, snapshotID, snapshotID, snapshotsJson, snapshotsLogEntriesJson))
}
