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
	"fmt"
	"testing"
)

var metadataParseBenchmarkSink int

func BenchmarkParseMetadataBytes(b *testing.B) {
	for _, tc := range []struct {
		name               string
		schemaCount        int
		partitionSpecCount int
		snapshotCount      int
		propertyCount      int
	}{
		{name: "schemas=1/specs=1/snapshots=10", schemaCount: 1, partitionSpecCount: 1, snapshotCount: 10},
		{name: "schemas=32/specs=32/snapshots=1000", schemaCount: 32, partitionSpecCount: 32, snapshotCount: 1_000, propertyCount: 16},
		{name: "schemas=256/specs=256/snapshots=10000", schemaCount: 256, partitionSpecCount: 256, snapshotCount: 10_000, propertyCount: 64},
	} {
		b.Run(tc.name, func(b *testing.B) {
			data := benchmarkMetadataJSON(tc.schemaCount, tc.partitionSpecCount, tc.snapshotCount, tc.propertyCount)
			if _, err := ParseMetadataBytes(data); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for range b.N {
				metadata, err := ParseMetadataBytes(data)
				if err != nil {
					b.Fatal(err)
				}
				metadataParseBenchmarkSink = metadata.Version()
			}
		})
	}
}

func benchmarkMetadataJSON(schemaCount, partitionSpecCount, snapshotCount, propertyCount int) []byte {
	const minuteInMS = int64(60_000)

	metadata := map[string]any{
		"format-version":        2,
		"table-uuid":            "9c12d441-03fe-4693-9a96-a0705ddf69c1",
		"location":              "s3://bucket/test/location",
		"last-sequence-number":  snapshotCount,
		"last-updated-ms":       int64(snapshotCount+1) * minuteInMS,
		"last-column-id":        1,
		"current-schema-id":     schemaCount - 1,
		"default-spec-id":       0,
		"last-partition-id":     1000 + partitionSpecCount - 1,
		"sort-orders":           []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"default-sort-order-id": 0,
		"schemas":               benchmarkSchemas(schemaCount),
		"partition-specs":       benchmarkPartitionSpecs(partitionSpecCount),
		"properties":            benchmarkMetadataProperties(propertyCount),
		"snapshots":             benchmarkParseMetadataSnapshots(snapshotCount, minuteInMS),
		"snapshot-log":          benchmarkParseMetadataSnapshotLogEntries(snapshotCount, minuteInMS),
		"metadata-log":          []any{map[string]any{"metadata-file": "s3://bucket/metadata/v1.json", "timestamp-ms": int64(0)}},
		"current-snapshot-id":   int64(snapshotCount),
		"refs":                  map[string]any{"main": map[string]any{"snapshot-id": int64(snapshotCount), "type": "branch"}},
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}

	return data
}

func benchmarkSchemas(count int) []any {
	schemas := make([]any, count)
	for i := range schemas {
		schemas[i] = map[string]any{
			"type":      "struct",
			"schema-id": i,
			"fields": []any{map[string]any{
				"id":       1,
				"name":     "id",
				"required": true,
				"type":     "long",
			}},
		}
	}

	return schemas
}

func benchmarkPartitionSpecs(count int) []any {
	specs := make([]any, count)
	for i := range specs {
		specs[i] = map[string]any{
			"spec-id": i,
			"fields": []any{map[string]any{
				"name":      "id",
				"transform": "identity",
				"source-id": 1,
				"field-id":  1000 + i,
			}},
		}
	}

	return specs
}

func benchmarkMetadataProperties(count int) map[string]string {
	properties := make(map[string]string, count)
	for i := range count {
		properties[fmt.Sprintf("property-%d", i)] = "value"
	}

	return properties
}

func benchmarkParseMetadataSnapshots(count int, minuteInMS int64) []any {
	snapshots := make([]any, count)
	for i := range snapshots {
		id := int64(i + 1)
		snapshot := map[string]any{
			"snapshot-id":     id,
			"timestamp-ms":    id * minuteInMS,
			"sequence-number": i + 1,
			"summary":         map[string]any{"operation": "append"},
			"manifest-list":   fmt.Sprintf("s3://bucket/manifests/%d.avro", id),
		}
		if i > 0 {
			snapshot["parent-snapshot-id"] = id - 1
		}
		snapshots[i] = snapshot
	}

	return snapshots
}

func benchmarkParseMetadataSnapshotLogEntries(count int, minuteInMS int64) []any {
	entries := make([]any, count)
	for i := range entries {
		id := int64(i + 1)
		entries[i] = map[string]any{
			"snapshot-id":  id,
			"timestamp-ms": id * minuteInMS,
		}
	}

	return entries
}
