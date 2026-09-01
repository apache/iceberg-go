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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
)

var metadataParseBenchmarkSink Metadata

var metadataParseBenchmarkSizes = []struct {
	name  string
	bytes int
}{
	{name: "100KiB", bytes: 100 << 10},
	{name: "500KiB", bytes: 500 << 10},
	{name: "1MiB", bytes: 1 << 20},
	{name: "2MiB", bytes: 2 << 20},
	{name: "5MiB", bytes: 5 << 20},
}

// BenchmarkParseMetadataBytes measures parsing by serialized byte size for two
// common shapes. Snapshot history stresses metadata with many nested objects;
// properties isolates document size that does not come from snapshot history.
func BenchmarkParseMetadataBytes(b *testing.B) {
	profiles := []struct {
		name  string
		build func(testing.TB, int) []byte
	}{
		{name: "snapshot-history", build: snapshotHistoryMetadataForBenchmark},
		{name: "properties", build: propertyMetadataForBenchmark},
		{name: "legacy-field-id-fallback", build: legacyFieldIDFallbackMetadataForBenchmark},
	}

	for _, profile := range profiles {
		for _, size := range metadataParseBenchmarkSizes {
			raw := profile.build(b, size.bytes)
			b.Run(fmt.Sprintf("profile=%s/target=%s/metadata=%d-bytes", profile.name, size.name, len(raw)), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				b.ReportMetric(float64(len(raw)), "metadata-bytes")

				for range b.N {
					metadata, err := ParseMetadataBytes(raw)
					if err != nil {
						b.Fatal(err)
					}
					metadataParseBenchmarkSink = metadata
				}
			})
		}
	}
}

func snapshotHistoryMetadataForBenchmark(tb testing.TB, targetBytes int) []byte {
	tb.Helper()

	count := max(1, targetBytes/300)
	var raw []byte
	for range 5 {
		raw = buildSnapshotHistoryMetadata(tb, count)
		adjusted := max(1, count*targetBytes/len(raw))
		if adjusted == count {
			break
		}
		count = adjusted
	}

	return raw
}

func buildSnapshotHistoryMetadata(tb testing.TB, count int) []byte {
	tb.Helper()

	fields := benchmarkMetadataFields(tb)
	snapshots := make([]Snapshot, count)
	for i := range snapshots {
		snapshots[i] = Snapshot{
			SnapshotID:     int64(i + 1),
			SequenceNumber: int64(i),
			TimestampMs:    1_700_000_000_000 + int64(i),
			ManifestList:   "s3://warehouse/db/table/metadata/" + strings.Repeat("m", 96) + strconv.Itoa(i) + ".avro",
			Summary: &Summary{
				Operation: OpAppend,
				Properties: iceberg.Properties{
					"added-data-files": "1",
					"added-records":    "1000",
				},
			},
		}
	}
	fields["snapshots"] = mustMarshalBenchmarkJSON(tb, snapshots)
	fields["last-sequence-number"] = json.RawMessage(strconv.AppendInt(nil, int64(count-1), 10))
	fields["current-snapshot-id"] = json.RawMessage(strconv.AppendInt(nil, int64(count), 10))
	fields["refs"] = mustMarshalBenchmarkJSON(tb, map[string]SnapshotRef{
		MainBranch: {SnapshotID: int64(count), SnapshotRefType: BranchRef},
	})
	delete(fields, "snapshot-log")

	return mustMarshalBenchmarkJSON(tb, fields)
}

func propertyMetadataForBenchmark(tb testing.TB, targetBytes int) []byte {
	return propertyMetadataFromExampleForBenchmark(tb, ExampleTableMetadataV2, targetBytes)
}

func legacyFieldIDFallbackMetadataForBenchmark(tb testing.TB, targetBytes int) []byte {
	tb.Helper()

	fields := benchmarkMetadataFieldsFromExample(tb, ExampleTableMetadataV1)
	var spec []map[string]json.RawMessage
	if err := json.Unmarshal(fields["partition-spec"], &spec); err != nil {
		tb.Fatal(err)
	}
	for _, field := range spec {
		delete(field, "field-id")
	}
	fields["partition-spec"] = mustMarshalBenchmarkJSON(tb, spec)

	return propertyMetadataFromFieldsForBenchmark(tb, fields, targetBytes)
}

func propertyMetadataFromExampleForBenchmark(tb testing.TB, example string, targetBytes int) []byte {
	tb.Helper()

	return propertyMetadataFromFieldsForBenchmark(tb, benchmarkMetadataFieldsFromExample(tb, example), targetBytes)
}

func propertyMetadataFromFieldsForBenchmark(tb testing.TB, base map[string]json.RawMessage, targetBytes int) []byte {
	tb.Helper()

	count := max(1, targetBytes/300)
	var raw []byte
	for range 5 {
		fields := make(map[string]json.RawMessage, len(base)+1)
		for key, value := range base {
			fields[key] = value
		}
		properties := make(iceberg.Properties, count)
		for i := range count {
			properties[fmt.Sprintf("property-%06d", i)] = strings.Repeat("v", 256)
		}
		fields["properties"] = mustMarshalBenchmarkJSON(tb, properties)
		raw = mustMarshalBenchmarkJSON(tb, fields)

		adjusted := max(1, count*targetBytes/len(raw))
		if adjusted == count {
			break
		}
		count = adjusted
	}

	return raw
}

func benchmarkMetadataFields(tb testing.TB) map[string]json.RawMessage {
	return benchmarkMetadataFieldsFromExample(tb, ExampleTableMetadataV2)
}

func benchmarkMetadataFieldsFromExample(tb testing.TB, example string) map[string]json.RawMessage {
	tb.Helper()

	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(example), &fields); err != nil {
		tb.Fatal(err)
	}

	return fields
}

func mustMarshalBenchmarkJSON(tb testing.TB, value any) json.RawMessage {
	tb.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		tb.Fatal(err)
	}

	return raw
}
