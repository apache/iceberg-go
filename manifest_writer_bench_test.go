// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iceberg

import (
	"fmt"
	"io"
	"testing"
)

func BenchmarkManifestWriterPartitionSummaries(b *testing.B) {
	for _, entryCount := range []int{100, 1_000, 10_000} {
		schema, spec, snapshotID, entries := manifestWriterBenchmarkData(b, entryCount)

		b.Run(fmt.Sprintf("entries_%d", entryCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				writer, err := NewManifestWriter(2, io.Discard, spec, schema, snapshotID)
				if err != nil {
					b.Fatal(err)
				}
				for _, entry := range entries {
					if err := writer.Add(entry); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := writer.ToManifestFile("manifest.avro", 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkManifestWriterPartitionSummaryFinalization(b *testing.B) {
	for _, entryCount := range []int{100, 1_000, 10_000} {
		schema, spec, snapshotID, entries := manifestWriterBenchmarkData(b, entryCount)

		b.Run(fmt.Sprintf("entries_%d", entryCount), func(b *testing.B) {
			writer, err := NewManifestWriter(2, io.Discard, spec, schema, snapshotID)
			if err != nil {
				b.Fatal(err)
			}
			for _, entry := range entries {
				if err := writer.Add(entry); err != nil {
					b.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := writer.ToManifestFile("manifest.avro", 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func manifestWriterBenchmarkData(b *testing.B, entryCount int) (*Schema, PartitionSpec, int64, []ManifestEntry) {
	b.Helper()

	schema := NewSchema(0,
		NestedField{ID: 1, Name: "region", Type: PrimitiveTypes.String},
		NestedField{ID: 2, Name: "day", Type: PrimitiveTypes.Date},
		NestedField{ID: 3, Name: "bucket", Type: PrimitiveTypes.Int32},
		NestedField{ID: 4, Name: "shard", Type: PrimitiveTypes.Binary},
	)
	spec := NewPartitionSpec(
		PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "region", Transform: IdentityTransform{}},
		PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "day", Transform: IdentityTransform{}},
		PartitionField{SourceIDs: []int{3}, FieldID: 1002, Name: "bucket", Transform: IdentityTransform{}},
		PartitionField{SourceIDs: []int{4}, FieldID: 1003, Name: "shard", Transform: IdentityTransform{}},
	)
	snapshotID := int64(1234)
	sequenceNumber := int64(1)
	entries := make([]ManifestEntry, entryCount)
	for i := range entries {
		builder, err := NewDataFileBuilder(
			spec,
			EntryContentData,
			fmt.Sprintf("s3://bucket/data/%05d.parquet", i),
			ParquetFile,
			map[int]any{
				1000: fmt.Sprintf("region-%02d", i%32),
				1001: Date(i % 365),
				1002: int32(i % 128),
				1003: []byte{byte(i >> 8), byte(i)},
			},
			nil,
			nil,
			1_000,
			1_000_000,
		)
		if err != nil {
			b.Fatal(err)
		}
		entries[i] = NewManifestEntry(
			EntryStatusADDED,
			&snapshotID,
			&sequenceNumber,
			&sequenceNumber,
			builder.Build(),
		)
	}

	return schema, spec, snapshotID, entries
}
