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
	"encoding/json"
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// These counts generate metadata payloads of approximately 7 KiB, 58 KiB,
// 569 KiB, 2 MB, and 5.6 MiB. Benchmark names and metrics report actual bytes.
var benchSnapshotCounts = []int64{10, 100, 1000, 3418, 10000}

func extractMetadata(tb testing.TB, body []byte) json.RawMessage {
	tb.Helper()
	var top map[string]json.RawMessage
	require.NoError(tb, json.Unmarshal(body, &top))

	return top["metadata"]
}

func metadataBenchmarkName(raw json.RawMessage) string {
	return fmt.Sprintf("metadata=%d-bytes", len(raw))
}

// BenchmarkStageFullParse measures table.ParseMetadataBytes, the call the REST client
// actually makes on both LoadTable and CommitTable responses.
func BenchmarkStageFullParse(b *testing.B) {
	for _, n := range benchSnapshotCounts {
		body := makeTableResponseWithSnapshots(n)
		rawMeta := extractMetadata(b, body)
		b.Run(metadataBenchmarkName(rawMeta), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.ReportMetric(float64(len(rawMeta)), "metadata-bytes")
			b.ReportMetric(float64(n), "snapshots")
			for range b.N {
				meta, err := table.ParseMetadataBytes(rawMeta)
				if err != nil {
					b.Fatal(err)
				}
				if meta.CurrentSnapshot() == nil {
					b.Fatal("missing current snapshot")
				}
			}
		})
	}
}

// BenchmarkStageDeferredParse measures the parse-time lazy materialization
// path used for CommitTable responses when snapshot-loading-mode is refs.
func BenchmarkStageDeferredParse(b *testing.B) {
	for _, n := range benchSnapshotCounts {
		body := makeTableResponseWithSnapshots(n)
		rawMeta := extractMetadata(b, body)
		b.Run(metadataBenchmarkName(rawMeta), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.ReportMetric(float64(len(rawMeta)), "metadata-bytes")
			b.ReportMetric(float64(n), "snapshots")
			for range b.N {
				meta, err := table.ParseMetadataBytesDeferredSnapshots(rawMeta)
				if err != nil {
					b.Fatal(err)
				}
				if meta.CurrentSnapshot() == nil {
					b.Fatal("missing current snapshot")
				}
			}
		})
	}
}

// BenchmarkStageDeferredHistoricalLookup measures parsing followed by a lookup
// of one unreferenced historical snapshot. The indexed lazy path should decode
// only that snapshot rather than materializing the complete history.
func BenchmarkStageDeferredHistoricalLookup(b *testing.B) {
	for _, n := range benchSnapshotCounts {
		body := makeTableResponseWithSnapshots(n)
		rawMeta := extractMetadata(b, body)
		b.Run(metadataBenchmarkName(rawMeta), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.ReportMetric(float64(len(rawMeta)), "metadata-bytes")
			b.ReportMetric(float64(n), "snapshots")
			for range b.N {
				meta, err := table.ParseMetadataBytesDeferredSnapshots(rawMeta)
				if err != nil {
					b.Fatal(err)
				}
				if meta.SnapshotByID(0) == nil {
					b.Fatal("missing historical snapshot")
				}
			}
		})
	}
}

// BenchmarkStageDeferredAllSnapshots measures the intentional fallback that
// parses and then materializes the complete snapshot history.
func BenchmarkStageDeferredAllSnapshots(b *testing.B) {
	for _, n := range benchSnapshotCounts {
		body := makeTableResponseWithSnapshots(n)
		rawMeta := extractMetadata(b, body)
		b.Run(metadataBenchmarkName(rawMeta), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			b.ReportMetric(float64(len(rawMeta)), "metadata-bytes")
			b.ReportMetric(float64(n), "snapshots")
			for range b.N {
				meta, err := table.ParseMetadataBytesDeferredSnapshots(rawMeta)
				if err != nil {
					b.Fatal(err)
				}
				if len(meta.Snapshots()) != int(n) {
					b.Fatal("incomplete snapshot history")
				}
			}
		})
	}
}

// BenchmarkStageParseThenMetadataBuilder measures the path taken when a caller
// starts another transaction from the Table returned by Commit. Builder
// creation requires complete snapshot history, so this exposes the cost of
// indexing deferred snapshots and then fully materializing them.
func BenchmarkStageParseThenMetadataBuilder(b *testing.B) {
	for _, n := range benchSnapshotCounts {
		body := makeTableResponseWithSnapshots(n)
		rawMeta := extractMetadata(b, body)
		for _, mode := range []struct {
			name  string
			parse func([]byte) (table.Metadata, error)
		}{
			{name: "eager", parse: table.ParseMetadataBytes},
			{name: "deferred", parse: table.ParseMetadataBytesDeferredSnapshots},
		} {
			b.Run(metadataBenchmarkName(rawMeta)+"/"+mode.name, func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()
				b.ReportMetric(float64(len(rawMeta)), "metadata-bytes")
				b.ReportMetric(float64(n), "snapshots")
				for range b.N {
					meta, err := mode.parse(rawMeta)
					if err != nil {
						b.Fatal(err)
					}
					builder, err := table.MetadataBuilderFromBase(meta, "")
					if err != nil {
						b.Fatal(err)
					}
					if builder == nil {
						b.Fatal("missing metadata builder")
					}
				}
			})
		}
	}
}

// BenchmarkStageDeferredRetainedHeap measures the live heap held by one
// deferred metadata object before and after full snapshot materialization.
// Run with -benchtime=1x: retained-heap measurements describe one object and
// cannot be meaningfully averaged across b.N independent lifecycles.
func BenchmarkStageDeferredRetainedHeap(b *testing.B) {
	if b.N != 1 {
		b.Skip("retained-heap benchmark requires -benchtime=1x")
	}

	for _, n := range benchSnapshotCounts {
		body := makeTableResponseWithSnapshots(n)
		rawMeta := extractMetadata(b, body)
		b.Run(metadataBenchmarkName(rawMeta), func(b *testing.B) {
			meta, err := table.ParseMetadataBytesDeferredSnapshots(rawMeta)
			if err != nil {
				b.Fatal(err)
			}

			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			materializeSnapshots(b, meta, n)
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			b.ReportMetric(float64(len(rawMeta)), "metadata-bytes")
			b.ReportMetric(float64(before.HeapAlloc), "heap-before-bytes")
			b.ReportMetric(float64(after.HeapAlloc), "heap-after-bytes")
			delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			b.ReportMetric(float64(delta), "heap-delta-bytes")
			b.ReportMetric(float64(delta)/float64(len(rawMeta)), "heap-growth-ratio")
			b.ReportMetric(0, "ns/op")
			runtime.KeepAlive(meta)
			runtime.KeepAlive(rawMeta)
			runtime.KeepAlive(body)
		})
	}
}

// Keep the defensive copy returned by Snapshots out of the post-call heap
// sample so it measures metadata-owned state rather than caller-owned memory.
func materializeSnapshots(b *testing.B, meta table.Metadata, expected int64) {
	b.Helper()
	if len(meta.Snapshots()) != int(expected) {
		b.Fatal("incomplete snapshot history")
	}
}
