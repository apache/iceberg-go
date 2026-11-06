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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

var dynamicOverwriteBenchEntries int64

type dynamicOverwriteBenchmarkIO struct {
	*trackingIO
	opens int
}

func (fs *dynamicOverwriteBenchmarkIO) Open(ctx context.Context, path string) (iceio.File, error) {
	fs.opens++

	return fs.trackingIO.Open(ctx, path)
}

func BenchmarkDynamicOverwriteManifestPlanning(b *testing.B) {
	tests := []struct {
		manifests  int
		partitions int
	}{
		{manifests: 256, partitions: 1},
		{manifests: 1024, partitions: 4},
		{manifests: 4096, partitions: 16},
	}

	for _, tc := range tests {
		b.Run(fmt.Sprintf("manifests=%d/partitions=%d", tc.manifests, tc.partitions), func(b *testing.B) {
			schema := simpleSchema()
			spec := iceberg.NewPartitionSpecID(4, iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
			})
			manifests, partitions, fs := newDynamicOverwriteBenchmarkSnapshot(b, tc.manifests, tc.partitions, spec, schema)
			ctx := context.Background()

			b.Run("before-all-manifests", func(b *testing.B) {
				fs.opens = 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					var entries int64
					for _, manifest := range manifests {
						for _, err := range manifest.Entries(ctx, fs, true) {
							if err != nil {
								b.Fatal(err)
							}
							entries++
						}
					}
					dynamicOverwriteBenchEntries = entries
				}
				b.StopTimer()
				b.ReportMetric(float64(tc.manifests), "manifests/op")
				b.ReportMetric(float64(fs.opens)/float64(b.N), "manifest-opens/op")
			})

			b.Run("after-summary-pruning", func(b *testing.B) {
				fs.opens = 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					candidates, err := dynamicOverwriteManifestCandidates(ctx, manifests, spec, schema, partitions)
					if err != nil {
						b.Fatal(err)
					}
					var (
						entries         int64
						candidatesCount int
					)
					for idx, manifest := range manifests {
						if !candidates[idx] {
							continue
						}
						candidatesCount++
						for _, err := range manifest.Entries(ctx, fs, true) {
							if err != nil {
								b.Fatal(err)
							}
							entries++
						}
					}
					if candidatesCount != tc.partitions {
						b.Fatalf("selected %d manifests, want %d", candidatesCount, tc.partitions)
					}
					dynamicOverwriteBenchEntries = entries
				}
				b.StopTimer()
				b.ReportMetric(float64(tc.manifests), "manifests/op")
				b.ReportMetric(float64(tc.partitions), "candidate-manifests/op")
				b.ReportMetric(float64(fs.opens)/float64(b.N), "manifest-opens/op")
			})
		})
	}
}

func newDynamicOverwriteBenchmarkSnapshot(
	b *testing.B,
	manifestCount, partitionCount int,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
) ([]iceberg.ManifestFile, []map[int]any, *dynamicOverwriteBenchmarkIO) {
	b.Helper()
	ctx := context.Background()
	tracking := newTrackingIO()
	fs := &dynamicOverwriteBenchmarkIO{trackingIO: tracking}
	manifests := make([]iceberg.ManifestFile, manifestCount)
	for i := range manifestCount {
		value := int32(i)
		file := newTestDataFile(b, spec, fmt.Sprintf("mem://benchmark/data-%05d.parquet", i), map[int]any{1000: value})
		snapshotID := int64(1)
		entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, file).
			DataSequenceNumber(1).
			Build()
		path := fmt.Sprintf("mem://benchmark/manifest-%05d.avro", i)
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry})
		if err != nil {
			b.Fatal(err)
		}
		if err := tracking.WriteFile(ctx, path, buf.Bytes()); err != nil {
			b.Fatal(err)
		}
		manifests[i] = manifest
	}

	partitions := make([]map[int]any, partitionCount)
	manifestStep := manifestCount / partitionCount
	for i := range partitionCount {
		partitions[i] = map[int]any{1000: int32(i * manifestStep)}
	}

	return manifests, partitions, fs
}
