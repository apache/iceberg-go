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
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var dynamicOverwriteCandidateCount int

func BenchmarkDynamicOverwriteManifestCandidates(b *testing.B) {
	tests := []struct {
		manifests  int
		partitions int
	}{
		{manifests: 256, partitions: 1},
		{manifests: 1024, partitions: 4},
		{manifests: 4096, partitions: 16},
	}

	schema := simpleSchema()
	spec := iceberg.NewPartitionSpecID(4, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	ctx := context.Background()

	for _, tc := range tests {
		b.Run(fmt.Sprintf("manifests=%d/partitions=%d", tc.manifests, tc.partitions), func(b *testing.B) {
			manifests := make([]iceberg.ManifestFile, tc.manifests)
			for i := range tc.manifests {
				value := int32(i)
				bound, err := iceberg.Int32Literal(value).MarshalBinary()
				if err != nil {
					b.Fatal(err)
				}
				manifests[i] = iceberg.NewManifestBuilder(2, fmt.Sprintf("manifest-%05d.avro", i), 1,
					int32(spec.ID()), 1).
					Partitions([]iceberg.FieldSummary{{LowerBound: &bound, UpperBound: &bound}}).
					Build()
			}

			partitions := make([]map[int]any, tc.partitions)
			manifestStep := tc.manifests / tc.partitions
			for i := range tc.partitions {
				partitions[i] = map[int]any{1000: int32(i * manifestStep)}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				candidates, err := dynamicOverwriteManifestCandidates(ctx, manifests, spec, schema, partitions)
				if err != nil {
					b.Fatal(err)
				}
				count := 0
				for _, candidate := range candidates {
					if candidate {
						count++
					}
				}
				if count != tc.partitions {
					b.Fatalf("selected %d manifests, want %d", count, tc.partitions)
				}
				dynamicOverwriteCandidateCount = count
			}
			b.ReportMetric(float64(tc.manifests), "manifests/op")
			b.ReportMetric(float64(tc.partitions), "partitions/op")
			b.ReportMetric(float64(dynamicOverwriteCandidateCount), "candidate-manifests/op")
		})
	}
}
