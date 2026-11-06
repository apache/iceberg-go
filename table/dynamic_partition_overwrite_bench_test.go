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
	const manifestCount = 256
	const targetPartition = int32(1123)

	schema := simpleSchema()
	spec := iceberg.NewPartitionSpecID(4, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	manifests := make([]iceberg.ManifestFile, manifestCount)
	for i := range manifestCount {
		value := int32(1000 + i)
		if i == 123 {
			value = targetPartition
		}
		bound, err := iceberg.Int32Literal(value).MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		manifests[i] = iceberg.NewManifestBuilder(2, fmt.Sprintf("manifest-%03d.avro", i), 1,
			int32(spec.ID()), 1).
			Partitions([]iceberg.FieldSummary{{LowerBound: &bound, UpperBound: &bound}}).
			Build()
	}
	partitions := []map[int]any{{1000: targetPartition}}
	ctx := context.Background()

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
		if count != 1 {
			b.Fatalf("selected %d manifests, want 1", count)
		}
		dynamicOverwriteCandidateCount = count
	}
	b.ReportMetric(float64(manifestCount), "manifests/op")
	b.ReportMetric(float64(dynamicOverwriteCandidateCount), "candidate-manifests/op")
}
