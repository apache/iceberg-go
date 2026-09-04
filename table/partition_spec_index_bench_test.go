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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
)

var partitionSpecLookupBenchmarkSink int

func BenchmarkPartitionSpecByID(b *testing.B) {
	for _, specCount := range []int{1, 4, 8, 16, 32, 256, 2_048} {
		specs := partitionSpecBenchmarkSpecs(specCount)
		metadata := commonMetadata{
			Specs:              specs,
			DefaultSpecID:      specs[specCount-1].ID(),
			partitionSpecIndex: buildPartitionSpecIndex(specs),
		}

		for _, tt := range []struct {
			name string
			id   int
		}{
			{name: "first", id: specs[0].ID()},
			{name: "middle", id: specs[specCount/2].ID()},
			{name: "last", id: specs[specCount-1].ID()},
			{name: "miss", id: -1},
		} {
			b.Run(specCountName(specCount)+"/"+tt.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(specCount), "specs")
				b.ResetTimer()
				for range b.N {
					spec := metadata.PartitionSpecByID(tt.id)
					if spec == nil {
						partitionSpecLookupBenchmarkSink = -1

						continue
					}
					partitionSpecLookupBenchmarkSink = spec.ID()
				}
			})
		}
	}
}

func BenchmarkMetadataBuilderGetSpecByID(b *testing.B) {
	for _, specCount := range []int{1, 4, 8, 16, 32, 256, 2_048} {
		specs := partitionSpecBenchmarkSpecs(specCount)
		builder := MetadataBuilder{
			specs:              specs,
			partitionSpecIndex: buildPartitionSpecIndex(specs),
		}

		for _, tt := range []struct {
			name string
			id   int
		}{
			{name: "first", id: specs[0].ID()},
			{name: "middle", id: specs[specCount/2].ID()},
			{name: "last", id: specs[specCount-1].ID()},
			{name: "miss", id: -1},
		} {
			b.Run(specCountName(specCount)+"/"+tt.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(float64(specCount), "specs")
				b.ResetTimer()
				for range b.N {
					spec, err := builder.GetSpecByID(tt.id)
					if err != nil {
						partitionSpecLookupBenchmarkSink = -1

						continue
					}
					partitionSpecLookupBenchmarkSink = spec.ID()
				}
			})
		}
	}
}

func BenchmarkCurrentPartitionSpec(b *testing.B) {
	for _, specCount := range []int{1, 4, 8, 16, 32, 256, 2_048} {
		specs := partitionSpecBenchmarkSpecs(specCount)
		metadata := commonMetadata{
			Specs:              specs,
			DefaultSpecID:      specs[specCount-1].ID(),
			partitionSpecIndex: buildPartitionSpecIndex(specs),
		}

		b.Run(specCountName(specCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(specCount), "specs")
			b.ResetTimer()
			for range b.N {
				spec := metadata.PartitionSpec()
				partitionSpecLookupBenchmarkSink = spec.ID()
			}
		})
	}
}

func partitionSpecBenchmarkSpecs(count int) []iceberg.PartitionSpec {
	specs := make([]iceberg.PartitionSpec, count)
	for i := range count {
		specs[i] = iceberg.NewPartitionSpecID(i)
	}

	return specs
}

func specCountName(count int) string {
	return "specs=" + strconv.Itoa(count)
}
