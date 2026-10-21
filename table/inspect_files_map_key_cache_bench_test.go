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
	"fmt"
	"sort"
	"testing"
)

var inspectSortedStatFieldIDsBenchmarkSink int64

// BenchmarkInspectSortedStatFieldIDs isolates map-key ordering and value
// lookups. BenchmarkInspectContentFileAppenderDataFileStats measures the
// complete Arrow metadata appender instead.
func BenchmarkInspectSortedStatFieldIDs(b *testing.B) {
	const mapsPerOp = 256

	for _, width := range []int{8, 32, 128} {
		for _, changingKeys := range []bool{false, true} {
			name := fmt.Sprintf("width=%d/changing=%t", width, changingKeys)
			b.Run(name, func(b *testing.B) {
				maps := make([]map[int]int64, mapsPerOp)
				for i := range maps {
					firstID := 1
					if changingKeys {
						firstID = 1 + i%17*1000
					}
					values := make(map[int]int64, width)
					for field := range width {
						values[firstID+field] = int64(i + field)
					}
					maps[i] = values
				}

				b.Run("sort_each", func(b *testing.B) {
					var scratch []int
					var sum int64
					b.ReportAllocs()
					b.ReportMetric(mapsPerOp, "files/op")
					b.ReportMetric(mapsPerOp*6, "maps/op")
					b.ResetTimer()
					for range b.N {
						for _, values := range maps {
							for range 6 {
								scratch = scratch[:0]
								for id := range values {
									scratch = append(scratch, id)
								}
								sort.Ints(scratch)
								for _, id := range scratch {
									sum += int64(id) + values[id]
								}
							}
						}
					}
					inspectSortedStatFieldIDsBenchmarkSink = sum
				})
				b.Run("reuse_verified", func(b *testing.B) {
					var cache inspectSortedMapIDs
					var sum int64
					b.ReportAllocs()
					b.ReportMetric(mapsPerOp, "files/op")
					b.ReportMetric(mapsPerOp*6, "maps/op")
					b.ResetTimer()
					for range b.N {
						for _, values := range maps {
							for range 6 {
								for _, id := range sortedInspectMapIDs(values, &cache) {
									sum += int64(id) + values[id]
								}
							}
						}
					}
					inspectSortedStatFieldIDsBenchmarkSink = sum
				})
			})
		}
	}
}
