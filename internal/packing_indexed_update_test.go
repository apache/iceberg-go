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

package internal

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"testing"
)

// recomputeAllPackingMinimums preserves the old update implementation so the
// test and benchmark can compare both paths without a separate checkout.
func recomputeAllPackingMinimums(idx *packingWeightIndex, bin int, weight int64) {
	node := idx.size + bin
	idx.weights[node] = weight
	for node /= 2; node > 0; node /= 2 {
		idx.weights[node] = min(idx.weights[node*2], idx.weights[node*2+1])
	}
}

func TestPackingWeightIndexUpdateMatchesFullRecompute(t *testing.T) {
	for _, capacity := range []int{1, 2, 3, 7, 32, 128, 512, 2048} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			original := newPackingWeightIndex(capacity)
			optimized := newPackingWeightIndex(capacity)
			rng := rand.New(rand.NewSource(int64(capacity)))

			for step := range 2_000 {
				bin := rng.Intn(capacity)
				weight := int64(rng.Intn(128))
				if step%13 == 0 {
					// Evictions reset a leaf to the empty-bin sentinel.
					weight = math.MaxInt64
				}
				recomputeAllPackingMinimums(&original, bin, weight)
				optimized.update(bin, weight)

				if !slices.Equal(original.weights, optimized.weights) {
					t.Fatalf("step %d, bin %d: incorrect subtree minimums", step, bin)
				}

				start := rng.Intn(capacity)
				end := start + 1 + rng.Intn(capacity-start)
				limit := int64(rng.Intn(128))
				if got, want := optimized.firstFit(start, end, limit),
					original.firstFit(start, end, limit); got != want {
					t.Fatalf("step %d: firstFit(%d, %d, %d) = %d, want %d",
						step, start, end, limit, got, want)
				}
			}
		})
	}
}

var packingIndexUpdateBenchmarkSink int64

func BenchmarkPackingWeightIndexUpdate(b *testing.B) {
	const period = 8192
	for _, capacity := range []int{128, 512, 2048} {
		b.Run(fmt.Sprintf("bins=%d", capacity), func(b *testing.B) {
			slots := make([]int, period)
			weights := make([]int64, period)
			for i := range slots {
				slots[i] = (i*73 + 23) % capacity
				weights[i] = 64 + int64((i*37)%64)
			}

			for _, optimized := range []bool{false, true} {
				name := "before"
				if optimized {
					name = "after"
				}
				b.Run(name, func(b *testing.B) {
					idx := newPackingWeightIndex(capacity)
					for i := range capacity {
						recomputeAllPackingMinimums(&idx, i, 64+int64(i%64))
					}
					b.ReportAllocs()
					b.ResetTimer()
					if optimized {
						for i := range b.N {
							j := i & (period - 1)
							idx.update(slots[j], weights[j])
						}
					} else {
						for i := range b.N {
							j := i & (period - 1)
							recomputeAllPackingMinimums(&idx, slots[j], weights[j])
						}
					}
					packingIndexUpdateBenchmarkSink = idx.weights[1]
				})
			}
		})
	}
}
