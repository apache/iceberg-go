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

package internal_test

import (
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go/internal"
)

func TestIndexedFIFOPackingMatchesLinear(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for _, lookback := range []int{31, 32, 64, 128, 512} {
		for trial := range 36 {
			target := []int64{0, 10, 128, math.MaxInt64 / 2, math.MaxInt64}[trial%5]
			items := make([]int, 2*lookback+32)
			weights := make([]int64, len(items))
			for i := range items {
				items[i] = i
				switch trial % 3 {
				case 0:
					weights[i] = int64(1 + rng.Intn(128))
				case 1:
					weights[i] = int64(65 + rng.Intn(64))
				default:
					weights[i] = int64(rng.Intn(161) - 20)
				}
			}
			switch trial % 11 {
			case 0:
				weights[len(weights)/2] = math.MaxInt64
			case 1:
				weights[len(weights)/2] = math.MinInt64
			}

			weight := func(index int) int64 { return weights[index] }
			packer := internal.SlicePacker[int]{
				TargetWeight: target,
				Lookback:     lookback,
			}

			linear := slices.Collect(internal.PackingIterator(
				slices.Values(items), target, lookback, weight, false))
			if got := packer.Pack(items, weight); !reflect.DeepEqual(got, linear) {
				t.Fatalf("Pack lookback=%d trial=%d: got %v, want %v",
					lookback, trial, got, linear)
			}

			reversed := func(yield func(int) bool) {
				for _, item := range slices.Backward(items) {
					if !yield(item) {
						return
					}
				}
			}
			linearEnd := slices.Collect(internal.PackingIterator(
				reversed, target, lookback, weight, false))
			slices.Reverse(linearEnd)
			for _, bin := range linearEnd {
				slices.Reverse(bin)
			}
			if got := packer.PackEnd(items, weight); !reflect.DeepEqual(got, linearEnd) {
				t.Fatalf("PackEnd lookback=%d trial=%d: got %v, want %v",
					lookback, trial, got, linearEnd)
			}
		}
	}
}

var indexedFIFOBenchmarkSink [][]int64

func BenchmarkIndexedFIFOSlicePacking(b *testing.B) {
	for _, lookback := range []int{128, 512} {
		for _, workload := range []string{"mixed", "large"} {
			items := make([]int64, 16_384)
			for i := range items {
				switch workload {
				case "mixed":
					items[i] = int64(1 + (i*73)%128)
				case "large":
					items[i] = int64(65 + i%64)
				}
			}

			weight := func(value int64) int64 { return value }
			packer := internal.SlicePacker[int64]{
				TargetWeight: 128,
				Lookback:     lookback,
			}

			for _, mode := range []string{"linear", "indexed"} {
				b.Run("lookback="+strconv.Itoa(lookback)+"/"+workload+"/"+mode, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()

					for range b.N {
						if mode == "linear" {
							indexedFIFOBenchmarkSink = slices.Collect(internal.PackingIterator(
								slices.Values(items), 128, lookback, weight, false))
						} else {
							indexedFIFOBenchmarkSink = packer.Pack(items, weight)
						}
					}
				})
			}
		}
	}
}
