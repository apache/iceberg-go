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
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go/internal"
)

func TestPackingFIFOSingleLookback(t *testing.T) {
	tests := []struct {
		name    string
		weights []int64
		target  int64
		want    [][]int
		wantEnd [][]int
	}{
		{name: "empty", target: 10, wantEnd: [][]int{}},
		{name: "single", weights: []int64{7}, target: 10, want: [][]int{{0}}, wantEnd: [][]int{{0}}},
		{name: "one bin", weights: []int64{3, 3, 4}, target: 10, want: [][]int{{0, 1, 2}}, wantEnd: [][]int{{0, 1, 2}}},
		{
			name: "repeated evictions", weights: []int64{6, 7, 8, 9, 10}, target: 10,
			want: [][]int{{0}, {1}, {2}, {3}, {4}}, wantEnd: [][]int{{0}, {1}, {2}, {3}, {4}},
		},
		{
			name: "exact fits", weights: []int64{6, 4, 10, 5, 5}, target: 10,
			want: [][]int{{0, 1}, {2}, {3, 4}}, wantEnd: [][]int{{0, 1}, {2}, {3, 4}},
		},
		{
			name: "different directions", weights: []int64{1, 2, 3, 4, 5}, target: 8,
			want: [][]int{{0, 1, 2}, {3}, {4}}, wantEnd: [][]int{{0, 1}, {2, 3}, {4}},
		},
		{
			name: "oversized and zero", weights: []int64{12, 1, 9, 15, 0, 10}, target: 10,
			want: [][]int{{0}, {1, 2}, {3}, {4, 5}}, wantEnd: [][]int{{0}, {1, 2}, {3}, {4, 5}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := make([]int, len(tt.weights))
			for i := range items {
				items[i] = i
			}
			original := slices.Clone(items)
			weight := func(i int) int64 { return tt.weights[i] }
			packer := internal.SlicePacker[int]{TargetWeight: tt.target, Lookback: 1}

			if got := packer.Pack(items, weight); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Pack() = %v, want %v", got, tt.want)
			}
			if got := packer.PackEnd(items, weight); !reflect.DeepEqual(got, tt.wantEnd) {
				t.Fatalf("PackEnd() = %v, want %v", got, tt.wantEnd)
			}
			if !slices.Equal(items, original) {
				t.Fatalf("packing changed input: got %v, want %v", items, original)
			}
		})
	}
}

func TestPackingFIFOSingleLookbackEarlyStop(t *testing.T) {
	for _, itemCount := range []int{1, 100} {
		t.Run(strconv.Itoa(itemCount), func(t *testing.T) {
			consumed := 0
			input := func(yield func(int) bool) {
				for i := range itemCount {
					consumed++
					if !yield(i) {
						return
					}
				}
			}
			weightCalls := 0
			weight := func(int) int64 {
				weightCalls++

				return 1
			}
			yielded := 0
			for bin := range internal.PackingIterator(input, 1, 1, weight, false) {
				yielded++
				if !slices.Equal(bin, []int{0}) {
					t.Fatalf("first bin = %v, want [0]", bin)
				}

				break
			}
			wantConsumed := min(itemCount, 2)
			if yielded != 1 || consumed != wantConsumed || weightCalls != wantConsumed {
				t.Fatalf("yielded=%d consumed=%d weightCalls=%d; want 1, %d, %d",
					yielded, consumed, weightCalls, wantConsumed, wantConsumed)
			}
		})
	}
}

var fifoPackingBenchmarkSink [][]int64

func BenchmarkSlicePackerFIFO(b *testing.B) {
	for _, lookback := range []int{1, 10, 64} {
		for _, workload := range []string{"single", "mixed", "small"} {
			for _, method := range []string{"Pack", "PackEnd"} {
				b.Run(method+"/lookback="+strconv.Itoa(lookback)+"/"+workload, func(b *testing.B) {
					items := make([]int64, 4096)
					for i := range items {
						switch workload {
						case "single":
							items[i] = 65 + int64(i%64)
						case "mixed":
							items[i] = 1 + int64((i*73)%128)
						case "small":
							items[i] = 1 + int64(i%8)
						}
					}
					packer := internal.SlicePacker[int64]{TargetWeight: 128, Lookback: lookback}
					pack := packer.Pack
					if method == "PackEnd" {
						pack = packer.PackEnd
					}
					weight := func(v int64) int64 { return v }

					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						fifoPackingBenchmarkSink = pack(items, weight)
					}
				})
			}
		}
	}
}
