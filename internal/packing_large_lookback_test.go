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
	"reflect"
	"slices"
	"testing"
)

func TestLargeLookbackIndexedSelection(t *testing.T) {
	tests := []struct {
		name     string
		count    int
		lookback int
		target   int64
		largest  bool
		want     bool
	}{
		{"small existing indexed plan", 64, 32, 128, false, true},
		{"old indexed boundary", 16_384, 8192, 128, false, true},
		{"past old limit", 16_384, 8193, 128, false, true},
		{"large lookback", 24_000, 10_000, 128, false, true},
		{"effectively unbounded", 16_384, math.MaxInt, 128, false, true},
		{"below large plan cutoff", 16_383, math.MaxInt, 128, false, false},
		{"small input", 8000, 10_000, 128, false, false},
		{"largest bin first", 24_000, 10_000, 128, true, false},
		{"negative target", 24_000, 10_000, -1, false, false},
		{"overflowing target", 24_000, 10_000, math.MaxInt64, false, false},
		{"zero lookback", 24_000, 0, 128, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packer := SlicePacker[int]{
				TargetWeight:    tt.target,
				Lookback:        tt.lookback,
				LargestBinFirst: tt.largest,
			}
			if got := packer.shouldUseIndexedFIFO(tt.count); got != tt.want {
				t.Fatalf("shouldUseIndexedFIFO(%d) = %t, want %t", tt.count, got, tt.want)
			}
		})
	}
}

func TestLargeLookbackPackingMatchesLinear(t *testing.T) {
	tests := []struct {
		name     string
		count    int
		lookback int
		mode     string
	}{
		{"old index limit", 16_384, 8193, "large"},
		{"many open bins", 20_000, 10_000, "mixed"},
		{"unbounded lookback", 16_384, math.MaxInt, "large"},
		{"fall back after negative weight", 16_384, 8193, "negative"},
		{"fall back after oversized weight", 16_384, 8193, "oversized"},
		{"single bin, no eager ring", 16_384, math.MaxInt, "single"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := make([]int, tt.count)
			weights := make([]int64, tt.count)
			rng := rand.New(rand.NewSource(42))
			for i := range items {
				items[i] = i
				switch tt.mode {
				case "mixed":
					weights[i] = int64(1 + rng.Intn(128))
				case "single":
					weights[i] = 0
				default:
					weights[i] = int64(65 + i%64)
				}
			}
			if tt.mode == "negative" {
				weights[tt.count/2] = -1
			}
			if tt.mode == "oversized" {
				weights[tt.count/2] = math.MaxInt64
			}
			weight := func(index int) int64 { return weights[index] }
			packer := SlicePacker[int]{TargetWeight: 128, Lookback: tt.lookback}
			want := slices.Collect(PackingIterator(
				slices.Values(items), 128, tt.lookback, weight, false))
			if got := packer.Pack(items, weight); !reflect.DeepEqual(got, want) {
				t.Fatalf("Pack differs from linear first-fit (%d items, lookback %d)",
					tt.count, tt.lookback)
			}

			reversed := func(yield func(int) bool) {
				for _, item := range slices.Backward(items) {
					if !yield(item) {
						return
					}
				}
			}
			wantEnd := slices.Collect(PackingIterator(
				reversed, 128, tt.lookback, weight, false))
			slices.Reverse(wantEnd)
			for _, bin := range wantEnd {
				slices.Reverse(bin)
			}
			if got := packer.PackEnd(items, weight); !reflect.DeepEqual(got, wantEnd) {
				t.Fatalf("PackEnd differs from linear first-fit (%d items, lookback %d)",
					tt.count, tt.lookback)
			}
		})
	}
}

var largeLookbackPackingBenchmarkSink [][]int64

// This compares the original linear fallback with SlicePacker's selected
// implementation on the same input. Both modes must return the same bins.
func BenchmarkSlicePackerLargeLookback(b *testing.B) {
	for _, tc := range []struct {
		count    int
		lookback int
	}{
		{16_384, 8193},
		{24_000, 10_000},
		{24_000, 1_000_000},
	} {
		b.Run(fmt.Sprintf("files=%d/lookback=%d", tc.count, tc.lookback), func(b *testing.B) {
			items := make([]int64, tc.count)
			for i := range items {
				items[i] = int64(65 + i%64)
			}
			weight := func(item int64) int64 { return item }
			packer := SlicePacker[int64]{TargetWeight: 128, Lookback: tc.lookback}
			for _, mode := range []string{"linear", "indexed"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(tc.count), "files/op")
					b.ResetTimer()
					for range b.N {
						if mode == "linear" {
							largeLookbackPackingBenchmarkSink = slices.Collect(PackingIterator(
								slices.Values(items), 128, tc.lookback, weight, false))
						} else {
							largeLookbackPackingBenchmarkSink = packer.Pack(items, weight)
						}
					}
					if len(largeLookbackPackingBenchmarkSink) != tc.count {
						b.Fatalf("expected %d bins, got %d", tc.count, len(largeLookbackPackingBenchmarkSink))
					}
				})
			}
		})
	}
}
