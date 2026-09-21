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

package mumbling

import (
	"math/rand"
	"testing"
)

// descriptor distributions modelling the container-descriptor arrays that PFOR
// compresses in a Mumbling bitmap. Each value is a container descriptor byte:
// 0x20 = dense (32-byte container), 0-31 = sparse length.
func makeDescriptors(kind string, n int, rng *rand.Rand) (descs []int, containerBytes int) {
	descs = make([]int, n)
	for i := range descs {
		switch kind {
		case "all-dense":
			descs[i] = 0x20
		case "uniform-sparse": // every container holds 5 positions
			descs[i] = 5
		case "random-sparse": // 0-31 set positions
			descs[i] = rng.Intn(32)
		case "mixed": // ~half dense, half sparse
			if rng.Intn(2) == 0 {
				descs[i] = 0x20
			} else {
				descs[i] = rng.Intn(32)
			}
		}
		if descs[i]&denseContainerBit == denseContainerBit {
			containerBytes += denseBytes
		} else {
			containerBytes += descs[i]
		}
	}

	return descs, containerBytes
}

// TestPFORSizeAnalysis reports descriptor-array size with PFOR on vs off, and the
// net whole-bitmap size saved, per (distribution, container count).
func TestPFORSizeAnalysis(t *testing.T) {
	rng := rand.New(rand.NewSource(1938745))
	counts := []int{8, 64, 256, 1024, 8192}
	kinds := []string{"all-dense", "uniform-sparse", "random-sparse", "mixed"}

	t.Logf("%-15s %6s %8s %8s %8s  %10s  %14s  %12s",
		"distribution", "nCont", "rawDesc", "pforDesc", "descCmp", "container", "descFracTotal", "wholeSaved")
	for _, kind := range kinds {
		for _, n := range counts {
			descs, containerBytes := makeDescriptors(kind, n, rng)
			rawDesc := n // PFOR OFF: 1 byte per descriptor
			pforDesc := len(pforEncode(descs))
			totalOff := headerSize + rawDesc + containerBytes
			totalOn := headerSize + pforDesc + containerBytes
			descCmp := float64(pforDesc) / float64(rawDesc)             // PFOR desc / raw desc
			descFrac := float64(rawDesc) / float64(totalOff)            // descriptors as share of bitmap
			wholeSaved := float64(totalOff-totalOn) / float64(totalOff) // net saving on whole bitmap
			t.Logf("%-15s %6d %8d %8d %7.1f%% %10d %13.1f%% %11.2f%%",
				kind, n, rawDesc, pforDesc, descCmp*100, containerBytes, descFrac*100, wholeSaved*100)

			// sanity: decoding the PFOR descriptors reproduces them
			back, err := pforDecode(pforEncode(descs), n)
			if err != nil || !equal(back, descs) {
				t.Fatalf("%s n=%d: descriptor round-trip failed", kind, n)
			}
		}
	}
}

// --- Benchmarks: PFOR vs raw (the "PFOR off" alternative) ---

func benchDescriptors(n int) []int {
	rng := rand.New(rand.NewSource(1938745))
	d, _ := makeDescriptors("mixed", n, rng)

	return d
}

func BenchmarkPForEncode1024(b *testing.B) {
	descs := benchDescriptors(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pforEncode(descs)
	}
}

func BenchmarkPForDecode1024(b *testing.B) {
	descs := benchDescriptors(1024)
	enc := pforEncode(descs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pforDecode(enc, len(descs))
	}
}

// BenchmarkRawEncode1024 is the "PFOR off" baseline: descriptors stored raw.
func BenchmarkRawEncode1024(b *testing.B) {
	descs := benchDescriptors(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]byte, len(descs))
		for j, d := range descs {
			out[j] = byte(d)
		}
		_ = out
	}
}

func BenchmarkRawDecode1024(b *testing.B) {
	descs := benchDescriptors(1024)
	raw := make([]byte, len(descs))
	for j, d := range descs {
		raw[j] = byte(d)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]int, len(raw))
		for j, x := range raw {
			out[j] = int(x)
		}
		_ = out
	}
}
