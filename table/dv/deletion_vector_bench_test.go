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

package dv

import "testing"

var benchmarkSerializedDV []byte

func BenchmarkSerializeDV(b *testing.B) {
	for _, tt := range []struct {
		name              string
		positions         int
		stride            uint64
		explicitPositions []uint64
	}{
		{name: "sparse-1k", positions: 1_000, stride: 1_024},
		{name: "sparse-100k", positions: 100_000, stride: 32},
		{name: "sparse-1m", positions: 1_000_000, stride: 4},
		{name: "two-buckets", explicitPositions: []uint64{0, 1 << 32}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			bitmap := NewRoaringPositionBitmap()
			if tt.explicitPositions != nil {
				for _, position := range tt.explicitPositions {
					bitmap.Set(position)
				}
			} else {
				for i := range tt.positions {
					bitmap.Set(uint64(i) * tt.stride)
				}
			}

			// SerializeDV run-length-optimizes the bitmap in place. Warm up once
			// so the timed loop measures steady-state re-serialization.
			sample, err := SerializeDV(bitmap)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(len(sample)))
			for range b.N {
				benchmarkSerializedDV, err = SerializeDV(bitmap)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
