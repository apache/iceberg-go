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

var benchmarkKeepMask []byte

func BenchmarkKeepMaskBytes(b *testing.B) {
	const length int64 = 1 << 20

	for _, tt := range []struct {
		name       string
		deletions  int
		outOfRange bool
		dense      bool
	}{
		{name: "sparse-10", deletions: 10},
		{name: "sparse-1000", deletions: 1000},
		{name: "medium-100000", deletions: 100000},
		{name: "dense-range", dense: true},
		{name: "sparse-out-of-range", deletions: 1, outOfRange: true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			bitmap := NewRoaringPositionBitmap()
			benchmarkLength := length
			if tt.outOfRange {
				benchmarkLength = 64
				bitmap.Set(1 << 24)
			} else if tt.dense {
				bitmap.SetRange(0, uint64(length))
			} else {
				for i := range tt.deletions {
					bitmap.Set(uint64(i) * uint64(length) / uint64(tt.deletions))
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkKeepMask = bitmap.KeepMaskBytes(benchmarkLength)
			}
		})
	}
}
