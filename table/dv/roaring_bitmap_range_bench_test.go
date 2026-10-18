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

var keepMaskBenchmarkSink []byte

func BenchmarkKeepMaskBatchRange(b *testing.B) {
	const (
		fileRows  int64  = 10_000_000
		start     uint64 = 4_000_000
		batchSize uint64 = 65_536
	)
	bitmap := NewRoaringPositionBitmap()
	for i := range uint64(16) {
		bitmap.Set(start + 1 + i*2_048)
	}
	bitmap.Set(uint64(fileRows - 1))

	b.Run("full_file", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			keepMaskBenchmarkSink = bitmap.KeepMaskBytes(fileRows)
		}
	})
	b.Run("affected_batch", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			keepMaskBenchmarkSink = bitmap.KeepMaskForRange(start, start+batchSize)
		}
	})
	b.Run("clean_batch", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			keepMaskBenchmarkSink = bitmap.KeepMaskForRange(0, batchSize)
		}
	})
}
