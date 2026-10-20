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

import (
	"math/rand"
	"testing"
)

func TestKeepMaskForRange(t *testing.T) {
	const boundary uint64 = 1 << 32

	for _, tc := range []struct {
		name      string
		start     uint64
		length    uint64
		positions []uint64
	}{
		{name: "empty range", start: 100, positions: []uint64{100}},
		{name: "clean range", start: 100, length: 9, positions: []uint64{99, 109}},
		{name: "unaligned offset", start: 5, length: 9, positions: []uint64{5, 7, 11, 13}},
		{name: "one byte", start: 10, length: 8, positions: []uint64{10, 17}},
		{name: "across buckets", start: boundary - 3, length: 9, positions: []uint64{boundary - 1, boundary, boundary + 3}},
		{name: "high bucket", start: (1 << 63) + 11, length: 11, positions: []uint64{(1 << 63) + 12, (1 << 63) + 20}},
		{name: "dense", start: 101, length: 25},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bitmap := NewRoaringPositionBitmap()
			for _, pos := range tc.positions {
				bitmap.Set(pos)
			}
			if tc.name == "dense" {
				bitmap.SetRange(100, 128)
				bitmap.RunLengthEncode()
			}

			assertKeepMaskForRange(t, bitmap, tc.start, tc.length)
		})
	}
}

func TestKeepMaskForRangeRandomized(t *testing.T) {
	random := rand.New(rand.NewSource(27))
	for _, base := range []uint64{0, 31, (1 << 32) - 50, 1 << 32, (3 << 32) + 50} {
		for _, length := range []uint64{0, 1, 7, 8, 9, 63, 64, 127} {
			bitmap := NewRoaringPositionBitmap()
			for range 80 {
				bitmap.Set(base + uint64(random.Intn(150)))
			}
			assertKeepMaskForRange(t, bitmap, base, length)
		}
	}
}

func assertKeepMaskForRange(t *testing.T, bitmap *RoaringPositionBitmap, start, length uint64) {
	t.Helper()
	got := bitmap.KeepMaskForRange(start, start+length)
	hasDelete := false
	for i := range length {
		if bitmap.Contains(start + i) {
			hasDelete = true

			break
		}
	}
	if !hasDelete {
		if got != nil {
			t.Fatalf("clean range [%d, %d): expected nil mask, got %v", start, start+length, got)
		}

		return
	}
	if want := int((length + 7) / 8); len(got) != want {
		t.Fatalf("mask length = %d, want %d", len(got), want)
	}
	for i := range length {
		keep := got[i>>3]&(1<<(i&7)) != 0
		if want := !bitmap.Contains(start + i); keep != want {
			t.Errorf("position %d: keep = %t, want %t", start+i, keep, want)
		}
	}
	for i := length; i < uint64(len(got)*8); i++ {
		if got[i>>3]&(1<<(i&7)) != 0 {
			t.Errorf("padding bit %d not cleared", i)
		}
	}
}
