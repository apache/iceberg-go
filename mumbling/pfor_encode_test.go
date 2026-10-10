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
	"bytes"
	"math/rand"
	"testing"
)

// TestPForEncodeSpecBytes checks the encoder against the worked byte examples in
// format/mumbling-spec.md.
// The tie-break case (last) is the one where a naive port would diverge: the
// impl prefers the larger primary width on a size tie, so [6,34,8,7] encodes to
// 05 00 06 07 04 10 (NOT the spec's decode-fixture 32 01 06 09 01 E0). Matching
// this exactly is the strongest single signal of cross-client agreement.
func TestPForEncodeSpecBytes(t *testing.T) {
	cases := []struct {
		name   string
		values []int
		want   []byte
	}{
		{"all-zero-256", repeat(0, 256), []byte{0x00, 0x00, 0x00}},
		{"all-five-51", repeat(5, 51), []byte{0x00, 0x00, 0x05}},
		{"two-exceptions-8", []int{0, 0, 0, 0, 0xFF, 0, 0, 0xFE}, []byte{0x80, 0x02, 0x00, 0x04, 0x07, 0xFF, 0xFE}},
		{"width2-no-exc-3", []int{6, 7, 8}, []byte{0x02, 0x00, 0x06, 0x18}},
		{"width2-tiebreak-4", []int{6, 34, 8, 7}, []byte{0x05, 0x00, 0x06, 0x07, 0x04, 0x10}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pforEncode(tc.values)
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("encode = % X, want % X", got, tc.want)
			}
			// and it must decode back
			back, err := pforDecode(got, len(tc.values))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !equal(back, tc.values) {
				t.Fatalf("round-trip = %v, want %v", back, tc.values)
			}
		})
	}
}

// TestPForRoundTripFuzz encodes then decodes random unsigned-byte arrays of
// varied lengths (crossing the 256-value chunk boundary) and asserts identity.
func TestPForRoundTripFuzz(t *testing.T) {
	rng := rand.New(rand.NewSource(1938745))
	for _, n := range []int{1, 7, 8, 9, 255, 256, 257, 512, 1000} {
		for trial := 0; trial < 50; trial++ {
			vals := make([]int, n)
			maxV := rng.Intn(256) // random dynamic range
			for i := range vals {
				vals[i] = rng.Intn(maxV + 1)
			}
			enc := pforEncode(vals)
			dec, err := pforDecode(enc, n)
			if err != nil {
				t.Fatalf("n=%d decode: %v", n, err)
			}
			if !equal(dec, vals) {
				t.Fatalf("n=%d round-trip mismatch", n)
			}
		}
	}
}
