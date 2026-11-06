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
	"errors"
	"math/rand"
	"testing"
)

// TestCapsRejected asserts New rejects headers that exceed the spec caps.
func TestCapsRejected(t *testing.T) {
	// container count 8193 (> 8192): bytes 4-5 LE = 0x2001
	over := []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x20}
	if _, err := New(over); !errors.Is(err, ErrCapExceeded) {
		t.Fatalf("container count > 8192: want ErrCapExceeded, got %v", err)
	}
	// container count exactly 8192 is allowed by the header check
	atCap := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x20}
	if _, err := New(atCap); err != nil {
		t.Fatalf("8192 containers should pass the header check: %v", err)
	}
	// cardinality 2,097,153 (> 2,097,152): bytes 1-3 LE
	c := maxCardinality + 1
	overCard := []byte{0x01, byte(c), byte(c >> 8), byte(c >> 16), 0x01, 0x00}
	if _, err := New(overCard); !errors.Is(err, ErrCapExceeded) {
		t.Fatalf("cardinality > 2,097,152: want ErrCapExceeded, got %v", err)
	}
}

// TestMalformedNeverPanics feeds adversarial byte slices and requires that
// New + IsSet return an error or a value, never panic.
func TestMalformedNeverPanics(t *testing.T) {
	seeds := [][]byte{
		{},                             // empty
		{0x01},                         // partial header
		{0x01, 0x00, 0x00, 0x00, 0x00}, // 5 bytes (one short)
		// version ok, claims 1 container but no descriptor/container bytes
		{0x01, 0x01, 0x00, 0x00, 0x01, 0x00},
		// claims 3 containers, truncated mid-descriptor
		{0x01, 0x03, 0x00, 0x00, 0x03, 0x00, 0x00},
		// dense descriptor (0x20) but no 32-byte container follows
		{0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20},
		// 1 container (partial chunk, len 1); PFOR chunk declares 1 exception
		// whose offset byte (5) exceeds the chunk length -> must error, not panic
		{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x05},
	}
	rng := rand.New(rand.NewSource(20260803))
	for i := 0; i < 2000; i++ {
		n := rng.Intn(40)
		b := make([]byte, n)
		for j := range b {
			b[j] = byte(rng.Intn(256))
		}
		seeds = append(seeds, b)
	}

	for idx, b := range seeds {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("seed %d (% X) panicked: %v", idx, b, r)
				}
			}()
			bm, err := New(b)
			if err != nil {
				return // rejected cleanly - fine
			}
			// probe a spread of positions across the container space
			for _, pos := range []int{0, 1, 255, 256, 511, 1 << 20, 1<<21 - 1} {
				_, _ = bm.IsSet(pos) // must not panic; error is acceptable
			}
		}()
	}
}

// TestExceptionOffsetOutOfRange feeds a partial chunk (len 1) whose PFOR
// exception offset points past the chunk. IsSet must return an error, not panic.
func TestExceptionOffsetOutOfRange(t *testing.T) {
	buf := []byte{
		0x01, 0x00, 0x00, 0x00, 0x01, 0x00, // v1, card 0, containerCount 1
		0x01, 0x01, 0x00, // chunk: b1=1, b2=0, excCount=1, base=0
		0x00, // primary: 1 value * 1 bit -> 1 byte
		0x05, // exception offset = 5 (>= chunk len 1)
	}
	bm, err := New(buf)
	if err != nil {
		t.Fatalf("New should accept the header: %v", err)
	}
	if _, err := bm.IsSet(0); !errors.Is(err, ErrMalformed) {
		t.Fatalf("out-of-range exception offset: want ErrMalformed, got %v", err)
	}
}
