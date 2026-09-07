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
	"testing"
)

// TestPFORSpecVectors decodes the worked byte examples from format/mumbling-spec.md.
func TestPFORSpecVectors(t *testing.T) {
	cases := []struct {
		name  string
		bytes []byte
		count int
		want  []int
	}{
		{"all-zero-256", []byte{0x00, 0x00, 0x00}, 256, repeat(0, 256)},
		{"all-five-51", []byte{0x00, 0x00, 0x05}, 51, repeat(5, 51)},
		{"two-exceptions-8", []byte{0x80, 0x02, 0x00, 0x04, 0x07, 0xFF, 0xFE}, 8, []int{0, 0, 0, 0, 0xFF, 0, 0, 0xFE}},
		{"width2-no-exc-3", []byte{0x02, 0x00, 0x06, 0x18}, 3, []int{6, 7, 8}},
		{"width2-one-exc-4", []byte{0x32, 0x01, 0x06, 0x09, 0x01, 0xE0}, 4, []int{6, 34, 8, 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := make([]int, tc.count)
			read, err := decodeChunks(tc.bytes, 0, out, tc.count)
			if err != nil {
				t.Fatalf("decodeChunks error: %v", err)
			}
			if read != len(tc.bytes) {
				t.Fatalf("bytes read = %d, want %d", read, len(tc.bytes))
			}
			if !equal(out, tc.want) {
				t.Fatalf("decoded = %v, want %v", out, tc.want)
			}
		})
	}
}

// TestSparseContainer checks IsSet/Cardinality for the spec's sparse example (positions 0, 34, 255).
func TestSparseContainer(t *testing.T) {
	buf := []byte{
		0x01, 0x03, 0x00, 0x00, 0x01, 0x00, // header: version 1, cardinality 3, 1 container
		0x00, 0x00, 0x03, // descriptors: PFOR of [3]
		0x00, 0x22, 0xFF, // sparse container: positions 0, 34, 255
	}
	bm, err := New(buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if bm.Cardinality() != 3 {
		t.Fatalf("cardinality = %d, want 3", bm.Cardinality())
	}
	assertSet(t, bm, map[int]bool{0: true, 34: true, 255: true, 1: false, 33: false, 256: false})
}

// TestDenseContainer checks IsSet/Cardinality for a dense container with positions 0-31 set.
func TestDenseContainer(t *testing.T) {
	buf := []byte{0x01, 0x20, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20} // header + descriptors: PFOR of [0x20]
	container := make([]byte, denseBytes)
	container[0], container[1], container[2], container[3] = 0xFF, 0xFF, 0xFF, 0xFF
	buf = append(buf, container...)

	bm, err := New(buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if bm.Cardinality() != 32 {
		t.Fatalf("cardinality = %d, want 32", bm.Cardinality())
	}
	assertSet(t, bm, map[int]bool{0: true, 15: true, 31: true, 32: false, 100: false})
}

func TestHeaderErrors(t *testing.T) {
	if _, err := New([]byte{0x01, 0x00}); !errors.Is(err, ErrTruncated) {
		t.Fatalf("too-small buffer: want ErrTruncated, got %v", err)
	}
	if _, err := New([]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00}); !errors.Is(err, ErrMalformed) {
		t.Fatalf("unsupported version: want ErrMalformed, got %v", err)
	}
}

func assertSet(t *testing.T, bm *Bitmap, expect map[int]bool) {
	t.Helper()
	for pos, want := range expect {
		got, err := bm.IsSet(pos)
		if err != nil {
			t.Fatalf("IsSet(%d): %v", pos, err)
		}
		if got != want {
			t.Errorf("IsSet(%d) = %v, want %v", pos, got, want)
		}
	}
}

func repeat(v, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = v
	}

	return out
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
