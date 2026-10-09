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
	"slices"
	"testing"
)

type splitOffsetsTestFile struct {
	offsets           []int64
	splitOffsetsCalls int
}

func (f *splitOffsetsTestFile) SplitOffsets() []int64 {
	f.splitOffsetsCalls++

	return f.offsets
}

type splitOffsetsReferenceTestFile struct {
	splitOffsetsTestFile
	borrowedOffsets []int64
	refCalls        int
}

func (f *splitOffsetsReferenceTestFile) DataFileSplitOffsetsRef(DataFileRef) []int64 {
	f.refCalls++

	return f.borrowedOffsets
}

func splitOffsetsTestCases() []struct {
	name    string
	offsets []int64
} {
	populated := make([]int64, 3, 8)
	copy(populated, []int64{12, 40, 96})

	return []struct {
		name    string
		offsets []int64
	}{
		{name: "nil"},
		{name: "empty", offsets: []int64{}},
		{name: "populated with spare capacity", offsets: populated},
	}
}

func TestBorrowedDataFileSplitOffsetsUsesPublicFallbackOnly(t *testing.T) {
	for _, tt := range splitOffsetsTestCases() {
		t.Run(tt.name, func(t *testing.T) {
			file := &splitOffsetsTestFile{offsets: tt.offsets}

			got := BorrowedDataFileSplitOffsets(file)
			if !slices.Equal(got, tt.offsets) || (got == nil) != (tt.offsets == nil) {
				t.Fatalf("BorrowedDataFileSplitOffsets() = %v, want %v", got, tt.offsets)
			}
			if cap(got) != len(got) {
				t.Fatalf("BorrowedDataFileSplitOffsets() capacity = %d, want %d", cap(got), len(got))
			}
			if file.splitOffsetsCalls != 1 {
				t.Fatalf("SplitOffsets() called %d times, want 1", file.splitOffsetsCalls)
			}
		})
	}
}

func TestBorrowedDataFileSplitOffsetsUsesBorrowedReference(t *testing.T) {
	for _, tt := range splitOffsetsTestCases() {
		t.Run(tt.name, func(t *testing.T) {
			file := &splitOffsetsReferenceTestFile{borrowedOffsets: tt.offsets}

			got := BorrowedDataFileSplitOffsets(file)
			if !slices.Equal(got, tt.offsets) || (got == nil) != (tt.offsets == nil) {
				t.Fatalf("BorrowedDataFileSplitOffsets() = %v, want %v", got, tt.offsets)
			}
			if len(got) > 0 && &got[0] != &tt.offsets[0] {
				t.Fatal("BorrowedDataFileSplitOffsets() copied the borrowed offsets")
			}
			if cap(got) != len(got) {
				t.Fatalf("BorrowedDataFileSplitOffsets() capacity = %d, want %d", cap(got), len(got))
			}
			if file.refCalls != 1 {
				t.Fatalf("DataFileSplitOffsetsRef() called %d times, want 1", file.refCalls)
			}
			if file.splitOffsetsCalls != 0 {
				t.Fatal("public getter was called for borrowed offsets")
			}
		})
	}
}
