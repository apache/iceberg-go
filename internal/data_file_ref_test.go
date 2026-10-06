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

type splitOffsetsCollectionsTestFile struct {
	offsets              []int64
	splitOffsetsCalls    int
	otherCollectionCalls int
}

func (f *splitOffsetsCollectionsTestFile) ColumnSizes() map[int]int64 {
	f.otherCollectionCalls++

	return nil
}

func (f *splitOffsetsCollectionsTestFile) KeyMetadata() []byte {
	f.otherCollectionCalls++

	return nil
}

func (f *splitOffsetsCollectionsTestFile) SplitOffsets() []int64 {
	f.splitOffsetsCalls++

	return f.offsets
}

func (f *splitOffsetsCollectionsTestFile) EqualityFieldIDs() []int {
	f.otherCollectionCalls++

	return nil
}

type splitOffsetsReferenceTestFile struct {
	splitOffsetsCollectionsTestFile
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
	return []struct {
		name    string
		offsets []int64
	}{
		{name: "nil"},
		{name: "empty", offsets: []int64{}},
		{name: "populated", offsets: []int64{12, 40, 96}},
	}
}

func TestBorrowedDataFileSplitOffsetsUsesPublicFallbackOnly(t *testing.T) {
	for _, tt := range splitOffsetsTestCases() {
		t.Run(tt.name, func(t *testing.T) {
			file := &splitOffsetsCollectionsTestFile{offsets: tt.offsets}

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
			if file.otherCollectionCalls != 0 {
				t.Fatalf("other collection getters called %d times, want 0", file.otherCollectionCalls)
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
			if file.splitOffsetsCalls != 0 || file.otherCollectionCalls != 0 {
				t.Fatal("public collection getters were called for borrowed offsets")
			}
		})
	}
}
