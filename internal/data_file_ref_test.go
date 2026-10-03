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

func TestBorrowedDataFileSplitOffsetsUsesPublicFallbackOnly(t *testing.T) {
	want := []int64{12, 40, 96}
	file := &splitOffsetsCollectionsTestFile{offsets: want}

	got := BorrowedDataFileSplitOffsets(file)
	if !slices.Equal(got, want) {
		t.Fatalf("BorrowedDataFileSplitOffsets() = %v, want %v", got, want)
	}
	if file.splitOffsetsCalls != 1 {
		t.Fatalf("SplitOffsets() called %d times, want 1", file.splitOffsetsCalls)
	}
	if file.otherCollectionCalls != 0 {
		t.Fatalf("other collection getters called %d times, want 0", file.otherCollectionCalls)
	}
}

func TestBorrowedDataFileSplitOffsetsUsesBorrowedReference(t *testing.T) {
	borrowed := []int64{12, 40, 96}
	file := &splitOffsetsReferenceTestFile{borrowedOffsets: borrowed}

	got := BorrowedDataFileSplitOffsets(file)
	if !slices.Equal(got, borrowed) {
		t.Fatalf("BorrowedDataFileSplitOffsets() = %v, want %v", got, borrowed)
	}
	if len(got) > 0 && &got[0] != &borrowed[0] {
		t.Fatal("BorrowedDataFileSplitOffsets() copied the borrowed offsets")
	}
	if file.refCalls != 1 {
		t.Fatalf("DataFileSplitOffsetsRef() called %d times, want 1", file.refCalls)
	}
	if file.splitOffsetsCalls != 0 || file.otherCollectionCalls != 0 {
		t.Fatal("public collection getters were called for borrowed offsets")
	}
}
