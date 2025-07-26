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

package deletes

import (
	"fmt"
	"iter"
	"slices"
	"strings"
)

// SetPositionDeleteIndex uses a set-based approach for storing position deletes.
// This is efficient for cases where deletes are sparse within files.
type SetPositionDeleteIndex struct {
	// fileDeletes maps file paths to sets of deleted positions
	fileDeletes map[string]map[int64]struct{}
	// totalCount tracks the total number of position deletes across all files
	totalCount int
}

// NewSetPositionDeleteIndex creates a new set-based position delete index.
func NewSetPositionDeleteIndex() *SetPositionDeleteIndex {
	return &SetPositionDeleteIndex{
		fileDeletes: make(map[string]map[int64]struct{}),
		totalCount:  0,
	}
}

// IsDeleted returns true if the position is marked as deleted in the given file.
func (idx *SetPositionDeleteIndex) IsDeleted(filePath string, position int64) bool {
	positions, exists := idx.fileDeletes[filePath]
	if !exists || position < 0 {
		return false
	}
	_, isDeleted := positions[position]
	return isDeleted
}

// Get returns all deleted positions for the given file path.
func (idx *SetPositionDeleteIndex) Get(filePath string) []int64 {
	positions, exists := idx.fileDeletes[filePath]
	if !exists {
		return nil
	}
	
	result := make([]int64, 0, len(positions))
	for pos := range positions {
		result = append(result, pos)
	}
	
	// Sort positions for consistent ordering
	slices.Sort(result)
	return result
}

// Add adds a position delete to the index.
func (idx *SetPositionDeleteIndex) Add(filePath string, position int64) {
	if position < 0 {
		return
	}
	
	positions, exists := idx.fileDeletes[filePath]
	if !exists {
		positions = make(map[int64]struct{})
		idx.fileDeletes[filePath] = positions
	}
	
	// Check if position is already deleted
	if _, alreadyDeleted := positions[position]; !alreadyDeleted {
		positions[position] = struct{}{}
		idx.totalCount++
	}
}

// Remove removes a position delete from the index.
func (idx *SetPositionDeleteIndex) Remove(filePath string, position int64) bool {
	positions, exists := idx.fileDeletes[filePath]
	if !exists || position < 0 {
		return false
	}
	
	if _, isDeleted := positions[position]; isDeleted {
		delete(positions, position)
		idx.totalCount--
		
		// Remove the file entry if no more deletes exist for this file
		if len(positions) == 0 {
			delete(idx.fileDeletes, filePath)
		}
		return true
	}
	return false
}

// Files returns an iterator over all file paths that have position deletes.
func (idx *SetPositionDeleteIndex) Files() iter.Seq[string] {
	return func(yield func(string) bool) {
		for filePath := range idx.fileDeletes {
			if !yield(filePath) {
				return
			}
		}
	}
}

// Deletes returns an iterator over all position deletes for a given file.
func (idx *SetPositionDeleteIndex) Deletes(filePath string) iter.Seq[int64] {
	return func(yield func(int64) bool) {
		positions, exists := idx.fileDeletes[filePath]
		if !exists {
			return
		}
		
		// Get sorted positions for consistent iteration order
		sortedPositions := make([]int64, 0, len(positions))
		for pos := range positions {
			sortedPositions = append(sortedPositions, pos)
		}
		slices.Sort(sortedPositions)
		
		for _, pos := range sortedPositions {
			if !yield(pos) {
				return
			}
		}
	}
}

// Count returns the total number of position deletes across all files.
func (idx *SetPositionDeleteIndex) Count() int {
	return idx.totalCount
}

// CountForFile returns the number of position deletes for a specific file.
func (idx *SetPositionDeleteIndex) CountForFile(filePath string) int {
	positions, exists := idx.fileDeletes[filePath]
	if !exists {
		return 0
	}
	return len(positions)
}

// IsEmpty returns true if there are no position deletes in the index.
func (idx *SetPositionDeleteIndex) IsEmpty() bool {
	return idx.totalCount == 0
}

// Clear removes all position deletes from the index.
func (idx *SetPositionDeleteIndex) Clear() {
	idx.fileDeletes = make(map[string]map[int64]struct{})
	idx.totalCount = 0
}

// String returns a string representation of the index.
func (idx *SetPositionDeleteIndex) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("SetPositionDeleteIndex{files=%d, totalDeletes=%d, ", 
		len(idx.fileDeletes), idx.totalCount))
	
	if len(idx.fileDeletes) <= 3 {
		sb.WriteString("deletes=[")
		first := true
		for filePath, positions := range idx.fileDeletes {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s:%d", filePath, len(positions)))
			first = false
		}
		sb.WriteString("]")
	} else {
		sb.WriteString("deletes=...")
	}
	
	sb.WriteString("}")
	return sb.String()
}

// SetPositionDeleteIndexBuilder builds a set-based position delete index.
type SetPositionDeleteIndexBuilder struct {
	index *SetPositionDeleteIndex
}

// NewSetPositionDeleteIndexBuilder creates a new builder for set position delete indexes.
func NewSetPositionDeleteIndexBuilder() *SetPositionDeleteIndexBuilder {
	return &SetPositionDeleteIndexBuilder{
		index: NewSetPositionDeleteIndex(),
	}
}

// Add adds a position delete to the builder.
func (b *SetPositionDeleteIndexBuilder) Add(filePath string, position int64) PositionDeleteIndexBuilder {
	b.index.Add(filePath, position)
	return b
}

// AddPositionDelete adds a PositionDelete to the builder.
func (b *SetPositionDeleteIndexBuilder) AddPositionDelete(delete *PositionDelete) PositionDeleteIndexBuilder {
	if delete != nil {
		b.index.Add(delete.FilePath, delete.Position)
	}
	return b
}

// AddAll adds multiple position deletes to the builder.
func (b *SetPositionDeleteIndexBuilder) AddAll(filePath string, positions []int64) PositionDeleteIndexBuilder {
	for _, pos := range positions {
		b.index.Add(filePath, pos)
	}
	return b
}

// Build creates the position delete index.
func (b *SetPositionDeleteIndexBuilder) Build() PositionDeleteIndex {
	return b.index
} 