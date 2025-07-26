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
	"math"
	"slices"
	"strings"
)

// BitmapPositionDeleteIndex uses a bitmap-based approach for storing position deletes.
// This is efficient for cases where deletes are dense or clustered within files.
type BitmapPositionDeleteIndex struct {
	// fileDeletes maps file paths to their corresponding position delete bitmaps
	fileDeletes map[string]*positionBitmap
	// totalCount tracks the total number of position deletes across all files
	totalCount int
}

// positionBitmap represents a bitmap for tracking deleted positions in a file.
type positionBitmap struct {
	// bits stores the actual bitmap data
	bits []uint64
	// count tracks the number of set bits (deleted positions)
	count int
	// maxPos tracks the maximum position that has been set
	maxPos int64
}

const (
	// bitsPerWord is the number of bits in a uint64
	bitsPerWord = 64
	// logBitsPerWord is log2(bitsPerWord) for efficient division
	logBitsPerWord = 6
)

// NewBitmapPositionDeleteIndex creates a new bitmap-based position delete index.
func NewBitmapPositionDeleteIndex() *BitmapPositionDeleteIndex {
	return &BitmapPositionDeleteIndex{
		fileDeletes: make(map[string]*positionBitmap),
		totalCount:  0,
	}
}

// newPositionBitmap creates a new position bitmap.
func newPositionBitmap() *positionBitmap {
	return &positionBitmap{
		bits:   make([]uint64, 0),
		count:  0,
		maxPos: -1,
	}
}

// IsDeleted returns true if the position is marked as deleted in the given file.
func (idx *BitmapPositionDeleteIndex) IsDeleted(filePath string, position int64) bool {
	bitmap, exists := idx.fileDeletes[filePath]
	if !exists || position < 0 {
		return false
	}
	return bitmap.isSet(position)
}

// Get returns all deleted positions for the given file path.
func (idx *BitmapPositionDeleteIndex) Get(filePath string) []int64 {
	bitmap, exists := idx.fileDeletes[filePath]
	if !exists {
		return nil
	}
	return bitmap.getSetPositions()
}

// Add adds a position delete to the index.
func (idx *BitmapPositionDeleteIndex) Add(filePath string, position int64) {
	if position < 0 {
		return
	}
	
	bitmap, exists := idx.fileDeletes[filePath]
	if !exists {
		bitmap = newPositionBitmap()
		idx.fileDeletes[filePath] = bitmap
	}
	
	if bitmap.set(position) {
		idx.totalCount++
	}
}

// Remove removes a position delete from the index.
func (idx *BitmapPositionDeleteIndex) Remove(filePath string, position int64) bool {
	bitmap, exists := idx.fileDeletes[filePath]
	if !exists || position < 0 {
		return false
	}
	
	if bitmap.unset(position) {
		idx.totalCount--
		
		// Remove the bitmap if it's empty
		if bitmap.count == 0 {
			delete(idx.fileDeletes, filePath)
		}
		return true
	}
	return false
}

// Files returns an iterator over all file paths that have position deletes.
func (idx *BitmapPositionDeleteIndex) Files() iter.Seq[string] {
	return func(yield func(string) bool) {
		for filePath := range idx.fileDeletes {
			if !yield(filePath) {
				return
			}
		}
	}
}

// Deletes returns an iterator over all position deletes for a given file.
func (idx *BitmapPositionDeleteIndex) Deletes(filePath string) iter.Seq[int64] {
	return func(yield func(int64) bool) {
		bitmap, exists := idx.fileDeletes[filePath]
		if !exists {
			return
		}
		
		for _, pos := range bitmap.getSetPositions() {
			if !yield(pos) {
				return
			}
		}
	}
}

// Count returns the total number of position deletes across all files.
func (idx *BitmapPositionDeleteIndex) Count() int {
	return idx.totalCount
}

// CountForFile returns the number of position deletes for a specific file.
func (idx *BitmapPositionDeleteIndex) CountForFile(filePath string) int {
	bitmap, exists := idx.fileDeletes[filePath]
	if !exists {
		return 0
	}
	return bitmap.count
}

// IsEmpty returns true if there are no position deletes in the index.
func (idx *BitmapPositionDeleteIndex) IsEmpty() bool {
	return idx.totalCount == 0
}

// Clear removes all position deletes from the index.
func (idx *BitmapPositionDeleteIndex) Clear() {
	idx.fileDeletes = make(map[string]*positionBitmap)
	idx.totalCount = 0
}

// String returns a string representation of the index.
func (idx *BitmapPositionDeleteIndex) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("BitmapPositionDeleteIndex{files=%d, totalDeletes=%d, ", 
		len(idx.fileDeletes), idx.totalCount))
	
	if len(idx.fileDeletes) <= 3 {
		sb.WriteString("deletes=[")
		first := true
		for filePath, bitmap := range idx.fileDeletes {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s:%d", filePath, bitmap.count))
			first = false
		}
		sb.WriteString("]")
	} else {
		sb.WriteString("deletes=...")
	}
	
	sb.WriteString("}")
	return sb.String()
}

// positionBitmap methods

// set sets the bit at the given position and returns true if it was not already set.
func (pb *positionBitmap) set(position int64) bool {
	wordIndex := position >> logBitsPerWord
	bitIndex := position & (bitsPerWord - 1)
	
	// Expand the bitmap if necessary
	for int64(len(pb.bits)) <= wordIndex {
		pb.bits = append(pb.bits, 0)
	}
	
	word := &pb.bits[wordIndex]
	mask := uint64(1) << bitIndex
	
	if *word&mask != 0 {
		return false // Already set
	}
	
	*word |= mask
	pb.count++
	if position > pb.maxPos {
		pb.maxPos = position
	}
	return true
}

// unset clears the bit at the given position and returns true if it was set.
func (pb *positionBitmap) unset(position int64) bool {
	wordIndex := position >> logBitsPerWord
	bitIndex := position & (bitsPerWord - 1)
	
	if wordIndex >= int64(len(pb.bits)) {
		return false
	}
	
	word := &pb.bits[wordIndex]
	mask := uint64(1) << bitIndex
	
	if *word&mask == 0 {
		return false // Already unset
	}
	
	*word &^= mask
	pb.count--
	
	// Update maxPos if we removed the highest position
	if position == pb.maxPos {
		pb.updateMaxPos()
	}
	
	return true
}

// isSet returns true if the bit at the given position is set.
func (pb *positionBitmap) isSet(position int64) bool {
	wordIndex := position >> logBitsPerWord
	bitIndex := position & (bitsPerWord - 1)
	
	if wordIndex >= int64(len(pb.bits)) {
		return false
	}
	
	return pb.bits[wordIndex]&(uint64(1)<<bitIndex) != 0
}

// getSetPositions returns all positions that are set in the bitmap.
func (pb *positionBitmap) getSetPositions() []int64 {
	if pb.count == 0 {
		return nil
	}
	
	positions := make([]int64, 0, pb.count)
	
	for wordIndex, word := range pb.bits {
		if word == 0 {
			continue
		}
		
		basePos := int64(wordIndex) << logBitsPerWord
		for bitIndex := 0; bitIndex < bitsPerWord; bitIndex++ {
			if word&(uint64(1)<<bitIndex) != 0 {
				positions = append(positions, basePos+int64(bitIndex))
			}
		}
	}
	
	return positions
}

// updateMaxPos recalculates the maximum position in the bitmap.
func (pb *positionBitmap) updateMaxPos() {
	pb.maxPos = -1
	
	for wordIndex := len(pb.bits) - 1; wordIndex >= 0; wordIndex-- {
		word := pb.bits[wordIndex]
		if word == 0 {
			continue
		}
		
		// Find the highest set bit in this word
		basePos := int64(wordIndex) << logBitsPerWord
		for bitIndex := bitsPerWord - 1; bitIndex >= 0; bitIndex-- {
			if word&(uint64(1)<<bitIndex) != 0 {
				pb.maxPos = basePos + int64(bitIndex)
				return
			}
		}
	}
}

// BitmapPositionDeleteIndexBuilder builds a bitmap-based position delete index.
type BitmapPositionDeleteIndexBuilder struct {
	index *BitmapPositionDeleteIndex
}

// NewBitmapPositionDeleteIndexBuilder creates a new builder for bitmap position delete indexes.
func NewBitmapPositionDeleteIndexBuilder() *BitmapPositionDeleteIndexBuilder {
	return &BitmapPositionDeleteIndexBuilder{
		index: NewBitmapPositionDeleteIndex(),
	}
}

// Add adds a position delete to the builder.
func (b *BitmapPositionDeleteIndexBuilder) Add(filePath string, position int64) PositionDeleteIndexBuilder {
	b.index.Add(filePath, position)
	return b
}

// AddPositionDelete adds a PositionDelete to the builder.
func (b *BitmapPositionDeleteIndexBuilder) AddPositionDelete(delete *PositionDelete) PositionDeleteIndexBuilder {
	if delete != nil {
		b.index.Add(delete.FilePath, delete.Position)
	}
	return b
}

// AddAll adds multiple position deletes to the builder.
func (b *BitmapPositionDeleteIndexBuilder) AddAll(filePath string, positions []int64) PositionDeleteIndexBuilder {
	for _, pos := range positions {
		b.index.Add(filePath, pos)
	}
	return b
}

// Build creates the position delete index.
func (b *BitmapPositionDeleteIndexBuilder) Build() PositionDeleteIndex {
	return b.index
} 