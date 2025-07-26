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
	"sort"
)

// PositionDelete represents a position delete record that marks a specific
// row at a given position in a data file as deleted.
type PositionDelete struct {
	// FilePath is the path of the data file containing the deleted row
	FilePath string
	// Position is the 0-based row position in the file that is deleted
	Position int64
}

// NewPositionDelete creates a new PositionDelete with the given file path and position.
func NewPositionDelete(filePath string, position int64) *PositionDelete {
	return &PositionDelete{
		FilePath: filePath,
		Position: position,
	}
}

// String returns a string representation of the position delete.
func (pd *PositionDelete) String() string {
	return fmt.Sprintf("PositionDelete{file=%s, pos=%d}", pd.FilePath, pd.Position)
}

// Equal checks if two PositionDelete instances are equal.
func (pd *PositionDelete) Equal(other *PositionDelete) bool {
	if pd == nil && other == nil {
		return true
	}
	if pd == nil || other == nil {
		return false
	}
	return pd.FilePath == other.FilePath && pd.Position == other.Position
}

// Compare compares two PositionDelete instances for ordering.
// Comparison is done first by file path, then by position.
func (pd *PositionDelete) Compare(other *PositionDelete) int {
	if pd == nil && other == nil {
		return 0
	}
	if pd == nil {
		return -1
	}
	if other == nil {
		return 1
	}
	
	// First compare by file path
	if pd.FilePath != other.FilePath {
		if pd.FilePath < other.FilePath {
			return -1
		}
		return 1
	}
	
	// Then compare by position
	if pd.Position < other.Position {
		return -1
	}
	if pd.Position > other.Position {
		return 1
	}
	return 0
}

// PositionDeleteSlice provides sorting functionality for slices of PositionDelete.
type PositionDeleteSlice []*PositionDelete

func (pds PositionDeleteSlice) Len() int           { return len(pds) }
func (pds PositionDeleteSlice) Less(i, j int) bool { return pds[i].Compare(pds[j]) < 0 }
func (pds PositionDeleteSlice) Swap(i, j int)      { pds[i], pds[j] = pds[j], pds[i] }

// Sort sorts the slice of position deletes.
func (pds PositionDeleteSlice) Sort() {
	sort.Sort(pds)
}

// PositionDeleteIndex provides efficient lookups for position deletes by file path.
type PositionDeleteIndex struct {
	deletesByFile map[string][]int64
}

// NewPositionDeleteIndex creates a new index from a slice of position deletes.
func NewPositionDeleteIndex(deletes []*PositionDelete) *PositionDeleteIndex {
	index := &PositionDeleteIndex{
		deletesByFile: make(map[string][]int64),
	}
	
	for _, delete := range deletes {
		index.deletesByFile[delete.FilePath] = append(index.deletesByFile[delete.FilePath], delete.Position)
	}
	
	// Sort positions for each file for efficient processing
	for _, positions := range index.deletesByFile {
		sort.Slice(positions, func(i, j int) bool {
			return positions[i] < positions[j]
		})
	}
	
	return index
}

// GetDeletedPositions returns all deleted positions for a specific file path.
func (pdi *PositionDeleteIndex) GetDeletedPositions(filePath string) []int64 {
	return pdi.deletesByFile[filePath]
}

// IsDeleted checks if a specific position in the specified file is deleted.
func (pdi *PositionDeleteIndex) IsDeleted(filePath string, position int64) bool {
	positions := pdi.GetDeletedPositions(filePath)
	// Binary search since positions are sorted
	left, right := 0, len(positions)-1
	for left <= right {
		mid := (left + right) / 2
		if positions[mid] == position {
			return true
		}
		if positions[mid] < position {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
} 