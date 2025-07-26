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
	"strings"
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
	if other == nil {
		return false
	}
	return pd.FilePath == other.FilePath && pd.Position == other.Position
}

// Compare compares two PositionDelete instances for ordering.
// Returns:
//   -1 if pd < other
//    0 if pd == other  
//    1 if pd > other
func (pd *PositionDelete) Compare(other *PositionDelete) int {
	if other == nil {
		return 1
	}
	
	// First compare by file path
	if cmp := strings.Compare(pd.FilePath, other.FilePath); cmp != 0 {
		return cmp
	}
	
	// Then compare by position
	if pd.Position < other.Position {
		return -1
	} else if pd.Position > other.Position {
		return 1
	}
	
	return 0
}

// PositionDeleteSlice provides sorting functionality for slices of PositionDelete.
type PositionDeleteSlice []*PositionDelete

func (pds PositionDeleteSlice) Len() int           { return len(pds) }
func (pds PositionDeleteSlice) Less(i, j int) bool { return pds[i].Compare(pds[j]) < 0 }
func (pds PositionDeleteSlice) Swap(i, j int)      { pds[i], pds[j] = pds[j], pds[i] } 