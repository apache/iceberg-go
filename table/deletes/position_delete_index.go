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
)

// PositionDeleteIndex provides an interface for indexing and querying position deletes
// for efficient lookup of deleted positions in data files.
type PositionDeleteIndex interface {
	// IsDeleted returns true if the position is marked as deleted in the given file.
	IsDeleted(filePath string, position int64) bool
	
	// Get returns all deleted positions for the given file path.
	// Returns nil if no deletes exist for the file.
	Get(filePath string) []int64
	
	// Add adds a position delete to the index.
	Add(filePath string, position int64)
	
	// Remove removes a position delete from the index.
	Remove(filePath string, position int64) bool
	
	// Files returns an iterator over all file paths that have position deletes.
	Files() iter.Seq[string]
	
	// Deletes returns an iterator over all position deletes for a given file.
	Deletes(filePath string) iter.Seq[int64]
	
	// Count returns the total number of position deletes across all files.
	Count() int
	
	// CountForFile returns the number of position deletes for a specific file.
	CountForFile(filePath string) int
	
	// IsEmpty returns true if there are no position deletes in the index.
	IsEmpty() bool
	
	// Clear removes all position deletes from the index.
	Clear()
	
	// String returns a string representation of the index.
	String() string
}

// PositionDeleteIndexBuilder provides a builder interface for creating position delete indexes.
type PositionDeleteIndexBuilder interface {
	// Add adds a position delete to the builder.
	Add(filePath string, position int64) PositionDeleteIndexBuilder
	
	// AddPositionDelete adds a PositionDelete to the builder.
	AddPositionDelete(delete *PositionDelete) PositionDeleteIndexBuilder
	
	// AddAll adds multiple position deletes to the builder.
	AddAll(filePath string, positions []int64) PositionDeleteIndexBuilder
	
	// Build creates the position delete index.
	Build() PositionDeleteIndex
}

// DeleteIndexType represents different types of position delete indexes.
type DeleteIndexType int

const (
	// BitmapIndexType uses bitmap-based indexing for dense position deletes.
	BitmapIndexType DeleteIndexType = iota
	// SetIndexType uses set-based indexing for sparse position deletes.
	SetIndexType
)

// String returns the string representation of the DeleteIndexType.
func (t DeleteIndexType) String() string {
	switch t {
	case BitmapIndexType:
		return "BITMAP"
	case SetIndexType:
		return "SET"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

// PositionDeleteIndexFactory creates position delete indexes of different types.
type PositionDeleteIndexFactory struct{}

// NewPositionDeleteIndexFactory creates a new factory for position delete indexes.
func NewPositionDeleteIndexFactory() *PositionDeleteIndexFactory {
	return &PositionDeleteIndexFactory{}
}

// Create creates a position delete index of the specified type.
func (f *PositionDeleteIndexFactory) Create(indexType DeleteIndexType) PositionDeleteIndex {
	switch indexType {
	case BitmapIndexType:
		return NewBitmapPositionDeleteIndex()
	case SetIndexType:
		return NewSetPositionDeleteIndex()
	default:
		return NewSetPositionDeleteIndex() // Default to set-based index
	}
}

// CreateBuilder creates a position delete index builder of the specified type.
func (f *PositionDeleteIndexFactory) CreateBuilder(indexType DeleteIndexType) PositionDeleteIndexBuilder {
	switch indexType {
	case BitmapIndexType:
		return NewBitmapPositionDeleteIndexBuilder()
	case SetIndexType:
		return NewSetPositionDeleteIndexBuilder()
	default:
		return NewSetPositionDeleteIndexBuilder() // Default to set-based builder
	}
} 