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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// PositionDeleteIndexUtil provides utility functions for working with position delete indexes.
type PositionDeleteIndexUtil struct{}

// DefaultThreshold for choosing between bitmap and set-based indexes
const DefaultDensityThreshold = 0.1 // 10% density

// Merge combines multiple position delete indexes into a single index.
// The result uses the most appropriate index type based on the combined data.
func (*PositionDeleteIndexUtil) Merge(indexes ...PositionDeleteIndex) PositionDeleteIndex {
	if len(indexes) == 0 {
		return NewSetPositionDeleteIndex()
	}
	
	if len(indexes) == 1 {
		return indexes[0]
	}
	
	// Count total deletes and collect all files
	totalDeletes := 0
	allFiles := make(map[string]struct{})
	
	for _, idx := range indexes {
		totalDeletes += idx.Count()
		for filePath := range idx.Files() {
			allFiles[filePath] = struct{}{}
		}
	}
	
	// Choose index type based on data characteristics
	var result PositionDeleteIndex
	if shouldUseBitmapIndex(totalDeletes, len(allFiles)) {
		result = NewBitmapPositionDeleteIndex()
	} else {
		result = NewSetPositionDeleteIndex()
	}
	
	// Merge all deletes
	for _, idx := range indexes {
		for filePath := range idx.Files() {
			for position := range idx.Deletes(filePath) {
				result.Add(filePath, position)
			}
		}
	}
	
	return result
}

// Filter creates a new index containing only deletes that match the provided predicate.
func (*PositionDeleteIndexUtil) Filter(index PositionDeleteIndex, predicate func(string, int64) bool) PositionDeleteIndex {
	factory := NewPositionDeleteIndexFactory()
	
	// Use the same type as the input index
	var indexType DeleteIndexType
	switch index.(type) {
	case *BitmapPositionDeleteIndex:
		indexType = BitmapIndexType
	default:
		indexType = SetIndexType
	}
	
	result := factory.Create(indexType)
	
	for filePath := range index.Files() {
		for position := range index.Deletes(filePath) {
			if predicate(filePath, position) {
				result.Add(filePath, position)
			}
		}
	}
	
	return result
}

// FilterByFilePrefix creates a new index containing only deletes for files with the specified prefix.
func (util *PositionDeleteIndexUtil) FilterByFilePrefix(index PositionDeleteIndex, prefix string) PositionDeleteIndex {
	return util.Filter(index, func(filePath string, _ int64) bool {
		return strings.HasPrefix(filePath, prefix)
	})
}

// FilterByPositionRange creates a new index containing only deletes within the specified position range.
func (util *PositionDeleteIndexUtil) FilterByPositionRange(index PositionDeleteIndex, minPos, maxPos int64) PositionDeleteIndex {
	return util.Filter(index, func(_ string, position int64) bool {
		return position >= minPos && position <= maxPos
	})
}

// Subtract removes all deletes from the second index from the first index.
func (*PositionDeleteIndexUtil) Subtract(minuend, subtrahend PositionDeleteIndex) PositionDeleteIndex {
	factory := NewPositionDeleteIndexFactory()
	
	// Use the same type as the minuend index
	var indexType DeleteIndexType
	switch minuend.(type) {
	case *BitmapPositionDeleteIndex:
		indexType = BitmapIndexType
	default:
		indexType = SetIndexType
	}
	
	result := factory.Create(indexType)
	
	// Add all deletes from minuend that are not in subtrahend
	for filePath := range minuend.Files() {
		for position := range minuend.Deletes(filePath) {
			if !subtrahend.IsDeleted(filePath, position) {
				result.Add(filePath, position)
			}
		}
	}
	
	return result
}

// Intersect creates a new index containing only deletes that exist in both indexes.
func (*PositionDeleteIndexUtil) Intersect(first, second PositionDeleteIndex) PositionDeleteIndex {
	factory := NewPositionDeleteIndexFactory()
	
	// Use bitmap if either input uses bitmap
	var indexType DeleteIndexType
	switch {
	case isInstanceOf[*BitmapPositionDeleteIndex](first) || isInstanceOf[*BitmapPositionDeleteIndex](second):
		indexType = BitmapIndexType
	default:
		indexType = SetIndexType
	}
	
	result := factory.Create(indexType)
	
	// Add deletes that exist in both indexes
	for filePath := range first.Files() {
		for position := range first.Deletes(filePath) {
			if second.IsDeleted(filePath, position) {
				result.Add(filePath, position)
			}
		}
	}
	
	return result
}

// ToArrowTable converts a position delete index to an Arrow table.
func (*PositionDeleteIndexUtil) ToArrowTable(ctx context.Context, index PositionDeleteIndex) arrow.Table {
	mem := memory.DefaultAllocator
	
	// Count total records
	totalRecords := index.Count()
	if totalRecords == 0 {
		// Return empty table with correct schema
		filePathBuilder := array.NewStringBuilder(mem)
		positionBuilder := array.NewInt64Builder(mem)
		
		filePathArray := filePathBuilder.NewArray()
		positionArray := positionBuilder.NewArray()
		
		filePathBuilder.Release()
		positionBuilder.Release()
		
		return array.NewTable(
			arrow.NewSchema([]arrow.Field{
				{Name: "file_path", Type: arrow.BinaryTypes.String},
				{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
			[]arrow.Column{
				arrow.NewColumn(arrow.Field{Name: "file_path", Type: arrow.BinaryTypes.String}, 
					arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArray})),
				arrow.NewColumn(arrow.Field{Name: "pos", Type: arrow.PrimitiveTypes.Int64}, 
					arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{positionArray})),
			},
		)
	}
	
	filePathBuilder := array.NewStringBuilder(mem)
	positionBuilder := array.NewInt64Builder(mem)
	
	filePathBuilder.Reserve(totalRecords)
	positionBuilder.Reserve(totalRecords)
	
	// Collect all deletes sorted by file path and position
	type deleteRecord struct {
		filePath string
		position int64
	}
	
	records := make([]deleteRecord, 0, totalRecords)
	for filePath := range index.Files() {
		for position := range index.Deletes(filePath) {
			records = append(records, deleteRecord{
				filePath: filePath,
				position: position,
			})
		}
	}
	
	// Sort for consistent ordering
	sort.Slice(records, func(i, j int) bool {
		if records[i].filePath != records[j].filePath {
			return records[i].filePath < records[j].filePath
		}
		return records[i].position < records[j].position
	})
	
	// Build arrays
	for _, record := range records {
		filePathBuilder.Append(record.filePath)
		positionBuilder.Append(record.position)
	}
	
	filePathArray := filePathBuilder.NewArray()
	positionArray := positionBuilder.NewArray()
	
	filePathBuilder.Release()
	positionBuilder.Release()
	
	table := array.NewTable(
		arrow.NewSchema([]arrow.Field{
			{Name: "file_path", Type: arrow.BinaryTypes.String},
			{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		[]arrow.Column{
			arrow.NewColumn(arrow.Field{Name: "file_path", Type: arrow.BinaryTypes.String}, 
				arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArray})),
			arrow.NewColumn(arrow.Field{Name: "pos", Type: arrow.PrimitiveTypes.Int64}, 
				arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{positionArray})),
		},
	)
	
	return table
}

// FromArrowTable creates a position delete index from an Arrow table.
func (*PositionDeleteIndexUtil) FromArrowTable(table arrow.Table) (PositionDeleteIndex, error) {
	defer table.Release()
	
	if table.NumRows() == 0 {
		return NewSetPositionDeleteIndex(), nil
	}
	
	// Find file_path and pos columns
	schema := table.Schema()
	filePathColIdx := -1
	posColIdx := -1
	
	for i, field := range schema.Fields() {
		switch field.Name {
		case "file_path":
			filePathColIdx = i
		case "pos":
			posColIdx = i
		}
	}
	
	if filePathColIdx == -1 || posColIdx == -1 {
		return nil, fmt.Errorf("table must have 'file_path' and 'pos' columns")
	}
	
	filePathColumn := table.Column(filePathColIdx)
	posColumn := table.Column(posColIdx)
	
	// Determine appropriate index type
	numRows := int(table.NumRows())
	numFiles := estimateNumFiles(filePathColumn)
	
	var index PositionDeleteIndex
	if shouldUseBitmapIndex(numRows, numFiles) {
		index = NewBitmapPositionDeleteIndex()
	} else {
		index = NewSetPositionDeleteIndex()
	}
	
	// Read data from Arrow table
	for chunkIdx := 0; chunkIdx < filePathColumn.Data().Len(); chunkIdx++ {
		filePathChunk := filePathColumn.Data().Chunk(chunkIdx).(*array.String)
		posChunk := posColumn.Data().Chunk(chunkIdx).(*array.Int64)
		
		for i := 0; i < filePathChunk.Len(); i++ {
			if !filePathChunk.IsNull(i) && !posChunk.IsNull(i) {
				filePath := filePathChunk.Value(i)
				position := posChunk.Value(i)
				index.Add(filePath, position)
			}
		}
	}
	
	return index, nil
}

// OptimizeIndex converts the index to the most appropriate type based on data characteristics.
func (*PositionDeleteIndexUtil) OptimizeIndex(index PositionDeleteIndex) PositionDeleteIndex {
	if index.IsEmpty() {
		return NewSetPositionDeleteIndex()
	}
	
	numDeletes := index.Count()
	numFiles := 0
	for range index.Files() {
		numFiles++
	}
	
	currentIsBitmap := isInstanceOf[*BitmapPositionDeleteIndex](index)
	shouldBeBitmap := shouldUseBitmapIndex(numDeletes, numFiles)
	
	// No optimization needed
	if currentIsBitmap == shouldBeBitmap {
		return index
	}
	
	// Convert to appropriate type
	factory := NewPositionDeleteIndexFactory()
	var newIndex PositionDeleteIndex
	
	if shouldBeBitmap {
		newIndex = factory.Create(BitmapIndexType)
	} else {
		newIndex = factory.Create(SetIndexType)
	}
	
	// Copy all deletes
	for filePath := range index.Files() {
		for position := range index.Deletes(filePath) {
			newIndex.Add(filePath, position)
		}
	}
	
	return newIndex
}

// Helper functions

// shouldUseBitmapIndex determines if a bitmap index is more appropriate than a set index.
func shouldUseBitmapIndex(numDeletes, numFiles int) bool {
	if numDeletes == 0 {
		return false
	}
	
	if numFiles == 0 {
		return false
	}
	
	// Use bitmap for dense deletes or when we have many deletes per file
	avgDeletesPerFile := float64(numDeletes) / float64(numFiles)
	return avgDeletesPerFile > (1.0 / DefaultDensityThreshold)
}

// isInstanceOf checks if a value is an instance of a specific type.
func isInstanceOf[T any](v interface{}) bool {
	_, ok := v.(T)
	return ok
}

// estimateNumFiles estimates the number of unique files in a file path column.
func estimateNumFiles(column arrow.Column) int {
	uniqueFiles := make(map[string]struct{})
	
	for chunkIdx := 0; chunkIdx < column.Data().Len(); chunkIdx++ {
		chunk := column.Data().Chunk(chunkIdx).(*array.String)
		
		for i := 0; i < chunk.Len() && len(uniqueFiles) < 1000; i++ { // Limit to prevent memory issues
			if !chunk.IsNull(i) {
				uniqueFiles[chunk.Value(i)] = struct{}{}
			}
		}
		
		if len(uniqueFiles) >= 1000 {
			break // Estimate is good enough
		}
	}
	
	return len(uniqueFiles)
} 