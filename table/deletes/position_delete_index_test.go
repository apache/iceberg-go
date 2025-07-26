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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionDeleteIndexTypes(t *testing.T) {
	tests := []struct {
		name      string
		indexType DeleteIndexType
		expected  string
	}{
		{"bitmap index", BitmapIndexType, "BITMAP"},
		{"set index", SetIndexType, "SET"},
		{"unknown index", DeleteIndexType(999), "UNKNOWN(999)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.indexType.String())
		})
	}
}

func TestPositionDeleteIndexFactory(t *testing.T) {
	factory := NewPositionDeleteIndexFactory()

	// Test creating bitmap index
	bitmapIndex := factory.Create(BitmapIndexType)
	assert.IsType(t, &BitmapPositionDeleteIndex{}, bitmapIndex)

	// Test creating set index
	setIndex := factory.Create(SetIndexType)
	assert.IsType(t, &SetPositionDeleteIndex{}, setIndex)

	// Test creating unknown type (should default to set)
	unknownIndex := factory.Create(DeleteIndexType(999))
	assert.IsType(t, &SetPositionDeleteIndex{}, unknownIndex)

	// Test creating bitmap builder
	bitmapBuilder := factory.CreateBuilder(BitmapIndexType)
	assert.IsType(t, &BitmapPositionDeleteIndexBuilder{}, bitmapBuilder)

	// Test creating set builder
	setBuilder := factory.CreateBuilder(SetIndexType)
	assert.IsType(t, &SetPositionDeleteIndexBuilder{}, setBuilder)
}

// Test both bitmap and set implementations
func TestPositionDeleteIndexImplementations(t *testing.T) {
	implementations := []struct {
		name      string
		createIdx func() PositionDeleteIndex
	}{
		{
			name:      "BitmapPositionDeleteIndex",
			createIdx: func() PositionDeleteIndex { return NewBitmapPositionDeleteIndex() },
		},
		{
			name:      "SetPositionDeleteIndex",
			createIdx: func() PositionDeleteIndex { return NewSetPositionDeleteIndex() },
		},
	}

	for _, impl := range implementations {
		t.Run(impl.name, func(t *testing.T) {
			testPositionDeleteIndexBasicOperations(t, impl.createIdx)
			testPositionDeleteIndexIterators(t, impl.createIdx)
			testPositionDeleteIndexEdgeCases(t, impl.createIdx)
		})
	}
}

func testPositionDeleteIndexBasicOperations(t *testing.T, createIdx func() PositionDeleteIndex) {
	idx := createIdx()

	// Test empty index
	assert.True(t, idx.IsEmpty())
	assert.Equal(t, 0, idx.Count())
	assert.False(t, idx.IsDeleted("file1.parquet", 0))
	assert.Nil(t, idx.Get("file1.parquet"))

	// Add some deletes
	idx.Add("file1.parquet", 10)
	idx.Add("file1.parquet", 20)
	idx.Add("file2.parquet", 5)

	// Test basic queries
	assert.False(t, idx.IsEmpty())
	assert.Equal(t, 3, idx.Count())
	assert.Equal(t, 2, idx.CountForFile("file1.parquet"))
	assert.Equal(t, 1, idx.CountForFile("file2.parquet"))
	assert.Equal(t, 0, idx.CountForFile("nonexistent.parquet"))

	// Test IsDeleted
	assert.True(t, idx.IsDeleted("file1.parquet", 10))
	assert.True(t, idx.IsDeleted("file1.parquet", 20))
	assert.True(t, idx.IsDeleted("file2.parquet", 5))
	assert.False(t, idx.IsDeleted("file1.parquet", 15))
	assert.False(t, idx.IsDeleted("file3.parquet", 10))

	// Test Get
	file1Deletes := idx.Get("file1.parquet")
	assert.ElementsMatch(t, []int64{10, 20}, file1Deletes)

	file2Deletes := idx.Get("file2.parquet")
	assert.ElementsMatch(t, []int64{5}, file2Deletes)

	// Test duplicate add (should not increase count)
	idx.Add("file1.parquet", 10)
	assert.Equal(t, 3, idx.Count())

	// Test Remove
	assert.True(t, idx.Remove("file1.parquet", 10))
	assert.False(t, idx.IsDeleted("file1.parquet", 10))
	assert.Equal(t, 2, idx.Count())
	assert.Equal(t, 1, idx.CountForFile("file1.parquet"))

	// Test removing non-existent delete
	assert.False(t, idx.Remove("file1.parquet", 999))
	assert.False(t, idx.Remove("nonexistent.parquet", 10))

	// Test Clear
	idx.Clear()
	assert.True(t, idx.IsEmpty())
	assert.Equal(t, 0, idx.Count())
}

func testPositionDeleteIndexIterators(t *testing.T, createIdx func() PositionDeleteIndex) {
	idx := createIdx()

	// Add test data
	testData := map[string][]int64{
		"file1.parquet": {10, 20, 30},
		"file2.parquet": {5, 15},
		"file3.parquet": {100},
	}

	for filePath, positions := range testData {
		for _, pos := range positions {
			idx.Add(filePath, pos)
		}
	}

	// Test Files iterator
	var files []string
	for filePath := range idx.Files() {
		files = append(files, filePath)
	}
	slices.Sort(files)
	expectedFiles := []string{"file1.parquet", "file2.parquet", "file3.parquet"}
	assert.Equal(t, expectedFiles, files)

	// Test Deletes iterator for each file
	for filePath, expectedPositions := range testData {
		var actualPositions []int64
		for pos := range idx.Deletes(filePath) {
			actualPositions = append(actualPositions, pos)
		}
		slices.Sort(actualPositions)
		slices.Sort(expectedPositions)
		assert.Equal(t, expectedPositions, actualPositions, "Positions for %s", filePath)
	}

	// Test Deletes iterator for non-existent file
	var nonExistentPositions []int64
	for pos := range idx.Deletes("nonexistent.parquet") {
		nonExistentPositions = append(nonExistentPositions, pos)
	}
	assert.Empty(t, nonExistentPositions)
}

func testPositionDeleteIndexEdgeCases(t *testing.T, createIdx func() PositionDeleteIndex) {
	idx := createIdx()

	// Test negative positions (should be ignored)
	idx.Add("file1.parquet", -1)
	idx.Add("file1.parquet", -100)
	assert.True(t, idx.IsEmpty())

	// Test large positions
	largePos := int64(1000000000)
	idx.Add("file1.parquet", largePos)
	assert.True(t, idx.IsDeleted("file1.parquet", largePos))
	assert.Equal(t, 1, idx.Count())

	// Test zero position
	idx.Add("file1.parquet", 0)
	assert.True(t, idx.IsDeleted("file1.parquet", 0))
	assert.Equal(t, 2, idx.Count())

	// Test empty file path
	idx.Add("", 10)
	assert.True(t, idx.IsDeleted("", 10))
	assert.Equal(t, 3, idx.Count())

	// Test removing from empty index after clear
	idx.Clear()
	assert.False(t, idx.Remove("file1.parquet", 10))
}

func TestPositionDeleteIndexBuilders(t *testing.T) {
	// Test bitmap builder
	t.Run("BitmapBuilder", func(t *testing.T) {
		builder := NewBitmapPositionDeleteIndexBuilder()
		
		delete1 := NewPositionDelete("file1.parquet", 10)
		delete2 := NewPositionDelete("file2.parquet", 20)
		
		index := builder.
			Add("file1.parquet", 5).
			AddPositionDelete(delete1).
			AddPositionDelete(nil). // Should be ignored
			AddAll("file2.parquet", []int64{15, 20, 25}).
			AddPositionDelete(delete2). // Duplicate, should not increase count
			Build()
		
		assert.Equal(t, 5, index.Count())
		assert.True(t, index.IsDeleted("file1.parquet", 5))
		assert.True(t, index.IsDeleted("file1.parquet", 10))
		assert.True(t, index.IsDeleted("file2.parquet", 15))
		assert.True(t, index.IsDeleted("file2.parquet", 20))
		assert.True(t, index.IsDeleted("file2.parquet", 25))
	})

	// Test set builder
	t.Run("SetBuilder", func(t *testing.T) {
		builder := NewSetPositionDeleteIndexBuilder()
		
		delete1 := NewPositionDelete("file1.parquet", 10)
		
		index := builder.
			Add("file1.parquet", 5).
			AddPositionDelete(delete1).
			AddAll("file2.parquet", []int64{15, 20}).
			Build()
		
		assert.Equal(t, 4, index.Count())
		assert.True(t, index.IsDeleted("file1.parquet", 5))
		assert.True(t, index.IsDeleted("file1.parquet", 10))
		assert.True(t, index.IsDeleted("file2.parquet", 15))
		assert.True(t, index.IsDeleted("file2.parquet", 20))
	})
}

func TestPositionDeleteIndexString(t *testing.T) {
	// Test bitmap index string representation
	bitmapIdx := NewBitmapPositionDeleteIndex()
	bitmapIdx.Add("file1.parquet", 10)
	bitmapIdx.Add("file2.parquet", 20)
	
	bitmapStr := bitmapIdx.String()
	assert.Contains(t, bitmapStr, "BitmapPositionDeleteIndex")
	assert.Contains(t, bitmapStr, "files=2")
	assert.Contains(t, bitmapStr, "totalDeletes=2")

	// Test set index string representation
	setIdx := NewSetPositionDeleteIndex()
	setIdx.Add("file1.parquet", 10)
	setIdx.Add("file2.parquet", 20)
	
	setStr := setIdx.String()
	assert.Contains(t, setStr, "SetPositionDeleteIndex")
	assert.Contains(t, setStr, "files=2")
	assert.Contains(t, setStr, "totalDeletes=2")

	// Test with many files (should show "deletes=...")
	for i := 0; i < 5; i++ {
		bitmapIdx.Add(fmt.Sprintf("file%d.parquet", i), int64(i))
		setIdx.Add(fmt.Sprintf("file%d.parquet", i), int64(i))
	}
	
	assert.Contains(t, bitmapIdx.String(), "deletes=...")
	assert.Contains(t, setIdx.String(), "deletes=...")
}

func BenchmarkPositionDeleteIndexAdd(b *testing.B) {
	implementations := []struct {
		name      string
		createIdx func() PositionDeleteIndex
	}{
		{"Bitmap", func() PositionDeleteIndex { return NewBitmapPositionDeleteIndex() }},
		{"Set", func() PositionDeleteIndex { return NewSetPositionDeleteIndex() }},
	}

	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			idx := impl.createIdx()
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.Add("file.parquet", int64(i))
			}
		})
	}
}

func BenchmarkPositionDeleteIndexIsDeleted(b *testing.B) {
	implementations := []struct {
		name      string
		createIdx func() PositionDeleteIndex
	}{
		{"Bitmap", func() PositionDeleteIndex { return NewBitmapPositionDeleteIndex() }},
		{"Set", func() PositionDeleteIndex { return NewSetPositionDeleteIndex() }},
	}

	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			idx := impl.createIdx()
			
			// Pre-populate with some data
			for i := 0; i < 1000; i += 2 { // Every other position
				idx.Add("file.parquet", int64(i))
			}
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.IsDeleted("file.parquet", int64(i%1000))
			}
		})
	}
} 