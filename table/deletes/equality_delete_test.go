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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEqualityDelete(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		fieldIDs []int
		values   map[int]interface{}
	}{
		{
			name:     "single field equality delete",
			filePath: "/path/to/data.parquet",
			fieldIDs: []int{1},
			values:   map[int]interface{}{1: "test_value"},
		},
		{
			name:     "multi field equality delete",
			filePath: "/path/to/data2.parquet",
			fieldIDs: []int{1, 2, 3},
			values:   map[int]interface{}{1: "test", 2: int32(42), 3: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delete := NewEqualityDelete(tt.filePath, tt.fieldIDs, tt.values)
			
			assert.Equal(t, tt.filePath, delete.FilePath)
			assert.Equal(t, tt.fieldIDs, delete.EqualityFieldIDs)
			assert.Equal(t, tt.values, delete.Values)
			
			// Test string representation
			str := delete.String()
			assert.Contains(t, str, tt.filePath)
			assert.Contains(t, str, "EqualityDelete")
		})
	}
}

func TestEqualityDeleteEqual(t *testing.T) {
	delete1 := NewEqualityDelete("/path/file.parquet", []int{1, 2}, map[int]interface{}{
		1: "value1",
		2: int32(42),
	})
	
	delete2 := NewEqualityDelete("/path/file.parquet", []int{1, 2}, map[int]interface{}{
		1: "value1",
		2: int32(42),
	})
	
	delete3 := NewEqualityDelete("/path/file.parquet", []int{1, 2}, map[int]interface{}{
		1: "different",
		2: int32(42),
	})

	assert.True(t, delete1.Equal(delete2))
	assert.True(t, delete2.Equal(delete1))
	assert.False(t, delete1.Equal(delete3))
	assert.False(t, delete3.Equal(delete1))
	
	// Test nil cases
	assert.True(t, (*EqualityDelete)(nil).Equal(nil))
	assert.False(t, delete1.Equal(nil))
	assert.False(t, (*EqualityDelete)(nil).Equal(delete1))
}

func TestEqualityDeleteCompare(t *testing.T) {
	delete1 := NewEqualityDelete("/path/a.parquet", []int{1}, map[int]interface{}{1: "a"})
	delete2 := NewEqualityDelete("/path/b.parquet", []int{1}, map[int]interface{}{1: "a"})
	delete3 := NewEqualityDelete("/path/a.parquet", []int{1}, map[int]interface{}{1: "b"})

	assert.Equal(t, -1, delete1.Compare(delete2)) // a.parquet < b.parquet
	assert.Equal(t, 1, delete2.Compare(delete1))  // b.parquet > a.parquet
	assert.Equal(t, -1, delete1.Compare(delete3)) // "a" < "b"
	assert.Equal(t, 0, delete1.Compare(delete1))  // equal to itself
}

func TestEqualityDeleteMatches(t *testing.T) {
	delete := NewEqualityDelete("/path/file.parquet", []int{1, 2}, map[int]interface{}{
		1: "test_value",
		2: int32(42),
	})

	tests := []struct {
		name     string
		record   map[int]interface{}
		expected bool
	}{
		{
			name: "exact match",
			record: map[int]interface{}{
				1: "test_value",
				2: int32(42),
				3: "other_field",
			},
			expected: true,
		},
		{
			name: "partial match",
			record: map[int]interface{}{
				1: "test_value",
				2: int32(43), // Different value
			},
			expected: false,
		},
		{
			name: "missing field",
			record: map[int]interface{}{
				1: "test_value",
				// Missing field 2
			},
			expected: false,
		},
		{
			name: "type mismatch",
			record: map[int]interface{}{
				1: "test_value",
				2: "42", // String instead of int32
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := delete.Matches(tt.record)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEqualityDeleteSlice(t *testing.T) {
	deletes := []*EqualityDelete{
		NewEqualityDelete("/path/c.parquet", []int{1}, map[int]interface{}{1: "c"}),
		NewEqualityDelete("/path/a.parquet", []int{1}, map[int]interface{}{1: "a"}),
		NewEqualityDelete("/path/b.parquet", []int{1}, map[int]interface{}{1: "b"}),
	}

	slice := EqualityDeleteSlice(deletes)
	slice.Sort()

	// Should be sorted by file path
	assert.Equal(t, "/path/a.parquet", slice[0].FilePath)
	assert.Equal(t, "/path/b.parquet", slice[1].FilePath)
	assert.Equal(t, "/path/c.parquet", slice[2].FilePath)
}

func TestEqualityDeleteIndex(t *testing.T) {
	deletes := []*EqualityDelete{
		NewEqualityDelete("/path/file1.parquet", []int{1}, map[int]interface{}{1: "delete_me"}),
		NewEqualityDelete("/path/file1.parquet", []int{1}, map[int]interface{}{1: "also_delete"}),
		NewEqualityDelete("/path/file2.parquet", []int{1}, map[int]interface{}{1: "different_file"}),
	}

	index := NewEqualityDeleteIndex(deletes)

	// Test getting deletes for specific file
	file1Deletes := index.GetDeletesForFile("/path/file1.parquet")
	assert.Len(t, file1Deletes, 2)

	file2Deletes := index.GetDeletesForFile("/path/file2.parquet")
	assert.Len(t, file2Deletes, 1)

	nonExistentDeletes := index.GetDeletesForFile("/path/nonexistent.parquet")
	assert.Len(t, nonExistentDeletes, 0)

	// Test deletion check
	record1 := map[int]interface{}{1: "delete_me"}
	assert.True(t, index.IsDeleted("/path/file1.parquet", record1))

	record2 := map[int]interface{}{1: "keep_me"}
	assert.False(t, index.IsDeleted("/path/file1.parquet", record2))

	record3 := map[int]interface{}{1: "different_file"}
	assert.True(t, index.IsDeleted("/path/file2.parquet", record3))
	assert.False(t, index.IsDeleted("/path/file1.parquet", record3))
}

func TestPositionDelete(t *testing.T) {
	delete := NewPositionDelete("/path/to/file.parquet", 42)
	
	assert.Equal(t, "/path/to/file.parquet", delete.FilePath)
	assert.Equal(t, int64(42), delete.Position)
	
	str := delete.String()
	assert.Contains(t, str, "PositionDelete")
	assert.Contains(t, str, "/path/to/file.parquet")
	assert.Contains(t, str, "42")
}

func TestPositionDeleteEqual(t *testing.T) {
	delete1 := NewPositionDelete("/path/file.parquet", 42)
	delete2 := NewPositionDelete("/path/file.parquet", 42)
	delete3 := NewPositionDelete("/path/file.parquet", 43)
	delete4 := NewPositionDelete("/path/other.parquet", 42)

	assert.True(t, delete1.Equal(delete2))
	assert.False(t, delete1.Equal(delete3))
	assert.False(t, delete1.Equal(delete4))
	
	// Test nil cases
	assert.True(t, (*PositionDelete)(nil).Equal(nil))
	assert.False(t, delete1.Equal(nil))
	assert.False(t, (*PositionDelete)(nil).Equal(delete1))
}

func TestPositionDeleteIndex(t *testing.T) {
	deletes := []*PositionDelete{
		NewPositionDelete("/path/file1.parquet", 10),
		NewPositionDelete("/path/file1.parquet", 20),
		NewPositionDelete("/path/file1.parquet", 5),
		NewPositionDelete("/path/file2.parquet", 15),
	}

	index := NewPositionDeleteIndex(deletes)

	// Test getting positions for specific file
	file1Positions := index.GetDeletedPositions("/path/file1.parquet")
	require.Len(t, file1Positions, 3)
	// Should be sorted
	assert.Equal(t, []int64{5, 10, 20}, file1Positions)

	file2Positions := index.GetDeletedPositions("/path/file2.parquet")
	require.Len(t, file2Positions, 1)
	assert.Equal(t, []int64{15}, file2Positions)

	// Test deletion check
	assert.True(t, index.IsDeleted("/path/file1.parquet", 10))
	assert.True(t, index.IsDeleted("/path/file1.parquet", 5))
	assert.False(t, index.IsDeleted("/path/file1.parquet", 25))
	assert.False(t, index.IsDeleted("/path/nonexistent.parquet", 10))
} 