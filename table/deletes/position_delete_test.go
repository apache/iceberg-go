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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPositionDelete(t *testing.T) {
	filePath := "/path/to/file.parquet"
	position := int64(42)

	delete := NewPositionDelete(filePath, position)

	assert.Equal(t, filePath, delete.FilePath)
	assert.Equal(t, position, delete.Position)
}

func TestPositionDeleteString(t *testing.T) {
	delete := NewPositionDelete("/path/to/file.parquet", 123)
	expected := "PositionDelete{file=/path/to/file.parquet, pos=123}"
	assert.Equal(t, expected, delete.String())
}

func TestPositionDeleteEqual(t *testing.T) {
	delete1 := NewPositionDelete("/path/to/file.parquet", 123)
	delete2 := NewPositionDelete("/path/to/file.parquet", 123)
	delete3 := NewPositionDelete("/path/to/other.parquet", 123)
	delete4 := NewPositionDelete("/path/to/file.parquet", 456)

	// Test equality
	assert.True(t, delete1.Equal(delete2))
	assert.True(t, delete2.Equal(delete1))

	// Test inequality
	assert.False(t, delete1.Equal(delete3))
	assert.False(t, delete1.Equal(delete4))
	assert.False(t, delete1.Equal(nil))
}

func TestPositionDeleteCompare(t *testing.T) {
	tests := []struct {
		name     string
		delete1  *PositionDelete
		delete2  *PositionDelete
		expected int
	}{
		{
			name:     "equal deletes",
			delete1:  NewPositionDelete("/path/to/file.parquet", 123),
			delete2:  NewPositionDelete("/path/to/file.parquet", 123),
			expected: 0,
		},
		{
			name:     "different files, same position",
			delete1:  NewPositionDelete("/path/to/a.parquet", 123),
			delete2:  NewPositionDelete("/path/to/b.parquet", 123),
			expected: -1,
		},
		{
			name:     "same file, different positions",
			delete1:  NewPositionDelete("/path/to/file.parquet", 100),
			delete2:  NewPositionDelete("/path/to/file.parquet", 200),
			expected: -1,
		},
		{
			name:     "reverse file order",
			delete1:  NewPositionDelete("/path/to/z.parquet", 123),
			delete2:  NewPositionDelete("/path/to/a.parquet", 123),
			expected: 1,
		},
		{
			name:     "reverse position order",
			delete1:  NewPositionDelete("/path/to/file.parquet", 200),
			delete2:  NewPositionDelete("/path/to/file.parquet", 100),
			expected: 1,
		},
		{
			name:     "compare with nil",
			delete1:  NewPositionDelete("/path/to/file.parquet", 123),
			delete2:  nil,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.delete1.Compare(tt.delete2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPositionDeleteSliceSorting(t *testing.T) {
	deletes := PositionDeleteSlice{
		NewPositionDelete("/path/to/c.parquet", 300),
		NewPositionDelete("/path/to/a.parquet", 100),
		NewPositionDelete("/path/to/b.parquet", 200),
		NewPositionDelete("/path/to/a.parquet", 50),
		NewPositionDelete("/path/to/a.parquet", 150),
	}

	sort.Sort(deletes)

	expected := []*PositionDelete{
		NewPositionDelete("/path/to/a.parquet", 50),
		NewPositionDelete("/path/to/a.parquet", 100),
		NewPositionDelete("/path/to/a.parquet", 150),
		NewPositionDelete("/path/to/b.parquet", 200),
		NewPositionDelete("/path/to/c.parquet", 300),
	}

	require.Equal(t, len(expected), len(deletes))
	for i, expectedDelete := range expected {
		assert.True(t, expectedDelete.Equal(deletes[i]),
			"Position %d: expected %s, got %s", i, expectedDelete, deletes[i])
	}
}

func TestPositionDeleteSliceInterface(t *testing.T) {
	deletes := PositionDeleteSlice{
		NewPositionDelete("/path/to/b.parquet", 200),
		NewPositionDelete("/path/to/a.parquet", 100),
	}

	// Test Len
	assert.Equal(t, 2, deletes.Len())

	// Test Less
	assert.True(t, deletes.Less(1, 0))  // a.parquet:100 < b.parquet:200
	assert.False(t, deletes.Less(0, 1)) // b.parquet:200 > a.parquet:100

	// Test Swap
	deletes.Swap(0, 1)
	assert.Equal(t, "/path/to/a.parquet", deletes[0].FilePath)
	assert.Equal(t, "/path/to/b.parquet", deletes[1].FilePath)
}

func BenchmarkPositionDeleteCreate(b *testing.B) {
	filePath := "/path/to/file.parquet"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewPositionDelete(filePath, int64(i))
	}
}

func BenchmarkPositionDeleteCompare(b *testing.B) {
	delete1 := NewPositionDelete("/path/to/file1.parquet", 123)
	delete2 := NewPositionDelete("/path/to/file2.parquet", 456)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = delete1.Compare(delete2)
	}
}

func BenchmarkPositionDeleteSliceSort(b *testing.B) {
	// Create a large slice of position deletes
	deletes := make(PositionDeleteSlice, 1000)
	for i := 0; i < 1000; i++ {
		// Create somewhat random data
		fileNum := i % 10
		position := int64(1000 - i) // Reverse order to force sorting
		deletes[i] = NewPositionDelete(fmt.Sprintf("/path/to/file%d.parquet", fileNum), position)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a copy for each iteration
		testDeletes := make(PositionDeleteSlice, len(deletes))
		copy(testDeletes, deletes)
		sort.Sort(testDeletes)
	}
} 