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
	"strings"
)

// EqualityDelete represents an equality delete record that marks all rows
// in a data file with matching values for the specified equality fields as deleted.
type EqualityDelete struct {
	// FilePath is the path of the data file containing the deleted rows
	FilePath string
	// Values contains the values for each equality field that identify rows to delete
	Values map[int]interface{}
	// EqualityFieldIDs are the field IDs that define the equality condition
	EqualityFieldIDs []int
}

// NewEqualityDelete creates a new EqualityDelete with the given file path, field IDs, and values.
func NewEqualityDelete(filePath string, equalityFieldIDs []int, values map[int]interface{}) *EqualityDelete {
	return &EqualityDelete{
		FilePath:         filePath,
		EqualityFieldIDs: equalityFieldIDs,
		Values:           values,
	}
}

// String returns a string representation of the equality delete.
func (ed *EqualityDelete) String() string {
	var parts []string
	for _, fieldID := range ed.EqualityFieldIDs {
		if value, ok := ed.Values[fieldID]; ok {
			parts = append(parts, fmt.Sprintf("%d=%v", fieldID, value))
		}
	}
	return fmt.Sprintf("EqualityDelete{file=%s, fields=[%s]}", ed.FilePath, strings.Join(parts, ", "))
}

// Equal checks if two EqualityDelete instances are equal.
func (ed *EqualityDelete) Equal(other *EqualityDelete) bool {
	if ed == nil && other == nil {
		return true
	}
	if ed == nil || other == nil {
		return false
	}
	if ed.FilePath != other.FilePath {
		return false
	}
	if len(ed.EqualityFieldIDs) != len(other.EqualityFieldIDs) {
		return false
	}
	
	// Check equality field IDs match
	for i, fieldID := range ed.EqualityFieldIDs {
		if fieldID != other.EqualityFieldIDs[i] {
			return false
		}
	}
	
	// Check values match for all equality fields
	for _, fieldID := range ed.EqualityFieldIDs {
		if ed.Values[fieldID] != other.Values[fieldID] {
			return false
		}
	}
	
	return true
}

// Compare compares two EqualityDelete instances for ordering.
// Comparison is done first by file path, then by equality field values in field ID order.
func (ed *EqualityDelete) Compare(other *EqualityDelete) int {
	if ed == nil && other == nil {
		return 0
	}
	if ed == nil {
		return -1
	}
	if other == nil {
		return 1
	}
	
	// First compare by file path
	if ed.FilePath != other.FilePath {
		if ed.FilePath < other.FilePath {
			return -1
		}
		return 1
	}
	
	// Then compare by equality field IDs and values
	minLen := len(ed.EqualityFieldIDs)
	if len(other.EqualityFieldIDs) < minLen {
		minLen = len(other.EqualityFieldIDs)
	}
	
	for i := 0; i < minLen; i++ {
		fieldID1 := ed.EqualityFieldIDs[i]
		fieldID2 := other.EqualityFieldIDs[i]
		
		if fieldID1 != fieldID2 {
			if fieldID1 < fieldID2 {
				return -1
			}
			return 1
		}
		
		// Compare values - this is simplified, a full implementation would need type-specific comparison
		val1 := fmt.Sprintf("%v", ed.Values[fieldID1])
		val2 := fmt.Sprintf("%v", other.Values[fieldID2])
		if val1 != val2 {
			if val1 < val2 {
				return -1
			}
			return 1
		}
	}
	
	// If all compared fields are equal, compare by length
	if len(ed.EqualityFieldIDs) != len(other.EqualityFieldIDs) {
		if len(ed.EqualityFieldIDs) < len(other.EqualityFieldIDs) {
			return -1
		}
		return 1
	}
	
	return 0
}

// Matches checks if this equality delete matches a given record.
// The record should contain values for all the equality fields.
func (ed *EqualityDelete) Matches(record map[int]interface{}) bool {
	for _, fieldID := range ed.EqualityFieldIDs {
		deleteValue, hasDeleteValue := ed.Values[fieldID]
		recordValue, hasRecordValue := record[fieldID]
		
		// If either value is missing, no match
		if !hasDeleteValue || !hasRecordValue {
			return false
		}
		
		// Values must be equal for all equality fields
		if deleteValue != recordValue {
			return false
		}
	}
	return true
}

// EqualityDeleteSlice provides sorting functionality for slices of EqualityDelete.
type EqualityDeleteSlice []*EqualityDelete

func (eds EqualityDeleteSlice) Len() int           { return len(eds) }
func (eds EqualityDeleteSlice) Less(i, j int) bool { return eds[i].Compare(eds[j]) < 0 }
func (eds EqualityDeleteSlice) Swap(i, j int)      { eds[i], eds[j] = eds[j], eds[i] }

// Sort sorts the slice of equality deletes.
func (eds EqualityDeleteSlice) Sort() {
	sort.Sort(eds)
}

// EqualityDeleteIndex provides efficient lookups for equality deletes by file path.
type EqualityDeleteIndex struct {
	deletesByFile map[string][]*EqualityDelete
}

// NewEqualityDeleteIndex creates a new index from a slice of equality deletes.
func NewEqualityDeleteIndex(deletes []*EqualityDelete) *EqualityDeleteIndex {
	index := &EqualityDeleteIndex{
		deletesByFile: make(map[string][]*EqualityDelete),
	}
	
	for _, delete := range deletes {
		index.deletesByFile[delete.FilePath] = append(index.deletesByFile[delete.FilePath], delete)
	}
	
	// Sort deletes for each file for efficient processing
	for _, fileDeletes := range index.deletesByFile {
		sort.Sort(EqualityDeleteSlice(fileDeletes))
	}
	
	return index
}

// GetDeletesForFile returns all equality deletes for a specific file path.
func (edi *EqualityDeleteIndex) GetDeletesForFile(filePath string) []*EqualityDelete {
	return edi.deletesByFile[filePath]
}

// IsDeleted checks if a record in the specified file should be deleted based on equality deletes.
func (edi *EqualityDeleteIndex) IsDeleted(filePath string, record map[int]interface{}) bool {
	deletes := edi.GetDeletesForFile(filePath)
	for _, delete := range deletes {
		if delete.Matches(record) {
			return true
		}
	}
	return false
} 