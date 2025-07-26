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

	"github.com/apache/iceberg-go"
)

// EqualityDelete represents an equality-based delete operation.
// It specifies that all rows with matching values for specified equality fields
// should be deleted from the given file path.
type EqualityDelete struct {
	// FilePath is the path to the data file containing rows to be deleted
	FilePath string
	// Values maps field IDs to the values that rows must match to be deleted
	Values map[int]interface{}
}

// NewEqualityDelete creates a new equality delete record.
func NewEqualityDelete(filePath string, fieldValues map[int]interface{}) *EqualityDelete {
	return &EqualityDelete{
		FilePath: filePath,
		Values:   fieldValues,
	}
}

// String returns a string representation of the equality delete.
func (e *EqualityDelete) String() string {
	return fmt.Sprintf("EqualityDelete{filePath=%s, values=%v}", e.FilePath, e.Values)
}

// AddFieldValue adds a field value to match for deletion.
func (e *EqualityDelete) AddFieldValue(fieldID int, value interface{}) {
	if e.Values == nil {
		e.Values = make(map[int]interface{})
	}
	e.Values[fieldID] = value
}

// GetFieldValue retrieves the value for a specific field ID.
func (e *EqualityDelete) GetFieldValue(fieldID int) (interface{}, bool) {
	value, exists := e.Values[fieldID]
	return value, exists
}

// GetEqualityFields returns the field IDs that are part of this equality delete.
func (e *EqualityDelete) GetEqualityFields() []int {
	fields := make([]int, 0, len(e.Values))
	for fieldID := range e.Values {
		fields = append(fields, fieldID)
	}
	return fields
}

// ValidateAgainstSchema validates that all field IDs in this equality delete
// exist in the given table schema.
func (e *EqualityDelete) ValidateAgainstSchema(schema *iceberg.Schema) error {
	for fieldID := range e.Values {
		_, found := schema.FindFieldByID(fieldID)
		if !found {
			return fmt.Errorf("field ID %d not found in schema", fieldID)
		}
	}
	return nil
}

// PositionDelete represents a position-based delete operation.
// It specifies that the row at a specific position within a data file should be deleted.
type PositionDelete struct {
	// FilePath is the path to the data file containing the row to be deleted
	FilePath string
	// Position is the 0-based position of the row within the file
	Position int64
}

// NewPositionDelete creates a new position delete record.
func NewPositionDelete(filePath string, position int64) *PositionDelete {
	return &PositionDelete{
		FilePath: filePath,
		Position: position,
	}
}

// String returns a string representation of the position delete.
func (p *PositionDelete) String() string {
	return fmt.Sprintf("PositionDelete{filePath=%s, position=%d}", p.FilePath, p.Position)
}

// IsValid checks if the position delete has valid values.
func (p *PositionDelete) IsValid() bool {
	return p.FilePath != "" && p.Position >= 0
} 