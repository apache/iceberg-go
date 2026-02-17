// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use it except in compliance
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

package iceberg

// Row lineage metadata column field IDs (v3+). Reserved IDs are Integer.MAX_VALUE - 107 and 108
// per the Iceberg spec (Metadata Columns / Row Lineage).
const (
	// RowIDFieldID is the field ID for _row_id (optional long). A unique long identifier for every row.
	RowIDFieldID = 2147483540
	// LastUpdatedSequenceNumberFieldID is the field ID for _last_updated_sequence_number (optional long).
	// The sequence number of the commit that last updated the row.
	LastUpdatedSequenceNumberFieldID = 2147483539
)

// Row lineage metadata column names (v3+).
const (
	RowIDColumnName                         = "_row_id"
	LastUpdatedSequenceNumberColumnName     = "_last_updated_sequence_number"
)

// RowID returns a NestedField for _row_id (optional long) for use in schemas that include row lineage.
func RowID() NestedField {
	return NestedField{
		ID:        RowIDFieldID,
		Name:      RowIDColumnName,
		Required:  false,
		Doc:       "Implicit row ID that is automatically assigned",
		Type:      Int64Type{},
	}
}

// LastUpdatedSequenceNumber returns a NestedField for _last_updated_sequence_number (optional long).
func LastUpdatedSequenceNumber() NestedField {
	return NestedField{
		ID:       LastUpdatedSequenceNumberFieldID,
		Name:     LastUpdatedSequenceNumberColumnName,
		Required: false,
		Doc:      "Sequence number when the row was last updated",
		Type:     Int64Type{},
	}
}

// IsMetadataColumn returns true if the field ID is a reserved metadata column (e.g. row lineage).
func IsMetadataColumn(fieldID int) bool {
	return fieldID == RowIDFieldID || fieldID == LastUpdatedSequenceNumberFieldID
}
