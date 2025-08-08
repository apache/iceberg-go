// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

import (
	"encoding/json"
	"iter"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestParseRequirementBytes(t *testing.T) {
	testCases := []struct {
		name        string
		data        []byte
		expected    table.Requirement
		expectedErr error
	}{
		{
			name:        "Should parse an assert create",
			data:        []byte(`{"type": "assert-create"}`),
			expected:    table.AssertCreate(),
			expectedErr: nil,
		},
		{
			name:        "Should parse an assert table uuid",
			data:        []byte(`{"type": "assert-table-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"}`),
			expected:    table.AssertTableUUID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")),
			expectedErr: nil,
		},
		{
			name:        "Should parse an assert ref snapshot id",
			data:        []byte(`{"type": "assert-ref-snapshot-id", "ref": "branch"}`),
			expected:    table.AssertRefSnapshotID("branch", nil),
			expectedErr: nil,
		},
		{
			name:        "Should parse an assert default spec id",
			data:        []byte(`{"type": "assert-default-spec-id", "default-spec-id": 42}`),
			expected:    table.AssertDefaultSpecID(42),
			expectedErr: nil,
		},
		{
			name:        "Should parse an assert current schema id",
			data:        []byte(`{"type": "assert-current-schema-id", "current-schema-id": 10}`),
			expected:    table.AssertCurrentSchemaID(10),
			expectedErr: nil,
		},
		{
			name:        "Should parse an assert default sort order",
			data:        []byte(`{"type": "assert-default-sort-order-id", "default-sort-order-id": 12}`),
			expected:    table.AssertDefaultSortOrderID(12),
			expectedErr: nil,
		},
		{
			name:        "Should parse an assert last assigned field",
			data:        []byte(`{"type": "assert-last-assigned-field-id", "last-assigned-field-id": 13}`),
			expected:    table.AssertLastAssignedFieldID(13),
			expectedErr: nil,
		},
		{
			name:        "Should parse an assert last assigned partition",
			data:        []byte(`{"type": "assert-last-assigned-partition-id", "last-assigned-partition-id": 13}`),
			expected:    table.AssertLastAssignedPartitionID(13),
			expectedErr: nil,
		},
		{
			name:        "invalid requirement",
			data:        []byte(`{"type": "invalid"}`),
			expected:    nil,
			expectedErr: table.ErrInvalidRequirement,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := table.ParseRequirementBytes(tc.data)
			assert.Equal(t, tc.expected, actual)
			assert.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestParseRequirementList(t *testing.T) {
	t.Run("should parse a list of requirements", func(t *testing.T) {
		jsonData := []byte(`[
			{"type": "assert-create"},
			{"type": "assert-table-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"},
			{"type": "assert-default-spec-id", "default-spec-id": 1}
		]`)

		expected := table.Requirements{
			table.AssertCreate(),
			table.AssertTableUUID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")),
			table.AssertDefaultSpecID(1),
		}

		var actual table.Requirements
		err := json.Unmarshal(jsonData, &actual)

		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("should handle an empty list", func(t *testing.T) {
		jsonData := []byte(`[]`)
		var actual table.Requirements
		err := json.Unmarshal(jsonData, &actual)
		assert.NoError(t, err)
		assert.Empty(t, actual)
	})

	t.Run("should return an error for an unknown requirement type in the list", func(t *testing.T) {
		jsonData := []byte(`[
			{"type": "assert-create"},
			{"type": "assert-foo-bar"}
		]`)

		var actual table.Requirements
		err := json.Unmarshal(jsonData, &actual)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown requirement type: assert-foo-bar")
	})

	t.Run("should return an error for invalid json", func(t *testing.T) {
		jsonData := []byte(`[{"type": "assert-create"},]`) // trailing comma
		var actual table.Requirements
		err := json.Unmarshal(jsonData, &actual)
		assert.Error(t, err)
	})
}

// mockMetadata is a correct and complete mock implementation of the
// table.Metadata interface, used for testing requirement validation.
type mockMetadata struct {
	uuid                uuid.UUID
	currentSchemaID     int
	defaultSortOrderID  int
	defaultSpecID       int
	lastAssignedFieldID int
	lastPartitionID     int
	refs                map[string]table.SnapshotRef
	// Add any other fields you need for testing here
}

// Ensure the mock fully implements the interface at compile time.
var _ table.Metadata = (*mockMetadata)(nil)

func (m *mockMetadata) Version() int             { return 2 }
func (m *mockMetadata) TableUUID() uuid.UUID     { return m.uuid }
func (m *mockMetadata) Location() string         { return "s3://bucket/location" }
func (m *mockMetadata) LastUpdatedMillis() int64 { return 1234567890 }
func (m *mockMetadata) LastColumnID() int        { return m.lastAssignedFieldID }
func (m *mockMetadata) Schemas() []*iceberg.Schema {
	return []*iceberg.Schema{{ID: m.currentSchemaID}}
}
func (m *mockMetadata) CurrentSchema() *iceberg.Schema {
	return &iceberg.Schema{ID: m.currentSchemaID}
}
func (m *mockMetadata) PartitionSpecs() []iceberg.PartitionSpec { return nil }
func (m *mockMetadata) PartitionSpec() iceberg.PartitionSpec {
	return *iceberg.UnpartitionedSpec
}
func (m *mockMetadata) DefaultPartitionSpec() int   { return m.defaultSpecID }
func (m *mockMetadata) LastPartitionSpecID() *int   { return &m.lastPartitionID }
func (m *mockMetadata) Snapshots() []table.Snapshot { return nil }
func (m *mockMetadata) SnapshotByID(id int64) *table.Snapshot {
	for _, ref := range m.refs {
		if ref.SnapshotID == id {
			return &table.Snapshot{SnapshotID: id}
		}
	}
	return nil
}
func (m *mockMetadata) SnapshotByName(name string) *table.Snapshot {
	if ref, ok := m.refs[name]; ok {
		return m.SnapshotByID(ref.SnapshotID)
	}
	return nil
}
func (m *mockMetadata) CurrentSnapshot() *table.Snapshot {
	if ref, ok := m.refs["main"]; ok {
		return m.SnapshotByID(ref.SnapshotID)
	}
	return nil
}
func (m *mockMetadata) Ref() table.SnapshotRef {
	if ref, ok := m.refs["main"]; ok {
		return ref
	}
	return table.SnapshotRef{}
}
func (m *mockMetadata) Refs() iter.Seq2[string, table.SnapshotRef] {
	return func(yield func(string, table.SnapshotRef) bool) {
		for k, v := range m.refs {
			if !yield(k, v) {
				return
			}
		}
	}
}
func (m *mockMetadata) SnapshotLogs() iter.Seq[table.SnapshotLogEntry]  { return nil }
func (m *mockMetadata) SortOrder() table.SortOrder                      { return table.UnsortedSortOrder }
func (m *mockMetadata) SortOrders() []table.SortOrder                   { return nil }
func (m *mockMetadata) DefaultSortOrder() int                           { return m.defaultSortOrderID }
func (m *mockMetadata) Properties() iceberg.Properties                  { return iceberg.Properties{} }
func (m *mockMetadata) PreviousFiles() iter.Seq[table.MetadataLogEntry] { return nil }
func (m *mockMetadata) Equals(other table.Metadata) bool                { return false }
func (m *mockMetadata) NameMapping() iceberg.NameMapping                { return nil }
func (m *mockMetadata) LastSequenceNumber() int64                       { return 0 }

func TestRequirementValidation(t *testing.T) {
	testUUID := uuid.New()
	snapshotID := int64(100)
	mockMeta := &mockMetadata{
		uuid:                testUUID,
		currentSchemaID:     1,
		defaultSortOrderID:  2,
		defaultSpecID:       3,
		lastAssignedFieldID: 4,
		lastPartitionID:     5,
		refs: map[string]table.SnapshotRef{
			"main": {SnapshotID: snapshotID, SnapshotRefType: "branch"},
		},
	}

	testCases := []struct {
		name           string
		requirement    table.Requirement
		expectedType   string
		metaToValidate table.Metadata // Use nil for non-existent table
		expectError    bool
		errorContains  string
	}{
		// --- AssertCreate ---
		{
			name:           "AssertCreate - Success (meta is nil)",
			requirement:    table.AssertCreate(),
			expectedType:   "assert-create",
			metaToValidate: nil,
			expectError:    false,
		},
		{
			name:           "AssertCreate - Failure (meta exists)",
			requirement:    table.AssertCreate(),
			expectedType:   "assert-create",
			metaToValidate: mockMeta,
			expectError:    true,
			errorContains:  "Table already exists",
		},
		// --- AssertTableUUID ---
		{
			name:           "AssertTableUUID - Success",
			requirement:    table.AssertTableUUID(testUUID),
			expectedType:   "assert-table-uuid",
			metaToValidate: mockMeta,
			expectError:    false,
		},
		{
			name:           "AssertTableUUID - Failure (mismatched UUID)",
			requirement:    table.AssertTableUUID(uuid.New()),
			expectedType:   "assert-table-uuid",
			metaToValidate: mockMeta,
			expectError:    true,
			errorContains:  "UUID mismatch",
		},
		{
			name:           "AssertTableUUID - Failure (meta is nil)",
			requirement:    table.AssertTableUUID(testUUID),
			expectedType:   "assert-table-uuid",
			metaToValidate: nil,
			expectError:    true,
			errorContains:  "metadata does not exist",
		},
		// --- AssertCurrentSchemaID ---
		{
			name:           "AssertCurrentSchemaID - Success",
			requirement:    table.AssertCurrentSchemaID(1),
			expectedType:   "assert-current-schema-id",
			metaToValidate: mockMeta,
			expectError:    false,
		},
		{
			name:           "AssertCurrentSchemaID - Failure (mismatched ID)",
			requirement:    table.AssertCurrentSchemaID(99),
			expectedType:   "assert-current-schema-id",
			metaToValidate: mockMeta,
			expectError:    true,
			errorContains:  "current schema id has changed",
		},
		// --- AssertRefSnapshotID ---
		{
			name:           "AssertRefSnapshotID - Success (ref exists)",
			requirement:    table.AssertRefSnapshotID("main", &snapshotID),
			expectedType:   "assert-ref-snapshot-id",
			metaToValidate: mockMeta,
			expectError:    false,
		},
		{
			name:           "AssertRefSnapshotID - Success (ref does not exist)",
			requirement:    table.AssertRefSnapshotID("new-branch", nil),
			expectedType:   "assert-ref-snapshot-id",
			metaToValidate: mockMeta,
			expectError:    false,
		},
		{
			name:           "AssertRefSnapshotID - Failure (mismatched snapshot ID)",
			requirement:    table.AssertRefSnapshotID("main", func() *int64 { id := int64(99); return &id }()),
			expectedType:   "assert-ref-snapshot-id",
			metaToValidate: mockMeta,
			expectError:    true,
			errorContains:  "has changed",
		},
		{
			name:           "AssertRefSnapshotID - Failure (ref exists when it shouldn't)",
			requirement:    table.AssertRefSnapshotID("main", nil),
			expectedType:   "assert-ref-snapshot-id",
			metaToValidate: mockMeta,
			expectError:    true,
			errorContains:  "was created concurrently",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test GetType()
			assert.Equal(t, tc.expectedType, tc.requirement.GetType(), "GetType() mismatch")

			// Test Validate()
			err := tc.requirement.Validate(tc.metaToValidate)

			if tc.expectError {
				assert.Error(t, err, "Expected an error but got nil")
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains, "Error message did not contain expected text")
				}
			} else {
				assert.NoError(t, err, "Expected no error but got one")
			}
		})
	}
}
