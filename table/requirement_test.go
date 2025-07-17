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
	"testing"

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
