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
