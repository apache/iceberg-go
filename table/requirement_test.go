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
	"fmt"
	"testing"

	"github.com/DataDog/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			data:        []byte(`{"type": "assert-ref-snapshot-id", "ref": "branch", "snapshot-id": null}`),
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
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
			} else {
				require.NoError(t, err)
			}
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

func TestParseRequirementListReplacesExistingSlice(t *testing.T) {
	var requirements table.Requirements
	require.NoError(t, json.Unmarshal([]byte(`[
		{"type": "assert-create"}
	]`), &requirements))

	require.NoError(t, json.Unmarshal([]byte(`[
		{"type": "assert-default-spec-id", "default-spec-id": 1}
	]`), &requirements))
	require.Len(t, requirements, 1)
	assert.Equal(t, table.AssertDefaultSpecID(1), requirements[0])

	previous := append(table.Requirements(nil), requirements...)
	err := json.Unmarshal([]byte(`[
		{"type": "assert-create"},
		{"type": "assert-foo-bar"}
	]`), &requirements)
	require.Error(t, err)
	assert.Equal(t, previous, requirements)

	require.NoError(t, json.Unmarshal([]byte(`[]`), &requirements))
	assert.Empty(t, requirements)
}

func TestParseRequirementRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name          string
		data          string
		expectedField string
	}{
		{name: "missing type", data: `{}`, expectedField: "type"},
		{name: "null type", data: `{"type":null}`, expectedField: "type"},
		{name: "table uuid", data: `{"type":"assert-table-uuid"}`, expectedField: "uuid"},
		{name: "null table uuid", data: `{"type":"assert-table-uuid","uuid":null}`, expectedField: "uuid"},
		{name: "missing ref", data: `{"type":"assert-ref-snapshot-id"}`, expectedField: "ref"},
		{name: "null ref", data: `{"type":"assert-ref-snapshot-id","ref":null}`, expectedField: "ref"},
		{name: "missing snapshot id", data: `{"type":"assert-ref-snapshot-id","ref":"main"}`, expectedField: "snapshot-id"},
		{name: "default spec id", data: `{"type":"assert-default-spec-id"}`, expectedField: "default-spec-id"},
		{name: "null default spec id", data: `{"type":"assert-default-spec-id","default-spec-id":null}`, expectedField: "default-spec-id"},
		{name: "current schema id", data: `{"type":"assert-current-schema-id"}`, expectedField: "current-schema-id"},
		{name: "null current schema id", data: `{"type":"assert-current-schema-id","current-schema-id":null}`, expectedField: "current-schema-id"},
		{name: "default sort order id", data: `{"type":"assert-default-sort-order-id"}`, expectedField: "default-sort-order-id"},
		{name: "null default sort order id", data: `{"type":"assert-default-sort-order-id","default-sort-order-id":null}`, expectedField: "default-sort-order-id"},
		{name: "last assigned field id", data: `{"type":"assert-last-assigned-field-id"}`, expectedField: "last-assigned-field-id"},
		{name: "null last assigned field id", data: `{"type":"assert-last-assigned-field-id","last-assigned-field-id":null}`, expectedField: "last-assigned-field-id"},
		{name: "last assigned partition id", data: `{"type":"assert-last-assigned-partition-id"}`, expectedField: "last-assigned-partition-id"},
		{name: "null last assigned partition id", data: `{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":null}`, expectedField: "last-assigned-partition-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedError := fmt.Sprintf("missing required field %q", tt.expectedField)

			_, err := table.ParseRequirementBytes([]byte(tt.data))
			require.ErrorIs(t, err, table.ErrInvalidRequirement)
			require.ErrorContains(t, err, expectedError)

			var requirements table.Requirements
			err = json.Unmarshal([]byte("["+tt.data+"]"), &requirements)
			require.ErrorIs(t, err, table.ErrInvalidRequirement)
			require.ErrorContains(t, err, expectedError)
		})
	}
}

func TestParseRequirementAcceptsExplicitZero(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		expected table.Requirement
	}{
		{name: "default spec id", data: `{"type":"assert-default-spec-id","default-spec-id":0}`, expected: table.AssertDefaultSpecID(0)},
		{name: "current schema id", data: `{"type":"assert-current-schema-id","current-schema-id":0}`, expected: table.AssertCurrentSchemaID(0)},
		{name: "default sort order id", data: `{"type":"assert-default-sort-order-id","default-sort-order-id":0}`, expected: table.AssertDefaultSortOrderID(0)},
		{name: "last assigned field id", data: `{"type":"assert-last-assigned-field-id","last-assigned-field-id":0}`, expected: table.AssertLastAssignedFieldID(0)},
		{name: "last assigned partition id", data: `{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":0}`, expected: table.AssertLastAssignedPartitionID(0)},
		{name: "snapshot id", data: `{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":0}`, expected: table.AssertRefSnapshotID("main", ptr(int64(0)))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := table.ParseRequirementBytes([]byte(tt.data))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)

			var requirements table.Requirements
			require.NoError(t, json.Unmarshal([]byte("["+tt.data+"]"), &requirements))
			require.Len(t, requirements, 1)
			assert.Equal(t, tt.expected, requirements[0])
		})
	}
}

func TestParseRequirementRefRequiresRefButAllowsNullSnapshotID(t *testing.T) {
	data := []byte(`{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}`)

	actual, err := table.ParseRequirementBytes(data)
	require.NoError(t, err)
	assert.Equal(t, table.AssertRefSnapshotID("main", nil), actual)

	var requirements table.Requirements
	require.NoError(t, json.Unmarshal([]byte("["+string(data)+"]"), &requirements))
	require.Len(t, requirements, 1)
	assert.Equal(t, table.AssertRefSnapshotID("main", nil), requirements[0])
}

func TestParseRequirementRefAcceptsNumericSnapshotID(t *testing.T) {
	actual, err := table.ParseRequirementBytes([]byte(`{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":42}`))
	require.NoError(t, err)
	assert.Equal(t, table.AssertRefSnapshotID("main", ptr(int64(42))), actual)
}

func TestAssertRefSnapshotIDValidate(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(table.ExampleTableMetadataV2))
	require.NoError(t, err)

	t.Run("matching ref passes", func(t *testing.T) {
		req := table.AssertRefSnapshotID("test", ptr(int64(3051729675574597004)))
		assert.NoError(t, req.Validate(meta))
	})

	t.Run("mismatched snapshot id includes expected and found", func(t *testing.T) {
		req := table.AssertRefSnapshotID("test", ptr(int64(1)))
		err := req.Validate(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"test"`)
		assert.Contains(t, err.Error(), "expected id 1")
		assert.Contains(t, err.Error(), "found 3051729675574597004")
		assert.Contains(t, err.Error(), "has changed")
	})

	t.Run("nil expected but ref exists includes found snapshot", func(t *testing.T) {
		req := table.AssertRefSnapshotID("test", nil)
		err := req.Validate(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"test"`)
		assert.Contains(t, err.Error(), "was created concurrently")
	})

	t.Run("ref missing but expected includes expected snapshot", func(t *testing.T) {
		req := table.AssertRefSnapshotID("nonexistent", ptr(int64(42)))
		err := req.Validate(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"nonexistent"`)
		assert.Contains(t, err.Error(), "is missing")
		assert.Contains(t, err.Error(), "expected 42")
	})

	t.Run("nil metadata returns error", func(t *testing.T) {
		req := table.AssertRefSnapshotID("main", ptr(int64(1)))
		assert.Error(t, req.Validate(nil))
	})

	t.Run("nil expected and ref missing passes", func(t *testing.T) {
		req := table.AssertRefSnapshotID("nonexistent", nil)
		assert.NoError(t, req.Validate(meta))
	})
}
