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

package table

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshalUpdates(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket",
		},
	)
	sortOrder := SortOrder{
		OrderID: 22,
		Fields: []SortField{
			{SourceID: 19, Transform: iceberg.IdentityTransform{}, NullOrder: NullsFirst},
			{SourceID: 25, Transform: iceberg.BucketTransform{NumBuckets: 4}, Direction: SortDESC},
			{SourceID: 22, Transform: iceberg.VoidTransform{}, Direction: SortASC},
		},
	}

	testCases := []struct {
		name        string
		data        []byte
		expected    Updates
		expectedErr bool
		errContains string
	}{
		{
			name: "should unmarshal a list of updates",
			data: []byte(`[
				{"action": "assign-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"},
				{"action": "upgrade-format-version", "format-version": 2},
				{"action": "set-location", "location": "s3://bucket/new-location"},
				{"action": "set-properties", "updates": {"key1": "value1"}},
				{"action": "remove-properties", "removals": ["key2"]}
			]`),
			expected: Updates{
				NewAssignUUIDUpdate(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")),
				NewUpgradeFormatVersionUpdate(2),
				NewSetLocationUpdate("s3://bucket/new-location"),
				NewSetPropertiesUpdate(iceberg.Properties{"key1": "value1"}),
				NewRemovePropertiesUpdate([]string{"key2"}),
			},
			expectedErr: false,
		},
		{
			name: "should unmarshal a complex list of updates",
			data: []byte(`[
  {
    "action": "add-schema",
    "schema": {
      "type": "struct",
      "schema-id": 1,
      "fields": [
        {
          "id": 1,
          "name": "foo",
          "required": true,
          "type": "string"
        }
      ]
    },
    "last-column-id": 1
  },
  {
    "action": "add-spec",
    "spec": {
      "spec-id": 3,
      "fields": [
        {
          "source-id": 1,
          "field-id": 1000,
          "transform": "truncate[19]",
          "name": "str_truncate"
        },
        {
          "source-id": 2,
          "field-id": 1001,
          "transform": "bucket[25]",
          "name": "int_bucket"
        }
      ]
    }
  },
  {
    "action": "add-sort-order",
    "sort-order": {
      "order-id": 22,
      "fields": [
        {
          "source-id": 19,
          "transform": "identity",
          "direction": "asc",
          "null-order": "nulls-first"
        },
        {
          "source-id": 25,
          "transform": "bucket[4]",
          "direction": "desc",
          "null-order": "nulls-last"
        },
        {
          "source-id": 22,
          "transform": "void",
          "direction": "asc",
          "null-order": "nulls-first"
        }
      ]
    }
  },
  {
    "action": "set-current-schema",
    "schema-id": 1
  }
]`),
			expected: Updates{
				NewAddSchemaUpdate(iceberg.NewSchema(1,
					iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.StringType{}, Required: true},
				)),
				NewAddPartitionSpecUpdate(
					&spec, false),
				NewAddSortOrderUpdate(&sortOrder, false),
				NewSetCurrentSchemaUpdate(1),
			},
			expectedErr: false,
		},
		{
			name:        "should handle an empty list",
			data:        []byte(`[]`),
			expected:    nil,
			expectedErr: false,
		},
		{
			name:        "should error on unknown action",
			data:        []byte(`[{"action": "unknown-action"}]`),
			expected:    nil,
			expectedErr: true,
			errContains: "unknown update action: unknown-action",
		},
		{
			name:        "should error on invalid json value",
			data:        []byte(`[{"action": "assign-uuid", "uuid": "not-a-uuid"}]`),
			expected:    nil,
			expectedErr: true,
		},
		{
			name:        "should error on invalid json array syntax",
			data:        []byte(`[{"action": "set-location", "location": "loc1"},]`),
			expected:    nil,
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var actual Updates
			err := json.Unmarshal(tc.data, &actual)

			if tc.expectedErr {
				assert.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tc.expected), len(actual))
				for idx, u := range actual {
					switch u.Action() {
					case "add-schema":
						expectedAddSchema := u.(*addSchemaUpdate)
						actualAddSchema := actual[idx].(*addSchemaUpdate)
						assert.True(t, expectedAddSchema.Schema.Equals(actualAddSchema.Schema))
						assert.Equal(t, actualAddSchema.initial, expectedAddSchema.initial)
					case "add-partition-spec":
						expectedAddPartitionSpec := u.(*addPartitionSpecUpdate)
						actualAddPartitionSpec := actual[idx].(*addPartitionSpecUpdate)
						assert.True(t, expectedAddPartitionSpec.Spec.Equals(*actualAddPartitionSpec.Spec))
						assert.Equal(t, actualAddPartitionSpec.initial, expectedAddPartitionSpec.initial)
					case "add-sort-order":
						expectedAddSortOrder := u.(*addSortOrderUpdate)
						actualAddSortOrder := actual[idx].(*addSortOrderUpdate)
						assert.True(t, expectedAddSortOrder.SortOrder.Equals(*actualAddSortOrder.SortOrder))
						assert.Equal(t, actualAddSortOrder.initial, expectedAddSortOrder.initial)
					default:
						assert.Equal(t, u, actual[idx])
					}
				}
			}
		})
	}
}

func TestRemoveSchemas(t *testing.T) {
	var builder *MetadataBuilder
	removeSchemas := removeSchemasUpdate{
		SchemaIds: []int64{},
	}
	t.Run("remove schemas should fail", func(t *testing.T) {
		if err := removeSchemas.Apply(builder); !errors.Is(err, iceberg.ErrNotImplemented) {
			t.Fatalf("Expected unimplemented error, got %v", err)
		}
	})
}

func TestRemovePartitionSpecs(t *testing.T) {
	var builder *MetadataBuilder
	removeSpecs := removeSpecUpdate{
		SpecIds: []int64{},
	}
	t.Run("remove specs should fail", func(t *testing.T) {
		if err := removeSpecs.Apply(builder); !errors.Is(err, iceberg.ErrNotImplemented) {
			t.Fatalf("Expected unimplemented error, got %v", err)
		}
	})
}
