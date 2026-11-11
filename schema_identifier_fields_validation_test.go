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

package iceberg

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateIdentifierFields(t *testing.T) {
	requiredID := NestedField{ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true}
	tests := []struct {
		name   string
		ids    []int
		fields []NestedField
		error  string
	}{
		{"root", []int{1}, []NestedField{requiredID}, ""},
		{"nested required struct", []int{2}, []NestedField{{
			ID: 1, Name: "outer", Required: true,
			Type: &StructType{FieldList: []NestedField{{
				ID: 2, Name: "id", Type: PrimitiveTypes.Int64, Required: true,
			}}},
		}}, ""},
		{"unknown field", []int{99}, []NestedField{requiredID}, "does not exist"},
		{"optional field", []int{1}, []NestedField{{
			ID: 1, Name: "id", Type: PrimitiveTypes.Int64,
		}}, "must be required"},
		{"float", []int{1}, []NestedField{{
			ID: 1, Name: "id", Type: PrimitiveTypes.Float32, Required: true,
		}}, "float or double"},
		{"double", []int{1}, []NestedField{{
			ID: 1, Name: "id", Type: PrimitiveTypes.Float64, Required: true,
		}}, "float or double"},
		{"struct", []int{1}, []NestedField{{
			ID: 1, Name: "id", Type: &StructType{FieldList: nil}, Required: true,
		}}, "must be primitive"},
		{"optional struct parent", []int{2}, []NestedField{{
			ID: 1, Name: "outer",
			Type: &StructType{FieldList: []NestedField{{
				ID: 2, Name: "id", Type: PrimitiveTypes.Int64, Required: true,
			}}},
		}}, "optional field"},
		{"list element", []int{2}, []NestedField{{
			ID: 1, Name: "items", Required: true,
			Type: &ListType{ElementID: 2, Element: PrimitiveTypes.Int64, ElementRequired: true},
		}}, "cannot be nested"},
		{"map value", []int{3}, []NestedField{{
			ID: 1, Name: "tags", Required: true,
			Type: &MapType{
				KeyID: 2, KeyType: PrimitiveTypes.String,
				ValueID: 3, ValueType: PrimitiveTypes.Int64, ValueRequired: true,
			},
		}}, "cannot be nested"},
		{"duplicate identifier", []int{1, 1}, []NestedField{requiredID}, "duplicate identifier"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := NewSchemaWithIdentifiers(1, tc.ids, tc.fields...)
			err := schema.ValidateIdentifierFields()
			if tc.error == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidSchema)
				require.ErrorContains(t, err, tc.error)
			}

			// Serialization alone is not a validation boundary. Parsing and
			// committing table metadata must still reject invalid identifiers.
			data, err := json.Marshal(schema)
			require.NoError(t, err)
			var read Schema
			err = json.Unmarshal(data, &read)
			switch {
			case tc.error == "":
				require.NoError(t, err)
				require.Equal(t, tc.ids, read.IdentifierFieldIDs)
			case tc.name == "duplicate identifier":
				require.NoError(t, err)
				require.Equal(t, []int{1}, read.IdentifierFieldIDs)
			default:
				require.ErrorIs(t, err, ErrInvalidSchema)
				require.ErrorContains(t, err, tc.error)
			}
		})
	}
}

func TestUnmarshalSchemaDeduplicatesIdentifierIDs(t *testing.T) {
	var schema Schema
	err := json.Unmarshal([]byte(`{
		"type": "struct",
		"schema-id": 3,
		"identifier-field-ids": [2, 1, 2, 1, 2],
		"fields": [
			{"id": 1, "name": "id", "type": "long", "required": true},
			{"id": 2, "name": "tenant", "type": "string", "required": true}
		]
	}`), &schema)
	require.NoError(t, err)
	require.Equal(t, []int{2, 1}, schema.IdentifierFieldIDs)

	data, err := json.Marshal(&schema)
	require.NoError(t, err)
	var roundTrip Schema
	require.NoError(t, json.Unmarshal(data, &roundTrip))
	require.Equal(t, []int{2, 1}, roundTrip.IdentifierFieldIDs)
	require.True(t, schema.Equals(&roundTrip))
}

func TestUnmarshalSchemaRejectsInvalidIdentifierAndKeepsOldState(t *testing.T) {
	schema := NewSchema(7,
		NestedField{ID: 1, Name: "original", Required: true, Type: PrimitiveTypes.Int64},
	)
	err := json.Unmarshal([]byte(`{
		"type": "struct",
		"schema-id": 8,
		"identifier-field-ids": [2],
		"fields": [{"id": 2, "name": "invalid", "type": "string", "required": false}]
	}`), schema)
	require.ErrorIs(t, err, ErrInvalidSchema)
	require.Equal(t, 7, schema.ID)
	require.Equal(t, "original", schema.Field(0).Name)
	require.Empty(t, schema.IdentifierFieldIDs)
}
