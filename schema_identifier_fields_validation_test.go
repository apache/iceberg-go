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
				data, err := json.Marshal(schema)
				require.NoError(t, err)
				var read Schema
				require.NoError(t, json.Unmarshal(data, &read))
				require.Equal(t, tc.ids, read.IdentifierFieldIDs)
			} else {
				require.ErrorIs(t, err, ErrInvalidSchema)
				require.ErrorContains(t, err, tc.error)
				_, err = json.Marshal(schema)
				require.ErrorIs(t, err, ErrInvalidSchema)
			}
		})
	}
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
