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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNestedTypeConstructorsMatchLiterals pins that each constructor builds the
// same value, with the same JSON, as the composite literal it replaces.
func TestNestedTypeConstructorsMatchLiterals(t *testing.T) {
	fields := []NestedField{
		{ID: 1, Name: "a", Type: PrimitiveTypes.Int32, Required: true},
		{ID: 2, Name: "b", Type: PrimitiveTypes.String},
	}

	tests := []struct {
		name        string
		constructed Type
		literal     Type
	}{
		{"struct", NewStructType(fields...), &StructType{fields: fields}},
		{"empty struct", NewStructType(), &StructType{}},
		{"empty non-nil struct", NewStructType([]NestedField{}...), &StructType{fields: []NestedField{}}},
		{"list", NewListType(3, PrimitiveTypes.Int64, true), &ListType{elementID: 3, element: PrimitiveTypes.Int64, elementRequired: true}},
		{
			"map",
			NewMapType(4, PrimitiveTypes.String, 5, PrimitiveTypes.Float64, false),
			&MapType{keyID: 4, keyType: PrimitiveTypes.String, valueID: 5, valueType: PrimitiveTypes.Float64, valueRequired: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.literal, tt.constructed)
			assert.True(t, tt.constructed.Equals(tt.literal))

			want, err := json.Marshal(tt.literal)
			require.NoError(t, err)
			got, err := json.Marshal(tt.constructed)
			require.NoError(t, err)
			assert.JSONEq(t, string(want), string(got))
		})
	}
}
