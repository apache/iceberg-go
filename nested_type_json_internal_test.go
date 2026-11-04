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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNestedTypeJSONRoundTripsEveryField fails when a field is added to one of
// these types without a value in its fixture, and when the JSON methods drop a
// field. It walks unexported fields too, so it keeps working after #2062.
func TestNestedTypeJSONRoundTripsEveryField(t *testing.T) {
	t.Run("StructType", func(t *testing.T) {
		requireJSONRoundTripsEveryField(t, StructType{FieldList: []NestedField{
			{ID: 1, Name: "a", Type: PrimitiveTypes.Int32, Required: true},
		}})
	})
	t.Run("ListType", func(t *testing.T) {
		requireJSONRoundTripsEveryField(t, ListType{
			ElementID: 1, Element: PrimitiveTypes.String, ElementRequired: true,
		})
	})
	t.Run("MapType", func(t *testing.T) {
		requireJSONRoundTripsEveryField(t, MapType{
			KeyID: 1, KeyType: PrimitiveTypes.String,
			ValueID: 2, ValueType: PrimitiveTypes.Int32, ValueRequired: true,
		})
	})
	t.Run("NestedField", func(t *testing.T) {
		requireJSONRoundTripsEveryField(t, NestedField{
			ID: 1, Name: "a", Type: PrimitiveTypes.String, Required: true,
			Doc: "doc", InitialDefault: "initial", WriteDefault: "write",
		})
	})
}

func requireJSONRoundTripsEveryField[T any](t *testing.T, value T) {
	t.Helper()

	v := reflect.ValueOf(value)
	for i := range v.NumField() {
		require.Falsef(t, v.Field(i).IsZero(), "populate %s.%s in the fixture",
			v.Type().Name(), v.Type().Field(i).Name)
	}

	data, err := json.Marshal(&value)
	require.NoError(t, err)

	var decoded T
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, value, decoded)
}
