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

package iceberg_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeID(v int) *int { return &v }

var tableNameMappingNested = iceberg.NameMapping{
	{FieldID: makeID(1), Names: []string{"foo"}},
	{FieldID: makeID(2), Names: []string{"bar"}},
	{FieldID: makeID(3), Names: []string{"baz"}},
	{
		FieldID: makeID(4), Names: []string{"qux"},
		Fields: []iceberg.MappedField{{FieldID: makeID(5), Names: []string{"element"}}},
	},
	{FieldID: makeID(6), Names: []string{"quux"}, Fields: []iceberg.MappedField{
		{FieldID: makeID(7), Names: []string{"key"}},
		{FieldID: makeID(8), Names: []string{"value"}, Fields: []iceberg.MappedField{
			{FieldID: makeID(9), Names: []string{"key"}},
			{FieldID: makeID(10), Names: []string{"value"}},
		}},
	}},
	{FieldID: makeID(11), Names: []string{"location"}, Fields: []iceberg.MappedField{
		{FieldID: makeID(12), Names: []string{"element"}, Fields: []iceberg.MappedField{
			{FieldID: makeID(13), Names: []string{"latitude"}},
			{FieldID: makeID(14), Names: []string{"longitude"}},
		}},
	}},
	{FieldID: makeID(15), Names: []string{"person"}, Fields: []iceberg.MappedField{
		{FieldID: makeID(16), Names: []string{"name"}},
		{FieldID: makeID(17), Names: []string{"age"}},
	}},
}

func TestJsonMappedField(t *testing.T) {
	tests := []struct {
		name string
		str  string
		exp  iceberg.MappedField
	}{
		{
			"simple", `{"field-id": 1, "names": ["id", "record_id"]}`,
			iceberg.MappedField{FieldID: makeID(1), Names: []string{"id", "record_id"}},
		},
		{
			"with null fields", `{"field-id": 1, "names": ["id", "record_id"], "fields": null}`,
			iceberg.MappedField{FieldID: makeID(1), Names: []string{"id", "record_id"}},
		},
		{"no names", `{"field-id": 1, "names": []}`, iceberg.MappedField{FieldID: makeID(1), Names: []string{}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n iceberg.MappedField
			require.NoError(t, json.Unmarshal([]byte(tt.str), &n))
			assert.Equal(t, tt.exp, n)
		})
	}
}

func TestNameMappingFromJson(t *testing.T) {
	mapping := `[
		{"names": ["foo", "bar"]},
		{"field-id": 1, "names": ["id", "record_id"]},
		{"field-id": 2, "names": ["data"]},
		{"field-id": 3, "names": ["location"], "fields": [
			{"field-id": 4, "names": ["latitude", "lat"]},
			{"field-id": 5, "names": ["longitude", "long"]}
		]}
	]`

	var nm iceberg.NameMapping
	require.NoError(t, json.Unmarshal([]byte(mapping), &nm))

	assert.Equal(t, nm, iceberg.NameMapping{
		{FieldID: nil, Names: []string{"foo", "bar"}},
		{FieldID: makeID(1), Names: []string{"id", "record_id"}},
		{FieldID: makeID(2), Names: []string{"data"}},
		{FieldID: makeID(3), Names: []string{"location"}, Fields: []iceberg.MappedField{
			{FieldID: makeID(4), Names: []string{"latitude", "lat"}},
			{FieldID: makeID(5), Names: []string{"longitude", "long"}},
		}},
	})
}

func TestNameMappingToJson(t *testing.T) {
	result, err := json.Marshal(tableNameMappingNested)
	require.NoError(t, err)
	assert.JSONEq(t, `[
  		{"field-id": 1, "names": ["foo"]},
		{"field-id": 2, "names": ["bar"]},
  		{"field-id": 3, "names": ["baz"]},
  		{"field-id": 4, "names": ["qux"], "fields": [{"field-id": 5, "names": ["element"]}]},		
  		{"field-id": 6, "names": ["quux"], "fields": [
      		{"field-id": 7, "names": ["key"]},
      		{"field-id": 8, "names": ["value"], "fields": [
          		{"field-id": 9, "names": ["key"]},
          		{"field-id": 10, "names": ["value"]}
        	]}
    	]},
  		{"field-id": 11, "names": ["location"], "fields": [
      		{"field-id": 12, "names": ["element"], "fields": [
          		{"field-id": 13, "names": ["latitude"]},
          		{"field-id": 14, "names": ["longitude"]}
        	]}
    	]},
  		{"field-id": 15, "names": ["person"], "fields": [
      		{"field-id": 16, "names": ["name"]},
      		{"field-id": 17, "names": ["age"]}
    	]}
]`, string(result))
}

func TestNameMappingToString(t *testing.T) {
	assert.Equal(t, `[
	([foo] -> ?)
	([id, record_id] -> 1)
	([data] -> 2)
	([location] -> 3 ([lat, latitude] -> 4), ([long, longitude] -> 5))
]`, iceberg.NameMapping{
		{Names: []string{"foo"}},
		{FieldID: makeID(1), Names: []string{"id", "record_id"}},
		{FieldID: makeID(2), Names: []string{"data"}},
		{FieldID: makeID(3), Names: []string{"location"}, Fields: []iceberg.MappedField{
			{FieldID: makeID(4), Names: []string{"lat", "latitude"}},
			{FieldID: makeID(5), Names: []string{"long", "longitude"}},
		}},
	}.String())
}
