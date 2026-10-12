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

var tableNameMappingNested = iceberg.NameMapping{
	{FieldID: new(1), Names: []string{"foo"}},
	{FieldID: new(2), Names: []string{"bar"}},
	{FieldID: new(3), Names: []string{"baz"}},
	{
		FieldID: new(4), Names: []string{"qux"},
		Fields: []iceberg.MappedField{{FieldID: new(5), Names: []string{"element"}}},
	},
	{FieldID: new(6), Names: []string{"quux"}, Fields: []iceberg.MappedField{
		{FieldID: new(7), Names: []string{"key"}},
		{FieldID: new(8), Names: []string{"value"}, Fields: []iceberg.MappedField{
			{FieldID: new(9), Names: []string{"key"}},
			{FieldID: new(10), Names: []string{"value"}},
		}},
	}},
	{FieldID: new(11), Names: []string{"location"}, Fields: []iceberg.MappedField{
		{FieldID: new(12), Names: []string{"element"}, Fields: []iceberg.MappedField{
			{FieldID: new(13), Names: []string{"latitude"}},
			{FieldID: new(14), Names: []string{"longitude"}},
		}},
	}},
	{FieldID: new(15), Names: []string{"person"}, Fields: []iceberg.MappedField{
		{FieldID: new(16), Names: []string{"name"}},
		{FieldID: new(17), Names: []string{"age"}},
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
			iceberg.MappedField{FieldID: new(1), Names: []string{"id", "record_id"}},
		},
		{
			"with null fields", `{"field-id": 1, "names": ["id", "record_id"], "fields": null}`,
			iceberg.MappedField{FieldID: new(1), Names: []string{"id", "record_id"}},
		},
		{"no names", `{"field-id": 1, "names": []}`, iceberg.MappedField{FieldID: new(1), Names: []string{}}},
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
		{FieldID: new(1), Names: []string{"id", "record_id"}},
		{FieldID: new(2), Names: []string{"data"}},
		{FieldID: new(3), Names: []string{"location"}, Fields: []iceberg.MappedField{
			{FieldID: new(4), Names: []string{"latitude", "lat"}},
			{FieldID: new(5), Names: []string{"longitude", "long"}},
		}},
	})
}

func TestApplyNameMappingUsesAlias(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 100, Name: "renamed", Type: iceberg.PrimitiveTypes.String},
	)
	mapping := iceberg.NameMapping{
		{FieldID: new(1), Names: []string{"legacy", "renamed"}},
	}

	result, err := iceberg.ApplyNameMapping(schema, mapping)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Field(0).ID)
}

func TestNameMappingAccessorRemainsComparable(t *testing.T) {
	first := iceberg.NameMappingAccessor{}
	second := iceberg.NameMappingAccessor{}
	accessors := map[iceberg.NameMappingAccessor]struct{}{first: {}}

	_, ok := accessors[second]
	assert.True(t, ok)
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
		{FieldID: new(1), Names: []string{"id", "record_id"}},
		{FieldID: new(2), Names: []string{"data"}},
		{FieldID: new(3), Names: []string{"location"}, Fields: []iceberg.MappedField{
			{FieldID: new(4), Names: []string{"lat", "latitude"}},
			{FieldID: new(5), Names: []string{"long", "longitude"}},
		}},
	}.String())
}

func TestUpdateNameMapping(t *testing.T) {
	originalMapping := iceberg.NameMapping{
		{FieldID: new(1), Names: []string{"foo"}},
		{FieldID: new(2), Names: []string{"bar"}},
		{FieldID: new(3), Names: []string{"baz"}},
		{FieldID: new(4), Names: []string{"qux"}, Fields: []iceberg.MappedField{
			{FieldID: new(5), Names: []string{"element"}},
		}},
		{FieldID: new(6), Names: []string{"quux"}, Fields: []iceberg.MappedField{
			{FieldID: new(7), Names: []string{"key"}},
			{FieldID: new(8), Names: []string{"value"}, Fields: []iceberg.MappedField{
				{FieldID: new(9), Names: []string{"key"}},
				{FieldID: new(10), Names: []string{"value"}},
			}},
		}},
		{FieldID: new(11), Names: []string{"location"}, Fields: []iceberg.MappedField{
			{FieldID: new(12), Names: []string{"element"}, Fields: []iceberg.MappedField{
				{FieldID: new(13), Names: []string{"latitude"}},
				{FieldID: new(14), Names: []string{"longitude"}},
			}},
		}},
		{FieldID: new(15), Names: []string{"person"}, Fields: []iceberg.MappedField{
			{FieldID: new(16), Names: []string{"name"}},
			{FieldID: new(17), Names: []string{"age"}},
		}},
	}

	t.Run("no updates or adds", func(t *testing.T) {
		result, err := iceberg.UpdateNameMapping(originalMapping, map[int]iceberg.NestedField{}, map[int][]iceberg.NestedField{})
		require.NoError(t, err)
		assert.Equal(t, originalMapping, result)
	})

	t.Run("result does not alias the original", func(t *testing.T) {
		fieldID, childID, anonymousChildID := 1, 2, 3
		parentNames := make([]string, 2)
		parentNames[0] = "parent"
		original := iceberg.NameMapping{
			{
				FieldID: &fieldID,
				Names:   parentNames[:1],
				Fields: []iceberg.MappedField{{
					FieldID: &childID,
					Names:   []string{"child"},
				}},
			},
			{
				Names: []string{"anonymous"},
				Fields: []iceberg.MappedField{{
					FieldID: &anonymousChildID,
					Names:   []string{"anonymous-child"},
				}},
			},
		}

		result, err := iceberg.UpdateNameMapping(
			original,
			map[int]iceberg.NestedField{1: {ID: 1, Name: "renamed-parent"}},
			map[int][]iceberg.NestedField{},
		)
		require.NoError(t, err)
		assert.Empty(t, parentNames[1])

		*result[0].FieldID = 10
		result[0].Names[0] = "changed-parent"
		*result[0].Fields[0].FieldID = 20
		result[0].Fields[0].Names[0] = "changed-child"
		result[1].Names[0] = "changed-anonymous"
		*result[1].Fields[0].FieldID = 30
		result[1].Fields[0].Names[0] = "changed-anonymous-child"

		assert.Equal(t, 1, *original[0].FieldID)
		assert.Equal(t, []string{"parent"}, original[0].Names)
		assert.Equal(t, 2, *original[0].Fields[0].FieldID)
		assert.Equal(t, []string{"child"}, original[0].Fields[0].Names)
		assert.Equal(t, []string{"anonymous"}, original[1].Names)
		assert.Equal(t, 3, *original[1].Fields[0].FieldID)
		assert.Equal(t, []string{"anonymous-child"}, original[1].Fields[0].Names)
	})

	t.Run("update nested field under anonymous parent", func(t *testing.T) {
		childID := 1
		original := iceberg.NameMapping{{
			Names: []string{"anonymous"},
			Fields: []iceberg.MappedField{{
				FieldID: &childID,
				Names:   []string{"child"},
			}},
		}}

		result, err := iceberg.UpdateNameMapping(
			original,
			map[int]iceberg.NestedField{1: {ID: 1, Name: "renamed-child"}},
			map[int][]iceberg.NestedField{},
		)
		require.NoError(t, err)
		assert.Equal(t, []string{"child", "renamed-child"}, result[0].Fields[0].Names)
	})

	t.Run("remove reassigned name from anonymous field", func(t *testing.T) {
		fieldID := 1
		original := iceberg.NameMapping{
			{Names: []string{"renamed"}},
			{FieldID: &fieldID, Names: []string{"original"}},
		}

		result, err := iceberg.UpdateNameMapping(
			original,
			map[int]iceberg.NestedField{1: {ID: 1, Name: "renamed"}},
			map[int][]iceberg.NestedField{},
		)
		require.NoError(t, err)
		assert.Equal(t, iceberg.NameMapping{{
			FieldID: new(1),
			Names:   []string{"original", "renamed"},
		}}, result)
	})

	t.Run("update mapping with updates and adds", func(t *testing.T) {
		updates := map[int]iceberg.NestedField{
			1: {ID: 1, Name: "foo_update", Type: &iceberg.StringType{}},
		}
		adds := map[int][]iceberg.NestedField{
			-1: {
				{ID: 18, Name: "add_18", Type: &iceberg.StringType{}},
			},
			15: {
				{ID: 19, Name: "name", Type: &iceberg.StringType{}},
				{ID: 20, Name: "add_20", Type: &iceberg.StringType{}},
			},
		}

		result, err := iceberg.UpdateNameMapping(originalMapping, updates, adds)
		require.NoError(t, err)

		expected := iceberg.NameMapping{
			{FieldID: new(1), Names: []string{"foo", "foo_update"}},
			{FieldID: new(2), Names: []string{"bar"}},
			{FieldID: new(3), Names: []string{"baz"}},
			{FieldID: new(4), Names: []string{"qux"}, Fields: []iceberg.MappedField{
				{FieldID: new(5), Names: []string{"element"}},
			}},
			{FieldID: new(6), Names: []string{"quux"}, Fields: []iceberg.MappedField{
				{FieldID: new(7), Names: []string{"key"}},
				{FieldID: new(8), Names: []string{"value"}, Fields: []iceberg.MappedField{
					{FieldID: new(9), Names: []string{"key"}},
					{FieldID: new(10), Names: []string{"value"}},
				}},
			}},
			{FieldID: new(11), Names: []string{"location"}, Fields: []iceberg.MappedField{
				{FieldID: new(12), Names: []string{"element"}, Fields: []iceberg.MappedField{
					{FieldID: new(13), Names: []string{"latitude"}},
					{FieldID: new(14), Names: []string{"longitude"}},
				}},
			}},
			{FieldID: new(15), Names: []string{"person"}, Fields: []iceberg.MappedField{
				{FieldID: new(17), Names: []string{"age"}},
				{FieldID: new(19), Names: []string{"name"}},
				{FieldID: new(20), Names: []string{"add_20"}},
			}},
			{FieldID: new(18), Names: []string{"add_18"}},
		}

		assert.Equal(t, expected, result)
	})

	t.Run("update field names only", func(t *testing.T) {
		updates := map[int]iceberg.NestedField{
			1: {ID: 1, Name: "new_foo", Type: &iceberg.StringType{}},
			2: {ID: 2, Name: "new_bar", Type: &iceberg.StringType{}},
		}
		adds := map[int][]iceberg.NestedField{}

		result, err := iceberg.UpdateNameMapping(originalMapping, updates, adds)
		require.NoError(t, err)

		expected := iceberg.NameMapping{
			{FieldID: new(1), Names: []string{"foo", "new_foo"}},
			{FieldID: new(2), Names: []string{"bar", "new_bar"}},
			{FieldID: new(3), Names: []string{"baz"}},
			{FieldID: new(4), Names: []string{"qux"}, Fields: []iceberg.MappedField{
				{FieldID: new(5), Names: []string{"element"}},
			}},
			{FieldID: new(6), Names: []string{"quux"}, Fields: []iceberg.MappedField{
				{FieldID: new(7), Names: []string{"key"}},
				{FieldID: new(8), Names: []string{"value"}, Fields: []iceberg.MappedField{
					{FieldID: new(9), Names: []string{"key"}},
					{FieldID: new(10), Names: []string{"value"}},
				}},
			}},
			{FieldID: new(11), Names: []string{"location"}, Fields: []iceberg.MappedField{
				{FieldID: new(12), Names: []string{"element"}, Fields: []iceberg.MappedField{
					{FieldID: new(13), Names: []string{"latitude"}},
					{FieldID: new(14), Names: []string{"longitude"}},
				}},
			}},
			{FieldID: new(15), Names: []string{"person"}, Fields: []iceberg.MappedField{
				{FieldID: new(16), Names: []string{"name"}},
				{FieldID: new(17), Names: []string{"age"}},
			}},
		}

		assert.Equal(t, expected, result)
	})

	t.Run("add new fields only", func(t *testing.T) {
		updates := map[int]iceberg.NestedField{}
		adds := map[int][]iceberg.NestedField{
			-1: {
				{ID: 21, Name: "new_root_field", Type: &iceberg.StringType{}},
			},
			15: {
				{ID: 22, Name: "email", Type: &iceberg.StringType{}},
			},
		}

		result, err := iceberg.UpdateNameMapping(originalMapping, updates, adds)
		require.NoError(t, err)

		expected := iceberg.NameMapping{
			{FieldID: new(1), Names: []string{"foo"}},
			{FieldID: new(2), Names: []string{"bar"}},
			{FieldID: new(3), Names: []string{"baz"}},
			{FieldID: new(4), Names: []string{"qux"}, Fields: []iceberg.MappedField{
				{FieldID: new(5), Names: []string{"element"}},
			}},
			{FieldID: new(6), Names: []string{"quux"}, Fields: []iceberg.MappedField{
				{FieldID: new(7), Names: []string{"key"}},
				{FieldID: new(8), Names: []string{"value"}, Fields: []iceberg.MappedField{
					{FieldID: new(9), Names: []string{"key"}},
					{FieldID: new(10), Names: []string{"value"}},
				}},
			}},
			{FieldID: new(11), Names: []string{"location"}, Fields: []iceberg.MappedField{
				{FieldID: new(12), Names: []string{"element"}, Fields: []iceberg.MappedField{
					{FieldID: new(13), Names: []string{"latitude"}},
					{FieldID: new(14), Names: []string{"longitude"}},
				}},
			}},
			{FieldID: new(15), Names: []string{"person"}, Fields: []iceberg.MappedField{
				{FieldID: new(16), Names: []string{"name"}},
				{FieldID: new(17), Names: []string{"age"}},
				{FieldID: new(22), Names: []string{"email"}},
			}},
			{FieldID: new(21), Names: []string{"new_root_field"}},
		}

		assert.Equal(t, expected, result)
	})
}

func TestUpdateNameMappingReassignedAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		names     []string
		remaining []string
		anonymous bool
		emptyName bool
	}{
		{name: "leading", names: []string{"taken", "old", "historical"}, remaining: []string{"old", "historical"}},
		{name: "middle", names: []string{"old", "taken", "historical"}, remaining: []string{"old", "historical"}},
		{name: "trailing", names: []string{"old", "historical", "taken"}, remaining: []string{"old", "historical"}},
		{name: "duplicates and empty alias", names: []string{"taken", "old", "taken", "old", ""}, remaining: []string{"old", "old", ""}},
		{name: "all removed", names: []string{"taken", "taken"}},
		{name: "nil names"},
		{name: "empty names", names: []string{}},
		{name: "anonymous", names: []string{"taken", "anonymous"}, remaining: []string{"anonymous"}, anonymous: true},
		{name: "anonymous removed", names: []string{"taken"}, anonymous: true},
		{name: "empty alias assigned", names: []string{"old", "", "", "last"}, remaining: []string{"old", "last"}, emptyName: true},
		{name: "anonymous empty alias removed", names: []string{""}, anonymous: true, emptyName: true},
	}
	for _, tt := range tests {
		for _, operation := range []string{"rename", "add"} {
			t.Run(tt.name+"/"+operation, func(t *testing.T) {
				t.Parallel()

				backing := make([]string, len(tt.names)+1)
				copy(backing, tt.names)
				backing[len(tt.names)] = "spare"
				names := backing[:len(tt.names)]
				if tt.names == nil {
					names = nil
				}
				fieldID := new(1)
				if tt.anonymous {
					fieldID = nil
				}
				original := iceberg.NameMapping{
					{FieldID: fieldID, Names: names, Fields: []iceberg.MappedField{{FieldID: new(4), Names: []string{"child"}}}},
					{FieldID: new(2), Names: []string{"other"}},
				}
				before, err := json.Marshal(original)
				require.NoError(t, err)
				var updates map[int]iceberg.NestedField
				var adds map[int][]iceberg.NestedField
				assignedName := "taken"
				if tt.emptyName {
					assignedName = ""
				}
				if operation == "rename" {
					updates = map[int]iceberg.NestedField{2: {ID: 2, Name: assignedName}}
				} else {
					adds = map[int][]iceberg.NestedField{-1: {{ID: 3, Name: assignedName, Type: iceberg.PrimitiveTypes.Int32}}}
				}

				var expected iceberg.NameMapping
				if len(tt.remaining) > 0 {
					expected = append(expected, iceberg.MappedField{
						FieldID: fieldID,
						Names:   tt.remaining,
						Fields:  []iceberg.MappedField{{FieldID: new(4), Names: []string{"child"}}},
					})
				}
				expected = append(expected, iceberg.MappedField{FieldID: new(2), Names: []string{"other"}})
				if operation == "rename" {
					expected[len(expected)-1].Names = []string{"other", assignedName}
				} else {
					expected = append(expected, iceberg.MappedField{FieldID: new(3), Names: []string{assignedName}})
				}

				result, err := iceberg.UpdateNameMapping(original, updates, adds)
				require.NoError(t, err)
				assert.Equal(t, expected, result)
				assert.Equal(t, "spare", backing[len(tt.names)])
				again, err := iceberg.UpdateNameMapping(original, updates, adds)
				require.NoError(t, err)
				assert.Equal(t, expected, again)

				for i := range result {
					for _, name := range result[i].Names[len(result[i].Names):cap(result[i].Names)] {
						assert.Empty(t, name)
					}
					if result[i].FieldID != nil {
						*result[i].FieldID = 99
					}
					result[i].Names[0] = "changed"
					if len(result[i].Fields) > 0 {
						*result[i].Fields[0].FieldID = 98
						result[i].Fields[0].Names[0] = "changed-child"
					}
				}
				after, err := json.Marshal(original)
				require.NoError(t, err)
				assert.Equal(t, before, after)
				assert.Equal(t, expected, again)
			})
		}
	}
}

func TestUpdateNameMappingNestedReassignments(t *testing.T) {
	t.Parallel()

	original := iceberg.NameMapping{{
		FieldID: new(10),
		Names:   []string{"parent"},
		Fields: []iceberg.MappedField{
			{FieldID: new(1), Names: []string{"first", "taken", "keep", "taken", "added", "last"}},
			{FieldID: new(2), Names: []string{"other"}},
		},
	}}
	before, err := json.Marshal(original)
	require.NoError(t, err)
	result, err := iceberg.UpdateNameMapping(original,
		map[int]iceberg.NestedField{2: {ID: 2, Name: "taken"}},
		map[int][]iceberg.NestedField{10: {{ID: 3, Name: "added", Type: iceberg.PrimitiveTypes.String}}},
	)
	require.NoError(t, err)
	assert.Equal(t, iceberg.NameMapping{{
		FieldID: new(10),
		Names:   []string{"parent"},
		Fields: []iceberg.MappedField{
			{FieldID: new(1), Names: []string{"first", "keep", "last"}},
			{FieldID: new(2), Names: []string{"other", "taken"}},
			{FieldID: new(3), Names: []string{"added"}},
		},
	}}, result)
	result[0].Names[0] = "changed-parent"
	result[0].Fields[0].Names[0] = "changed-child"
	*result[0].Fields[0].FieldID = 99
	after, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestUpdateNameMappingKeepsAliasesAssignedToSameField(t *testing.T) {
	t.Parallel()

	original := iceberg.NameMapping{
		{FieldID: new(1), Names: []string{"old", "taken", "taken", "old"}},
		{FieldID: new(2), Names: []string{"taken", "other"}},
	}
	result, err := iceberg.UpdateNameMapping(original, map[int]iceberg.NestedField{1: {ID: 1, Name: "taken"}}, nil)
	require.NoError(t, err)
	assert.Equal(t, iceberg.NameMapping{
		{FieldID: new(1), Names: []string{"old", "taken", "taken", "old"}},
		{FieldID: new(2), Names: []string{"other"}},
	}, result)
	assert.Equal(t, []string{"old", "taken", "taken", "old"}, original[0].Names)
	assert.Equal(t, []string{"taken", "other"}, original[1].Names)
}
