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

package udf

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustPrimitive(t *testing.T, s string) PrimitiveType {
	t.Helper()
	p, err := PrimitiveTypeOf(s)
	require.NoError(t, err)

	return p
}

func TestPrimitiveTypeOf(t *testing.T) {
	valid := []string{
		"boolean", "int", "long", "float", "double", "date", "time",
		"timestamp", "timestamptz", "string", "uuid", "binary",
		"decimal(9,2)", "fixed[16]", "variant", "unknown", "geometry",
	}
	for _, s := range valid {
		p, err := PrimitiveTypeOf(s)
		require.NoError(t, err, s)
		assert.Equal(t, s, p.String())
	}

	invalid := []string{
		"",
		"decimal(9, 2)", // spaces are not allowed in type strings
		"int ",
		" int",
		"va\"riant",
		"va'riant",
		"foo",
		"struct", // nested types must be JSON objects
		"list",
		"map",
		// canonical definition-id forms are not valid type strings either
		"list<int>",
		"map<string,int>",
		"struct<id:int>",
	}
	for _, s := range invalid {
		_, err := PrimitiveTypeOf(s)
		assert.ErrorIs(t, err, ErrInvalidUDFType, s)
	}
}

func TestTypeCanonicalStrings(t *testing.T) {
	intType := mustPrimitive(t, "int")
	stringType := mustPrimitive(t, "string")

	tests := []struct {
		typ      Type
		expected string
	}{
		{intType, "int"},
		{mustPrimitive(t, "decimal(9,2)"), "decimal(9,2)"},
		{ListType{Element: intType}, "list<int>"},
		{MapType{Key: stringType, Value: intType}, "map<string,int>"},
		{StructType{Fields: []StructField{
			{Name: "id", Type: intType},
			{Name: "name", Type: stringType},
		}}, "struct<id:int,name:string>"},
		{ListType{Element: StructType{Fields: []StructField{{Name: "id", Type: intType}}}}, "list<struct<id:int>>"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.typ.String())
	}
}

func TestCanonicalDefinitionID(t *testing.T) {
	intType := mustPrimitive(t, "int")
	stringType := mustPrimitive(t, "string")

	id, err := CanonicalDefinitionID(nil)
	require.NoError(t, err)
	assert.Equal(t, "", id)

	id, err = CanonicalDefinitionID([]Parameter{{Name: "x", Type: intType}})
	require.NoError(t, err)
	assert.Equal(t, "int", id)

	id, err = CanonicalDefinitionID([]Parameter{{Name: "x", Type: intType}, {Name: "y", Type: stringType}})
	require.NoError(t, err)
	assert.Equal(t, "int,string", id)

	// example from the UDF spec's Definition ID section
	id, err = CanonicalDefinitionID([]Parameter{
		{Name: "a", Type: intType},
		{Name: "b", Type: ListType{Element: intType}},
		{Name: "c", Type: StructType{Fields: []StructField{
			{Name: "id", Type: intType},
			{Name: "name", Type: stringType},
		}}},
	})
	require.NoError(t, err)
	assert.Equal(t, "int,list<int>,struct<id:int,name:string>", id)

	_, err = CanonicalDefinitionID([]Parameter{{Name: "x", Type: nil}})
	assert.ErrorIs(t, err, ErrInvalidUDFType)

	_, err = CanonicalDefinitionID([]Parameter{{Name: "x", Type: ListType{}}})
	assert.ErrorIs(t, err, ErrInvalidUDFType)
}

func TestTypeEquals(t *testing.T) {
	intType := mustPrimitive(t, "int")
	stringType := mustPrimitive(t, "string")

	assert.True(t, intType.Equals(mustPrimitive(t, "int")))
	assert.False(t, intType.Equals(stringType))
	assert.True(t, ListType{Element: intType}.Equals(ListType{Element: intType}))
	assert.False(t, ListType{Element: intType}.Equals(ListType{Element: stringType}))
	assert.False(t, ListType{Element: intType}.Equals(intType))
	assert.True(t, MapType{Key: stringType, Value: intType}.Equals(MapType{Key: stringType, Value: intType}))
	assert.False(t, MapType{Key: stringType, Value: intType}.Equals(MapType{Key: stringType, Value: stringType}))

	s1 := StructType{Fields: []StructField{{Name: "id", Type: intType}}}
	s2 := StructType{Fields: []StructField{{Name: "id", Type: intType}}}
	s3 := StructType{Fields: []StructField{{Name: "other", Type: intType}}}
	assert.True(t, s1.Equals(s2))
	assert.False(t, s1.Equals(s3))
	assert.False(t, s1.Equals(StructType{}))
}

func TestUnmarshalTypeJSON(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected string
	}{
		{"primitive", `"int"`, "int"},
		{"decimal", `"decimal(9,2)"`, "decimal(9,2)"},
		{"list", `{"type": "list", "element": "string"}`, "list<string>"},
		{"map", `{"type": "map", "key": "string", "value": "int"}`, "map<string,int>"},
		{
			"struct",
			`{"type": "struct", "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}`,
			"struct<id:int,name:string>",
		},
		{
			"nested",
			`{"type": "map", "key": "string", "value": {"type": "list", "element": {"type": "struct", "fields": [{"name": "id", "type": "int"}]}}}`,
			"map<string,list<struct<id:int>>>",
		},
		{
			// fields beyond the ones the UDF spec requires must be ignored
			"extra fields ignored",
			`{"type": "list", "element-id": 3, "element-required": true, "element": "string"}`,
			"list<string>",
		},
		{
			"extra struct field attrs ignored",
			`{"type": "struct", "fields": [{"name": "id", "type": "int", "id": 1, "required": true, "doc": "x"}]}`,
			"struct<id:int>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, err := unmarshalType([]byte(tt.json))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, typ.String())

			// round-trip: marshal and re-parse to the same type
			data, err := json.Marshal(typ)
			require.NoError(t, err)
			again, err := unmarshalType(data)
			require.NoError(t, err)
			assert.True(t, typ.Equals(again))
		})
	}
}

func TestUnmarshalTypeErrors(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"invalid primitive", `"foo"`},
		{"primitive with space", `"decimal(9, 2)"`},
		{"unsupported object", `{"type": "unknown-thing"}`},
		{"object without type", `{"element": "string"}`},
		{"list without element", `{"type": "list"}`},
		{"map without key", `{"type": "map", "value": "int"}`},
		{"map without value", `{"type": "map", "key": "string"}`},
		{"struct without fields", `{"type": "struct"}`},
		{"struct field without name", `{"type": "struct", "fields": [{"type": "int"}]}`},
		{"struct field without type", `{"type": "struct", "fields": [{"name": "id"}]}`},
		{"not a string or object", `5`},
		{"non-string type discriminator", `{"type": 5}`},
		{"list with invalid element", `{"type": "list", "element": "foo"}`},
		{"map with invalid key", `{"type": "map", "key": "foo", "value": "int"}`},
		{"map with invalid value", `{"type": "map", "key": "string", "value": "foo"}`},
		{"struct field with invalid type", `{"type": "struct", "fields": [{"name": "id", "type": "foo"}]}`},
		{"struct with malformed fields", `{"type": "struct", "fields": 5}`},
		{"nested list string", `"list<int>"`},
		{"nested map string", `"map<string,int>"`},
		{"nested struct string", `"struct<id:int>"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := unmarshalType([]byte(tt.json))
			assert.ErrorIs(t, err, ErrInvalidUDFType)
		})
	}
}

// sealedFakeType exercises validateType's guard against Type implementations
// beyond the ones this package defines.
type sealedFakeType struct{}

func (sealedFakeType) String() string                  { return "fake" }
func (sealedFakeType) Equals(Type) bool                { return false }
func (sealedFakeType) canonicalTo(sb *strings.Builder) {}

func TestValidateType(t *testing.T) {
	intType := mustPrimitive(t, "int")

	require.NoError(t, validateType(intType))
	require.NoError(t, validateType(StructType{}))

	assert.ErrorIs(t, validateType(nil), ErrInvalidUDFType)
	assert.ErrorIs(t, validateType(PrimitiveType{}), ErrInvalidUDFType)
	assert.ErrorIs(t, validateType(ListType{}), ErrInvalidUDFType)
	assert.ErrorIs(t, validateType(MapType{Key: intType}), ErrInvalidUDFType)
	assert.ErrorIs(t, validateType(MapType{Value: intType}), ErrInvalidUDFType)
	assert.ErrorIs(t, validateType(StructType{Fields: []StructField{{Name: "", Type: intType}}}), ErrInvalidUDFType)
	assert.ErrorIs(t, validateType(StructType{Fields: []StructField{{Name: "id", Type: nil}}}), ErrInvalidUDFType)
	assert.ErrorIs(t, validateType(sealedFakeType{}), ErrInvalidUDFType)

	duplicated := StructType{Fields: []StructField{
		{Name: "a", Type: intType},
		{Name: "a", Type: mustPrimitive(t, "string")},
	}}
	err := validateType(duplicated)
	require.ErrorIs(t, err, ErrInvalidUDFType)
	assert.ErrorContains(t, err, `duplicate field name "a"`)
}
