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

func TestTypesBasic(t *testing.T) {
	tests := []struct {
		expected string
		typ      iceberg.Type
	}{
		{"boolean", iceberg.PrimitiveTypes.Bool},
		{"int", iceberg.PrimitiveTypes.Int32},
		{"long", iceberg.PrimitiveTypes.Int64},
		{"float", iceberg.PrimitiveTypes.Float32},
		{"double", iceberg.PrimitiveTypes.Float64},
		{"date", iceberg.PrimitiveTypes.Date},
		{"time", iceberg.PrimitiveTypes.Time},
		{"timestamp", iceberg.PrimitiveTypes.Timestamp},
		{"timestamptz", iceberg.PrimitiveTypes.TimestampTz},
		{"timestamp_ns", iceberg.PrimitiveTypes.TimestampNs},
		{"timestamptz_ns", iceberg.PrimitiveTypes.TimestampTzNs},
		{"uuid", iceberg.PrimitiveTypes.UUID},
		{"binary", iceberg.PrimitiveTypes.Binary},
		{"unknown", iceberg.PrimitiveTypes.Unknown},
		{"fixed[5]", iceberg.FixedTypeOf(5)},
		{"decimal(9, 4)", iceberg.DecimalTypeOf(9, 4)},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			data := `{
				"id": 1,
				"name": "test",
				"type": "` + tt.expected + `",
				"required": false
			}`

			var n iceberg.NestedField
			require.NoError(t, json.Unmarshal([]byte(data), &n))
			assert.Truef(t, n.Type.Equals(tt.typ), "expected: %s\ngot: %s", tt.typ, n.Type)

			out, err := json.Marshal(n)
			require.NoError(t, err)
			assert.JSONEq(t, data, string(out))
		})
	}
}

func TestFixedType(t *testing.T) {
	typ := iceberg.FixedTypeOf(5)
	assert.Equal(t, 5, typ.Len())
	assert.Equal(t, "fixed[5]", typ.String())
	assert.True(t, typ.Equals(iceberg.FixedTypeOf(5)))
	assert.False(t, typ.Equals(iceberg.FixedTypeOf(6)))
}

func TestDecimalType(t *testing.T) {
	typ := iceberg.DecimalTypeOf(9, 2)
	assert.Equal(t, 9, typ.Precision())
	assert.Equal(t, 2, typ.Scale())
	assert.Equal(t, "decimal(9, 2)", typ.String())
	assert.True(t, typ.Equals(iceberg.DecimalTypeOf(9, 2)))
	assert.False(t, typ.Equals(iceberg.DecimalTypeOf(9, 3)))
}

func TestStructType(t *testing.T) {
	typ := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 1, Name: "required_field", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			{ID: 2, Name: "optional_field", Type: iceberg.FixedTypeOf(5), Required: false},
			{ID: 3, Name: "required_field", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 4, Name: "optional_field", Type: iceberg.DecimalTypeOf(8, 2), Required: false},
					{ID: 5, Name: "required_field", Type: iceberg.PrimitiveTypes.Int64, Required: false},
				},
			}, Required: false},
		},
	}

	assert.Len(t, typ.FieldList, 3)
	assert.False(t, typ.Equals(&iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 1, Name: "optional_field", Type: iceberg.PrimitiveTypes.Int32, Required: true}}}))
	out, err := json.Marshal(typ)
	require.NoError(t, err)

	var actual iceberg.StructType
	require.NoError(t, json.Unmarshal(out, &actual))
	assert.True(t, typ.Equals(&actual))
}

func TestListType(t *testing.T) {
	typ := &iceberg.ListType{
		ElementID:       1,
		ElementRequired: false,
		Element: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 2, Name: "required_field", Type: iceberg.DecimalTypeOf(8, 2), Required: true},
				{ID: 3, Name: "optional_field", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			},
		},
	}

	assert.IsType(t, (*iceberg.StructType)(nil), typ.ElementField().Type)
	assert.Len(t, typ.ElementField().Type.(iceberg.NestedType).Fields(), 2)
	assert.Equal(t, 1, typ.ElementField().ID)
	assert.False(t, typ.Equals(&iceberg.ListType{
		ElementID:       1,
		ElementRequired: true,
		Element: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 2, Name: "required_field", Type: iceberg.DecimalTypeOf(8, 2), Required: true},
			},
		},
	}))

	out, err := json.Marshal(typ)
	require.NoError(t, err)

	var actual iceberg.ListType
	require.NoError(t, json.Unmarshal(out, &actual))
	assert.True(t, typ.Equals(&actual))
}

func TestMapType(t *testing.T) {
	typ := &iceberg.MapType{
		KeyID:         1,
		KeyType:       iceberg.PrimitiveTypes.Float64,
		ValueID:       2,
		ValueType:     iceberg.PrimitiveTypes.UUID,
		ValueRequired: false,
	}

	assert.IsType(t, iceberg.PrimitiveTypes.Float64, typ.KeyField().Type)
	assert.Equal(t, 1, typ.KeyField().ID)
	assert.IsType(t, iceberg.PrimitiveTypes.UUID, typ.ValueField().Type)
	assert.Equal(t, 2, typ.ValueField().ID)
	assert.False(t, typ.Equals(&iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.Int64,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.UUID, ValueRequired: false,
	}))
	assert.False(t, typ.Equals(&iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.Float64,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.String, ValueRequired: true,
	}))

	out, err := json.Marshal(typ)
	require.NoError(t, err)

	var actual iceberg.MapType
	require.NoError(t, json.Unmarshal(out, &actual))
	assert.True(t, typ.Equals(&actual))
}

var NonParameterizedTypes = []iceberg.Type{
	iceberg.PrimitiveTypes.Bool,
	iceberg.PrimitiveTypes.Int32,
	iceberg.PrimitiveTypes.Int64,
	iceberg.PrimitiveTypes.Float32,
	iceberg.PrimitiveTypes.Float64,
	iceberg.PrimitiveTypes.Date,
	iceberg.PrimitiveTypes.Time,
	iceberg.PrimitiveTypes.Timestamp,
	iceberg.PrimitiveTypes.TimestampTz,
	iceberg.PrimitiveTypes.TimestampNs,
	iceberg.PrimitiveTypes.TimestampTzNs,
	iceberg.PrimitiveTypes.String,
	iceberg.PrimitiveTypes.Binary,
	iceberg.PrimitiveTypes.UUID,
	iceberg.PrimitiveTypes.Unknown,
}

func TestNonParameterizedTypeEquality(t *testing.T) {
	for i, in := range NonParameterizedTypes {
		for j, check := range NonParameterizedTypes {
			if i == j {
				assert.Truef(t, in.Equals(check), "expected %s == %s", in, check)
			} else {
				assert.Falsef(t, in.Equals(check), "expected %s != %s", in, check)
			}
		}
	}
}

func TestTypeStrings(t *testing.T) {
	tests := []struct {
		typ iceberg.Type
		str string
	}{
		{iceberg.PrimitiveTypes.Bool, "boolean"},
		{iceberg.PrimitiveTypes.Int32, "int"},
		{iceberg.PrimitiveTypes.Int64, "long"},
		{iceberg.PrimitiveTypes.Float32, "float"},
		{iceberg.PrimitiveTypes.Float64, "double"},
		{iceberg.PrimitiveTypes.Date, "date"},
		{iceberg.PrimitiveTypes.Time, "time"},
		{iceberg.PrimitiveTypes.Timestamp, "timestamp"},
		{iceberg.PrimitiveTypes.TimestampTz, "timestamptz"},
		{iceberg.PrimitiveTypes.TimestampNs, "timestamp_ns"},
		{iceberg.PrimitiveTypes.TimestampTzNs, "timestamptz_ns"},
		{iceberg.PrimitiveTypes.String, "string"},
		{iceberg.PrimitiveTypes.UUID, "uuid"},
		{iceberg.PrimitiveTypes.Binary, "binary"},
		{iceberg.PrimitiveTypes.Unknown, "unknown"},
		{iceberg.FixedTypeOf(22), "fixed[22]"},
		{iceberg.DecimalTypeOf(19, 25), "decimal(19, 25)"},
		{&iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 1, Name: "required_field", Type: iceberg.PrimitiveTypes.String, Required: true, Doc: "this is a doc"},
				{ID: 2, Name: "optional_field", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			},
		}, "struct<1: required_field: required string (this is a doc), 2: optional_field: required int>"},
		{&iceberg.ListType{
			ElementID: 22, Element: iceberg.PrimitiveTypes.String,
		}, "list<string>"},
		{
			&iceberg.MapType{KeyID: 19, KeyType: iceberg.PrimitiveTypes.String, ValueID: 25, ValueType: iceberg.PrimitiveTypes.Float64},
			"map<string, double>",
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.str, tt.typ.String())
	}
}

func TestUnknownTypeInNestedStructs(t *testing.T) {
	// Test UnknownType in all nested positions: top-level, struct fields, list elements, and map values
	listType := &iceberg.ListType{
		ElementID:       4,
		ElementRequired: false,
		Element:         iceberg.UnknownType{},
	}

	standaloneMapType := &iceberg.MapType{
		KeyID:         12,
		KeyType:       iceberg.StringType{},
		ValueID:       13,
		ValueType:     iceberg.UnknownType{},
		ValueRequired: false,
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "top", Type: iceberg.UnknownType{}, Required: false},
		iceberg.NestedField{ID: 3, Name: "arr", Type: listType, Required: false},
		iceberg.NestedField{ID: 5, Name: "struct", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 6, Name: "inner_op", Type: iceberg.UnknownType{}, Required: false},
				{ID: 7, Name: "inner_map", Type: &iceberg.MapType{
					KeyID: 8, KeyType: iceberg.StringType{},
					ValueID: 9, ValueType: iceberg.UnknownType{}, ValueRequired: false,
				}, Required: false},
				{ID: 10, Name: "struct_arr", Type: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 11, Name: "deep", Type: iceberg.UnknownType{}, Required: false},
					},
				}, Required: false},
			},
		}, Required: true},
		iceberg.NestedField{ID: 14, Name: "standalone_map", Type: standaloneMapType, Required: false},
	)

	out, err := json.Marshal(schema)
	require.NoError(t, err)

	var actual iceberg.Schema
	require.NoError(t, json.Unmarshal(out, &actual))

	// Test top-level UnknownType field
	topField, found := actual.FindFieldByName("top")
	require.True(t, found)
	assert.IsType(t, iceberg.UnknownType{}, topField.Type)
	assert.False(t, topField.Required)

	// Test UnknownType in list element
	arrField, found := actual.FindFieldByName("arr")
	require.True(t, found)
	arrType, ok := arrField.Type.(*iceberg.ListType)
	require.True(t, ok)
	assert.IsType(t, iceberg.UnknownType{}, arrType.Element)
	assert.False(t, arrType.ElementRequired)

	// Test list type JSON serialization
	listOut, err := json.Marshal(listType)
	require.NoError(t, err)
	var actualListType iceberg.ListType
	require.NoError(t, json.Unmarshal(listOut, &actualListType))
	assert.True(t, listType.Equals(&actualListType))
	assert.IsType(t, iceberg.UnknownType{}, actualListType.Element)

	// Test UnknownType in nested struct
	structField, found := actual.FindFieldByName("struct")
	require.True(t, found)
	structType, ok := structField.Type.(*iceberg.StructType)
	require.True(t, ok)

	var innerOp iceberg.NestedField
	for _, f := range structType.FieldList {
		if f.ID == 6 {
			innerOp = f

			break
		}
	}
	require.Equal(t, 6, innerOp.ID)
	assert.IsType(t, iceberg.UnknownType{}, innerOp.Type)
	assert.False(t, innerOp.Required)

	// Test UnknownType as map value within struct
	var innerMap iceberg.NestedField
	for _, f := range structType.FieldList {
		if f.ID == 7 {
			innerMap = f

			break
		}
	}
	require.Equal(t, 7, innerMap.ID)
	nestedMapType, ok := innerMap.Type.(*iceberg.MapType)
	require.True(t, ok)
	assert.IsType(t, iceberg.UnknownType{}, nestedMapType.ValueType)
	assert.False(t, nestedMapType.ValueRequired)

	// Test deep nesting: UnknownType in struct within struct
	var structArr iceberg.NestedField
	for _, f := range structType.FieldList {
		if f.ID == 10 {
			structArr = f

			break
		}
	}
	require.Equal(t, 10, structArr.ID)
	nestedStruct, ok := structArr.Type.(*iceberg.StructType)
	require.True(t, ok)
	var deep iceberg.NestedField
	for _, f := range nestedStruct.FieldList {
		if f.ID == 11 {
			deep = f

			break
		}
	}
	require.Equal(t, 11, deep.ID)
	assert.IsType(t, iceberg.UnknownType{}, deep.Type)
	assert.False(t, deep.Required)

	// Test standalone map with UnknownType value
	mapField, found := actual.FindFieldByName("standalone_map")
	require.True(t, found)
	actualMapType, ok := mapField.Type.(*iceberg.MapType)
	require.True(t, ok)
	assert.IsType(t, iceberg.UnknownType{}, actualMapType.ValueType)
	assert.False(t, actualMapType.ValueRequired)

	// Test map type JSON serialization
	mapOut, err := json.Marshal(standaloneMapType)
	require.NoError(t, err)
	var actualMapType2 iceberg.MapType
	require.NoError(t, json.Unmarshal(mapOut, &actualMapType2))
	assert.True(t, standaloneMapType.Equals(&actualMapType2))
	assert.IsType(t, iceberg.UnknownType{}, actualMapType2.ValueType)
}

func TestUnknownTypeEquality(t *testing.T) {
	unknown1 := iceberg.UnknownType{}
	unknown2 := iceberg.UnknownType{}

	assert.True(t, unknown1.Equals(unknown2))
	assert.True(t, unknown2.Equals(unknown1))
	assert.Equal(t, "unknown", unknown1.String())
	assert.Equal(t, "unknown", unknown2.String())
}
