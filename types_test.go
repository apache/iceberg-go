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
		{"geometry", iceberg.PrimitiveTypes.Geometry},
		{"geography", iceberg.PrimitiveTypes.Geography},
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
		{iceberg.PrimitiveTypes.Geometry, "geometry"},
		{iceberg.PrimitiveTypes.Geography, "geography"},
		{iceberg.GeometryTypeOf("srid:3857"), "geometry(srid:3857)"},
		{iceberg.GeographyTypeOf("srid:4326", iceberg.EdgeAlgorithmKarney), "geography(srid:4326, karney)"},
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
	// Test default CRS
	geomDefault := iceberg.GeometryTypeCRS84()
	assert.Equal(t, "geometry", geomDefault.String())
	assert.Equal(t, "OGC:CRS84", geomDefault.CRS())
	assert.True(t, geomDefault.Equals(iceberg.PrimitiveTypes.Geometry))

	// Test custom CRS
	geomCustom := iceberg.GeometryTypeOf("srid:3857")
	assert.Equal(t, "geometry(srid:3857)", geomCustom.String())
	assert.Equal(t, "srid:3857", geomCustom.CRS())
	assert.True(t, geomCustom.Equals(iceberg.GeometryTypeOf("srid:3857")))
	assert.False(t, geomCustom.Equals(geomDefault))

	// Test with default CRS explicitly
	geomDefault2 := iceberg.GeometryTypeOf("OGC:CRS84")
	assert.True(t, geomDefault2.Equals(geomDefault))
	assert.Equal(t, "geometry", geomDefault2.String())

	// Test JSON serialization
	data, err := json.Marshal(geomDefault)
	require.NoError(t, err)
	assert.Equal(t, `"geometry"`, string(data))

	data2, err := json.Marshal(geomCustom)
	require.NoError(t, err)
	assert.Equal(t, `"geometry(srid:3857)"`, string(data2))

	// Test JSON deserialization
	var geomFromJSON iceberg.GeometryType
	err = json.Unmarshal([]byte(`"geometry"`), &geomFromJSON)
	require.NoError(t, err)
	assert.True(t, geomFromJSON.Equals(geomDefault))

	err = json.Unmarshal([]byte(`"geometry(srid:4326)"`), &geomFromJSON)
	require.NoError(t, err)
	assert.Equal(t, "srid:4326", geomFromJSON.CRS())
}

func TestGeographyType(t *testing.T) {
	// Test default CRS and algorithm
	geogDefault := iceberg.GeographyTypeCRS84()
	assert.Equal(t, "geography", geogDefault.String())
	assert.Equal(t, "OGC:CRS84", geogDefault.CRS())
	assert.Equal(t, iceberg.EdgeAlgorithmSpherical, geogDefault.Algorithm())
	assert.True(t, geogDefault.Equals(iceberg.PrimitiveTypes.Geography))

	// Test custom CRS only
	geogCRS := iceberg.GeographyTypeOf("srid:4326", iceberg.EdgeAlgorithmSpherical)
	assert.Equal(t, "geography(srid:4326)", geogCRS.String())
	assert.Equal(t, "srid:4326", geogCRS.CRS())
	assert.Equal(t, iceberg.EdgeAlgorithmSpherical, geogCRS.Algorithm())

	// Test custom CRS and algorithm
	geogCustom := iceberg.GeographyTypeOf("srid:4269", iceberg.EdgeAlgorithmKarney)
	assert.Equal(t, "geography(srid:4269, karney)", geogCustom.String())
	assert.Equal(t, "srid:4269", geogCustom.CRS())
	assert.Equal(t, iceberg.EdgeAlgorithmKarney, geogCustom.Algorithm())
	assert.True(t, geogCustom.Equals(iceberg.GeographyTypeOf("srid:4269", iceberg.EdgeAlgorithmKarney)))
	assert.False(t, geogCustom.Equals(geogDefault))

	// Test JSON serialization
	data, err := json.Marshal(geogDefault)
	require.NoError(t, err)
	assert.Equal(t, `"geography"`, string(data))

	data2, err := json.Marshal(geogCustom)
	require.NoError(t, err)
	assert.Equal(t, `"geography(srid:4269, karney)"`, string(data2))

	// Test JSON deserialization
	var geogFromJSON iceberg.GeographyType
	err = json.Unmarshal([]byte(`"geography"`), &geogFromJSON)
	require.NoError(t, err)
	assert.True(t, geogFromJSON.Equals(geogDefault))

	err = json.Unmarshal([]byte(`"geography(srid:4326, vincenty)"`), &geogFromJSON)
	require.NoError(t, err)
	assert.Equal(t, "srid:4326", geogFromJSON.CRS())
	assert.Equal(t, iceberg.EdgeAlgorithmVincenty, geogFromJSON.Algorithm())
}

func TestEdgeAlgorithm(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected iceberg.EdgeAlgorithm
		wantErr  bool
	}{
		{"spherical", "spherical", iceberg.EdgeAlgorithmSpherical, false},
		{"SPHERICAL", "SPHERICAL", iceberg.EdgeAlgorithmSpherical, false},
		{"Spherical", "Spherical", iceberg.EdgeAlgorithmSpherical, false},
		{"vincenty", "vincenty", iceberg.EdgeAlgorithmVincenty, false},
		{"thomas", "thomas", iceberg.EdgeAlgorithmThomas, false},
		{"andoyer", "andoyer", iceberg.EdgeAlgorithmAndoyer, false},
		{"karney", "karney", iceberg.EdgeAlgorithmKarney, false},
		{"empty", "", iceberg.EdgeAlgorithmSpherical, false},
		{"invalid", "invalid", iceberg.EdgeAlgorithmSpherical, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := iceberg.EdgeAlgorithmFromName(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestPromoteTypeGeometryGeography(t *testing.T) {
	tests := []struct {
		name     string
		fileType iceberg.Type
		readType iceberg.Type
		wantErr  bool
	}{
		// BinaryType -> GeometryType should succeed
		{"BinaryToGeometry", iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.Geometry, false},
		{"BinaryToGeometryWithCRS", iceberg.PrimitiveTypes.Binary, iceberg.GeometryTypeOf("srid:3857"), false},
		// BinaryType -> GeographyType should succeed
		{"BinaryToGeography", iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.Geography, false},
		{"BinaryToGeographyWithCRS", iceberg.PrimitiveTypes.Binary, iceberg.GeographyTypeOf("srid:4326", iceberg.EdgeAlgorithmKarney), false},
		// GeometryType -> BinaryType should fail (one-way only)
		{"GeometryToBinary", iceberg.PrimitiveTypes.Geometry, iceberg.PrimitiveTypes.Binary, true},
		// GeographyType -> BinaryType should fail (one-way only)
		{"GeographyToBinary", iceberg.PrimitiveTypes.Geography, iceberg.PrimitiveTypes.Binary, true},
		// GeometryType -> GeographyType should fail (not compatible)
		{"GeometryToGeography", iceberg.PrimitiveTypes.Geometry, iceberg.PrimitiveTypes.Geography, true},
		// GeographyType -> GeometryType should fail (not compatible)
		{"GeographyToGeometry", iceberg.PrimitiveTypes.Geography, iceberg.PrimitiveTypes.Geometry, true},
		// Same types should succeed
		{"GeometryToGeometry", iceberg.PrimitiveTypes.Geometry, iceberg.PrimitiveTypes.Geometry, false},
		{"GeographyToGeography", iceberg.PrimitiveTypes.Geography, iceberg.PrimitiveTypes.Geography, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := iceberg.PromoteType(tt.fileType, tt.readType)
			if tt.wantErr {
				assert.Error(t, err, "expected error promoting %s to %s", tt.fileType, tt.readType)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err, "unexpected error promoting %s to %s", tt.fileType, tt.readType)
				assert.NotNil(t, result)
				assert.True(t, result.Equals(tt.readType), "promoted type should equal read type")
			}
		})
	}
}
