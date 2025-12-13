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

func TestGeometryType(t *testing.T) {
	t.Run("default CRS", func(t *testing.T) {
		geom := iceberg.GeometryType{}
		assert.Equal(t, "OGC:CRS84", geom.CRS())
		assert.Equal(t, "geometry", geom.String())
		assert.True(t, geom.Equals(iceberg.GeometryType{}))
	})

	t.Run("custom CRS", func(t *testing.T) {
		geom, err := iceberg.GeometryTypeOf("srid:3857")
		require.NoError(t, err)
		assert.Equal(t, "srid:3857", geom.CRS())
		assert.Equal(t, "geometry(srid:3857)", geom.String())
	})

	t.Run("CRS normalization", func(t *testing.T) {
		geom1, err := iceberg.GeometryTypeOf("OGC:CRS84")
		require.NoError(t, err)
		geom2 := iceberg.GeometryType{}
		assert.True(t, geom1.Equals(geom2))
		assert.Equal(t, "geometry", geom1.String())
	})

	t.Run("empty CRS error", func(t *testing.T) {
		_, err := iceberg.GeometryTypeOf("")
		assert.ErrorContains(t, err, "invalid CRS: (empty string)")
	})

	t.Run("JSON parsing - default", func(t *testing.T) {
		data := `{"id": 1, "name": "location", "type": "geometry", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geom, ok := n.Type.(iceberg.GeometryType)
		require.True(t, ok)
		assert.Equal(t, "OGC:CRS84", geom.CRS())

		out, err := json.Marshal(n)
		require.NoError(t, err)
		assert.JSONEq(t, data, string(out))
	})

	t.Run("JSON parsing - custom CRS", func(t *testing.T) {
		data := `{"id": 1, "name": "location", "type": "geometry(srid:4326)", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geom, ok := n.Type.(iceberg.GeometryType)
		require.True(t, ok)
		assert.Equal(t, "srid:4326", geom.CRS())

		out, err := json.Marshal(n)
		require.NoError(t, err)
		assert.JSONEq(t, data, string(out))
	})

	t.Run("JSON parsing - case insensitive", func(t *testing.T) {
		data := `{"id": 1, "name": "location", "type": "Geometry(SRID:4326)", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geom, ok := n.Type.(iceberg.GeometryType)
		require.True(t, ok)
		assert.Equal(t, "SRID:4326", geom.CRS())
	})

	t.Run("JSON parsing - whitespace tolerance", func(t *testing.T) {
		data := `{"id": 1, "name": "location", "type": "geometry( srid:4326 )", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geom, ok := n.Type.(iceberg.GeometryType)
		require.True(t, ok)
		assert.Equal(t, "srid:4326", geom.CRS())
	})
}

func TestGeographyType(t *testing.T) {
	t.Run("default CRS and algorithm", func(t *testing.T) {
		geog := iceberg.GeographyType{}
		assert.Equal(t, "OGC:CRS84", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithm(""), geog.Algorithm())
		assert.Equal(t, "geography", geog.String())
		assert.True(t, geog.Equals(iceberg.GeographyType{}))
	})

	t.Run("custom CRS only", func(t *testing.T) {
		geog, err := iceberg.GeographyTypeOf("srid:4269", "")
		require.NoError(t, err)
		assert.Equal(t, "srid:4269", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithm(""), geog.Algorithm())
		assert.Equal(t, "geography(srid:4269)", geog.String())
	})

	t.Run("default CRS with algorithm", func(t *testing.T) {
		geog, err := iceberg.GeographyTypeOf("OGC:CRS84", iceberg.EdgeAlgorithmKarney)
		require.NoError(t, err)
		assert.Equal(t, "OGC:CRS84", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithmKarney, geog.Algorithm())
		assert.Equal(t, "geography(OGC:CRS84, karney)", geog.String())
	})

	t.Run("custom CRS with algorithm", func(t *testing.T) {
		geog, err := iceberg.GeographyTypeOf("srid:4269", iceberg.EdgeAlgorithmKarney)
		require.NoError(t, err)
		assert.Equal(t, "srid:4269", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithmKarney, geog.Algorithm())
		assert.Equal(t, "geography(srid:4269, karney)", geog.String())
	})

	t.Run("empty CRS error", func(t *testing.T) {
		_, err := iceberg.GeographyTypeOf("", "")
		assert.ErrorContains(t, err, "invalid CRS: (empty string)")
	})

	t.Run("JSON parsing - default", func(t *testing.T) {
		data := `{"id": 1, "name": "area", "type": "geography", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geog, ok := n.Type.(iceberg.GeographyType)
		require.True(t, ok)
		assert.Equal(t, "OGC:CRS84", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithm(""), geog.Algorithm())

		out, err := json.Marshal(n)
		require.NoError(t, err)
		assert.JSONEq(t, data, string(out))
	})

	t.Run("JSON parsing - custom CRS", func(t *testing.T) {
		data := `{"id": 1, "name": "area", "type": "geography(srid:4269)", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geog, ok := n.Type.(iceberg.GeographyType)
		require.True(t, ok)
		assert.Equal(t, "srid:4269", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithm(""), geog.Algorithm())

		out, err := json.Marshal(n)
		require.NoError(t, err)
		assert.JSONEq(t, data, string(out))
	})

	t.Run("JSON parsing - custom CRS with algorithm", func(t *testing.T) {
		data := `{"id": 1, "name": "area", "type": "geography(srid:4269, karney)", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geog, ok := n.Type.(iceberg.GeographyType)
		require.True(t, ok)
		assert.Equal(t, "srid:4269", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithmKarney, geog.Algorithm())

		out, err := json.Marshal(n)
		require.NoError(t, err)
		assert.JSONEq(t, data, string(out))
	})

	t.Run("JSON parsing - case insensitive", func(t *testing.T) {
		data := `{"id": 1, "name": "area", "type": "Geography(SRID:4269, KARNEY)", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geog, ok := n.Type.(iceberg.GeographyType)
		require.True(t, ok)
		assert.Equal(t, "SRID:4269", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithmKarney, geog.Algorithm())
	})

	t.Run("JSON parsing - whitespace tolerance", func(t *testing.T) {
		data := `{"id": 1, "name": "area", "type": "geography( srid:4269 , karney )", "required": false}`
		var n iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(data), &n))
		geog, ok := n.Type.(iceberg.GeographyType)
		require.True(t, ok)
		assert.Equal(t, "srid:4269", geog.CRS())
		assert.Equal(t, iceberg.EdgeAlgorithmKarney, geog.Algorithm())
	})

	t.Run("JSON parsing - invalid algorithm", func(t *testing.T) {
		data := `{"id": 1, "name": "area", "type": "geography(srid:4269, invalid)", "required": false}`
		var n iceberg.NestedField
		err := json.Unmarshal([]byte(data), &n)
		assert.ErrorContains(t, err, "invalid edge interpolation algorithm")
	})
}

func TestEdgeAlgorithm(t *testing.T) {
	tests := []struct {
		input    string
		expected iceberg.EdgeAlgorithm
	}{
		{"spherical", iceberg.EdgeAlgorithmSpherical},
		{"vincenty", iceberg.EdgeAlgorithmVincenty},
		{"thomas", iceberg.EdgeAlgorithmThomas},
		{"andoyer", iceberg.EdgeAlgorithmAndoyer},
		{"karney", iceberg.EdgeAlgorithmKarney},
		{"SPHERICAL", iceberg.EdgeAlgorithmSpherical},
		{"Vincenty", iceberg.EdgeAlgorithmVincenty},
		{"KARNEY", iceberg.EdgeAlgorithmKarney},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			algo, err := iceberg.ParseEdgeAlgorithm(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, algo)
			assert.Equal(t, string(tt.expected), algo.String())
		})
	}

	t.Run("invalid algorithm", func(t *testing.T) {
		_, err := iceberg.ParseEdgeAlgorithm("invalid")
		assert.ErrorContains(t, err, "invalid edge interpolation algorithm")
	})
}
