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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ported from Java's TestReadabilityChecks.

func compatPrimitives(t *testing.T) []iceberg.PrimitiveType {
	t.Helper()

	geomCRS84, err := iceberg.GeometryTypeOf(iceberg.DefaultGeoCRS)
	require.NoError(t, err)
	geom3857, err := iceberg.GeometryTypeOf("srid:3857")
	require.NoError(t, err)
	geogCRS84, err := iceberg.GeographyTypeOf(iceberg.DefaultGeoCRS, "spherical")
	require.NoError(t, err)
	geog4269, err := iceberg.GeographyTypeOf("srid:4269", "spherical")
	require.NoError(t, err)
	geog4269Karney, err := iceberg.GeographyTypeOf("srid:4269", "karney")
	require.NoError(t, err)

	return []iceberg.PrimitiveType{
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
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(3),
		iceberg.FixedTypeOf(4),
		iceberg.PrimitiveTypes.Binary,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.DecimalTypeOf(11, 2),
		iceberg.DecimalTypeOf(9, 3),
		geomCRS84,
		geom3857,
		geogCRS84,
		geog4269,
		geog4269Karney,
	}
}

func required(id int, name string, typ iceberg.Type) iceberg.NestedField {
	return iceberg.NestedField{ID: id, Name: name, Type: typ, Required: true}
}

func optional(id int, name string, typ iceberg.Type) iceberg.NestedField {
	return iceberg.NestedField{ID: id, Name: name, Type: typ}
}

func schemaOf(fields ...iceberg.NestedField) *iceberg.Schema {
	return iceberg.NewSchema(0, fields...)
}

func writeErrors(t *testing.T, read, write *iceberg.Schema) []string {
	t.Helper()

	errs, err := iceberg.WriteCompatibilityErrors(read, write, true)
	require.NoError(t, err)

	return errs
}

func TestCompatibilityPrimitiveTypes(t *testing.T) {
	primitives := compatPrimitives(t)
	for _, from := range primitives {
		fromSchema := schemaOf(required(1, "from_field", from))
		for _, to := range primitives {
			errs := writeErrors(t, schemaOf(required(1, "to_field", to)), fromSchema)

			if iceberg.IsPromotionAllowed(from, to) {
				assert.Empty(t, errs, "%s -> %s", from, to)
			} else {
				require.Len(t, errs, 1, "%s -> %s", from, to)
				assert.Contains(t, errs[0], "cannot be promoted to")
			}
		}

		structSchema := schemaOf(required(1, "struct_field", &iceberg.StructType{
			FieldList: []iceberg.NestedField{required(2, "from", from)},
		}))
		errs := writeErrors(t, structSchema, fromSchema)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "cannot be read as a struct")

		listSchema := schemaOf(required(1, "list_field", &iceberg.ListType{
			ElementID: 2, Element: from, ElementRequired: true,
		}))
		errs = writeErrors(t, listSchema, fromSchema)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "cannot be read as a list")

		mapSchema := schemaOf(required(1, "map_field", &iceberg.MapType{
			KeyID: 2, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 3, ValueType: from, ValueRequired: true,
		}))
		errs = writeErrors(t, mapSchema, fromSchema)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0], "cannot be read as a map")
	}
}

func TestIsPromotionAllowed(t *testing.T) {
	tests := []struct {
		from, to iceberg.PrimitiveType
		allowed  bool
	}{
		{iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int32, true},
		{iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64, true},
		{iceberg.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int32, false},
		{iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64, true},
		{iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Float32, false},
		{iceberg.DecimalTypeOf(9, 2), iceberg.DecimalTypeOf(11, 2), true},
		{iceberg.DecimalTypeOf(11, 2), iceberg.DecimalTypeOf(9, 2), false},
		{iceberg.DecimalTypeOf(9, 2), iceberg.DecimalTypeOf(9, 3), false},
		{iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary, false},
		{iceberg.FixedTypeOf(16), iceberg.PrimitiveTypes.UUID, false},
		{iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.Timestamp, false},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.allowed, iceberg.IsPromotionAllowed(tt.from, tt.to), "%s -> %s", tt.from, tt.to)
	}
}

func TestCompatibilityVariantToVariant(t *testing.T) {
	errs := writeErrors(t,
		schemaOf(required(1, "to_field", iceberg.VariantType{})),
		schemaOf(required(1, "from_field", iceberg.VariantType{})))
	assert.Empty(t, errs)
}

func TestCompatibilityIncompatibleTypesToVariant(t *testing.T) {
	from := []iceberg.Type{
		&iceberg.StructType{FieldList: []iceberg.NestedField{required(1, "from", iceberg.PrimitiveTypes.Int32)}},
		&iceberg.MapType{
			KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 2, ValueType: iceberg.PrimitiveTypes.Int32, ValueRequired: true,
		},
		&iceberg.ListType{ElementID: 1, Element: iceberg.PrimitiveTypes.String, ElementRequired: true},
	}
	for _, p := range compatPrimitives(t) {
		from = append(from, p)
	}

	for _, typ := range from {
		errs := writeErrors(t,
			schemaOf(required(3, "to_field", iceberg.VariantType{})),
			schemaOf(required(3, "from_field", typ)))
		require.Len(t, errs, 1, "%s", typ)
		assert.Contains(t, errs[0], "cannot be read as a variant")
	}
}

func TestCompatibilityRequiredSchemaField(t *testing.T) {
	write := schemaOf(optional(1, "from_field", iceberg.PrimitiveTypes.Int32))
	read := schemaOf(required(1, "to_field", iceberg.PrimitiveTypes.Int32))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "should be required, but is optional")
}

func TestCompatibilityMissingSchemaField(t *testing.T) {
	write := schemaOf(required(0, "other_field", iceberg.PrimitiveTypes.Int32))
	read := schemaOf(required(1, "to_field", iceberg.PrimitiveTypes.Int32))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "is required, but is missing")
}

func nestedStruct(fields ...iceberg.NestedField) *iceberg.StructType {
	return &iceberg.StructType{FieldList: fields}
}

func TestCompatibilityRequiredStructField(t *testing.T) {
	write := schemaOf(required(0, "nested", nestedStruct(optional(1, "from_field", iceberg.PrimitiveTypes.Int32))))
	read := schemaOf(required(0, "nested", nestedStruct(required(1, "to_field", iceberg.PrimitiveTypes.Int32))))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "should be required, but is optional")
}

func TestCompatibilityMissingRequiredStructField(t *testing.T) {
	write := schemaOf(required(0, "nested", nestedStruct(optional(2, "from_field", iceberg.PrimitiveTypes.Int32))))
	read := schemaOf(required(0, "nested", nestedStruct(required(1, "to_field", iceberg.PrimitiveTypes.Int32))))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "is required, but is missing")
}

func TestCompatibilityMissingOptionalStructField(t *testing.T) {
	write := schemaOf(required(0, "nested", nestedStruct(required(2, "from_field", iceberg.PrimitiveTypes.Int32))))
	read := schemaOf(required(0, "nested", nestedStruct(optional(1, "to_field", iceberg.PrimitiveTypes.Int32))))

	assert.Empty(t, writeErrors(t, read, write))
}

func TestCompatibilityIncompatibleStructField(t *testing.T) {
	write := schemaOf(required(0, "nested", nestedStruct(required(1, "from_field", iceberg.PrimitiveTypes.Int32))))
	read := schemaOf(required(0, "nested", nestedStruct(required(1, "to_field", iceberg.PrimitiveTypes.Float32))))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "cannot be promoted to float")
}

func TestCompatibilityIncompatibleStructAndPrimitive(t *testing.T) {
	write := schemaOf(required(0, "nested", nestedStruct(required(1, "from_field", iceberg.PrimitiveTypes.String))))
	read := schemaOf(required(0, "nested", iceberg.PrimitiveTypes.String))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Equal(t, "nested: struct<1: from_field: required string> cannot be read as a string", errs[0])
}

func TestCompatibilityMultipleErrors(t *testing.T) {
	// required field is optional and cannot be promoted to the read type
	write := schemaOf(required(0, "nested", nestedStruct(optional(1, "from_field", iceberg.PrimitiveTypes.Int32))))
	read := schemaOf(required(0, "nested", nestedStruct(required(1, "to_field", iceberg.PrimitiveTypes.Float32))))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 2)
	assert.Contains(t, errs[0], "should be required, but is optional")
	assert.Contains(t, errs[1], "cannot be promoted to float")
}

func TestCompatibilityRequiredMapValue(t *testing.T) {
	write := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.Int32,
	}))
	read := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.Int32, ValueRequired: true,
	}))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "values should be required, but are optional")
}

func TestCompatibilityIncompatibleMapKey(t *testing.T) {
	write := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.Int32,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.String,
	}))
	read := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.Float64,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.String,
	}))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "cannot be promoted to double")
}

func TestCompatibilityIncompatibleMapValue(t *testing.T) {
	write := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.Int32,
	}))
	read := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.Float64,
	}))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "cannot be promoted to double")
}

func TestCompatibilityIncompatibleMapAndPrimitive(t *testing.T) {
	write := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 2, ValueType: iceberg.PrimitiveTypes.Int32,
	}))
	read := schemaOf(required(0, "map_field", iceberg.PrimitiveTypes.String))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Equal(t, "map_field: map<string, int> cannot be read as a string", errs[0])
}

func TestCompatibilityRequiredListElement(t *testing.T) {
	write := schemaOf(required(0, "list_field", &iceberg.ListType{ElementID: 1, Element: iceberg.PrimitiveTypes.Int32}))
	read := schemaOf(required(0, "list_field", &iceberg.ListType{ElementID: 1, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true}))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "elements should be required, but are optional")
}

func TestCompatibilityIncompatibleListElement(t *testing.T) {
	write := schemaOf(required(0, "list_field", &iceberg.ListType{ElementID: 1, Element: iceberg.PrimitiveTypes.Int32}))
	read := schemaOf(required(0, "list_field", &iceberg.ListType{ElementID: 1, Element: iceberg.PrimitiveTypes.String}))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0], "cannot be promoted to string")
}

func TestCompatibilityIncompatibleListAndPrimitive(t *testing.T) {
	write := schemaOf(required(0, "list_field", &iceberg.ListType{ElementID: 1, Element: iceberg.PrimitiveTypes.Int32}))
	read := schemaOf(required(0, "list_field", iceberg.PrimitiveTypes.String))

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Equal(t, "list_field: list<int> cannot be read as a string", errs[0])
}

func TestCompatibilityIncompatibleNestedListOfMap(t *testing.T) {
	write := schemaOf(required(0, "list_field", &iceberg.ListType{ElementID: 1, Element: &iceberg.MapType{
		KeyID: 2, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 3, ValueType: iceberg.PrimitiveTypes.Int32,
	}}))
	read := schemaOf(required(0, "list_field", &iceberg.ListType{ElementID: 1, Element: &iceberg.MapType{
		KeyID: 2, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 3, ValueType: iceberg.PrimitiveTypes.String,
	}}))

	errs := writeErrors(t, read, write)
	assert.Equal(t, []string{"list_field: int cannot be promoted to string"}, errs)
}

func TestCompatibilityIncompatibleMapOfStruct(t *testing.T) {
	write := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 2, ValueType: nestedStruct(required(3, "x", iceberg.PrimitiveTypes.Int32)),
	}))
	read := schemaOf(required(0, "map_field", &iceberg.MapType{
		KeyID: 1, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 2, ValueType: nestedStruct(required(3, "x", iceberg.PrimitiveTypes.Float64)),
	}))

	errs := writeErrors(t, read, write)
	assert.Equal(t, []string{"map_field.x: int cannot be promoted to double"}, errs)
}

func reorderedSchemas() (read, write *iceberg.Schema) {
	read = schemaOf(required(0, "nested", nestedStruct(
		required(1, "field_a", iceberg.PrimitiveTypes.Int32),
		required(2, "field_b", iceberg.PrimitiveTypes.Int32))))
	write = schemaOf(required(0, "nested", nestedStruct(
		required(2, "field_b", iceberg.PrimitiveTypes.Int32),
		required(1, "field_a", iceberg.PrimitiveTypes.Int32))))

	return read, write
}

func TestCompatibilityDifferentFieldOrdering(t *testing.T) {
	read, write := reorderedSchemas()

	errs, err := iceberg.WriteCompatibilityErrors(read, write, false)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func TestCompatibilityStructWriteReordering(t *testing.T) {
	// writes should not reorder fields
	read, write := reorderedSchemas()

	errs := writeErrors(t, read, write)
	require.Len(t, errs, 1)
	assert.Equal(t, "nested.field_b is out of order, before field_a", errs[0])
}

func TestCompatibilityStructWriteReorderingRenamed(t *testing.T) {
	// the message should use read-side names only
	read, _ := reorderedSchemas()
	write := schemaOf(required(0, "nested", nestedStruct(
		required(2, "old_b", iceberg.PrimitiveTypes.Int32),
		required(1, "old_a", iceberg.PrimitiveTypes.Int32))))

	errs := writeErrors(t, read, write)
	assert.Equal(t, []string{"nested.field_b is out of order, before field_a"}, errs)
}

func TestCompatibilityStructReadReordering(t *testing.T) {
	// reads should allow reordering
	read, write := reorderedSchemas()

	errs, err := iceberg.ReadCompatibilityErrors(read, write)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func TestCompatibilityCheckNullabilityRequiredSchemaField(t *testing.T) {
	write := schemaOf(optional(1, "from_field", iceberg.PrimitiveTypes.Int32))
	read := schemaOf(required(1, "to_field", iceberg.PrimitiveTypes.Int32))

	errs, err := iceberg.TypeCompatibilityErrors(read, write, true)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func TestCompatibilityCheckNullabilityRequiredStructField(t *testing.T) {
	write := schemaOf(required(0, "nested", nestedStruct(optional(1, "from_field", iceberg.PrimitiveTypes.Int32))))
	read := schemaOf(required(0, "nested", nestedStruct(required(1, "to_field", iceberg.PrimitiveTypes.Int32))))

	errs, err := iceberg.TypeCompatibilityErrors(read, write, true)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func TestCompatibilityTypeErrorsStillReportPromotion(t *testing.T) {
	write := schemaOf(optional(1, "f", iceberg.PrimitiveTypes.Int32))
	read := schemaOf(required(1, "f", iceberg.PrimitiveTypes.String))

	errs, err := iceberg.TypeCompatibilityErrors(read, write, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"f: int cannot be promoted to string"}, errs)
}

func TestReadCompatibilitySchemaEvolution(t *testing.T) {
	current := schemaOf(
		required(1, "id", iceberg.PrimitiveTypes.Int32),
		optional(2, "name", iceberg.PrimitiveTypes.String),
		optional(3, "tags", &iceberg.ListType{ElementID: 4, Element: iceberg.PrimitiveTypes.String}),
	)

	tests := []struct {
		name     string
		proposed *iceberg.Schema
		errs     []string
	}{
		{
			name: "promote, rename, drop and add optional",
			proposed: schemaOf(
				required(1, "id", iceberg.PrimitiveTypes.Int64),
				optional(2, "full_name", iceberg.PrimitiveTypes.String),
				optional(5, "added", iceberg.PrimitiveTypes.Bool),
			),
		},
		{
			name: "add required column",
			proposed: schemaOf(
				required(1, "id", iceberg.PrimitiveTypes.Int32),
				required(5, "added", iceberg.PrimitiveTypes.Bool),
			),
			errs: []string{"added is required, but is missing"},
		},
		{
			name: "make optional column required",
			proposed: schemaOf(
				required(1, "id", iceberg.PrimitiveTypes.Int32),
				required(2, "name", iceberg.PrimitiveTypes.String),
			),
			errs: []string{"name should be required, but is optional"},
		},
		{
			name: "narrow type",
			proposed: schemaOf(
				required(1, "id", iceberg.PrimitiveTypes.String),
			),
			errs: []string{"id: int cannot be promoted to string"},
		},
		{
			name: "nested list element error is prefixed with the field name",
			proposed: schemaOf(
				required(1, "id", iceberg.PrimitiveTypes.Int32),
				optional(3, "tags", &iceberg.ListType{ElementID: 4, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true}),
			),
			errs: []string{
				"tags: elements should be required, but are optional",
				"tags: string cannot be promoted to int",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs, err := iceberg.ReadCompatibilityErrors(tt.proposed, current)
			require.NoError(t, err)
			assert.Equal(t, tt.errs, errs)
		})
	}
}

func TestCompatibilityNestedFieldErrorPath(t *testing.T) {
	write := schemaOf(required(0, "outer", nestedStruct(
		required(1, "inner", nestedStruct(required(2, "leaf", iceberg.PrimitiveTypes.Int64))))))
	read := schemaOf(required(0, "outer", nestedStruct(
		required(1, "inner", nestedStruct(required(2, "leaf", iceberg.PrimitiveTypes.Int32))))))

	errs, err := iceberg.ReadCompatibilityErrors(read, write)
	require.NoError(t, err)
	assert.Equal(t, []string{"outer.inner.leaf: long cannot be promoted to int"}, errs)
}

func TestCompatibilityNilSchema(t *testing.T) {
	sc := schemaOf(required(1, "id", iceberg.PrimitiveTypes.Int32))

	for _, tc := range []struct{ read, write *iceberg.Schema }{{nil, sc}, {sc, nil}} {
		_, err := iceberg.ReadCompatibilityErrors(tc.read, tc.write)
		assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		assert.ErrorContains(t, err, "cannot check compatibility against nil schema")
	}
}
