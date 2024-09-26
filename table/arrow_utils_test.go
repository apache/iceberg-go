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

package table_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fieldIDMeta(id string) arrow.Metadata {
	return arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: id})
}

func TestArrowToIceberg(t *testing.T) {
	tests := []struct {
		dt  arrow.DataType
		ice iceberg.Type
		err string
	}{
		{&arrow.FixedSizeBinaryType{ByteWidth: 23}, iceberg.FixedTypeOf(23), ""},
		{&arrow.Decimal32Type{Precision: 8, Scale: 9}, iceberg.DecimalTypeOf(8, 9), ""},
		{&arrow.Decimal64Type{Precision: 15, Scale: 14}, iceberg.DecimalTypeOf(15, 14), ""},
		{&arrow.Decimal128Type{Precision: 26, Scale: 20}, iceberg.DecimalTypeOf(26, 20), ""},
		{&arrow.Decimal256Type{Precision: 8, Scale: 9}, nil, "unsupported arrow type for conversion - decimal256(8, 9)"},
		{arrow.FixedWidthTypes.Boolean, iceberg.PrimitiveTypes.Bool, ""},
		{arrow.PrimitiveTypes.Int8, iceberg.PrimitiveTypes.Int32, ""},
		{arrow.PrimitiveTypes.Uint8, iceberg.PrimitiveTypes.Int32, ""},
		{arrow.PrimitiveTypes.Int16, iceberg.PrimitiveTypes.Int32, ""},
		{arrow.PrimitiveTypes.Uint16, iceberg.PrimitiveTypes.Int32, ""},
		{arrow.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int32, ""},
		{arrow.PrimitiveTypes.Uint32, iceberg.PrimitiveTypes.Int32, ""},
		{arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64, ""},
		{arrow.PrimitiveTypes.Uint64, iceberg.PrimitiveTypes.Int64, ""},
		{arrow.FixedWidthTypes.Float16, iceberg.PrimitiveTypes.Float32, ""},
		{arrow.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float32, ""},
		{arrow.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Float64, ""},
		{arrow.FixedWidthTypes.Date32, iceberg.PrimitiveTypes.Date, ""},
		{arrow.FixedWidthTypes.Date64, nil, "unsupported arrow type for conversion - date64"},
		{arrow.FixedWidthTypes.Time32s, nil, "unsupported arrow type for conversion - time32[s]"},
		{arrow.FixedWidthTypes.Time32ms, nil, "unsupported arrow type for conversion - time32[ms]"},
		{arrow.FixedWidthTypes.Time64us, iceberg.PrimitiveTypes.Time, ""},
		{arrow.FixedWidthTypes.Time64ns, nil, "unsupported arrow type for conversion - time64[ns]"},
		{arrow.FixedWidthTypes.Timestamp_s, iceberg.PrimitiveTypes.TimestampTz, ""},
		{arrow.FixedWidthTypes.Timestamp_ms, iceberg.PrimitiveTypes.TimestampTz, ""},
		{arrow.FixedWidthTypes.Timestamp_us, iceberg.PrimitiveTypes.TimestampTz, ""},
		{arrow.FixedWidthTypes.Timestamp_ns, nil, "'ns' timestamp precision not supported"},
		{&arrow.TimestampType{Unit: arrow.Second}, iceberg.PrimitiveTypes.Timestamp, ""},
		{&arrow.TimestampType{Unit: arrow.Millisecond}, iceberg.PrimitiveTypes.Timestamp, ""},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, iceberg.PrimitiveTypes.Timestamp, ""},
		{&arrow.TimestampType{Unit: arrow.Nanosecond}, nil, "'ns' timestamp precision not supported"},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "US/Pacific"}, nil, "unsupported arrow type for conversion - timestamp[us, tz=US/Pacific]"},
		{arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String, ""},
		{arrow.BinaryTypes.LargeString, iceberg.PrimitiveTypes.String, ""},
		{arrow.BinaryTypes.StringView, nil, "unsupported arrow type for conversion - string_view"},
		{arrow.BinaryTypes.Binary, iceberg.PrimitiveTypes.Binary, ""},
		{arrow.BinaryTypes.LargeBinary, iceberg.PrimitiveTypes.Binary, ""},
		{arrow.BinaryTypes.BinaryView, nil, "unsupported arrow type for conversion - binary_view"},
		{extensions.NewUUIDType(), iceberg.PrimitiveTypes.UUID, ""},
		{arrow.StructOf(arrow.Field{
			Name:     "foo",
			Type:     arrow.BinaryTypes.LargeString,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				table.ArrowParquetFieldIDKey: "1", table.ArrowFieldDocKey: "foo doc",
			}),
		}, arrow.Field{
			Name:     "bar",
			Type:     arrow.PrimitiveTypes.Int32,
			Metadata: fieldIDMeta("2"),
		}, arrow.Field{
			Name:     "baz",
			Type:     arrow.FixedWidthTypes.Boolean,
			Nullable: true,
			Metadata: fieldIDMeta("3"),
		}), &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: "foo doc"},
				{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
			}}, ""},
		{arrow.ListOfField(arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: fieldIDMeta("1"),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, ""},
		{arrow.LargeListOfField(arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: fieldIDMeta("1"),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, ""},
		{arrow.FixedSizeListOfField(1, arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: fieldIDMeta("1"),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, ""},
		{arrow.MapOfWithMetadata(arrow.PrimitiveTypes.Int32,
			fieldIDMeta("1"),
			arrow.BinaryTypes.String, fieldIDMeta("2")),
			&iceberg.MapType{
				KeyID: 1, KeyType: iceberg.PrimitiveTypes.Int32,
				ValueID: 2, ValueType: iceberg.PrimitiveTypes.String, ValueRequired: false,
			}, ""},
		{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String}, iceberg.PrimitiveTypes.String, ""},
		{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.PrimitiveTypes.Int32}, iceberg.PrimitiveTypes.Int32, ""},
		{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int64,
			ValueType: arrow.PrimitiveTypes.Float64}, iceberg.PrimitiveTypes.Float64, ""},
		{arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String), iceberg.PrimitiveTypes.String, ""},
		{arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64), iceberg.PrimitiveTypes.Float64, ""},
		{arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int16), iceberg.PrimitiveTypes.Int32, ""},
	}

	for _, tt := range tests {
		t.Run(tt.dt.String(), func(t *testing.T) {
			out, err := table.ArrowTypeToIceberg(tt.dt, false)
			if tt.err == "" {
				require.NoError(t, err)
				assert.True(t, out.Equals(tt.ice), out.String(), tt.ice.String())
			} else {
				assert.ErrorContains(t, err, tt.err)
			}
		})
	}
}

func TestArrowSchemaToIceb(t *testing.T) {
	tests := []struct {
		name     string
		sc       *arrow.Schema
		expected string
		err      string
	}{
		{"simple", arrow.NewSchema([]arrow.Field{
			{Name: "foo", Nullable: true, Type: arrow.BinaryTypes.String,
				Metadata: fieldIDMeta("1")},
			{Name: "bar", Nullable: false, Type: arrow.PrimitiveTypes.Int32,
				Metadata: fieldIDMeta("2")},
			{Name: "baz", Nullable: true, Type: arrow.FixedWidthTypes.Boolean,
				Metadata: fieldIDMeta("3")},
		}, nil), `table {
	1: foo: optional string
	2: bar: required int
	3: baz: optional boolean
}`, ""},
		{"nested", arrow.NewSchema([]arrow.Field{
			{Name: "qux", Nullable: false, Metadata: fieldIDMeta("4"),
				Type: arrow.ListOfField(arrow.Field{
					Name:     "element",
					Type:     arrow.BinaryTypes.String,
					Metadata: fieldIDMeta("5"),
				})},
			{Name: "quux", Nullable: false, Metadata: fieldIDMeta("6"),
				Type: arrow.MapOfWithMetadata(arrow.BinaryTypes.String, fieldIDMeta("7"),
					arrow.MapOfWithMetadata(arrow.BinaryTypes.String, fieldIDMeta("9"),
						arrow.PrimitiveTypes.Int32, fieldIDMeta("10")), fieldIDMeta("8"))},
			{Name: "location", Nullable: false, Metadata: fieldIDMeta("11"),
				Type: arrow.ListOfField(
					arrow.Field{
						Name: "element", Metadata: fieldIDMeta("12"),
						Type: arrow.StructOf(
							arrow.Field{Name: "latitude", Nullable: true,
								Type: arrow.PrimitiveTypes.Float32, Metadata: fieldIDMeta("13")},
							arrow.Field{Name: "longitude", Nullable: true,
								Type: arrow.PrimitiveTypes.Float32, Metadata: fieldIDMeta("14")},
						)})},
			{Name: "person", Nullable: true, Metadata: fieldIDMeta("15"),
				Type: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: fieldIDMeta("16")},
					arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Metadata: fieldIDMeta("17")},
				)},
		}, nil), `table {
	4: qux: required list<string>
	6: quux: required map<string, map<string, int>>
	11: location: required list<struct<13: latitude: optional float, 14: longitude: optional float>>
	15: person: optional struct<16: name: optional string, 17: age: required int>
}`, ""},
		{"missing ids", arrow.NewSchema([]arrow.Field{
			{Name: "foo", Type: arrow.BinaryTypes.String, Nullable: false},
		}, nil), "", "arrow schema does not have field-ids and no name mapping provided"},
		{"missing ids partial", arrow.NewSchema([]arrow.Field{
			{Name: "foo", Type: arrow.BinaryTypes.String, Metadata: fieldIDMeta("1")},
			{Name: "bar", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		}, nil), "", "arrow schema does not have field-ids and no name mapping provided"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := table.ArrowSchemaToIceberg(tt.sc, true, nil)
			if tt.err == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, out.String())
			} else {
				assert.ErrorContains(t, err, tt.err)
			}
		})
	}
}

func makeID(v int) *int { return &v }

var (
	icebergSchemaNested = iceberg.NewSchema(0,
		iceberg.NestedField{
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{
			ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{
			ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{
			ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
				ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: false}},
		iceberg.NestedField{
			ID: 6, Name: "quux",
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &iceberg.MapType{
					KeyID:         9,
					KeyType:       iceberg.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     iceberg.PrimitiveTypes.Int32,
					ValueRequired: false,
				},
				ValueRequired: false,
			},
			Required: true},
		iceberg.NestedField{
			ID: 11, Name: "location", Type: &iceberg.ListType{
				ElementID: 12, Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: true},
						{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: true},
					},
				},
				ElementRequired: false},
			Required: true},
		iceberg.NestedField{
			ID:   15,
			Name: "person",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				},
			},
			Required: false,
		},
	)

	icebergSchemaSimple = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool},
	)
)

func TestArrowSchemaWithNameMapping(t *testing.T) {
	schemaWithoutIDs := arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bar", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "baz", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	schemaNestedWithoutIDs := arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bar", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "baz", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "qux", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
		{Name: "quux", Type: arrow.MapOf(arrow.BinaryTypes.String,
			arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)), Nullable: false},
		{Name: "location", Type: arrow.ListOf(arrow.StructOf(
			arrow.Field{Name: "latitude", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
			arrow.Field{Name: "longitude", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		)), Nullable: false},
		{Name: "person", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		), Nullable: true},
	}, nil)

	tests := []struct {
		name     string
		schema   *arrow.Schema
		mapping  table.NameMapping
		expected *iceberg.Schema
		err      string
	}{
		{"simple", schemaWithoutIDs, table.NameMapping{
			{FieldID: makeID(1), Names: []string{"foo"}},
			{FieldID: makeID(2), Names: []string{"bar"}},
			{FieldID: makeID(3), Names: []string{"baz"}},
		}, icebergSchemaSimple, ""},
		{"field missing", schemaWithoutIDs, table.NameMapping{
			{FieldID: makeID(1), Names: []string{"foo"}},
		}, nil, "field missing from name mapping: bar"},
		{"nested schema", schemaNestedWithoutIDs, table.NameMapping{
			{FieldID: makeID(1), Names: []string{"foo"}},
			{FieldID: makeID(2), Names: []string{"bar"}},
			{FieldID: makeID(3), Names: []string{"baz"}},
			{FieldID: makeID(4), Names: []string{"qux"},
				Fields: []table.MappedField{{FieldID: makeID(5), Names: []string{"element"}}}},
			{FieldID: makeID(6), Names: []string{"quux"}, Fields: []table.MappedField{
				{FieldID: makeID(7), Names: []string{"key"}},
				{FieldID: makeID(8), Names: []string{"value"}, Fields: []table.MappedField{
					{FieldID: makeID(9), Names: []string{"key"}},
					{FieldID: makeID(10), Names: []string{"value"}},
				}},
			}},
			{FieldID: makeID(11), Names: []string{"location"}, Fields: []table.MappedField{
				{FieldID: makeID(12), Names: []string{"element"}, Fields: []table.MappedField{
					{FieldID: makeID(13), Names: []string{"latitude"}},
					{FieldID: makeID(14), Names: []string{"longitude"}},
				}},
			}},
			{FieldID: makeID(15), Names: []string{"person"}, Fields: []table.MappedField{
				{FieldID: makeID(16), Names: []string{"name"}},
				{FieldID: makeID(17), Names: []string{"age"}},
			}},
		}, icebergSchemaNested, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := table.ArrowSchemaToIceberg(tt.schema, false, tt.mapping)
			if tt.err != "" {
				assert.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
				assert.True(t, tt.expected.Equals(out), out.String(), tt.expected.String())
			}
		})
	}
}
