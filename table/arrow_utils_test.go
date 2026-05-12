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
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fieldIDMeta(id string) arrow.Metadata {
	return arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: id})
}

func TestArrowToIceberg(t *testing.T) {
	tests := []struct {
		dt         arrow.DataType
		ice        iceberg.Type
		reciprocal bool
		err        string
	}{
		{&arrow.FixedSizeBinaryType{ByteWidth: 23}, iceberg.FixedTypeOf(23), true, ""},
		{&arrow.Decimal32Type{Precision: 8, Scale: 9}, iceberg.DecimalTypeOf(8, 9), false, ""},
		{&arrow.Decimal64Type{Precision: 15, Scale: 14}, iceberg.DecimalTypeOf(15, 14), false, ""},
		{&arrow.Decimal128Type{Precision: 26, Scale: 20}, iceberg.DecimalTypeOf(26, 20), true, ""},
		{&arrow.Decimal256Type{Precision: 8, Scale: 9}, nil, false, "unsupported arrow type for conversion - decimal256(8, 9)"},
		{arrow.FixedWidthTypes.Boolean, iceberg.PrimitiveTypes.Bool, true, ""},
		{arrow.Null, iceberg.PrimitiveTypes.Unknown, true, ""},
		{arrow.PrimitiveTypes.Int8, iceberg.PrimitiveTypes.Int32, false, ""},
		{arrow.PrimitiveTypes.Uint8, iceberg.PrimitiveTypes.Int32, false, ""},
		{arrow.PrimitiveTypes.Int16, iceberg.PrimitiveTypes.Int32, false, ""},
		{arrow.PrimitiveTypes.Uint16, iceberg.PrimitiveTypes.Int32, false, ""},
		{arrow.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int32, true, ""},
		{arrow.PrimitiveTypes.Uint32, iceberg.PrimitiveTypes.Int32, false, ""},
		{arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64, true, ""},
		{arrow.PrimitiveTypes.Uint64, iceberg.PrimitiveTypes.Int64, false, ""},
		{arrow.FixedWidthTypes.Float16, iceberg.PrimitiveTypes.Float32, false, ""},
		{arrow.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float32, true, ""},
		{arrow.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Float64, true, ""},
		{arrow.FixedWidthTypes.Date32, iceberg.PrimitiveTypes.Date, true, ""},
		{arrow.FixedWidthTypes.Date64, nil, false, "unsupported arrow type for conversion - date64"},
		{arrow.FixedWidthTypes.Time32s, nil, false, "unsupported arrow type for conversion - time32[s]"},
		{arrow.FixedWidthTypes.Time32ms, nil, false, "unsupported arrow type for conversion - time32[ms]"},
		{arrow.FixedWidthTypes.Time64us, iceberg.PrimitiveTypes.Time, true, ""},
		{arrow.FixedWidthTypes.Time64ns, nil, false, "unsupported arrow type for conversion - time64[ns]"},
		{arrow.FixedWidthTypes.Timestamp_s, iceberg.PrimitiveTypes.TimestampTz, false, ""},
		{arrow.FixedWidthTypes.Timestamp_ms, iceberg.PrimitiveTypes.TimestampTz, false, ""},
		{arrow.FixedWidthTypes.Timestamp_us, iceberg.PrimitiveTypes.TimestampTz, true, ""},
		{arrow.FixedWidthTypes.Timestamp_ns, nil, false, "'ns' timestamp precision not supported"},
		{&arrow.TimestampType{Unit: arrow.Second}, iceberg.PrimitiveTypes.Timestamp, false, ""},
		{&arrow.TimestampType{Unit: arrow.Millisecond}, iceberg.PrimitiveTypes.Timestamp, false, ""},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, iceberg.PrimitiveTypes.Timestamp, true, ""},
		{&arrow.TimestampType{Unit: arrow.Nanosecond}, nil, false, "'ns' timestamp precision not supported"},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "US/Pacific"}, nil, false, "unsupported arrow type for conversion - timestamp[us, tz=US/Pacific]"},
		{arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String, true, ""},
		{arrow.BinaryTypes.LargeString, iceberg.PrimitiveTypes.String, false, ""},
		{arrow.BinaryTypes.StringView, nil, false, "unsupported arrow type for conversion - string_view"},
		{arrow.BinaryTypes.Binary, iceberg.PrimitiveTypes.Binary, true, ""},
		{arrow.BinaryTypes.LargeBinary, iceberg.PrimitiveTypes.Binary, false, ""},
		{arrow.BinaryTypes.BinaryView, nil, false, "unsupported arrow type for conversion - binary_view"},
		{extensions.NewUUIDType(), iceberg.PrimitiveTypes.UUID, true, ""},
		{extensions.NewDefaultVariantType(), iceberg.VariantType{}, true, ""},
		{arrow.StructOf(arrow.Field{
			Name:     "foo",
			Type:     arrow.BinaryTypes.String,
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
			},
		}, true, ""},
		{arrow.ListOfField(arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: fieldIDMeta("1"),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, true, ""},
		{arrow.LargeListOfField(arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: fieldIDMeta("1"),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, false, ""},
		{arrow.FixedSizeListOfField(1, arrow.Field{
			Name:     "element",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: fieldIDMeta("1"),
		}), &iceberg.ListType{
			ElementID:       1,
			Element:         iceberg.PrimitiveTypes.Int32,
			ElementRequired: true,
		}, false, ""},
		{
			arrow.MapOfWithMetadata(arrow.PrimitiveTypes.Int32,
				fieldIDMeta("1"),
				arrow.BinaryTypes.String, fieldIDMeta("2")),
			&iceberg.MapType{
				KeyID: 1, KeyType: iceberg.PrimitiveTypes.Int32,
				ValueID: 2, ValueType: iceberg.PrimitiveTypes.String, ValueRequired: false,
			}, true, "",
		},
		{&arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}, iceberg.PrimitiveTypes.String, false, ""},
		{&arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.PrimitiveTypes.Int32,
		}, iceberg.PrimitiveTypes.Int32, false, ""},
		{&arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int64,
			ValueType: arrow.PrimitiveTypes.Float64,
		}, iceberg.PrimitiveTypes.Float64, false, ""},
		{arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String), iceberg.PrimitiveTypes.String, false, ""},
		{arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64), iceberg.PrimitiveTypes.Float64, false, ""},
		{arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int16), iceberg.PrimitiveTypes.Int32, false, ""},
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

			if tt.reciprocal {
				result, err := table.TypeToArrowType(tt.ice, true, false)
				require.NoError(t, err)
				assert.True(t, arrow.TypeEqual(tt.dt, result), tt.dt.String(), result.String())
			}
		})
	}
}

func TestArrowSchemaToIceberg(t *testing.T) {
	tests := []struct {
		name     string
		sc       *arrow.Schema
		expected string
		err      string
	}{
		{"simple", arrow.NewSchema([]arrow.Field{
			{
				Name: "foo", Nullable: true, Type: arrow.BinaryTypes.String,
				Metadata: fieldIDMeta("1"),
			},
			{
				Name: "bar", Nullable: false, Type: arrow.PrimitiveTypes.Int32,
				Metadata: fieldIDMeta("2"),
			},
			{
				Name: "baz", Nullable: true, Type: arrow.FixedWidthTypes.Boolean,
				Metadata: fieldIDMeta("3"),
			},
		}, nil), `table {
	1: foo: optional string
	2: bar: required int
	3: baz: optional boolean
}`, ""},
		{"nested", arrow.NewSchema([]arrow.Field{
			{
				Name: "qux", Nullable: false, Metadata: fieldIDMeta("4"),
				Type: arrow.ListOfField(arrow.Field{
					Name:     "element",
					Type:     arrow.BinaryTypes.String,
					Metadata: fieldIDMeta("5"),
				}),
			},
			{
				Name: "quux", Nullable: false, Metadata: fieldIDMeta("6"),
				Type: arrow.MapOfWithMetadata(arrow.BinaryTypes.String, fieldIDMeta("7"),
					arrow.MapOfWithMetadata(arrow.BinaryTypes.String, fieldIDMeta("9"),
						arrow.PrimitiveTypes.Int32, fieldIDMeta("10")), fieldIDMeta("8")),
			},
			{
				Name: "location", Nullable: false, Metadata: fieldIDMeta("11"),
				Type: arrow.ListOfField(
					arrow.Field{
						Name: "element", Metadata: fieldIDMeta("12"),
						Type: arrow.StructOf(
							arrow.Field{
								Name: "latitude", Nullable: true,
								Type: arrow.PrimitiveTypes.Float32, Metadata: fieldIDMeta("13"),
							},
							arrow.Field{
								Name: "longitude", Nullable: true,
								Type: arrow.PrimitiveTypes.Float32, Metadata: fieldIDMeta("14"),
							},
						),
					}),
			},
			{
				Name: "person", Nullable: true, Metadata: fieldIDMeta("15"),
				Type: arrow.StructOf(
					arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: fieldIDMeta("16")},
					arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Metadata: fieldIDMeta("17")},
				),
			},
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
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true,
		},
		iceberg.NestedField{
			ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true,
		},
		iceberg.NestedField{
			ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false,
		},
		iceberg.NestedField{
			ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
				ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: false,
			},
		},
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
			Required: true,
		},
		iceberg.NestedField{
			ID: 11, Name: "location", Type: &iceberg.ListType{
				ElementID: 12, Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: true},
						{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: true},
					},
				},
				ElementRequired: false,
			},
			Required: true,
		},
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

func TestArrowSchemaRoundTripConversion(t *testing.T) {
	schemas := []*iceberg.Schema{
		icebergSchemaSimple,
		icebergSchemaNested,
	}

	for _, tt := range schemas {
		sc, err := table.SchemaToArrowSchema(tt, nil, true, false)
		require.NoError(t, err)

		ice, err := table.ArrowSchemaToIceberg(sc, false, nil)
		require.NoError(t, err)

		assert.True(t, tt.Equals(ice), tt.String(), ice.String())
	}
}

func TestVariantArrowConversion(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.VariantType{}},
	)

	t.Run("round trip with field ids", func(t *testing.T) {
		arrowSc, err := table.SchemaToArrowSchema(sc, nil, true, false)
		require.NoError(t, err)

		assert.Equal(t, 2, arrowSc.NumFields())

		dataField := arrowSc.Field(1)
		ext, ok := dataField.Type.(arrow.ExtensionType)
		require.True(t, ok, "expected extension type, got %T", dataField.Type)
		assert.Equal(t, "parquet.variant", ext.ExtensionName())

		ice, err := table.ArrowSchemaToIceberg(arrowSc, false, nil)
		require.NoError(t, err)
		assert.True(t, ice.Field(1).Type.Equals(iceberg.VariantType{}))
		assert.True(t, ice.Field(0).Type.Equals(iceberg.PrimitiveTypes.Timestamp))
	})

	t.Run("large binary storage", func(t *testing.T) {
		arrowSc, err := table.SchemaToArrowSchema(sc, nil, true, true)
		require.NoError(t, err)

		dataField := arrowSc.Field(1)
		ext, ok := dataField.Type.(arrow.ExtensionType)
		require.True(t, ok)

		st, ok := ext.StorageType().(*arrow.StructType)
		require.True(t, ok, "expected struct storage type, got %T", ext.StorageType())
		assert.Equal(t, "metadata", st.Field(0).Name)
		assert.Equal(t, "value", st.Field(1).Name)
		assert.Equal(t, arrow.BinaryTypes.LargeBinary, st.Field(0).Type)
		assert.Equal(t, arrow.BinaryTypes.LargeBinary, st.Field(1).Type)
	})

	t.Run("multiple variant columns", func(t *testing.T) {
		multiSc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "v1", Type: iceberg.VariantType{}},
			iceberg.NestedField{ID: 3, Name: "v2", Type: iceberg.VariantType{}},
		)

		arrowSc, err := table.SchemaToArrowSchema(multiSc, nil, true, false)
		require.NoError(t, err)
		assert.Equal(t, 3, arrowSc.NumFields())

		for _, idx := range []int{1, 2} {
			ext, ok := arrowSc.Field(idx).Type.(arrow.ExtensionType)
			require.True(t, ok, "field %d should be extension type", idx)
			assert.Equal(t, "parquet.variant", ext.ExtensionName())
		}

		ice, err := table.ArrowSchemaToIceberg(arrowSc, false, nil)
		require.NoError(t, err)
		assert.True(t, ice.Field(1).Type.Equals(iceberg.VariantType{}))
		assert.True(t, ice.Field(2).Type.Equals(iceberg.VariantType{}))
	})
}

func TestVariantProjectionExclusion(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)

	projected, err := iceberg.PruneColumns(sc, map[int]iceberg.Void{1: {}}, false)
	require.NoError(t, err)
	assert.Equal(t, 1, projected.NumFields())
	assert.Equal(t, "id", projected.Field(0).Name)

	arrowSc, err := table.SchemaToArrowSchema(projected, nil, true, false)
	require.NoError(t, err)
	assert.Equal(t, 1, arrowSc.NumFields())
	assert.Equal(t, "id", arrowSc.Field(0).Name)
}

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
		mapping  iceberg.NameMapping
		expected *iceberg.Schema
		err      string
	}{
		{"simple", schemaWithoutIDs, iceberg.NameMapping{
			{FieldID: makeID(1), Names: []string{"foo"}},
			{FieldID: makeID(2), Names: []string{"bar"}},
			{FieldID: makeID(3), Names: []string{"baz"}},
		}, icebergSchemaSimple, ""},
		{"field missing", schemaWithoutIDs, iceberg.NameMapping{
			{FieldID: makeID(1), Names: []string{"foo"}},
		}, nil, "field missing from name mapping: bar"},
		{"nested schema", schemaNestedWithoutIDs, iceberg.NameMapping{
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

var (
	ArrowSchemaWithAllTimestampPrec = arrow.NewSchema([]arrow.Field{
		{Name: "timestamp_s", Type: &arrow.TimestampType{Unit: arrow.Second}, Nullable: true},
		{Name: "timestamptz_s", Type: arrow.FixedWidthTypes.Timestamp_s, Nullable: true},
		{Name: "timestamp_ms", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
		{Name: "timestamptz_ms", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
		{Name: "timestamp_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "timestamptz_us", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "timestamp_ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
		{Name: "timestamptz_ns", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: "timestamptz_us_etc_utc", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Etc/UTC"}, Nullable: true},
		{Name: "timestamptz_ns_z", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Z"}, Nullable: true},
		{Name: "timestamptz_s_0000", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "+00:00"}, Nullable: true},
	}, nil)

	ArrowSchemaWithAllMicrosecondsTimestampPrec = arrow.NewSchema([]arrow.Field{
		{Name: "timestamp_s", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "timestamptz_s", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "timestamp_ms", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "timestamptz_ms", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "timestamp_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "timestamptz_us", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "timestamp_ns", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "timestamptz_ns", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "timestamptz_us_etc_utc", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "timestamptz_ns_z", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "timestamptz_s_0000", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)

	TableSchemaWithAllMicrosecondsTimestampPrec = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "timestamp_s", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 2, Name: "timestamptz_s", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 3, Name: "timestamp_ms", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 4, Name: "timestamptz_ms", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 5, Name: "timestamp_us", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 6, Name: "timestamptz_us", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 7, Name: "timestamp_ns", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 8, Name: "timestamptz_ns", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 9, Name: "timestamptz_us_etc_utc", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 10, Name: "timestamptz_ns_z", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 11, Name: "timestamptz_s_0000", Type: iceberg.PrimitiveTypes.TimestampTz},
	)
)

func ArrowRecordWithAllTimestampPrec(mem memory.Allocator) arrow.RecordBatch {
	batch, _, err := array.RecordFromJSON(mem, ArrowSchemaWithAllTimestampPrec,
		strings.NewReader(`[
		{
			"timestamp_s": "2023-01-01T19:25:00-05:00",
			"timestamptz_s": "2023-01-01T19:25:00Z",
			"timestamp_ms": "2023-01-01T19:25:00.123-05:00",
			"timestamptz_ms": "2023-01-01T19:25:00.123Z",
			"timestamp_us": "2023-01-01T19:25:00.123456-05:00",
			"timestamptz_us": "2023-01-01T19:25:00.123456Z",
			"timestamp_ns": "2024-07-11T03:30:00.123456789-05:00",
			"timestamptz_ns": "2023-01-01T19:25:00.123456789Z",
			"timestamptz_us_etc_utc": "2023-01-01T19:25:00.123456Z",
			"timestamptz_ns_z": "2024-07-11T03:30:00.123456789Z",
			"timestamptz_s_0000": "2023-01-01T19:25:00Z"
		}, {}, {
			"timestamp_s": "2023-03-01T19:25:00-05:00",
			"timestamptz_s": "2023-03-01T19:25:00Z",
			"timestamp_ms": "2023-03-01T19:25:00.123-05:00",
			"timestamptz_ms": "2023-03-01T19:25:00.123Z",
			"timestamp_us": "2023-03-01T19:25:00.123456-05:00",
			"timestamptz_us": "2023-03-01T19:25:00.123456Z",
			"timestamp_ns": "2024-07-11T03:30:00.9876543210-05:00",
			"timestamptz_ns": "2023-03-01T19:25:00.9876543210Z",
			"timestamptz_us_etc_utc": "2023-03-01T19:25:00.123456Z",
			"timestamptz_ns_z": "2024-07-11T03:30:00.9876543210Z",
			"timestamptz_s_0000": "2023-03-01T19:25:00Z"
		}
	]`))
	if err != nil {
		panic(err)
	}

	return batch
}

func TestToRequestedSchemaTimestamps(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	batch := ArrowRecordWithAllTimestampPrec(mem)
	defer batch.Release()

	requestedSchema := TableSchemaWithAllMicrosecondsTimestampPrec
	fileSchema := requestedSchema

	converted, err := table.ToRequestedSchema(ctx, requestedSchema, fileSchema, batch, table.SchemaOptions{DowncastTimestamp: true})
	require.NoError(t, err)
	defer converted.Release()

	assert.True(t, converted.Schema().Equal(ArrowSchemaWithAllMicrosecondsTimestampPrec), "expected: %s\ngot: %s",
		ArrowSchemaWithAllMicrosecondsTimestampPrec, converted.Schema())

	for i, col := range batch.Columns() {
		convertedCol := converted.Column(i)
		if arrow.TypeEqual(col.DataType(), convertedCol.DataType()) {
			assert.True(t, array.Equal(col, convertedCol), "expected: %s\ngot: %s", col, convertedCol)
		} else {
			expected, err := compute.CastArray(context.Background(), col, compute.UnsafeCastOptions(convertedCol.DataType()))
			require.NoError(t, err)
			assert.True(t, array.Equal(expected, convertedCol), "expected: %s\ngot: %s", expected, convertedCol)
			expected.Release()
		}
	}
}

func TestToRequestedSchema(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "nested", Type: arrow.ListOfField(arrow.Field{
				Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
				Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"2"}),
			}),
			Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"1"}),
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	const data = `{"nested": [1, 2, 3]}
				  {"nested": [4, 5, 6]}`

	s := bufio.NewScanner(strings.NewReader(data))
	require.True(t, s.Scan())
	require.NoError(t, bldr.UnmarshalJSON(s.Bytes()))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	icesc, err := table.ArrowSchemaToIceberg(schema, false, nil)
	require.NoError(t, err)

	rec2, err := table.ToRequestedSchema(context.Background(), icesc, icesc, rec, table.SchemaOptions{DowncastTimestamp: true, IncludeFieldIDs: true})
	require.NoError(t, err)
	defer rec2.Release()

	assert.True(t, array.RecordEqual(rec, rec2))
}

func TestToRequestedSchemaWriteDefaults(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	requestedIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "event_date", Type: iceberg.PrimitiveTypes.Date, Required: false, WriteDefault: iceberg.Date(1234)},
	)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
		},
	}, nil)
	bldr := array.NewRecordBuilder(mem, arrowSchema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{UseWriteDefault: true})
	require.NoError(t, err)
	defer result.Release()

	require.EqualValues(t, 2, result.NumCols())
	dateCol := result.Column(1)
	require.Equal(t, arrow.DATE32, dateCol.DataType().ID(), "expected date32 column, got %s", dateCol.DataType())
	require.Equal(t, 3, dateCol.Len())
	dateArr := dateCol.(*array.Date32)
	for i := 0; i < dateArr.Len(); i++ {
		assert.Equal(t, arrow.Date32(1234), dateArr.Value(i), "row %d should have write-default value", i)
	}
}

func TestToRequestedSchemaRequiredFieldWithWriteDefault(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	// Required field with write-default: should be filled with the default value
	requestedIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.Int64, Required: true, WriteDefault: int64(42)},
	)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
		},
	}, nil)
	bldr := array.NewRecordBuilder(mem, arrowSchema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{UseWriteDefault: true})
	require.NoError(t, err)
	defer result.Release()

	require.EqualValues(t, 2, result.NumCols())
	catCol := result.Column(1)
	require.Equal(t, arrow.INT64, catCol.DataType().ID())
	require.Equal(t, 3, catCol.Len())
	int64Arr := catCol.(*array.Int64)
	for i := 0; i < int64Arr.Len(); i++ {
		assert.Equal(t, int64(42), int64Arr.Value(i), "row %d should have write-default value", i)
	}
}

func TestToRequestedSchemaRequiredFieldMissingNoDefault(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	// Required field without write-default: should return error
	requestedIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
		},
	}, nil)
	bldr := array.NewRecordBuilder(mem, arrowSchema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	_, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{UseWriteDefault: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required field is missing and has no default value")
}

func TestToRequestedSchemaRequiredFieldWithInitialDefault(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	// Required field with initial-default: should be filled on read path (useWriteDefault=false)
	requestedIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.Int64, Required: true, InitialDefault: int64(99)},
	)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
		},
	}, nil)
	bldr := array.NewRecordBuilder(mem, arrowSchema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{UseWriteDefault: false})
	require.NoError(t, err)
	defer result.Release()

	require.EqualValues(t, 2, result.NumCols())
	catCol := result.Column(1)
	require.Equal(t, arrow.INT64, catCol.DataType().ID())
	require.Equal(t, 3, catCol.Len())
	int64Arr := catCol.(*array.Int64)
	for i := 0; i < int64Arr.Len(); i++ {
		assert.Equal(t, int64(99), int64Arr.Value(i), "row %d should have initial-default value", i)
	}
}

func TestToRequestedSchemaWriteDefaultsTypes(t *testing.T) {
	ctx := context.Background()

	buildBaseRecord := func(mem memory.Allocator) arrow.RecordBatch {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{
				Name:     "id",
				Type:     arrow.PrimitiveTypes.Int32,
				Nullable: false,
				Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
			},
		}, nil)
		bldr := array.NewRecordBuilder(mem, arrowSchema)
		defer bldr.Release()
		bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)

		return bldr.NewRecordBatch()
	}

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	tests := []struct {
		name  string
		field iceberg.NestedField
		check func(t *testing.T, col arrow.Array)
	}{
		{
			name: "time write-default",
			field: iceberg.NestedField{
				ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Time,
				Required: false, WriteDefault: iceberg.Time(5000000),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIME64, col.DataType().ID())
				timeArr := col.(*array.Time64)
				for i := 0; i < timeArr.Len(); i++ {
					assert.Equal(t, arrow.Time64(5000000), timeArr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "timestamp write-default",
			field: iceberg.NestedField{
				ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp,
				Required: false, WriteDefault: iceberg.Timestamp(1700000000000000),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				tsArr := col.(*array.Timestamp)
				for i := 0; i < tsArr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000), tsArr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "uuid write-default",
			field: iceberg.NestedField{
				ID: 2, Name: "uid", Type: iceberg.UUIDType{},
				Required: false, WriteDefault: uuid.MustParse("f79c3e09-677c-4bbd-a479-512f87f77acf"),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.EXTENSION, col.DataType().ID())
				uuidArr := col.(*extensions.UUIDArray)
				expected := uuid.MustParse("f79c3e09-677c-4bbd-a479-512f87f77acf")
				for i := 0; i < uuidArr.Len(); i++ {
					assert.Equal(t, expected, uuidArr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "decimal write-default",
			field: iceberg.NestedField{
				ID: 2, Name: "price", Type: iceberg.DecimalTypeOf(10, 2),
				Required: false, WriteDefault: iceberg.Decimal{Val: decimal128.New(0, 12345), Scale: 2},
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DECIMAL128, col.DataType().ID())
				decArr := col.(*array.Decimal128)
				for i := 0; i < decArr.Len(); i++ {
					assert.Equal(t, decimal128.New(0, 12345), decArr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "bool write-default",
			field: iceberg.NestedField{
				ID: 2, Name: "flag", Type: iceberg.PrimitiveTypes.Bool,
				Required: false, WriteDefault: true,
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BOOL, col.DataType().ID())
				boolArr := col.(*array.Boolean)
				for i := 0; i < boolArr.Len(); i++ {
					assert.True(t, boolArr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "string write-default",
			field: iceberg.NestedField{
				ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String,
				Required: false, WriteDefault: "hello",
			},
			check: func(t *testing.T, col arrow.Array) {
				strArr := col.(*array.String)
				for i := 0; i < strArr.Len(); i++ {
					assert.Equal(t, "hello", strArr.Value(i), "row %d", i)
				}
			},
		},
		{
			// initial-default is set but must be ignored on the write path
			name: "ignores initial-default when write-default is set",
			field: iceberg.NestedField{
				ID: 2, Name: "dt", Type: iceberg.PrimitiveTypes.Date,
				Required: false, InitialDefault: iceberg.Date(100), WriteDefault: iceberg.Date(999),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(999), arr.Value(i), "row %d: should use write-default (999), not initial-default (100)", i)
				}
			},
		},
		{
			// json.Number is produced when metadata is decoded via json.Decoder.UseNumber()
			name: "date write-default as json.Number",
			field: iceberg.NestedField{
				ID: 2, Name: "dt", Type: iceberg.PrimitiveTypes.Date,
				Required: false, WriteDefault: json.Number("1234"),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(1234), arr.Value(i), "row %d", i)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			requestedIceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				tt.field,
			)

			rec := buildBaseRecord(mem)
			defer rec.Release()

			result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{UseWriteDefault: true})
			require.NoError(t, err)
			defer result.Release()

			require.EqualValues(t, 2, result.NumCols())
			tt.check(t, result.Column(1))
		})
	}
}

// TestToRequestedSchemaInitialDefaults is the read-path equivalent of
// TestToRequestedSchemaWriteDefaults: a missing column is filled using
// InitialDefault (useWriteDefault=false).
func TestToRequestedSchemaInitialDefaults(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	requestedIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "event_date", Type: iceberg.PrimitiveTypes.Date, Required: false, InitialDefault: iceberg.Date(1234)},
	)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
		},
	}, nil)
	bldr := array.NewRecordBuilder(mem, arrowSchema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{})
	require.NoError(t, err)
	defer result.Release()

	require.EqualValues(t, 2, result.NumCols())
	dateCol := result.Column(1)
	require.Equal(t, arrow.DATE32, dateCol.DataType().ID(), "expected date32 column, got %s", dateCol.DataType())
	require.Equal(t, 3, dateCol.Len())
	dateArr := dateCol.(*array.Date32)
	for i := 0; i < dateArr.Len(); i++ {
		assert.Equal(t, arrow.Date32(1234), dateArr.Value(i), "row %d should have initial-default value", i)
	}
}

// TestToRequestedSchemaInitialDefaultTypes is the read-path equivalent of
// TestToRequestedSchemaWriteDefaultsTypes: covers all Iceberg types using
// programmatically constructed NestedFields with InitialDefault set.
func TestToRequestedSchemaInitialDefaultTypes(t *testing.T) {
	ctx := context.Background()

	buildBaseRecord := func(mem memory.Allocator) arrow.RecordBatch {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{
				Name:     "id",
				Type:     arrow.PrimitiveTypes.Int32,
				Nullable: false,
				Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
			},
		}, nil)
		bldr := array.NewRecordBuilder(mem, arrowSchema)
		defer bldr.Release()
		bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)

		return bldr.NewRecordBatch()
	}

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	tests := []struct {
		name  string
		field iceberg.NestedField
		check func(t *testing.T, col arrow.Array)
	}{
		{
			name: "date initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "dt", Type: iceberg.PrimitiveTypes.Date,
				Required: false, InitialDefault: iceberg.Date(1234),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(1234), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "time initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Time,
				Required: false, InitialDefault: iceberg.Time(5000000),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIME64, col.DataType().ID())
				arr := col.(*array.Time64)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Time64(5000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "timestamp initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp,
				Required: false, InitialDefault: iceberg.Timestamp(1700000000000000),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "uuid initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "uid", Type: iceberg.UUIDType{},
				Required: false, InitialDefault: uuid.MustParse("f79c3e09-677c-4bbd-a479-512f87f77acf"),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.EXTENSION, col.DataType().ID())
				arr := col.(*extensions.UUIDArray)
				expected := uuid.MustParse("f79c3e09-677c-4bbd-a479-512f87f77acf")
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, expected, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "decimal initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "price", Type: iceberg.DecimalTypeOf(10, 2),
				Required: false, InitialDefault: iceberg.Decimal{Val: decimal128.New(0, 12345), Scale: 2},
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DECIMAL128, col.DataType().ID())
				arr := col.(*array.Decimal128)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, decimal128.New(0, 12345), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "bool initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "flag", Type: iceberg.PrimitiveTypes.Bool,
				Required: false, InitialDefault: true,
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BOOL, col.DataType().ID())
				arr := col.(*array.Boolean)
				for i := 0; i < arr.Len(); i++ {
					assert.True(t, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "string initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String,
				Required: false, InitialDefault: "hello",
			},
			check: func(t *testing.T, col arrow.Array) {
				arr := col.(*array.String)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, "hello", arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "binary initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.Binary,
				Required: false, InitialDefault: []byte("hello"),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BINARY, col.DataType().ID())
				arr := col.(*array.Binary)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, []byte("hello"), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "int32 initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "n", Type: iceberg.PrimitiveTypes.Int32,
				Required: false, InitialDefault: int32(42),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.INT32, col.DataType().ID())
				arr := col.(*array.Int32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, int32(42), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name: "int64 initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "n", Type: iceberg.PrimitiveTypes.Int64,
				Required: false, InitialDefault: int64(9999),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.INT64, col.DataType().ID())
				arr := col.(*array.Int64)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, int64(9999), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			// write-default is set but must be ignored on the read path
			name: "ignores write-default when initial-default is set",
			field: iceberg.NestedField{
				ID: 2, Name: "dt", Type: iceberg.PrimitiveTypes.Date,
				Required: false, InitialDefault: iceberg.Date(100), WriteDefault: iceberg.Date(999),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(100), arr.Value(i), "row %d: should use initial-default (100), not write-default (999)", i)
				}
			},
		},
		{
			// json.Number is produced when metadata is decoded via json.Decoder.UseNumber()
			name: "date initial-default as json.Number",
			field: iceberg.NestedField{
				ID: 2, Name: "dt", Type: iceberg.PrimitiveTypes.Date,
				Required: false, InitialDefault: json.Number("1234"),
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(1234), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			// no default set at all — column must be null
			name: "falls back to null when no initial-default",
			field: iceberg.NestedField{
				ID: 2, Name: "dt", Type: iceberg.PrimitiveTypes.Date,
				Required: false,
			},
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				for i := 0; i < col.Len(); i++ {
					assert.True(t, col.IsNull(i), "row %d should be null", i)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			requestedIceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				tt.field,
			)

			rec := buildBaseRecord(mem)
			defer rec.Release()

			result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{})
			require.NoError(t, err)
			defer result.Release()

			require.EqualValues(t, 2, result.NumCols())
			tt.check(t, result.Column(1))
		})
	}
}

// TestToRequestedSchemaInitialDefaultJSONRoundTrip is the read-path equivalent
// of TestToRequestedSchemaWriteDefaultJSONRoundTrip. It unmarshals NestedFields
// from JSON (as they arrive from a REST catalog or metadata file) and verifies
// that initial-default values are projected correctly with useWriteDefault=false.
func TestToRequestedSchemaInitialDefaultJSONRoundTrip(t *testing.T) {
	ctx := context.Background()

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	buildBaseRecord := func(mem memory.Allocator) arrow.RecordBatch {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{
				Name:     "id",
				Type:     arrow.PrimitiveTypes.Int32,
				Nullable: false,
				Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
			},
		}, nil)
		bldr := array.NewRecordBuilder(mem, arrowSchema)
		defer bldr.Release()
		bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)

		return bldr.NewRecordBatch()
	}

	unmarshalField := func(t *testing.T, fieldJSON string) iceberg.NestedField {
		t.Helper()
		var field iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(fieldJSON), &field))

		return field
	}

	tests := []struct {
		name      string
		fieldJSON string
		check     func(t *testing.T, col arrow.Array)
	}{
		{
			name:      "date as float64",
			fieldJSON: `{"id":2,"name":"dt","type":"date","required":false,"initial-default":1234}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(1234), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "time as float64",
			fieldJSON: `{"id":2,"name":"t","type":"time","required":false,"initial-default":5000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIME64, col.DataType().ID())
				arr := col.(*array.Time64)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Time64(5000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamp as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamp","required":false,"initial-default":1700000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamptz as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamptz","required":false,"initial-default":1700000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamp_ns as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamp_ns","required":false,"initial-default":1700000000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamptz_ns as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamptz_ns","required":false,"initial-default":1700000000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "uuid as string",
			fieldJSON: `{"id":2,"name":"uid","type":"uuid","required":false,"initial-default":"f79c3e09-677c-4bbd-a479-512f87f77acf"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.EXTENSION, col.DataType().ID())
				arr := col.(*extensions.UUIDArray)
				expected := uuid.MustParse("f79c3e09-677c-4bbd-a479-512f87f77acf")
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, expected, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "decimal as string",
			fieldJSON: `{"id":2,"name":"price","type":"decimal(10, 2)","required":false,"initial-default":"123.45"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DECIMAL128, col.DataType().ID())
				arr := col.(*array.Decimal128)
				expected := decimal128.FromI64(12345)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, expected, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "binary as base64 string",
			fieldJSON: `{"id":2,"name":"data","type":"binary","required":false,"initial-default":"` + base64.StdEncoding.EncodeToString([]byte("hello")) + `"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BINARY, col.DataType().ID())
				arr := col.(*array.Binary)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, []byte("hello"), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "fixed as base64 string",
			fieldJSON: `{"id":2,"name":"data","type":"fixed[5]","required":false,"initial-default":"` + base64.StdEncoding.EncodeToString([]byte("hello")) + `"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FIXED_SIZE_BINARY, col.DataType().ID())
				arr := col.(*array.FixedSizeBinary)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, []byte("hello"), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "bool",
			fieldJSON: `{"id":2,"name":"flag","type":"boolean","required":false,"initial-default":true}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BOOL, col.DataType().ID())
				arr := col.(*array.Boolean)
				for i := 0; i < arr.Len(); i++ {
					assert.True(t, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "int as float64",
			fieldJSON: `{"id":2,"name":"n","type":"int","required":false,"initial-default":42}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.INT32, col.DataType().ID())
				arr := col.(*array.Int32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, int32(42), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "long as float64",
			fieldJSON: `{"id":2,"name":"n","type":"long","required":false,"initial-default":42}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.INT64, col.DataType().ID())
				arr := col.(*array.Int64)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, int64(42), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "float as float64",
			fieldJSON: `{"id":2,"name":"n","type":"float","required":false,"initial-default":3.14}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FLOAT32, col.DataType().ID())
				arr := col.(*array.Float32)
				for i := 0; i < arr.Len(); i++ {
					assert.InDelta(t, float32(3.14), arr.Value(i), 0.001, "row %d", i)
				}
			},
		},
		{
			name:      "double as float64",
			fieldJSON: `{"id":2,"name":"n","type":"double","required":false,"initial-default":3.14}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FLOAT64, col.DataType().ID())
				arr := col.(*array.Float64)
				for i := 0; i < arr.Len(); i++ {
					assert.InDelta(t, float64(3.14), arr.Value(i), 0.0001, "row %d", i)
				}
			},
		},
		{
			name:      "string",
			fieldJSON: `{"id":2,"name":"s","type":"string","required":false,"initial-default":"hello"}`,
			check: func(t *testing.T, col arrow.Array) {
				arr := col.(*array.String)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, "hello", arr.Value(i), "row %d", i)
				}
			},
		},
		{
			// both defaults present — read path must use initial-default
			name:      "prefers initial-default over write-default on read path",
			fieldJSON: `{"id":2,"name":"dt","type":"date","required":false,"initial-default":100,"write-default":999}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(100), arr.Value(i), "row %d: should use initial-default (100), not write-default (999)", i)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			field := unmarshalField(t, tt.fieldJSON)

			requestedIceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				field,
			)

			rec := buildBaseRecord(mem)
			defer rec.Release()

			result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{})
			require.NoError(t, err)
			defer result.Release()

			require.EqualValues(t, 2, result.NumCols())
			tt.check(t, result.Column(1))
		})
	}
}

// TestToRequestedSchemaWriteDefaultJSONRoundTrip verifies that write-default
// values arriving from a REST catalog or metadata file (where encoding/json
// decodes numbers as float64 and strings as string) are handled correctly
// without panicking. Each sub-test unmarshals a NestedField from a raw JSON
// snippet and projects it through ToRequestedSchema.
func TestToRequestedSchemaWriteDefaultJSONRoundTrip(t *testing.T) {
	ctx := context.Background()

	fileIceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	buildBaseRecord := func(mem memory.Allocator) arrow.RecordBatch {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{
				Name:     "id",
				Type:     arrow.PrimitiveTypes.Int32,
				Nullable: false,
				Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
			},
		}, nil)
		bldr := array.NewRecordBuilder(mem, arrowSchema)
		defer bldr.Release()
		bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)

		return bldr.NewRecordBatch()
	}

	unmarshalField := func(t *testing.T, fieldJSON string) iceberg.NestedField {
		t.Helper()
		var field iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(fieldJSON), &field))

		return field
	}

	tests := []struct {
		name      string
		fieldJSON string
		check     func(t *testing.T, col arrow.Array)
	}{
		{
			name:      "date as float64",
			fieldJSON: `{"id":2,"name":"dt","type":"date","required":false,"write-default":1234}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DATE32, col.DataType().ID())
				arr := col.(*array.Date32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Date32(1234), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "time as float64",
			fieldJSON: `{"id":2,"name":"t","type":"time","required":false,"write-default":5000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIME64, col.DataType().ID())
				arr := col.(*array.Time64)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Time64(5000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamp as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamp","required":false,"write-default":1700000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamptz as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamptz","required":false,"write-default":1700000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamp_ns as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamp_ns","required":false,"write-default":1700000000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "uuid as string",
			fieldJSON: `{"id":2,"name":"uid","type":"uuid","required":false,"write-default":"f79c3e09-677c-4bbd-a479-512f87f77acf"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.EXTENSION, col.DataType().ID())
				arr := col.(*extensions.UUIDArray)
				expected := uuid.MustParse("f79c3e09-677c-4bbd-a479-512f87f77acf")
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, expected, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "decimal as string",
			fieldJSON: `{"id":2,"name":"price","type":"decimal(10, 2)","required":false,"write-default":"123.45"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.DECIMAL128, col.DataType().ID())
				arr := col.(*array.Decimal128)
				expected := decimal128.FromI64(12345)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, expected, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "binary as base64 string",
			fieldJSON: `{"id":2,"name":"data","type":"binary","required":false,"write-default":"` + base64.StdEncoding.EncodeToString([]byte("hello")) + `"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BINARY, col.DataType().ID())
				arr := col.(*array.Binary)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, []byte("hello"), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "fixed as base64 string",
			fieldJSON: `{"id":2,"name":"data","type":"fixed[5]","required":false,"write-default":"` + base64.StdEncoding.EncodeToString([]byte("hello")) + `"}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FIXED_SIZE_BINARY, col.DataType().ID())
				arr := col.(*array.FixedSizeBinary)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, []byte("hello"), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "timestamptz_ns as float64",
			fieldJSON: `{"id":2,"name":"ts","type":"timestamptz_ns","required":false,"write-default":1700000000000000000}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.TIMESTAMP, col.DataType().ID())
				arr := col.(*array.Timestamp)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, arrow.Timestamp(1700000000000000000), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "bool",
			fieldJSON: `{"id":2,"name":"flag","type":"boolean","required":false,"write-default":true}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.BOOL, col.DataType().ID())
				arr := col.(*array.Boolean)
				for i := 0; i < arr.Len(); i++ {
					assert.True(t, arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "int as float64",
			fieldJSON: `{"id":2,"name":"n","type":"int","required":false,"write-default":42}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.INT32, col.DataType().ID())
				arr := col.(*array.Int32)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, int32(42), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "long as float64",
			fieldJSON: `{"id":2,"name":"n","type":"long","required":false,"write-default":42}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.INT64, col.DataType().ID())
				arr := col.(*array.Int64)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, int64(42), arr.Value(i), "row %d", i)
				}
			},
		},
		{
			name:      "float as float64",
			fieldJSON: `{"id":2,"name":"n","type":"float","required":false,"write-default":3.14}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FLOAT32, col.DataType().ID())
				arr := col.(*array.Float32)
				for i := 0; i < arr.Len(); i++ {
					assert.InDelta(t, float32(3.14), arr.Value(i), 0.001, "row %d", i)
				}
			},
		},
		{
			name:      "double as float64",
			fieldJSON: `{"id":2,"name":"n","type":"double","required":false,"write-default":3.14}`,
			check: func(t *testing.T, col arrow.Array) {
				require.Equal(t, arrow.FLOAT64, col.DataType().ID())
				arr := col.(*array.Float64)
				for i := 0; i < arr.Len(); i++ {
					assert.InDelta(t, float64(3.14), arr.Value(i), 0.0001, "row %d", i)
				}
			},
		},
		{
			name:      "string",
			fieldJSON: `{"id":2,"name":"s","type":"string","required":false,"write-default":"hello"}`,
			check: func(t *testing.T, col arrow.Array) {
				arr := col.(*array.String)
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, "hello", arr.Value(i), "row %d", i)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			field := unmarshalField(t, tt.fieldJSON)

			requestedIceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				field,
			)

			rec := buildBaseRecord(mem)
			defer rec.Release()

			result, err := table.ToRequestedSchema(ctx, requestedIceSchema, fileIceSchema, rec, table.SchemaOptions{UseWriteDefault: true})
			require.NoError(t, err)
			defer result.Release()

			require.EqualValues(t, 2, result.NumCols())
			tt.check(t, result.Column(1))
		})
	}
}
