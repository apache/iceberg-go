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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	converted, err := table.ToRequestedSchema(ctx, requestedSchema, fileSchema, batch, true, false, false)
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

	rec2, err := table.ToRequestedSchema(context.Background(), icesc, icesc, rec, true, true, false)
	require.NoError(t, err)
	defer rec2.Release()

	assert.True(t, array.RecordEqual(rec, rec2))
}
