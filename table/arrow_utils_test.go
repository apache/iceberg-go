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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/geoarrow/geoarrow-go"
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
		{&arrow.FixedSizeBinaryType{ByteWidth: 0}, iceberg.FixedTypeOf(0), true, ""},
		{&arrow.FixedSizeBinaryType{ByteWidth: 23}, iceberg.FixedTypeOf(23), true, ""},
		{&arrow.Decimal32Type{Precision: 8, Scale: 2}, iceberg.DecimalTypeOf(8, 2), false, ""},
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
		{arrow.FixedWidthTypes.Timestamp_ns, iceberg.PrimitiveTypes.TimestampTzNs, true, ""},
		{&arrow.TimestampType{Unit: arrow.Second}, iceberg.PrimitiveTypes.Timestamp, false, ""},
		{&arrow.TimestampType{Unit: arrow.Millisecond}, iceberg.PrimitiveTypes.Timestamp, false, ""},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, iceberg.PrimitiveTypes.Timestamp, true, ""},
		{&arrow.TimestampType{Unit: arrow.Nanosecond}, iceberg.PrimitiveTypes.TimestampNs, true, ""},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "US/Pacific"}, nil, false, "unsupported arrow type for conversion - timestamp[us, tz=US/Pacific]"},
		{&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "US/Pacific"}, nil, false, "unsupported arrow type for conversion - timestamp[ns, tz=US/Pacific]"},
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

func assertGeoArrowWKB(t *testing.T, dt arrow.DataType, storage arrow.DataType, want geoarrow.Metadata) {
	t.Helper()

	wkb, ok := dt.(*geoarrow.WKBType)
	require.True(t, ok, "expected *geoarrow.WKBType, got %T", dt)
	assert.Equal(t, "geoarrow.wkb", wkb.ExtensionName())
	assert.True(t, arrow.TypeEqual(storage, wkb.StorageType()))
	assert.Equal(t, want.CRS, wkb.Metadata().CRS)
	assert.Equal(t, want.Edges, wkb.Metadata().Edges)
}

func assertGeoArrowWKBMetadataJSON(t *testing.T, dt arrow.DataType, storage arrow.DataType, wantJSON string) {
	t.Helper()

	wkb, ok := dt.(*geoarrow.WKBType)
	require.True(t, ok, "expected *geoarrow.WKBType, got %T", dt)
	assert.Equal(t, "geoarrow.wkb", wkb.ExtensionName())
	assert.True(t, arrow.TypeEqual(storage, wkb.StorageType()))

	var want map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(wantJSON), &want))

	meta := wkb.Metadata()

	if wantCRS, ok := want["crs"]; ok {
		assert.JSONEq(t, string(wantCRS), string(meta.CRS), "crs mismatch")
	} else {
		assert.Empty(t, meta.CRS, "expected omitted crs")
	}

	if wantEdges, ok := want["edges"]; ok {
		var edges string
		require.NoError(t, json.Unmarshal(wantEdges, &edges))
		assert.Equal(t, geoarrow.EdgeInterpolation(edges), meta.Edges, "edges mismatch")
	} else {
		assert.Empty(t, meta.Edges, "expected omitted edges")
	}
}

func jsonCRS(s string) json.RawMessage {
	raw, _ := json.Marshal(s)

	return raw
}

func TestIcebergGeoTypesToArrowSchema(t *testing.T) {
	geomSRID, err := iceberg.GeometryTypeOf("srid:4326")
	require.NoError(t, err)
	geogKarney, err := iceberg.GeographyTypeOf("srid:4269", "karney")
	require.NoError(t, err)
	geomSRID0, err := iceberg.GeometryTypeOf("srid:0")
	require.NoError(t, err)
	geomEPSG4267, err := iceberg.GeometryTypeOf("EPSG:4267")
	require.NoError(t, err)
	geogSpherical, err := iceberg.GeographyTypeOf("OGC:CRS84", "spherical")
	require.NoError(t, err)
	geogKarneyDefaultCRS, err := iceberg.GeographyTypeOf("OGC:CRS84", "karney")
	require.NoError(t, err)
	geogVincenty, err := iceberg.GeographyTypeOf("OGC:CRS84", "vincenty")
	require.NoError(t, err)
	geogAndoyer, err := iceberg.GeographyTypeOf("OGC:CRS84", "andoyer")
	require.NoError(t, err)
	geogThomas, err := iceberg.GeographyTypeOf("OGC:CRS84", "thomas")
	require.NoError(t, err)
	geogSRID0, err := iceberg.GeographyTypeOf("srid:0", "spherical")
	require.NoError(t, err)
	geogEPSG4267, err := iceberg.GeographyTypeOf("EPSG:4267", "spherical")
	require.NoError(t, err)
	geomCustomCRS, err := iceberg.GeometryTypeOf("my-custom-crs")
	require.NoError(t, err)
	defaultGeometry, err := iceberg.GeometryTypeOf("OGC:CRS84")
	require.NoError(t, err)
	geomEPSG3857, err := iceberg.GeometryTypeOf("EPSG:3857")
	require.NoError(t, err)
	geogEPSG4267Karney, err := iceberg.GeographyTypeOf("EPSG:4267", "karney")
	require.NoError(t, err)

	typeCases := []struct {
		name             string
		ice              iceberg.Type
		geoarrowMetaJSON string
	}{
		// Note that these tests below are based on arrow-rs tests (https://github.com/apache/arrow-rs/pull/10065)
		// Geometry with default CRS (defaults to OGC:CRS84 per Parquet spec)
		{
			name:             "geometry_default_crs",
			ice:              iceberg.GeometryType{},
			geoarrowMetaJSON: `{"crs":"OGC:CRS84"}`,
		},
		// Geometry with srid:0 should result in an unset (omitted) CRS
		{
			name:             "geometry_srid_0",
			ice:              geomSRID0,
			geoarrowMetaJSON: `{}`,
		},
		// Geometry with custom CRSes (authority:code and partial projjson)
		{
			name:             "geometry_epsg_4267",
			ice:              geomEPSG4267,
			geoarrowMetaJSON: `{"crs":"EPSG:4267"}`,
		},
		{
			name:             "geometry_custom_string_crs",
			ice:              geomCustomCRS,
			geoarrowMetaJSON: `{"crs":"my-custom-crs"}`,
		},
		// Geography with default CRS (default OGC:CRS84, spherical edges)
		{
			name:             "geography_default_crs",
			ice:              iceberg.GeographyType{},
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"spherical"}`,
		},
		// Geography with explicit edges
		{
			name:             "geography_explicit_spherical",
			ice:              geogSpherical,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"spherical"}`,
		},
		{
			name:             "geography_karney",
			ice:              geogKarneyDefaultCRS,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"karney"}`,
		},
		{
			name:             "geography_vincenty",
			ice:              geogVincenty,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"vincenty"}`,
		},
		{
			name:             "geography_andoyer",
			ice:              geogAndoyer,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"andoyer"}`,
		},
		{
			name:             "geography_thomas",
			ice:              geogThomas,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"thomas"}`,
		},
		// Geography with srid:0 should result in an unset (omitted) CRS and spherical edges
		{
			name:             "geography_srid_0",
			ice:              geogSRID0,
			geoarrowMetaJSON: `{"edges":"spherical"}`,
		},
		// Geography with custom CRSes (authority:code and partial projjson)
		{
			name:             "geography_epsg_4267",
			ice:              geogEPSG4267,
			geoarrowMetaJSON: `{"crs":"EPSG:4267","edges":"spherical"}`,
		},
		// Happy path SRID
		{
			name:             "geometry_srid_4326",
			ice:              geomSRID,
			geoarrowMetaJSON: `{"crs":"4326", "crs_type":"srid"}`,
		},
	}

	// The following tests focus on edge cases and pinning specific behavior for read case
	readOnlyCases := []struct {
		name             string
		ice              iceberg.Type
		geoarrowMetaJSON string
	}{
		{
			// Pin that a present crs_type without CRS still follows the srid:0 default.
			name:             "geometry_srid_0_with_crs_type",
			ice:              geomSRID0,
			geoarrowMetaJSON: `{"crs_type":"authority_code"}`,
		},
		{
			name:             "case_insensitive_default_geometry",
			ice:              defaultGeometry,
			geoarrowMetaJSON: `{"crs":"OgC:cRs84"}`,
		},
		{
			name:             "case_insensitive_default_geometry_epsg_4326",
			ice:              defaultGeometry,
			geoarrowMetaJSON: `{"crs":"EpSg:4326"}`,
		},
		{
			name:             "geometry_epsg_4326",
			ice:              defaultGeometry,
			geoarrowMetaJSON: `{"crs":"epsg:4326"}`,
		},
		{
			// EPSG:4326 is canonicalized before crs_type validation, so a mismatched type still maps to the default CRS.
			name:             "geometry_epsg_4326_incorrect_type",
			ice:              defaultGeometry,
			geoarrowMetaJSON: `{"crs":"EPSG:4326","crs_type":"projjson"}`,
		},
		{
			name:             "geometry_epsg_4326_wkt2_type",
			ice:              defaultGeometry,
			geoarrowMetaJSON: `{"crs":"EPSG:4326","crs_type":"wkt2:2019"}`,
		},

		// Translated from arrow-rs geo logical type read tests (https://github.com/apache/arrow-rs/pull/10065)
		// Geometry with no CRS should be GEOMETRY(srid:0)
		{
			name:             "geometry_no_crs",
			ice:              geomSRID0,
			geoarrowMetaJSON: `{}`,
		},
		// Geometry with string CRS
		{
			name:             "geometry_epsg_4267_from_crs",
			ice:              geomEPSG4267,
			geoarrowMetaJSON: `{"crs":"EPSG:4267"}`,
		},
		// Geometry with PROJJSON CRS
		{
			name:             "geometry_projjson_epsg_3857",
			ice:              geomEPSG3857,
			geoarrowMetaJSON: `{"crs":{"id":{"authority":"EPSG","code":3857}}}`,
		},
		// Geometry with lon/lat CRSes (canonically removed because lon/lat is the
		// default Iceberg CRS)
		{
			name:             "geometry_ogc_crs84_canonical",
			ice:              iceberg.GeometryType{},
			geoarrowMetaJSON: `{"crs":"OGC:CRS84"}`,
		},
		{
			name:             "geometry_epsg_4326_canonical",
			ice:              iceberg.GeometryType{},
			geoarrowMetaJSON: `{"crs":"EPSG:4326"}`,
		},
		{
			name:             "geometry_projjson_epsg_4326_canonical",
			ice:              iceberg.GeometryType{},
			geoarrowMetaJSON: `{"crs":{"id":{"authority":"EPSG","code":4326}}}`,
		},
		{
			name:             "geometry_projjson_epsg_4326_string_code_canonical",
			ice:              iceberg.GeometryType{},
			geoarrowMetaJSON: `{"crs":{"id":{"authority":"EPSG","code":"4326"}}}`,
		},
		{
			name:             "geometry_projjson_ogc_crs84_canonical",
			ice:              iceberg.GeometryType{},
			geoarrowMetaJSON: `{"crs":{"id":{"authority":"OGC","code":"CRS84"}}}`,
		},
		// Geography with no CRS, spherical edges
		{
			name:             "geography_no_crs_spherical",
			ice:              geogSRID0,
			geoarrowMetaJSON: `{"edges":"spherical"}`,
		},
		// Geography with OGC:CRS84 and spherical edges
		{
			name:             "geography_ogc_crs84_spherical_canonical",
			ice:              iceberg.GeographyType{},
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"spherical"}`,
		},
		// Geography with different edge algorithms
		{
			name:             "geography_ogc_crs84_karney",
			ice:              geogKarneyDefaultCRS,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"karney"}`,
		},
		{
			name:             "geography_ogc_crs84_vincenty",
			ice:              geogVincenty,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"vincenty"}`,
		},
		{
			name:             "geography_ogc_crs84_andoyer",
			ice:              geogAndoyer,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"andoyer"}`,
		},
		{
			name:             "geography_ogc_crs84_thomas",
			ice:              geogThomas,
			geoarrowMetaJSON: `{"crs":"OGC:CRS84","edges":"thomas"}`,
		},
		// Geography with custom CRS and edges
		{
			name:             "geography_epsg_4267_karney",
			ice:              geogEPSG4267Karney,
			geoarrowMetaJSON: `{"crs":"EPSG:4267","edges":"karney"}`,
		},
		// Geography with PROJJSON CRS
		{
			name:             "geography_projjson_epsg_4267_spherical",
			ice:              geogEPSG4267,
			geoarrowMetaJSON: `{"crs":{"id":{"authority":"EPSG","code":4267}},"edges":"spherical"}`,
		},
	}

	for _, tt := range typeCases {
		t.Run("iceberg_to_arrow/"+tt.name, func(t *testing.T) {
			result, err := table.TypeToArrowType(tt.ice, true, false)
			require.NoError(t, err)
			assertGeoArrowWKBMetadataJSON(t, result, arrow.BinaryTypes.Binary, tt.geoarrowMetaJSON)
		})
	}

	for _, tt := range typeCases {
		t.Run("iceberg_to_arrow_round_trip/"+tt.name, func(t *testing.T) {
			arrowType, err := table.TypeToArrowType(tt.ice, true, false)
			require.NoError(t, err)

			iceType, err := table.ArrowTypeToIceberg(arrowType, false)
			require.NoError(t, err)
			assert.True(t, tt.ice.Equals(iceType), "expected %s, got %s", tt.ice, iceType)
		})
	}

	for _, tt := range append(typeCases, readOnlyCases...) {
		t.Run("arrow_to_iceberg/"+tt.name, func(t *testing.T) {
			arrowType, err := geoarrow.NewWKBType().Deserialize(arrow.BinaryTypes.Binary, tt.geoarrowMetaJSON)
			require.NoError(t, err)

			iceType, err := table.ArrowTypeToIceberg(arrowType, false)
			require.NoError(t, err)
			assert.True(t, tt.ice.Equals(iceType), "expected %s, got %s", tt.ice, iceType)
		})
	}

	t.Run("type/geometry_vs_geography_edge_differentiation", func(t *testing.T) {
		geomResult, err := table.TypeToArrowType(iceberg.GeometryType{}, true, false)
		require.NoError(t, err)
		geogResult, err := table.TypeToArrowType(iceberg.GeographyType{}, true, false)
		require.NoError(t, err)

		geomWKB, ok := geomResult.(*geoarrow.WKBType)
		require.True(t, ok)
		geogWKB, ok := geogResult.(*geoarrow.WKBType)
		require.True(t, ok)

		assert.Equal(t, geoarrow.EdgePlanar, geomWKB.Metadata().Edges)
		assert.Equal(t, geoarrow.EdgeSpherical, geogWKB.Metadata().Edges)
	})

	t.Run("large_binary_storage", func(t *testing.T) {
		result, err := table.TypeToArrowType(iceberg.GeometryType{}, true, true)
		require.NoError(t, err)
		assertGeoArrowWKB(t, result, arrow.BinaryTypes.LargeBinary, geoarrow.Metadata{CRS: jsonCRS("OGC:CRS84")})
	})

	t.Run("epsg_4326_behavior", func(t *testing.T) {
		geom, err := iceberg.GeometryTypeOf("EPSG:4326")
		require.NoError(t, err)

		geomResult, err := table.TypeToArrowType(geom, true, false)
		require.NoError(t, err)

		geomWKB, ok := geomResult.(*geoarrow.WKBType)
		assert.True(t, ok)

		meta := geomWKB.Metadata()
		assert.Equal(t, jsonCRS("OGC:CRS84"), meta.CRS)
		assert.Equal(t, geoarrow.EdgePlanar, meta.Edges)
		assert.Equal(t, geoarrow.CRSTypeAuthorityCode, meta.CRSType)

		roundTripGeom, err := table.ArrowTypeToIceberg(geomWKB, false)
		require.NoError(t, err)

		g, ok := roundTripGeom.(iceberg.GeometryType)
		require.True(t, ok)

		assert.Equal(t, "OGC:CRS84", g.CRS())
		assert.Equal(t, defaultGeometry, g)
	})

	t.Run("geoarrow_wkb_without_edges_reads_as_geometry", func(t *testing.T) {
		arrowType, err := geoarrow.NewWKBType().Deserialize(arrow.BinaryTypes.Binary, `{"crs":"OGC:CRS84"}`)
		require.NoError(t, err)

		iceType, err := table.ArrowTypeToIceberg(arrowType, false)
		require.NoError(t, err)

		geom, ok := iceType.(iceberg.GeometryType)
		require.True(t, ok, "expected geometry, got %T", iceType)
		assert.Equal(t, "OGC:CRS84", geom.CRS())
	})

	t.Run("projjson_error_behavior", func(t *testing.T) {
		geom, err := iceberg.GeometryTypeOf("projjson:my-custom-crs")
		require.NoError(t, err)

		_, err = table.TypeToArrowType(geom, true, false)
		require.Error(t, err)
		require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		require.ErrorContains(t, err, "projjson CRS not supported yet")

		// Geography goes through VisitGeography; assert the projjson rejection
		// surfaces symmetrically through the visitor recover.
		geog, err := iceberg.GeographyTypeOf("projjson:my-custom-crs", "spherical")
		require.NoError(t, err)

		_, err = table.TypeToArrowType(geog, true, false)
		require.Error(t, err)
		require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		require.ErrorContains(t, err, "projjson CRS not supported yet")

		stringArrowType, err := geoarrow.NewWKBType().Deserialize(arrow.BinaryTypes.Binary,
			`{"crs_type":"projjson","crs":"EPSG:3857"}`)
		require.NoError(t, err)

		_, err = table.ArrowTypeToIceberg(stringArrowType, false)
		require.Error(t, err)
		require.ErrorContains(t, err, "CRS type projjson not supported for string CRS")

		arrowType, err := geoarrow.NewWKBType().Deserialize(arrow.BinaryTypes.Binary,
			`{"crs_type":"projjson","crs":{"id":{"authority":"OGC", "code":"CRS84"}}}`)
		require.NoError(t, err)

		g, err := table.ArrowTypeToIceberg(arrowType, false)
		require.NoError(t, err)
		assert.Equal(t, g, defaultGeometry)
	})

	t.Run("wkt2:2019_error_behavior", func(t *testing.T) {
		arrowType, err := geoarrow.NewWKBType().Deserialize(arrow.BinaryTypes.Binary,
			`{"crs_type":"wkt2:2019","crs":"GEOGCRS[\"WGS 84\",DATUM[\"World Geodetic System 1984\",ELLIPSOID[\"WGS 84\",6378137,298.257223563]],CS[ellipsoidal,2],AXIS[\"geodetic latitude (Lat)\",north],AXIS[\"geodetic longitude (Lon)\",east],ID[\"EPSG\",4326]]"}`)
		require.NoError(t, err)

		_, err = table.ArrowTypeToIceberg(arrowType, false)
		require.Error(t, err)
		require.ErrorContains(t, err, "CRS type wkt2:2019 not supported")

		arrowTypeWithoutCRSType, err := geoarrow.NewWKBType().Deserialize(arrow.BinaryTypes.Binary,
			`{"crs":"GEOGCRS[\"WGS 84\",DATUM[\"World Geodetic System 1984\",ELLIPSOID[\"WGS 84\",6378137,298.257223563]],CS[ellipsoidal,2],AXIS[\"geodetic latitude (Lat)\",north],AXIS[\"geodetic longitude (Lon)\",east],ID[\"EPSG\",4326]]"}`)
		require.NoError(t, err)

		_, err = table.ArrowTypeToIceberg(arrowTypeWithoutCRSType, false)
		require.Error(t, err)
		require.ErrorContains(t, err, "CRS type wkt2:2019 not supported")
	})

	t.Run("schema", func(t *testing.T) {
		iceSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "point", Type: iceberg.GeometryType{}, Required: false},
			iceberg.NestedField{ID: 2, Name: "loc", Type: geomSRID, Required: true},
			iceberg.NestedField{ID: 3, Name: "area", Type: iceberg.GeographyType{}, Required: false},
			iceberg.NestedField{ID: 4, Name: "region", Type: geogKarney, Required: false},
			iceberg.NestedField{
				ID:   5,
				Name: "locations",
				Type: &iceberg.ListType{
					ElementID:       6,
					Element:         geomSRID,
					ElementRequired: true,
				},
				Required: true,
			},
		)

		arrowSc, err := table.SchemaToArrowSchema(iceSchema, nil, true, false)
		require.NoError(t, err)
		require.Equal(t, 5, arrowSc.NumFields())

		wantFields := []struct {
			name     string
			nullable bool
			fieldID  string
			wantMeta geoarrow.Metadata
		}{
			{"point", true, "1", geoarrow.Metadata{CRS: jsonCRS("OGC:CRS84")}},
			{"loc", false, "2", geoarrow.Metadata{CRS: jsonCRS("4326"), CRSType: geoarrow.CRSTypeSRID}},
			{"area", true, "3", geoarrow.Metadata{CRS: jsonCRS("OGC:CRS84"), Edges: geoarrow.EdgeSpherical}},
			{"region", true, "4", geoarrow.Metadata{CRS: jsonCRS("4269"), CRSType: geoarrow.CRSTypeSRID, Edges: geoarrow.EdgeKarney}},
		}

		for i, want := range wantFields {
			field := arrowSc.Field(i)
			assert.Equal(t, want.name, field.Name)
			assert.Equal(t, want.nullable, field.Nullable)
			fieldID, ok := field.Metadata.GetValue(table.ArrowParquetFieldIDKey)
			require.True(t, ok)
			assert.Equal(t, want.fieldID, fieldID)
			assertGeoArrowWKB(t, field.Type, arrow.BinaryTypes.Binary, want.wantMeta)
		}

		listField := arrowSc.Field(4)
		assert.Equal(t, "locations", listField.Name)
		assert.False(t, listField.Nullable)
		listFieldID, ok := listField.Metadata.GetValue(table.ArrowParquetFieldIDKey)
		require.True(t, ok)
		assert.Equal(t, "5", listFieldID)
		listType, ok := listField.Type.(*arrow.ListType)
		require.True(t, ok, "expected list type, got %T", listField.Type)

		elemField := listType.ElemField()
		assert.Equal(t, "element", elemField.Name)
		assert.False(t, elemField.Nullable)
		elemFieldID, ok := elemField.Metadata.GetValue(table.ArrowParquetFieldIDKey)
		require.True(t, ok)
		assert.Equal(t, "6", elemFieldID)
		assertGeoArrowWKB(t, elemField.Type, arrow.BinaryTypes.Binary, geoarrow.Metadata{
			CRS:     jsonCRS("4326"),
			CRSType: geoarrow.CRSTypeSRID,
		})
	})
}

func TestVariantArrayBuilderLargeValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer bldr.Release()

	mkVariant := func(v any) variant.Value {
		var b variant.Builder
		require.NoError(t, b.Append(v))
		val, err := b.Build()
		require.NoError(t, err)

		return val
	}

	const arrayLen = 300
	elems := make([]any, arrayLen)
	for i := range elems {
		elems[i] = int64(i)
	}
	bldr.Append(mkVariant(elems))

	const objFields = 40
	obj := make(map[string]any, objFields)
	for i := 0; i < objFields; i++ {
		obj["k"+strconv.Itoa(i)] = int64(i)
	}
	bldr.Append(mkVariant(obj))

	arr := bldr.NewArray()
	defer arr.Release()

	varr, ok := arr.(*extensions.VariantArray)
	require.True(t, ok)
	require.Equal(t, 2, varr.Len())

	v0, err := varr.Value(0)
	require.NoError(t, err)
	require.NotEmpty(t, v0.Bytes())

	v1, err := varr.Value(1)
	require.NoError(t, err)
	require.NotEmpty(t, v1.Bytes())
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
		{Name: "timestamptz_ns_z", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "+00:00"}, Nullable: true},
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

func TestToRequestedSchemaTimestampDowncastFloorsNegativeNanoseconds(t *testing.T) {
	tests := []struct {
		name         string
		sourceArrow  *arrow.TimestampType
		sourceType   iceberg.Type
		requested    iceberg.Type
		expectedType *arrow.TimestampType
	}{
		{
			name:         "timestamp",
			sourceArrow:  &arrow.TimestampType{Unit: arrow.Nanosecond},
			sourceType:   iceberg.PrimitiveTypes.TimestampNs,
			requested:    iceberg.PrimitiveTypes.Timestamp,
			expectedType: &arrow.TimestampType{Unit: arrow.Microsecond},
		},
		{
			name:         "timestamptz",
			sourceArrow:  &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			sourceType:   iceberg.PrimitiveTypes.TimestampTzNs,
			requested:    iceberg.PrimitiveTypes.TimestampTz,
			expectedType: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: tt.sourceArrow, Nullable: true}}, nil)
			bldr := array.NewRecordBuilder(mem, arrowSchema)
			defer bldr.Release()
			bldr.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(-1_500))

			rec := bldr.NewRecordBatch()
			defer rec.Release()

			requested := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "ts", Type: tt.requested, Required: false},
			)
			fileSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "ts", Type: tt.sourceType, Required: false},
			)

			converted, err := table.ToRequestedSchema(ctx, requested, fileSchema, rec, table.SchemaOptions{
				DowncastTimestamp: true,
			})
			require.NoError(t, err)
			defer converted.Release()

			col := converted.Column(0).(*array.Timestamp)
			require.True(t, arrow.TypeEqual(tt.expectedType, col.DataType()))
			assert.Equal(t, arrow.Timestamp(-2), col.Value(0))
		})
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

// TestToRequestedSchemaListLargeTypeCoercion guards against the writer
// rejecting compacted batches: the projected list variant must follow
// UseLargeTypes, not the offset width the reader happened to hand back.
func TestToRequestedSchemaListLargeTypeCoercion(t *testing.T) {
	elem := arrow.Field{
		Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
		Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"2"}),
	}

	build := func(t *testing.T, listType arrow.DataType) (arrow.RecordBatch, *iceberg.Schema) {
		t.Helper()
		schema := arrow.NewSchema([]arrow.Field{{
			Name: "nested", Type: listType,
			Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"1"}),
		}}, nil)
		bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer bldr.Release()
		require.NoError(t, bldr.UnmarshalJSON([]byte(`{"nested": [1, 2, 3]}`)))
		rec := bldr.NewRecordBatch()
		icesc, err := table.ArrowSchemaToIceberg(schema, false, nil)
		require.NoError(t, err)

		return rec, icesc
	}

	t.Run("large_list downcast to list", func(t *testing.T) {
		rec, icesc := build(t, arrow.LargeListOfField(elem))
		defer rec.Release()

		out, err := table.ToRequestedSchema(context.Background(), icesc, icesc, rec, table.SchemaOptions{IncludeFieldIDs: true})
		require.NoError(t, err)
		defer out.Release()
		assert.Equal(t, arrow.LIST, out.Column(0).DataType().ID())
	})

	t.Run("list upcast to large_list", func(t *testing.T) {
		rec, icesc := build(t, arrow.ListOfField(elem))
		defer rec.Release()

		out, err := table.ToRequestedSchema(context.Background(), icesc, icesc, rec, table.SchemaOptions{IncludeFieldIDs: true, UseLargeTypes: true})
		require.NoError(t, err)
		defer out.Release()
		assert.Equal(t, arrow.LARGE_LIST, out.Column(0).DataType().ID())
	})
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

func TestToRequestedSchemaMissingNestedFieldID(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	// Create an Arrow schema that lacks the nested list completely
	schemaWithoutMetadata := arrow.NewSchema([]arrow.Field{
		{
			Name: "other_field", Type: arrow.PrimitiveTypes.Int32,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schemaWithoutMetadata)
	defer bldr.Release()

	const data = `{"other_field": 1}
				  {"other_field": 2}`

	s := bufio.NewScanner(strings.NewReader(data))
	for s.Scan() {
		require.NoError(t, bldr.UnmarshalJSON(s.Bytes()))
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// The file schema lacks the nested_list
	fileIcesc := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 10, Name: "other_field", Type: iceberg.PrimitiveTypes.Int32, Required: false,
	})

	// The requested schema has the nested_list
	reqIcesc := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 10, Name: "other_field", Type: iceberg.PrimitiveTypes.Int32, Required: false,
	}, iceberg.NestedField{
		ID: 11, Name: "nested_list", Type: &iceberg.ListType{
			ElementID: 12, Element: iceberg.PrimitiveTypes.String, ElementRequired: false,
		}, Required: false,
	})

	rec2, err := table.ToRequestedSchema(context.Background(), reqIcesc, fileIcesc, rec, table.SchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	defer rec2.Release()

	targetSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "other_field", Type: arrow.PrimitiveTypes.Int32, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "10"}),
		},
		{
			Name: "nested_list", Type: arrow.ListOfField(arrow.Field{
				Name: "element", Type: arrow.BinaryTypes.String, Nullable: true,
				Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "12"}),
			}),
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "11"}),
			Nullable: true,
		},
	}, nil)
	require.True(t, targetSchema.Equal(rec2.Schema()), "Schema is not perfectly equal")
}
