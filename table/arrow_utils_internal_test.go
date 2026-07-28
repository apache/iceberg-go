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

package table

import (
	"bytes"
	"cmp"
	"encoding/json"
	"math"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func constructTestTable(t *testing.T, writeStats []string) (*metadata.FileMetaData, Metadata) {
	tableMeta, err := ParseMetadataString(`{
		"format-version": 2,
        "location": "s3://bucket/test/location",
        "last-column-id": 7,
        "current-schema-id": 0,
		"last-updated-ms": -1,
		"last-partition-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "strings", "required": false, "type": "string"},
                    {"id": 2, "name": "floats", "required": false, "type": "float"},
                    {
                        "id": 3,
                        "name": "list",
                        "required": false,
                        "type": {"type": "list", "element-id": 6, "element": "long", "element-required": false}
                    },
                    {
                        "id": 4,
                        "name": "maps",
                        "required": false,
                        "type": {
                            "type": "map",
                            "key-id": 7,
                            "key": "long",
                            "value-id": 8,
                            "value": "long",
                            "value-required": false
                        }
                    },
                    {
                        "id": 5,
                        "name": "structs",
                        "required": false,
                        "type": {
                            "type": "struct",
                            "fields": [
                                {"id": 9, "name": "x", "required": false, "type": "long"},
                                {"id": 10, "name": "y", "required": false, "type": "float", "doc": "comment"}
                            ]
                        }
                    }
                ]
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "properties": {},
		"sort-orders": [{"order-id": 0, "fields": []}],
		"default-sort-order-id": 0
	}`)
	require.NoError(t, err)

	arrowSchema, err := SchemaToArrowSchema(tableMeta.Schemas()[0], nil, true, false)
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()

	bldr.Field(0).(*array.StringBuilder).AppendValues([]string{
		"zzzzzzzzzzzzzzzzzzzz", "rrrrrrrrrrrrrrrrrrrr", "", "aaaaaaaaaaaaaaaaaaaa",
	},
		[]bool{true, true, false, true})
	bldr.Field(1).(*array.Float32Builder).AppendValues(
		[]float32{3.14, float32(math.NaN()), 1.69, 100}, nil)
	lb := bldr.Field(2).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.Int64Builder)
	lb.Append(true)
	vb.AppendValues([]int64{1, 2, 3}, nil)
	lb.Append(true)
	vb.AppendValues([]int64{4, 5, 6}, nil)
	lb.AppendNull()
	lb.Append(true)
	vb.AppendValues([]int64{7, 8, 9}, nil)

	mb := bldr.Field(3).(*array.MapBuilder)
	kb := mb.KeyBuilder().(*array.Int64Builder)
	vb = mb.ItemBuilder().(*array.Int64Builder)
	mb.Append(true)
	kb.AppendValues([]int64{1, 2}, nil)
	vb.AppendValues([]int64{3, 4}, nil)
	mb.AppendNull()
	mb.Append(true)
	kb.Append(5)
	vb.Append(6)
	mb.Append(true)

	sb := bldr.Field(4).(*array.StructBuilder)
	xb := sb.FieldBuilder(0).(*array.Int64Builder)
	yb := sb.FieldBuilder(1).(*array.Float32Builder)
	sb.Append(true)
	xb.Append(1)
	yb.Append(0.2)
	sb.Append(true)
	xb.AppendNull()
	yb.Append(-1.34)
	sb.AppendNull()
	sb.Append(true)
	xb.Append(54)
	yb.AppendNull()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	var opts []parquet.WriterProperty
	if len(writeStats) == 0 {
		opts = append(opts, parquet.WithStats(true))
	} else {
		opts = append(opts, parquet.WithStats(false))
		for _, stat := range writeStats {
			opts = append(opts, parquet.WithStatsFor(stat, true))
		}
	}

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSchema, &buf,
		parquet.NewWriterProperties(opts...),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	return rdr.MetaData(), tableMeta
}

type FileStatsMetricsSuite struct {
	suite.Suite
}

func (suite *FileStatsMetricsSuite) getDataFile(meta iceberg.Properties, writeStats []string) iceberg.DataFile {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	fileMeta, tableMeta := constructTestTable(suite.T(), writeStats)
	require.NotNil(suite.T(), tableMeta)
	require.NotNil(suite.T(), fileMeta)

	schema := tableMeta.CurrentSchema()
	if len(meta) > 0 {
		bldr, err := MetadataBuilderFromBase(tableMeta, "")
		suite.Require().NoError(err)
		err = bldr.SetProperties(meta)
		suite.Require().NoError(err)
		tableMeta, err = bldr.Build()
		suite.Require().NoError(err)
	}

	collector, err := computeStatsPlan(schema, tableMeta.Properties())
	suite.Require().NoError(err)
	mapping, err := format.PathToIDMapping(schema)
	suite.Require().NoError(err)

	stats := format.DataFileStatsFromMeta(fileMeta, collector, mapping, nil, nil)

	return stats.ToDataFile(internal.DataFileOpts{
		Schema:      tableMeta.CurrentSchema(),
		Spec:        tableMeta.PartitionSpec(),
		Path:        "fake-path.parquet",
		Format:      iceberg.ParquetFile,
		Content:     iceberg.EntryContentData,
		FileSize:    fileMeta.GetSourceFileSize(),
		SortOrderID: tableMeta.SortOrder().OrderID(),
	})
}

func (suite *FileStatsMetricsSuite) TestRecordCount() {
	df := suite.getDataFile(nil, nil)
	suite.EqualValues(int64(4), df.Count())
}

func (suite *FileStatsMetricsSuite) TestValueCounts() {
	df := suite.getDataFile(nil, nil)
	suite.Len(df.ValueCounts(), 7)
	suite.EqualValues(4, df.ValueCounts()[1])
	suite.EqualValues(4, df.ValueCounts()[2])
	suite.EqualValues(10, df.ValueCounts()[6])
	suite.EqualValues(5, df.ValueCounts()[7])
	suite.EqualValues(5, df.ValueCounts()[8])
	suite.EqualValues(4, df.ValueCounts()[9])
	suite.EqualValues(4, df.ValueCounts()[10])
}

func (suite *FileStatsMetricsSuite) TestColumnSizes() {
	df := suite.getDataFile(nil, nil)
	suite.Len(df.ColumnSizes(), 7)
	suite.Greater(df.ColumnSizes()[1], int64(0))
	suite.Greater(df.ColumnSizes()[2], int64(0))
	suite.Greater(df.ColumnSizes()[6], int64(0))
	suite.Greater(df.ColumnSizes()[7], int64(0))
	suite.Greater(df.ColumnSizes()[8], int64(0))
}

func (suite *FileStatsMetricsSuite) TestOffsets() {
	df := suite.getDataFile(nil, nil)
	suite.Len(df.SplitOffsets(), 1)
	suite.EqualValues(4, df.SplitOffsets()[0])
}

func (suite *FileStatsMetricsSuite) TestNullValueCounts() {
	df := suite.getDataFile(nil, nil)
	suite.Len(df.NullValueCounts(), 7)
	suite.EqualValues(1, df.NullValueCounts()[1])
	suite.EqualValues(0, df.NullValueCounts()[2])
	suite.EqualValues(1, df.NullValueCounts()[6])
	suite.EqualValues(2, df.NullValueCounts()[7])
	suite.EqualValues(2, df.NullValueCounts()[8])
	suite.EqualValues(2, df.NullValueCounts()[9])
	suite.EqualValues(2, df.NullValueCounts()[10])

	// pqarrow doesn't currently write the NaN counts
}

func (suite *FileStatsMetricsSuite) TestBounds() {
	df := suite.getDataFile(nil, nil)
	suite.Len(df.LowerBoundValues(), 2)
	suite.Equal([]byte("aaaaaaaaaaaaaaaa"), df.LowerBoundValues()[1])
	lb, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.LowerBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(1.69), lb.(iceberg.Float32Literal).Value())

	suite.Len(df.UpperBoundValues(), 2)
	suite.Equal([]byte("zzzzzzzzzzzzzzz{"), df.UpperBoundValues()[1])
	ub, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.UpperBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(100), ub.(iceberg.Float32Literal).Value())
}

func (suite *FileStatsMetricsSuite) TestMetricsModeNone() {
	df := suite.getDataFile(iceberg.Properties{"write.metadata.metrics.default": "none"}, nil)
	suite.Len(df.ValueCounts(), 0)
	suite.Len(df.ColumnSizes(), 0)
	suite.Len(df.NullValueCounts(), 0)
	suite.Len(df.NaNValueCounts(), 0)
	suite.Len(df.LowerBoundValues(), 0)
	suite.Len(df.UpperBoundValues(), 0)
}

func (suite *FileStatsMetricsSuite) TestMetricsModeCounts() {
	df := suite.getDataFile(iceberg.Properties{"write.metadata.metrics.default": "counts"}, nil)
	suite.Len(df.ValueCounts(), 7)
	suite.Len(df.NullValueCounts(), 7)
	suite.Len(df.NaNValueCounts(), 0)
	suite.Len(df.LowerBoundValues(), 0)
	suite.Len(df.UpperBoundValues(), 0)
}

func (suite *FileStatsMetricsSuite) TestMetricsModeFull() {
	df := suite.getDataFile(iceberg.Properties{"write.metadata.metrics.default": "full"}, nil)
	suite.Len(df.ValueCounts(), 7)
	suite.Len(df.NullValueCounts(), 7)
	suite.Len(df.NaNValueCounts(), 0)
	suite.Len(df.LowerBoundValues(), 2)
	suite.Equal([]byte("aaaaaaaaaaaaaaaaaaaa"), df.LowerBoundValues()[1])
	lb, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.LowerBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(1.69), lb.(iceberg.Float32Literal).Value())

	suite.Len(df.UpperBoundValues(), 2)
	suite.Equal([]byte("zzzzzzzzzzzzzzzzzzzz"), df.UpperBoundValues()[1])
	ub, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.UpperBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(100), ub.(iceberg.Float32Literal).Value())
}

func (suite *FileStatsMetricsSuite) TestMetricsModeNonDefaultTrunc() {
	df := suite.getDataFile(iceberg.Properties{"write.metadata.metrics.default": "truncate(2)"}, nil)
	suite.Len(df.ValueCounts(), 7)
	suite.Len(df.NullValueCounts(), 7)
	suite.Len(df.NaNValueCounts(), 0)
	suite.Len(df.LowerBoundValues(), 2)
	suite.Equal([]byte("aa"), df.LowerBoundValues()[1])
	lb, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.LowerBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(1.69), lb.(iceberg.Float32Literal).Value())

	suite.Len(df.UpperBoundValues(), 2)
	suite.Equal([]byte("z{"), df.UpperBoundValues()[1])
	ub, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.UpperBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(100), ub.(iceberg.Float32Literal).Value())
}

func (suite *FileStatsMetricsSuite) TestColumnMetricsMode() {
	df := suite.getDataFile(iceberg.Properties{
		"write.metadata.metrics.default":        "truncate(2)",
		"write.metadata.metrics.column.strings": "none",
	}, nil)

	suite.Len(df.ValueCounts(), 6)
	suite.Len(df.NullValueCounts(), 6)
	suite.Len(df.NaNValueCounts(), 0)

	suite.Len(df.LowerBoundValues(), 1)
	lb, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.LowerBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(1.69), lb.(iceberg.Float32Literal).Value())

	suite.Len(df.UpperBoundValues(), 1)
	ub, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float32, df.UpperBoundValues()[2])
	suite.Require().NoError(err)
	suite.Equal(float32(100), ub.(iceberg.Float32Literal).Value())
}

func (suite *FileStatsMetricsSuite) TestReadMissingStats() {
	df := suite.getDataFile(nil, []string{"strings"})

	suite.Len(df.NullValueCounts(), 1)
	suite.Len(df.UpperBoundValues(), 1)
	suite.Len(df.LowerBoundValues(), 1)

	stringsColIdx := 1
	suite.Equal("aaaaaaaaaaaaaaaa", string(df.LowerBoundValues()[stringsColIdx]))
	suite.Equal("zzzzzzzzzzzzzzz{", string(df.UpperBoundValues()[stringsColIdx]))
	suite.EqualValues(1, df.NullValueCounts()[stringsColIdx])
}

func TestFileMetrics(t *testing.T) {
	suite.Run(t, new(FileStatsMetricsSuite))
}

var tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
	[]int{1},
	iceberg.NestedField{
		ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false,
	},
	iceberg.NestedField{
		ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	},
	iceberg.NestedField{
		ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false,
	},
	iceberg.NestedField{
		ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
			ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true,
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
				ValueRequired: true,
			},
			ValueRequired: true,
		},
		Required: true,
	},
	iceberg.NestedField{
		ID: 11, Name: "location", Type: &iceberg.ListType{
			ElementID: 12, Element: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
				},
			},
			ElementRequired: true,
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

func TestStatsTypes(t *testing.T) {
	statsCols, err := iceberg.PreOrderVisit(tableSchemaNested,
		&arrowStatsCollector{schema: tableSchemaNested, defaultMode: "full"})

	require.NoError(t, err)

	// field ids should be sorted
	assert.True(t, slices.IsSortedFunc(statsCols, func(a, b internal.StatisticsCollector) int {
		return cmp.Compare(a.FieldID, b.FieldID)
	}))

	actual := make([]iceberg.Type, len(statsCols))
	for i, col := range statsCols {
		actual[i] = col.IcebergTyp
	}

	assert.Equal(t, []iceberg.Type{
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.Int32,
	}, actual)
}

func TestIcebergCRSToGeoArrowMetadata(t *testing.T) {
	// Calling the converter directly (outside the schema visitor) must not panic:
	// an unsupported CRS comes back as an error, symmetric with the read path.
	t.Run("projjson without properties returns an error instead of panicking", func(t *testing.T) {
		_, err := icebergCRSToGeoArrowMetadata("projjson:my-custom-crs", nil)
		require.Error(t, err)
		require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		require.ErrorContains(t, err, "could not be resolved from table properties")
	})

	t.Run("projjson resolves from table properties", func(t *testing.T) {
		const projjson = `{"type":"GeographicCRS","name":"WGS 84"}`
		meta, err := icebergCRSToGeoArrowMetadata("projjson:my-custom-crs", iceberg.Properties{
			"my-custom-crs": projjson,
		})
		require.NoError(t, err)
		assert.Equal(t, geoarrow.CRSTypePROJJSON, meta.CRSType)
		assert.JSONEq(t, projjson, string(meta.CRS))
	})

	t.Run("supported CRS values map to the expected CRSType", func(t *testing.T) {
		cases := []struct {
			crs         string
			wantCRSType geoarrow.CRSType
		}{
			{"srid:4326", geoarrow.CRSTypeSRID},
			{"OGC:CRS84", geoarrow.CRSTypeAuthorityCode},
			{"EPSG:4326", geoarrow.CRSTypeAuthorityCode},
			{"EPSG:4267", geoarrow.CRSTypeAuthorityCode},
		}
		for _, tc := range cases {
			t.Run(tc.crs, func(t *testing.T) {
				meta, err := icebergCRSToGeoArrowMetadata(tc.crs, nil)
				require.NoError(t, err)
				assert.Equal(t, tc.wantCRSType, meta.CRSType)
			})
		}
	})

	t.Run("srid:0 maps to an omitted CRS", func(t *testing.T) {
		meta, err := icebergCRSToGeoArrowMetadata("srid:0", nil)
		require.NoError(t, err)
		assert.Empty(t, meta.CRS)
		assert.Empty(t, meta.CRSType)
	})
}

func TestGeoArrowCRSToIcebergCRS(t *testing.T) {
	stringCRS := func(s string) json.RawMessage {
		raw, _ := json.Marshal(s) //nolint:errcheck // Marshalling a string can't fail

		return raw
	}

	t.Run("accepts long authority code string", func(t *testing.T) {
		const crs = "EPSGAuthorityLongName:CODE-12345678901234567890"

		got, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{CRS: stringCRS(crs)})
		require.NoError(t, err)
		assert.Equal(t, crs, got)
	})

	t.Run("accepts arbitrary string CRS", func(t *testing.T) {
		got, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{CRS: stringCRS("custom")})
		require.NoError(t, err)
		assert.Equal(t, "custom", got)
	})

	t.Run("canonical string CRS wins over type annotation", func(t *testing.T) {
		for _, crsType := range []geoarrow.CRSType{
			geoarrow.CRSTypeSRID,
			geoarrow.CRSTypePROJJSON,
			geoarrow.CRSTypeWKT22019,
		} {
			for _, crs := range []string{"EPSG:4326", "OGC:CRS84"} {
				t.Run(string(crsType)+"/"+crs, func(t *testing.T) {
					got, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{
						CRS:     stringCRS(crs),
						CRSType: crsType,
					})
					require.NoError(t, err)
					assert.Equal(t, "OGC:CRS84", got)
				})
			}
		}
	})

	t.Run("numeric srid type annotation maps to srid", func(t *testing.T) {
		got, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{
			CRS:     stringCRS("3857"),
			CRSType: geoarrow.CRSTypeSRID,
		})
		require.NoError(t, err)
		assert.Equal(t, "srid:3857", got)
	})

	t.Run("rejects empty string CRS", func(t *testing.T) {
		_, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{CRS: stringCRS("")})
		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported CRS: empty string CRS")
	})

	t.Run("rejects projjson string CRS", func(t *testing.T) {
		_, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{
			CRS:     stringCRS("EPSG:3857"),
			CRSType: geoarrow.CRSTypePROJJSON,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, errPROJJSONStringCRSNotSupported)
	})

	t.Run("bare WKT2 string uses not supported error", func(t *testing.T) {
		for _, crs := range []string{
			`GEOGCRS["WGS 84",DATUM["World Geodetic System 1984"]]`,
			`COORDINATEMETADATA[SOURCECRS[GEOGCRS["WGS 84"]]]`,
			`COORDINATEOPERATION["operation"]`,
			`CRS["generic"]`,
		} {
			t.Run(crs, func(t *testing.T) {
				_, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{
					CRS: stringCRS(crs),
				})
				require.Error(t, err)
				assert.ErrorIs(t, err, errWKT2CRSNotSupported)
			})
		}
	})

	t.Run("json object errors identify the failing member", func(t *testing.T) {
		cases := []struct {
			name    string
			rawCRS  string
			wantErr string
		}{
			{
				name:    "missing id",
				rawCRS:  `{"type":"GeographicCRS"}`,
				wantErr: "unsupported CRS: missing id",
			},
			{
				name:    "invalid id",
				rawCRS:  `{"id":42}`,
				wantErr: "unsupported CRS: invalid id",
			},
			{
				name:    "missing authority",
				rawCRS:  `{"id":{"code":4326}}`,
				wantErr: "unsupported CRS: missing id.authority",
			},
			{
				name:    "invalid authority",
				rawCRS:  `{"id":{"authority":7,"code":4326}}`,
				wantErr: "unsupported CRS: invalid id.authority",
			},
			{
				name:    "empty authority",
				rawCRS:  `{"id":{"authority":"","code":4326}}`,
				wantErr: "unsupported CRS: empty id.authority",
			},
			{
				name:    "missing code",
				rawCRS:  `{"id":{"authority":"EPSG"}}`,
				wantErr: "unsupported CRS: missing id.code",
			},
			{
				name:    "invalid code",
				rawCRS:  `{"id":{"authority":"EPSG","code":{}}}`,
				wantErr: "unsupported CRS: invalid id.code",
			},
			{
				name:    "empty code",
				rawCRS:  `{"id":{"authority":"EPSG","code":""}}`,
				wantErr: "unsupported CRS: empty id.code",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := geoArrowCRSToIcebergCRS(geoarrow.Metadata{CRS: json.RawMessage(tc.rawCRS)})
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErr)
			})
		}
	})
}
