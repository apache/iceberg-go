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
	"math"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
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
        "properties": {}
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

	rec := bldr.NewRecord()
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
	fileMeta, tableMeta := constructTestTable(suite.T(), writeStats)
	require.NotNil(suite.T(), tableMeta)
	require.NotNil(suite.T(), fileMeta)

	schema := tableMeta.CurrentSchema()
	if len(meta) > 0 {
		bldr, err := MetadataBuilderFromBase(tableMeta)
		suite.Require().NoError(err)
		_, err = bldr.SetProperties(meta)
		suite.Require().NoError(err)
		tableMeta, err = bldr.Build()
		suite.Require().NoError(err)
	}

	collector, err := computeStatsPlan(schema, tableMeta.Properties())
	suite.Require().NoError(err)
	mapping, err := parquetPathToIDMapping(schema)
	suite.Require().NoError(err)

	stats := dataFileStatsFromParquetMetadata(fileMeta, collector, mapping)

	return stats.toDataFile(tableMeta.CurrentSchema(), tableMeta.PartitionSpec(), "fake-path.parquet",
		iceberg.ParquetFile, fileMeta.GetSourceFileSize())
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

func TestMetricsModePairs(t *testing.T) {
	tests := []struct {
		str      string
		expected metricsMode
	}{
		{"none", metricsMode{typ: metricModeNone}},
		{"nOnE", metricsMode{typ: metricModeNone}},
		{"counts", metricsMode{typ: metricModeCounts}},
		{"Counts", metricsMode{typ: metricModeCounts}},
		{"full", metricsMode{typ: metricModeFull}},
		{"FuLl", metricsMode{typ: metricModeFull}},
		{" FuLl", metricsMode{typ: metricModeFull}},
		{"truncate(16)", metricsMode{typ: metricModeTruncate, len: 16}},
		{"trUncatE(16)", metricsMode{typ: metricModeTruncate, len: 16}},
		{"trUncatE(7)", metricsMode{typ: metricModeTruncate, len: 7}},
		{"trUncatE(07)", metricsMode{typ: metricModeTruncate, len: 7}},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			mode, err := matchMetricsMode(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode)
		})
	}

	_, err := matchMetricsMode("truncatE(-7)")
	assert.ErrorContains(t, err, "malformed truncate metrics mode: truncatE(-7)")

	_, err = matchMetricsMode("truncatE(0)")
	assert.ErrorContains(t, err, "invalid truncate length: 0")
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
	assert.True(t, slices.IsSortedFunc(statsCols, func(a, b statisticsCollector) int {
		return cmp.Compare(a.fieldID, b.fieldID)
	}))

	actual := make([]iceberg.Type, len(statsCols))
	for i, col := range statsCols {
		actual[i] = col.icebergTyp
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

func constructTestTablePrimitiveTypes(t *testing.T) (*metadata.FileMetaData, Metadata) {
	tableMeta, err := ParseMetadataString(`{
        "format-version": 2,
        "location": "s3://bucket/test/location",
        "last-column-id": 7,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "booleans", "required": false, "type": "boolean"},
                    {"id": 2, "name": "ints", "required": false, "type": "int"},
                    {"id": 3, "name": "longs", "required": false, "type": "long"},
                    {"id": 4, "name": "floats", "required": false, "type": "float"},
                    {"id": 5, "name": "doubles", "required": false, "type": "double"},
                    {"id": 6, "name": "dates", "required": false, "type": "date"},
                    {"id": 7, "name": "times", "required": false, "type": "time"},
                    {"id": 8, "name": "timestamps", "required": false, "type": "timestamp"},
                    {"id": 9, "name": "timestamptzs", "required": false, "type": "timestamptz"},
                    {"id": 10, "name": "strings", "required": false, "type": "string"},
                    {"id": 11, "name": "uuids", "required": false, "type": "uuid"},
                    {"id": 12, "name": "binaries", "required": false, "type": "binary"},
					{"id": 13, "name": "small_dec", "required": false, "type": "decimal(8, 2)"},
					{"id": 14, "name": "med_dec", "required": false, "type": "decimal(16, 2)"},
					{"id": 15, "name": "large_dec", "required": false, "type": "decimal(24, 2)"}
                ]
            }
        ],
		"last-updated-ms": -1,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "properties": {}
	}`)
	require.NoError(t, err)

	arrowSchema, err := SchemaToArrowSchema(tableMeta.Schemas()[0], nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{
			"booleans": true, 
			"ints": 23,
			"longs": 54,
			"floats": 454.1223,
			"doubles": 8542.12,
			"dates": "2022-01-02",
			"times": "17:30:34",
			"timestamps": "2022-01-02T17:30:34.399",
			"timestamptzs": "2022-01-02T17:30:34.399",
			"strings": "hello",
			"uuids": "`+uuid.NewMD5(uuid.NameSpaceDNS, []byte("foo")).String()+`",
			"binaries": "aGVsbG8=",
			"small_dec": "123456.78",
			"med_dec": "12345678901234.56",
			"large_dec": "1234567890123456789012.34"
		},
		{
			"booleans": false,
			"ints": 89,
			"longs": 2,
			"floats": 24342.29,
			"doubles": -43.9,
			"dates": "2023-02-04",
			"times": "13:21:04",
			"timestamps": "2023-02-04T13:21:04.354",
			"timestamptzs": "2023-02-04T13:21:04.354",
			"strings": "world",
			"uuids": "`+uuid.NewMD5(uuid.NameSpaceDNS, []byte("bar")).String()+`",
			"binaries": "d29ybGQ=",
			"small_dec": "876543.21",
			"med_dec": "65432109876543.21",
			"large_dec": "4321098765432109876543.21"
		}
	]`))
	require.NoError(t, err)
	defer rec.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSchema, &buf,
		parquet.NewWriterProperties(parquet.WithStats(true),
			parquet.WithStoreDecimalAsInteger(true)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	return rdr.MetaData(), tableMeta
}

func assertBounds[T iceberg.LiteralType](t *testing.T, bound []byte, typ iceberg.Type, expected T) {
	lit, err := iceberg.LiteralFromBytes(typ, bound)
	require.NoError(t, err)
	assert.Equal(t, expected, lit.(iceberg.TypedLiteral[T]).Value())
}

func toDate(tm time.Time) iceberg.Date {
	return iceberg.Date(tm.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
}

func TestMetricsPrimitiveTypes(t *testing.T) {
	meta, tblMeta := constructTestTablePrimitiveTypes(t)
	require.NotNil(t, tblMeta)
	require.NotNil(t, meta)

	tblMeta.Properties()["write.metadata.metrics.default"] = "truncate(2)"

	collector, err := computeStatsPlan(tblMeta.CurrentSchema(), tblMeta.Properties())
	require.NoError(t, err)
	mapping, err := parquetPathToIDMapping(tblMeta.CurrentSchema())
	require.NoError(t, err)

	stats := dataFileStatsFromParquetMetadata(meta, collector, mapping)
	df := stats.toDataFile(tblMeta.CurrentSchema(), tblMeta.PartitionSpec(), "fake-path.parquet",
		iceberg.ParquetFile, meta.GetSourceFileSize())

	assert.Len(t, df.ValueCounts(), 15)
	assert.Len(t, df.NullValueCounts(), 15)
	assert.Len(t, df.NaNValueCounts(), 0)

	assert.Len(t, df.LowerBoundValues(), 15)
	assertBounds(t, df.LowerBoundValues()[1], iceberg.PrimitiveTypes.Bool, false)
	assertBounds(t, df.LowerBoundValues()[2], iceberg.PrimitiveTypes.Int32, int32(23))
	assertBounds(t, df.LowerBoundValues()[3], iceberg.PrimitiveTypes.Int64, int64(2))
	assertBounds(t, df.LowerBoundValues()[4], iceberg.PrimitiveTypes.Float32, float32(454.1223))
	assertBounds(t, df.LowerBoundValues()[5], iceberg.PrimitiveTypes.Float64, -43.9)
	assertBounds(t, df.LowerBoundValues()[6], iceberg.PrimitiveTypes.Date,
		toDate(time.Date(2022, time.January, 2, 0, 0, 0, 0, time.UTC)))
	assertBounds(t, df.LowerBoundValues()[7], iceberg.PrimitiveTypes.Time,
		iceberg.Time(time.Duration(13*time.Hour+21*time.Minute+4*time.Second).Microseconds()))
	assertBounds(t, df.LowerBoundValues()[8], iceberg.PrimitiveTypes.Timestamp,
		iceberg.Timestamp(time.Date(2022, time.January, 2, 17, 30, 34, 399000000, time.UTC).UnixMicro()))
	assertBounds(t, df.LowerBoundValues()[9], iceberg.PrimitiveTypes.TimestampTz,
		iceberg.Timestamp(time.Date(2022, time.January, 2, 17, 30, 34, 399000000, time.UTC).UnixMicro()))
	assertBounds(t, df.LowerBoundValues()[10], iceberg.PrimitiveTypes.String, "he")
	assertBounds(t, df.LowerBoundValues()[11], iceberg.PrimitiveTypes.UUID,
		uuid.NewMD5(uuid.NameSpaceDNS, []byte("foo")))
	assertBounds(t, df.LowerBoundValues()[12], iceberg.PrimitiveTypes.Binary, []byte("he"))
	assertBounds(t, df.LowerBoundValues()[13], iceberg.DecimalTypeOf(8, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(12345678),
		Scale: 2,
	})
	assertBounds(t, df.LowerBoundValues()[14], iceberg.DecimalTypeOf(16, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(1234567890123456),
		Scale: 2,
	})
	expected, _ := (&big.Int{}).SetString("123456789012345678901234", 10)
	assertBounds(t, df.LowerBoundValues()[15], iceberg.DecimalTypeOf(24, 2), iceberg.Decimal{
		Val:   decimal128.FromBigInt(expected),
		Scale: 2,
	})

	assert.Len(t, df.UpperBoundValues(), 15)
	assertBounds(t, df.UpperBoundValues()[1], iceberg.PrimitiveTypes.Bool, true)
	assertBounds(t, df.UpperBoundValues()[2], iceberg.PrimitiveTypes.Int32, int32(89))
	assertBounds(t, df.UpperBoundValues()[3], iceberg.PrimitiveTypes.Int64, int64(54))
	assertBounds(t, df.UpperBoundValues()[4], iceberg.PrimitiveTypes.Float32, float32(24342.29))
	assertBounds(t, df.UpperBoundValues()[5], iceberg.PrimitiveTypes.Float64, 8542.12)
	assertBounds(t, df.UpperBoundValues()[6], iceberg.PrimitiveTypes.Date,
		toDate(time.Date(2023, time.February, 4, 0, 0, 0, 0, time.UTC)))
	assertBounds(t, df.UpperBoundValues()[7], iceberg.PrimitiveTypes.Time,
		iceberg.Time(time.Duration(17*time.Hour+30*time.Minute+34*time.Second).Microseconds()))
	assertBounds(t, df.UpperBoundValues()[8], iceberg.PrimitiveTypes.Timestamp,
		iceberg.Timestamp(time.Date(2023, time.February, 4, 13, 21, 4, 354000000, time.UTC).UnixMicro()))
	assertBounds(t, df.UpperBoundValues()[9], iceberg.PrimitiveTypes.TimestampTz,
		iceberg.Timestamp(time.Date(2023, time.February, 4, 13, 21, 4, 354000000, time.UTC).UnixMicro()))
	assertBounds(t, df.UpperBoundValues()[10], iceberg.PrimitiveTypes.String, "wp")
	assertBounds(t, df.UpperBoundValues()[11], iceberg.PrimitiveTypes.UUID,
		uuid.NewMD5(uuid.NameSpaceDNS, []byte("bar")))
	assertBounds(t, df.UpperBoundValues()[12], iceberg.PrimitiveTypes.Binary, []byte("wp"))
	assertBounds(t, df.UpperBoundValues()[13], iceberg.DecimalTypeOf(8, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(87654321),
		Scale: 2,
	})
	assertBounds(t, df.UpperBoundValues()[14], iceberg.DecimalTypeOf(16, 2), iceberg.Decimal{
		Val:   decimal128.FromI64(6543210987654321),
		Scale: 2,
	})
	expectedUpper, _ := (&big.Int{}).SetString("432109876543210987654321", 10)
	assertBounds(t, df.UpperBoundValues()[15], iceberg.DecimalTypeOf(24, 2), iceberg.Decimal{
		Val:   decimal128.FromBigInt(expectedUpper),
		Scale: 2,
	})
}
