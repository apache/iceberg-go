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

package internal_test

import (
	"bytes"
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	internal2 "github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func constructTestTablePrimitiveTypes(t *testing.T) (*metadata.FileMetaData, table.Metadata) {
	tableMeta, err := table.ParseMetadataString(`{
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
		"last-partition-id": 0,	
		"last-updated-ms": -1,
        "default-spec-id": 0,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "properties": {}
	}`)
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(tableMeta.Schemas()[0], nil, true, false)
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
		parquet.NewWriterProperties(parquet.WithStats(true)),
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

func getCollector() map[int]internal.StatisticsCollector {
	modeTrunc := internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 2}
	modeFull := internal.MetricsMode{Typ: internal.MetricModeFull}

	return map[int]internal.StatisticsCollector{
		1: {
			FieldID:    1,
			Mode:       modeFull,
			ColName:    "booleans",
			IcebergTyp: iceberg.PrimitiveTypes.Bool,
		},
		2: {
			FieldID:    2,
			Mode:       modeFull,
			ColName:    "ints",
			IcebergTyp: iceberg.PrimitiveTypes.Int32,
		},
		3: {
			FieldID:    3,
			Mode:       modeFull,
			ColName:    "longs",
			IcebergTyp: iceberg.PrimitiveTypes.Int64,
		},
		4: {
			FieldID:    4,
			Mode:       modeFull,
			ColName:    "floats",
			IcebergTyp: iceberg.PrimitiveTypes.Float32,
		},
		5: {
			FieldID:    5,
			Mode:       modeFull,
			ColName:    "doubles",
			IcebergTyp: iceberg.PrimitiveTypes.Float64,
		},
		6: {
			FieldID:    6,
			Mode:       modeFull,
			ColName:    "dates",
			IcebergTyp: iceberg.PrimitiveTypes.Date,
		},
		7: {
			FieldID:    7,
			Mode:       modeFull,
			ColName:    "times",
			IcebergTyp: iceberg.PrimitiveTypes.Time,
		},
		8: {
			FieldID:    8,
			Mode:       modeFull,
			ColName:    "timestamps",
			IcebergTyp: iceberg.PrimitiveTypes.Timestamp,
		},
		9: {
			FieldID:    9,
			Mode:       modeFull,
			ColName:    "timestamptzs",
			IcebergTyp: iceberg.PrimitiveTypes.TimestampTz,
		},
		10: {
			FieldID:    10,
			Mode:       modeTrunc,
			ColName:    "strings",
			IcebergTyp: iceberg.PrimitiveTypes.String,
		},
		11: {
			FieldID:    11,
			Mode:       modeFull,
			ColName:    "uuids",
			IcebergTyp: iceberg.PrimitiveTypes.UUID,
		},
		12: {
			FieldID:    12,
			Mode:       modeTrunc,
			ColName:    "binaries",
			IcebergTyp: iceberg.PrimitiveTypes.Binary,
		},
		13: {
			FieldID:    13,
			Mode:       modeFull,
			ColName:    "small_dec",
			IcebergTyp: iceberg.DecimalTypeOf(8, 2),
		},
		14: {
			FieldID:    14,
			Mode:       modeFull,
			ColName:    "med_dec",
			IcebergTyp: iceberg.DecimalTypeOf(16, 2),
		},
		15: {
			FieldID:    15,
			Mode:       modeFull,
			ColName:    "large_dec",
			IcebergTyp: iceberg.DecimalTypeOf(24, 2),
		},
	}
}

func TestMetricsPrimitiveTypes(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)

	meta, tblMeta := constructTestTablePrimitiveTypes(t)
	require.NotNil(t, tblMeta)
	require.NotNil(t, meta)

	mapping, err := format.PathToIDMapping(tblMeta.CurrentSchema())
	require.NoError(t, err)

	stats := format.DataFileStatsFromMeta(internal.Metadata(meta), getCollector(), mapping)
	df := stats.ToDataFile(tblMeta.CurrentSchema(), tblMeta.PartitionSpec(), "fake-path.parquet",
		iceberg.ParquetFile, meta.GetSourceFileSize(), nil)

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

func TestWriteDataFileErrOnClose(t *testing.T) {
	ctx := context.Background()
	fm := internal.GetFileFormat(iceberg.ParquetFile)
	mockfs := internal2.MockFS{}
	mockfs.Test(t)

	mockfs.On("Create", "f").Return(&internal2.MockFile{
		ErrOnClose: true,
	}, nil)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: "nested",
			Type: arrow.ListOfField(arrow.Field{
				Name: "element", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
				Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"2"}),
			}),
			Metadata: arrow.NewMetadata([]string{table.ArrowParquetFieldIDKey}, []string{"1"}),
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	bldr.Field(0).AppendNull()
	defer bldr.Release()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	icesc, err := table.ArrowSchemaToIceberg(schema, false, nil)
	require.NoError(t, err)

	_, err = fm.WriteDataFile(ctx, &mockfs, nil, internal.WriteFileInfo{
		FileSchema: icesc,
		Spec:       iceberg.PartitionSpec{},
		FileName:   "f",
		StatsCols:  nil,
		WriteProps: []parquet.WriterProperty{},
	}, []arrow.RecordBatch{rec})
	require.ErrorContains(t, err, "error on close")
}
