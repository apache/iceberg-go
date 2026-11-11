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

package internal

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func compressionField(id int, name string, typ arrow.DataType) arrow.Field {
	return arrow.Field{
		Name: name, Type: typ,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(id)}),
	}
}

func TestParquetColumnCompressionOptions(t *testing.T) {
	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "items", Type: &iceberg.ListType{
			ElementID: 4, Element: iceberg.PrimitiveTypes.Int32,
		}},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		compressionField(1, "id", arrow.PrimitiveTypes.Int32),
		compressionField(2, "payload", arrow.BinaryTypes.String),
		compressionField(3, "items", arrow.ListOfField(compressionField(4, "element", arrow.PrimitiveTypes.Int32))),
	}, nil)
	format := parquetFormat{}
	props := iceberg.Properties{
		ParquetCompressionKey:                                     "zstd",
		ParquetCompressionLevelKey:                                "9",
		ParquetCompressionColumnCodecKeyPrefix + ".id":            "SNAPPY",
		ParquetCompressionColumnCodecKeyPrefix + ".items.element": "gzip",
		ParquetCompressionColumnLevelKeyPrefix + ".items.element": "6",
		ParquetCompressionColumnLevelKeyPrefix + ".payload":       "3",
	}

	config, ok := format.GetWriteProperties(props).(parquetColumnCompressionConfig)
	require.True(t, ok)
	opts, err := config.forSchema(fileSchema, arrowSchema, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	wp := parquet.NewWriterProperties(opts...)

	require.Equal(t, compress.Codecs.Zstd, wp.Compression())
	require.Equal(t, 9, wp.CompressionLevel())
	require.Equal(t, compress.Codecs.Snappy, wp.CompressionFor("id"))
	require.Equal(t, compress.DefaultCompressionLevel, wp.CompressionLevelFor("id"))
	require.Equal(t, compress.Codecs.Zstd, wp.CompressionFor("payload"))
	require.Equal(t, 3, wp.CompressionLevelFor("payload"))
	require.Equal(t, compress.Codecs.Gzip, wp.CompressionFor("items.list.element"))
	require.Equal(t, 6, wp.CompressionLevelFor("items.list.element"))
}

func TestParquetColumnCompressionMapPaths(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "attrs", Type: iceberg.NewMapType(2, iceberg.PrimitiveTypes.String, 3,
			iceberg.NewStructType(iceberg.NestedField{ID: 4, Name: "amount", Type: iceberg.PrimitiveTypes.Int64}), false),
	})
	arrowSchema := arrow.NewSchema([]arrow.Field{compressionField(1, "attrs", arrow.MapOfFields(
		compressionField(2, "key", arrow.BinaryTypes.String),
		compressionField(3, "value", arrow.StructOf(compressionField(4, "amount", arrow.PrimitiveTypes.Int64))),
	))}, nil)
	props := iceberg.Properties{
		ParquetCompressionColumnCodecKeyPrefix + ".attrs.key":          "snappy",
		ParquetCompressionColumnCodecKeyPrefix + ".attrs.value.amount": "lz4_raw",
		ParquetCompressionColumnLevelKeyPrefix + ".attrs.value.amount": "3",
	}
	config := (parquetFormat{}).GetWriteProperties(props).(parquetColumnCompressionConfig)
	opts, err := config.forSchema(schema, arrowSchema, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	wp := parquet.NewWriterProperties(opts...)
	require.Equal(t, compress.Codecs.Snappy, wp.CompressionFor("attrs.key_value.key"))
	require.Equal(t, compress.Codecs.Lz4Raw, wp.CompressionFor("attrs.key_value.value.amount"))
	require.Equal(t, 3, wp.CompressionLevelFor("attrs.key_value.value.amount"))
}

func TestParquetColumnCompressionInvalidValues(t *testing.T) {
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32,
	})
	arrowSchema := arrow.NewSchema([]arrow.Field{
		compressionField(1, "id", arrow.PrimitiveTypes.Int32),
	}, nil)
	tests := []struct {
		name  string
		props iceberg.Properties
		want  string
	}{
		{"unknown codec", iceberg.Properties{ParquetCompressionColumnCodecKeyPrefix + ".id": "invalid"}, "unsupported Parquet compression codec"},
		{"non-numeric level", iceberg.Properties{ParquetCompressionColumnLevelKeyPrefix + ".id": "abc"}, "invalid Parquet compression level"},
		{"oversized level", iceberg.Properties{ParquetCompressionColumnLevelKeyPrefix + ".id": "2147483648"}, "invalid Parquet compression level"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := (parquetFormat{}).GetWriteProperties(tt.props).(parquetColumnCompressionConfig)
			_, err := config.forSchema(fileSchema, arrowSchema, pqarrow.DefaultWriterProps())
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestParquetColumnCompressionMissingFieldIsIgnored(t *testing.T) {
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32,
	})
	arrowSchema := arrow.NewSchema([]arrow.Field{
		compressionField(1, "id", arrow.PrimitiveTypes.Int32),
	}, nil)
	props := iceberg.Properties{ParquetCompressionColumnCodecKeyPrefix + ".missing": "bad-codec"}
	config := (parquetFormat{}).GetWriteProperties(props).(parquetColumnCompressionConfig)
	opts, err := config.forSchema(fileSchema, arrowSchema, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.Equal(t, compress.Codecs.Zstd, parquet.NewWriterProperties(opts...).CompressionFor("id"))
}

func TestParquetColumnCompressionWritesMixedCodecs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		compressionField(1, "id", arrow.PrimitiveTypes.Int32),
		compressionField(2, "payload", arrow.BinaryTypes.String),
	}, nil)
	builder := array.NewRecordBuilder(mem, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "bb", "ccc"}, nil)
	batch := builder.NewRecordBatch()
	defer batch.Release()

	props := iceberg.Properties{
		ParquetCompressionKey:                          "zstd",
		ParquetCompressionLevelKey:                     "9",
		ParquetCompressionColumnCodecKeyPrefix + ".id": "snappy",
	}
	format := parquetFormat{}
	fs := iceio.NewMemFS()
	const path = "mem://column-compression/data.parquet"
	writer, err := format.NewFileWriter(context.Background(), fs, nil, WriteFileInfo{
		FileSchema: fileSchema,
		Spec:       *iceberg.UnpartitionedSpec,
		FileName:   path,
		WriteProps: format.GetWriteProperties(props),
		StatsCols: map[int]StatisticsCollector{
			1: {FieldID: 1, Mode: MetricsMode{Typ: MetricModeFull}, ColName: "id", IcebergTyp: iceberg.PrimitiveTypes.Int32},
			2: {FieldID: 2, Mode: MetricsMode{Typ: MetricModeFull}, ColName: "payload", IcebergTyp: iceberg.PrimitiveTypes.String},
		},
	}, arrowSchema)
	require.NoError(t, err)
	require.NoError(t, writer.Write(batch))
	_, err = writer.Close()
	require.NoError(t, err)

	input, err := fs.Open(context.Background(), path)
	require.NoError(t, err)
	defer input.Close()
	rdr, err := file.NewParquetReader(input)
	require.NoError(t, err)
	defer rdr.Close()
	rg := rdr.MetaData().RowGroup(0)
	first, err := rg.ColumnChunk(0)
	require.NoError(t, err)
	second, err := rg.ColumnChunk(1)
	require.NoError(t, err)
	require.Equal(t, compress.Codecs.Snappy, first.Compression())
	require.Equal(t, compress.Codecs.Zstd, second.Compression())
}

func TestParquetColumnCompressionWritesNestedCodecs(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "items", Type: iceberg.NewListType(2, iceberg.PrimitiveTypes.Int32, false)},
		iceberg.NestedField{ID: 3, Name: "attrs", Type: iceberg.NewMapType(4, iceberg.PrimitiveTypes.String, 5, iceberg.PrimitiveTypes.String, false)},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		compressionField(1, "items", arrow.ListOfField(compressionField(2, "element", arrow.PrimitiveTypes.Int32))),
		compressionField(3, "attrs", arrow.MapOfFields(compressionField(4, "key", arrow.BinaryTypes.String), compressionField(5, "value", arrow.BinaryTypes.String))),
	}, nil)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	builder := array.NewRecordBuilder(mem, arrowSchema)
	defer builder.Release()
	list := builder.Field(0).(*array.ListBuilder)
	list.Append(true)
	list.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	mapBuilder := builder.Field(1).(*array.MapBuilder)
	mapBuilder.Append(true)
	mapBuilder.KeyBuilder().(*array.StringBuilder).Append("k")
	mapBuilder.ItemBuilder().(*array.StringBuilder).Append("v")
	batch := builder.NewRecordBatch()
	defer batch.Release()

	props := iceberg.Properties{
		ParquetCompressionColumnCodecKeyPrefix + ".items.element": "gzip",
		ParquetCompressionColumnCodecKeyPrefix + ".attrs.key":     "snappy",
		ParquetCompressionColumnCodecKeyPrefix + ".attrs.value":   "lz4_raw",
	}
	fs := iceio.NewMemFS()
	format := parquetFormat{}
	const path = "mem://column-compression/nested.parquet"
	writer, err := format.NewFileWriter(context.Background(), fs, nil, WriteFileInfo{
		FileSchema: schema, FileName: path, Spec: *iceberg.UnpartitionedSpec, WriteProps: format.GetWriteProperties(props),
		StatsCols: map[int]StatisticsCollector{
			2: {FieldID: 2, Mode: MetricsMode{Typ: MetricModeNone}, ColName: "items.element", IcebergTyp: iceberg.PrimitiveTypes.Int32},
			4: {FieldID: 4, Mode: MetricsMode{Typ: MetricModeNone}, ColName: "attrs.key", IcebergTyp: iceberg.PrimitiveTypes.String},
			5: {FieldID: 5, Mode: MetricsMode{Typ: MetricModeNone}, ColName: "attrs.value", IcebergTyp: iceberg.PrimitiveTypes.String},
		},
	}, arrowSchema)
	require.NoError(t, err)
	require.NoError(t, writer.Write(batch))
	_, err = writer.Close()
	require.NoError(t, err)
	input, err := fs.Open(context.Background(), path)
	require.NoError(t, err)
	rdr, err := file.NewParquetReader(input)
	require.NoError(t, err)
	defer rdr.Close()
	for i, expected := range []struct {
		path  string
		codec compress.Compression
	}{
		{"items.list.element", compress.Codecs.Gzip},
		{"attrs.key_value.key", compress.Codecs.Snappy},
		{"attrs.key_value.value", compress.Codecs.Lz4Raw},
	} {
		require.Equal(t, expected.path, rdr.MetaData().Schema.Column(i).Path())
		col, err := rdr.MetaData().RowGroup(0).ColumnChunk(i)
		require.NoError(t, err)
		require.Equal(t, expected.codec, col.Compression())
	}
}

func TestParquetColumnCompressionLevelValidation(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	arrowSchema := arrow.NewSchema([]arrow.Field{compressionField(1, "id", arrow.PrimitiveTypes.Int32)}, nil)
	for _, tc := range []struct {
		codec string
		level string
		valid bool
	}{
		{"gzip", "-3", true},
		{"GZIP", "9", true},
		{"gzip", "10", false},
		{"zstd", "-5", true},
		{"zstd", "22", true},
		{"zstd", "23", false},
		{"brotli", "-1", true},
		{"brotli", "0", true},
		{"brotli", "11", true},
		{"brotli", "12", false},
		{"snappy", "100", true},
	} {
		t.Run(tc.codec+"/"+tc.level, func(t *testing.T) {
			props := iceberg.Properties{
				ParquetCompressionColumnCodecKeyPrefix + ".id": tc.codec,
				ParquetCompressionColumnLevelKeyPrefix + ".id": tc.level,
			}
			config := (parquetFormat{}).GetWriteProperties(props).(parquetColumnCompressionConfig)
			_, err := config.forSchema(schema, arrowSchema, pqarrow.DefaultWriterProps())
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				require.ErrorContains(t, err, ParquetCompressionColumnLevelKeyPrefix+".id")
			}
		})
	}

	t.Run("level uses global codec", func(t *testing.T) {
		props := iceberg.Properties{
			ParquetCompressionKey:                          "gzip",
			ParquetCompressionColumnLevelKeyPrefix + ".id": "10",
		}
		config := (parquetFormat{}).GetWriteProperties(props).(parquetColumnCompressionConfig)
		_, err := config.forSchema(schema, arrowSchema, pqarrow.DefaultWriterProps())
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	t.Run("same codec inherits level", func(t *testing.T) {
		props := iceberg.Properties{
			ParquetCompressionKey:                          "gzip",
			ParquetCompressionLevelKey:                     "9",
			ParquetCompressionColumnCodecKeyPrefix + ".id": "GZIP",
		}
		config := (parquetFormat{}).GetWriteProperties(props).(parquetColumnCompressionConfig)
		opts, err := config.forSchema(schema, arrowSchema, pqarrow.DefaultWriterProps())
		require.NoError(t, err)
		require.Equal(t, 9, parquet.NewWriterProperties(opts...).CompressionLevelFor("id"))
	})
}

func TestParquetColumnCompressionInvalidDoesNotCreateFile(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	arrowSchema := arrow.NewSchema([]arrow.Field{compressionField(1, "id", arrow.PrimitiveTypes.Int32)}, nil)
	for _, props := range []iceberg.Properties{
		{ParquetCompressionColumnCodecKeyPrefix + ".id": "invalid"},
		{ParquetCompressionColumnCodecKeyPrefix + ".id": "lzo"},
		{ParquetCompressionColumnCodecKeyPrefix + ".id": "gzip", ParquetCompressionColumnLevelKeyPrefix + ".id": "100"},
	} {
		fs := iceio.NewMemFS()
		const path = "mem://column-compression/invalid.parquet"
		format := parquetFormat{}
		_, err := format.NewFileWriter(context.Background(), fs, nil, WriteFileInfo{
			FileSchema: schema, FileName: path, WriteProps: format.GetWriteProperties(props),
		}, arrowSchema)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		_, err = fs.Open(context.Background(), path)
		require.Error(t, err)
	}
}

func TestParquetColumnCompressionIgnoredForDeleteFiles(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	arrowSchema := arrow.NewSchema([]arrow.Field{compressionField(1, "id", arrow.PrimitiveTypes.Int32)}, nil)
	for _, content := range []iceberg.ManifestEntryContent{iceberg.EntryContentEqDeletes, iceberg.EntryContentPosDeletes} {
		fs := iceio.NewMemFS()
		format := parquetFormat{}
		writer, err := format.NewFileWriter(context.Background(), fs, nil, WriteFileInfo{
			FileSchema: schema, FileName: "mem://column-compression/delete.parquet", Content: content,
			WriteProps: format.GetWriteProperties(iceberg.Properties{ParquetCompressionColumnCodecKeyPrefix + ".id": "invalid"}),
		}, arrowSchema)
		require.NoError(t, err)
		require.NoError(t, writer.Abort())
	}
}
