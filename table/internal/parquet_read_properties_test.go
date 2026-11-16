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
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestParquetReadPageLimitDefaultsAndValidation(t *testing.T) {
	props, err := newParquetReaderProperties(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, parquet.DefaultMaxCompressedPageSize, props.MaxCompressedPageSize)
	require.Equal(t, parquet.DefaultMaxUncompressedPageSize, props.MaxUncompressedPageSize)
	for _, key := range []string{ParquetMaxCompressedPageSizeBytesKey, ParquetMaxUncompressedPageSizeBytesKey} {
		for _, invalid := range []string{"", "0", "-1", "abc", "1.5", "2147483648", "9223372036854775808"} {
			t.Run(key+"/"+invalid, func(t *testing.T) {
				values := iceberg.Properties{key: invalid}
				require.ErrorIs(t, ValidateParquetReadProperties(values), iceberg.ErrInvalidArgument)
				_, err := newParquetReaderProperties(WithTableProperties(t.Context(), values), nil)
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				require.ErrorContains(t, err, key)
			})
		}
		for _, valid := range []string{"1", "2147483647"} {
			require.NoError(t, ValidateParquetReadProperties(iceberg.Properties{key: valid}))
		}
	}
}

func pageLimitFixture(t *testing.T, compressed, dictionary bool) ([]byte, []byte) {
	t.Helper()
	value := bytes.Repeat([]byte("page-limit"), 8192)
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.BinaryTypes.Binary,
		Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "1"}),
	}}, nil)
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.Append(value)
	column := builder.NewArray()
	defer column.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	defer record.Release()
	codec := compress.Codecs.Uncompressed
	if compressed {
		codec = compress.Codecs.Zstd
	}
	var output bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &output, parquet.NewWriterProperties(
		parquet.WithCompression(codec), parquet.WithDictionaryDefault(dictionary),
		parquet.WithDataPageSize(256*1024), parquet.WithDictionaryPageSizeLimit(256*1024)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	return output.Bytes(), value
}

func pageLimitSource(t *testing.T, data []byte) (icebergio.WriteFileIO, FileSource) {
	t.Helper()
	const location = "mem://page-limit-fixture/data.parquet"
	fs := icebergio.NewMemFS()
	require.NoError(t, fs.WriteFile(t.Context(), location, data))
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		location, iceberg.ParquetFile, nil, nil, nil, 1, int64(len(data)))
	require.NoError(t, err)
	source, err := GetFile(t.Context(), fs, builder.Build(), false)
	require.NoError(t, err)

	return fs, source
}

func readPageLimitValue(ctx context.Context, reader FileReader) ([]byte, error) {
	records, err := reader.GetRecords(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	defer records.Release()
	var value []byte
	for records.Next() {
		value = bytes.Clone(records.RecordBatch().Column(0).(*array.Binary).Value(0))
	}
	if err := records.Err(); err != nil && err != io.EOF {
		return nil, err
	}

	return value, nil
}

func TestParquetReadPageLimitsAcrossReaderPaths(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		data, want := pageLimitFixture(t, compressed, false)
		fs, source := pageLimitSource(t, data)
		for _, path := range []string{"open", "source", "cache"} {
			for _, limit := range []string{"compressed", "uncompressed", "generous"} {
				if compressed && limit == "compressed" {
					continue
				}
				t.Run(fmt.Sprintf("%t/%s/%s", compressed, path, limit), func(t *testing.T) {
					values := iceberg.Properties{ParquetMaxCompressedPageSizeBytesKey: "1048576", ParquetMaxUncompressedPageSizeBytesKey: "1048576"}
					switch limit {
					case "compressed":
						values[ParquetMaxCompressedPageSizeBytesKey] = "1024"
					case "uncompressed":
						values[ParquetMaxUncompressedPageSizeBytesKey] = "1024"
					}
					ctx := WithTableProperties(t.Context(), values)
					if path == "cache" {
						ctx = WithParquetMetadataCache(ctx)
					}
					for range 2 {
						var reader FileReader
						var err error
						if path == "open" {
							reader, err = (parquetFormat{}).Open(ctx, fs, "mem://page-limit-fixture/data.parquet")
						} else {
							reader, err = source.GetReader(ctx)
						}
						require.NoError(t, err)
						value, readErr := readPageLimitValue(ctx, reader)
						require.NoError(t, reader.Close())
						if limit == "generous" {
							require.NoError(t, readErr)
							require.Equal(t, want, value)
						} else {
							require.ErrorContains(t, readErr, limit+" page size")
						}
					}
				})
			}
		}
	}
}

func TestParquetMetadataCacheDoesNotCachePageLimits(t *testing.T) {
	data, want := pageLimitFixture(t, false, false)
	_, source := pageLimitSource(t, data)
	cache := WithParquetMetadataCache(t.Context())
	for _, size := range []string{"1048576", "1024", "1048576"} {
		ctx := WithTableProperties(cache, iceberg.Properties{ParquetMaxCompressedPageSizeBytesKey: size})
		reader, err := source.GetReader(ctx)
		require.NoError(t, err)
		value, readErr := readPageLimitValue(ctx, reader)
		require.NoError(t, reader.Close())
		if size == "1024" {
			require.ErrorContains(t, readErr, "compressed page size")
		} else {
			require.NoError(t, readErr)
			require.Equal(t, want, value)
		}
	}
}

func TestParquetDictionaryReaderKeepsPageLimits(t *testing.T) {
	data, _ := pageLimitFixture(t, false, true)
	metadataReader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer metadataReader.Close()
	configured := parquet.NewReaderProperties(memory.DefaultAllocator)
	configured.MaxCompressedPageSize = 1024
	reader, err := newParquetDictionaryReader(bytes.NewReader(data), metadataReader.MetaData(), memory.DefaultAllocator, configured)
	require.NoError(t, err)
	defer reader.Close()
	require.False(t, configured.BufferedStreamEnabled, "the temporary reader must not mutate shared properties")
	pages, err := reader.RowGroup(0).GetColumnPageReader(0)
	require.NoError(t, err)
	defer pages.Close()
	require.False(t, pages.Next())
	require.ErrorContains(t, pages.Err(), "compressed page size")
}
