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
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
)

// parquetColumnCompressionConfig defers per-column configuration until the
// physical Parquet paths are known. Iceberg and Parquet paths differ for lists
// and maps, so property names cannot be passed to Arrow unchanged.
type parquetColumnCompressionConfig struct {
	base   []parquet.WriterProperty
	codecs map[string]string
	levels map[string]string
}

func parquetCompressionCodec(value string) (compress.Compression, bool) {
	switch strings.ToLower(value) {
	case "snappy":
		return compress.Codecs.Snappy, true
	case "zstd":
		return compress.Codecs.Zstd, true
	case "uncompressed":
		return compress.Codecs.Uncompressed, true
	case "gzip":
		return compress.Codecs.Gzip, true
	case "brotli":
		return compress.Codecs.Brotli, true
	case "lz4":
		return compress.Codecs.Lz4, true
	case "lz4raw", "lz4_raw":
		return compress.Codecs.Lz4Raw, true
	case "lzo":
		return compress.Codecs.Lzo, true
	default:
		return compress.Codecs.Uncompressed, false
	}
}

func (config parquetColumnCompressionConfig) forSchema(
	fileSchema *iceberg.Schema, arrowSchema *arrow.Schema, arrProps pqarrow.ArrowWriterProperties,
) ([]parquet.WriterProperty, error) {
	if fileSchema == nil || arrowSchema == nil {
		return nil, fmt.Errorf("%w: per-column Parquet compression requires a file schema", iceberg.ErrInvalidArgument)
	}

	defaults := parquet.NewWriterProperties(config.base...)
	physicalSchema, err := pqarrow.ToParquet(arrowSchema, defaults, arrProps)
	if err != nil {
		return nil, fmt.Errorf("convert Parquet schema for per-column compression: %w", err)
	}

	opts := slices.Clone(config.base)
	matched := make(map[string]struct{})
	for i := range physicalSchema.NumColumns() {
		column := physicalSchema.Column(i)
		name, found := fileSchema.FindColumnName(int(column.SchemaNode().FieldID()))
		if !found {
			continue
		}

		codecValue, hasCodec := config.codecs[name]
		levelValue, hasLevel := config.levels[name]
		if !hasCodec && !hasLevel {
			continue
		}

		matched[name] = struct{}{}
		path := column.Path()
		codec := defaults.Compression()
		if hasCodec {
			var valid bool
			codec, valid = parquetCompressionCodec(codecValue)
			if !valid {
				return nil, fmt.Errorf("%w: unsupported Parquet compression codec %q for column %q",
					iceberg.ErrInvalidArgument, codecValue, name)
			}
			if _, err := compress.GetCodec(codec); err != nil {
				return nil, fmt.Errorf("%w: Parquet compression codec %q for column %q: %v",
					iceberg.ErrInvalidArgument, codecValue, name, err)
			}

			opts = append(opts, parquet.WithCompressionFor(path, codec))
			if !hasLevel && codec != defaults.Compression() {
				// Do not inherit a compression level configured for a different codec.
				opts = append(opts, parquet.WithCompressionLevelFor(path, compress.DefaultCompressionLevel))
			}
		}

		if hasLevel {
			level, err := strconv.ParseInt(levelValue, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid Parquet compression level %q for column %q: %v",
					iceberg.ErrInvalidArgument, levelValue, name, err)
			}
			key := ParquetCompressionColumnLevelKeyPrefix + "." + name
			if err := validateParquetCompressionLevel(key, strings.ToLower(codec.String()), int(level)); err != nil {
				return nil, err
			}
			opts = append(opts, parquet.WithCompressionLevelFor(path, int(level)))
		}
	}

	for name := range config.codecs {
		if _, ok := matched[name]; !ok {
			slog.Warn("skipping per-column Parquet compression for missing field", "column", name)
		}
	}
	for name := range config.levels {
		if _, ok := matched[name]; !ok {
			if _, alreadyLogged := config.codecs[name]; !alreadyLogged {
				slog.Warn("skipping per-column Parquet compression for missing field", "column", name)
			}
		}
	}

	return opts, nil
}
