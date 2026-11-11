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

// parquetColumnCompressionConfig resolves logical Iceberg field names to physical
// Parquet paths at write time. The path includes list/map wrappers that do not
// appear in Iceberg property names.
type parquetColumnCompressionConfig struct {
	base    []parquet.WriterProperty
	columns map[string]parquetColumnCompressionOverride
}

// Keep presence separate from value: an explicitly empty codec or level must
// fail validation instead of being treated as an absent override.
type parquetColumnCompressionOverride struct {
	codec    string
	level    string
	hasCodec bool
	hasLevel bool
}

func columnCompressionWriteProperties(props iceberg.Properties, base []parquet.WriterProperty) any {
	columns := make(map[string]parquetColumnCompressionOverride)
	for key, value := range props {
		if name, ok := strings.CutPrefix(key, ParquetCompressionColumnCodecKeyPrefix+"."); ok && name != "" {
			override := columns[name]
			override.codec = value
			override.hasCodec = true
			columns[name] = override
		} else if name, ok := strings.CutPrefix(key, ParquetCompressionColumnLevelKeyPrefix+"."); ok && name != "" {
			override := columns[name]
			override.level = value
			override.hasLevel = true
			columns[name] = override
		}
	}

	if len(columns) == 0 {
		return base
	}

	return parquetColumnCompressionConfig{base: base, columns: columns}
}

func (override parquetColumnCompressionOverride) appendWriterProperties(
	opts []parquet.WriterProperty, path, name string, defaultCodec compress.Compression,
) ([]parquet.WriterProperty, error) {
	codec := defaultCodec
	if override.hasCodec {
		var valid bool
		codec, valid = ParquetCompressionCodec(override.codec)
		if !valid {
			return nil, fmt.Errorf("%w: unsupported Parquet compression codec %q for column %q",
				iceberg.ErrInvalidArgument, override.codec, name)
		}
		if _, err := compress.GetCodec(codec); err != nil {
			return nil, fmt.Errorf("%w: Parquet compression codec %q for column %q: %v",
				iceberg.ErrInvalidArgument, override.codec, name, err)
		}

		opts = append(opts, parquet.WithCompressionFor(path, codec))
		if !override.hasLevel && codec != defaultCodec {
			// A codec override cannot inherit a level meant for another codec.
			opts = append(opts, parquet.WithCompressionLevelFor(path, compress.DefaultCompressionLevel))
		}
	}

	if override.hasLevel {
		level, err := strconv.ParseInt(override.level, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid Parquet compression level %q for column %q: %v",
				iceberg.ErrInvalidArgument, override.level, name, err)
		}
		key := ParquetCompressionColumnLevelKeyPrefix + "." + name
		if err := validateParquetCompressionLevel(key, strings.ToLower(codec.String()), int(level)); err != nil {
			return nil, err
		}
		opts = append(opts, parquet.WithCompressionLevelFor(path, int(level)))
	}

	return opts, nil
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
	matched := make(map[string]struct{}, len(config.columns))
	for i := range physicalSchema.NumColumns() {
		column := physicalSchema.Column(i)
		name, found := fileSchema.FindColumnName(int(column.SchemaNode().FieldID()))
		if !found {
			continue
		}

		override, ok := config.columns[name]
		if !ok {
			continue
		}

		matched[name] = struct{}{}
		opts, err = override.appendWriterProperties(opts, column.Path(), name, defaults.Compression())
		if err != nil {
			return nil, err
		}
	}

	for name := range config.columns {
		if _, ok := matched[name]; !ok {
			slog.Warn("skipping per-column Parquet compression for missing field", "column", name)
		}
	}

	return opts, nil
}
