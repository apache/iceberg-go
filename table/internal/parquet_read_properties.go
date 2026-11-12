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
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/iceberg-go"
)

const (
	ParquetMaxCompressedPageSizeBytesKey     = "read.parquet.max-compressed-page-size-bytes"
	ParquetMaxCompressedPageSizeBytesDefault = parquet.DefaultMaxCompressedPageSize

	ParquetMaxUncompressedPageSizeBytesKey     = "read.parquet.max-uncompressed-page-size-bytes"
	ParquetMaxUncompressedPageSizeBytesDefault = parquet.DefaultMaxUncompressedPageSize
)

func parquetPageLimit(props iceberg.Properties, key string, fallback int64) (int64, error) {
	raw, ok := props[key]
	if !ok {
		return fallback, nil
	}
	limit, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || limit <= 0 || limit > math.MaxInt32 {
		return 0, fmt.Errorf("%w: %s must be an integer between 1 and %d, got %q",
			iceberg.ErrInvalidArgument, key, math.MaxInt32, raw)
	}

	return limit, nil
}

// ValidateParquetReadProperties rejects invalid explicit page limits before a
// scan or delete starts reader goroutines. Parquet page sizes are signed int32.
func ValidateParquetReadProperties(props iceberg.Properties) error {
	if _, err := parquetPageLimit(props, ParquetMaxCompressedPageSizeBytesKey, ParquetMaxCompressedPageSizeBytesDefault); err != nil {
		return err
	}
	_, err := parquetPageLimit(props, ParquetMaxUncompressedPageSizeBytesKey, ParquetMaxUncompressedPageSizeBytesDefault)

	return err
}

func newParquetReaderProperties(ctx context.Context, mem memory.Allocator) (*parquet.ReaderProperties, error) {
	props := parquet.NewReaderProperties(mem)
	tableProps := TablePropertiesFromContext(ctx)
	var err error
	props.MaxCompressedPageSize, err = parquetPageLimit(tableProps, ParquetMaxCompressedPageSizeBytesKey, ParquetMaxCompressedPageSizeBytesDefault)
	if err != nil {
		return nil, err
	}
	props.MaxUncompressedPageSize, err = parquetPageLimit(tableProps, ParquetMaxUncompressedPageSizeBytesKey, ParquetMaxUncompressedPageSizeBytesDefault)
	if err != nil {
		return nil, err
	}

	return props, nil
}
