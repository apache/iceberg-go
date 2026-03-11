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
	"context"
	"fmt"
	"iter"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

// WriteRecordOption configures the behavior of WriteRecords.
type WriteRecordOption func(*writeRecordConfig)

type writeRecordConfig struct {
	targetFileSize int64
	writeUUID      *uuid.UUID
}

// WithTargetFileSize overrides the table's default target file size.
func WithTargetFileSize(size int64) WriteRecordOption {
	return func(c *writeRecordConfig) {
		c.targetFileSize = size
	}
}

// WithWriteUUID sets a specific UUID for file naming.
func WithWriteUUID(id uuid.UUID) WriteRecordOption {
	return func(c *writeRecordConfig) {
		c.writeUUID = &id
	}
}

// WriteRecords writes Arrow record batches to Parquet data files for the given
// table, returning an iterator of the resulting DataFile objects.
func WriteRecords(ctx context.Context, tbl *Table,
	schema *arrow.Schema,
	records iter.Seq2[arrow.RecordBatch, error],
	opts ...WriteRecordOption,
) iter.Seq2[iceberg.DataFile, error] {
	cfg := writeRecordConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	fs, err := tbl.fsF(ctx)
	if err != nil {
		return singleErrorIter(err)
	}

	writeFS, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return singleErrorIter(fmt.Errorf("%w: filesystem does not support writing", iceberg.ErrNotImplemented))
	}

	meta, _ := MetadataBuilderFromBase(tbl.metadata, tbl.metadataLocation)

	if cfg.targetFileSize > 0 {
		if meta.props == nil {
			meta.props = make(iceberg.Properties)
		}
		meta.props[WriteTargetFileSizeBytesKey] = strconv.FormatInt(cfg.targetFileSize, 10)
	}

	args := recordWritingArgs{
		sc:        schema,
		itr:       records,
		fs:        writeFS,
		writeUUID: cfg.writeUUID,
	}

	return recordsToDataFiles(ctx, tbl.Location(), meta, args)
}

func singleErrorIter(err error) iter.Seq2[iceberg.DataFile, error] {
	return func(yield func(iceberg.DataFile, error) bool) {
		yield(nil, err)
	}
}
