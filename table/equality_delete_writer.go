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
	"errors"
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

var ErrEmptyEqualityFieldIDs = errors.New("equality field IDs must not be empty")

// equalityDeleteSchema projects a table schema to only the fields specified
// by the given field IDs, using Schema.Select for proper nested field handling.
func equalityDeleteSchema(tableSchema *iceberg.Schema, fieldIDs []int) (*iceberg.Schema, error) {
	if len(fieldIDs) == 0 {
		return nil, ErrEmptyEqualityFieldIDs
	}

	names := make([]string, 0, len(fieldIDs))
	for _, id := range fieldIDs {
		name, ok := tableSchema.FindColumnName(id)
		if !ok {
			return nil, fmt.Errorf("%w: field ID %d not found in table schema", iceberg.ErrInvalidSchema, id)
		}

		names = append(names, name)
	}

	return tableSchema.Select(true, names...)
}

// WriteEqualityDeletes writes Arrow record batches as equality delete
// Parquet files and returns the resulting DataFiles. The returned files
// have ContentType == EntryContentEqDeletes and EqualityFieldIDs set,
// ready to be passed to [RowDelta.AddDeletes].
//
// The equalityFieldIDs identify which columns in the table schema form
// the delete key. The provided records must contain exactly those columns.
//
// The table must use format version 2 or higher.
//
// Note: partitioned tables are not yet supported for equality delete
// writing. See https://github.com/apache/iceberg-go/issues/808
//
// Usage:
//
//	deleteFiles, err := tx.WriteEqualityDeletes(ctx, []int{1, 2}, records)
//	rd := tx.NewRowDelta(nil)
//	rd.AddDeletes(deleteFiles...)
//	err = rd.Commit(ctx)
func (t *Transaction) WriteEqualityDeletes(ctx context.Context, equalityFieldIDs []int, records iter.Seq2[arrow.RecordBatch, error]) ([]iceberg.DataFile, error) {
	if t.meta.formatVersion < 2 {
		return nil, fmt.Errorf("equality deletes require table format version >= 2, got v%d",
			t.meta.formatVersion)
	}

	deleteSchema, err := equalityDeleteSchema(t.meta.CurrentSchema(), equalityFieldIDs)
	if err != nil {
		return nil, err
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}

	wfs, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil, errors.New("filesystem does not support writing")
	}

	arrowSc, err := SchemaToArrowSchema(deleteSchema, nil, true, false)
	if err != nil {
		return nil, err
	}

	writeUUID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate write UUID: %w", err)
	}

	args := recordWritingArgs{
		sc:        arrowSc,
		itr:       records,
		fs:        wfs,
		writeUUID: &writeUUID,
		counter:   internal.Counter(0),
	}

	dataFiles, err := equalityDeleteRecordsToDataFiles(ctx, t.tbl.Location(), t.meta, deleteSchema, equalityFieldIDs, args)
	if err != nil {
		return nil, err
	}

	var result []iceberg.DataFile
	for df, err := range dataFiles {
		if err != nil {
			return nil, err
		}

		result = append(result, df)
	}

	return result, nil
}

func equalityDeleteRecordsToDataFiles(ctx context.Context, rootLocation string, meta *MetadataBuilder, deleteSchema *iceberg.Schema, equalityFieldIDs []int, args recordWritingArgs) (ret iter.Seq2[iceberg.DataFile, error], retErr error) {
	if args.counter == nil {
		args.counter = internal.Counter(0)
	}

	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				retErr = fmt.Errorf("error encountered during equality delete file writing: %w", e)
			default:
				retErr = fmt.Errorf("error encountered during equality delete file writing: %v", e)
			}
		}
	}()

	if args.writeUUID == nil {
		u, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("failed to generate write UUID: %w", err)
		}
		args.writeUUID = &u
	}

	targetFileSize := int64(meta.props.GetInt(WriteTargetFileSizeBytesKey,
		WriteTargetFileSizeBytesDefault))

	cw := newConcurrentDataFileWriter(func(rootLocation string, fs iceio.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
		return newEqualityDeleteWriter(rootLocation, fs, meta, props, deleteSchema, equalityFieldIDs, opts...)
	}, withSchemaSanitization(false))

	latestMetadata, err := meta.Build()
	if err != nil {
		return nil, err
	}

	// TODO(#808): support partitioned tables for equality delete writing.
	// The partitioned fanout writer assumes partition source columns are
	// present in the record, but equality delete records only contain the
	// delete key columns. Needs either explicit partition values from the
	// caller (Java pattern) or automatic partition column inclusion.
	// See https://github.com/apache/iceberg-go/issues/808
	if !latestMetadata.PartitionSpec().IsUnpartitioned() {
		return nil, errors.New("equality delete writing for partitioned tables is not yet supported")
	}

	nextCount, stopCount := iter.Pull(args.counter)
	tasks := func(yield func(WriteTask) bool) {
		defer stopCount()

		fileCount := 0
		for batch := range binPackRecords(args.itr, defaultBinPackLookback, targetFileSize) {
			cnt, _ := nextCount()
			fileCount++
			t := WriteTask{
				Uuid:        *args.writeUUID,
				ID:          cnt,
				PartitionID: iceberg.UnpartitionedSpec.ID(),
				FileCount:   fileCount,
				Schema:      deleteSchema,
				Batches:     batch,
			}
			if !yield(t) {
				return
			}
		}
	}

	return cw.writeFiles(ctx, rootLocation, args.fs, meta, meta.props, nil, tasks), nil
}
