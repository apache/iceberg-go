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
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
)

func releasePackedRecords(records []arrow.RecordBatch) {
	for _, record := range records {
		record.Release()
	}
}

// binPackRecords transfers one retained reference per record to the consumer.
// A source error discards buffered bins instead of treating failure as EOF.
func binPackRecords(records iter.Seq2[arrow.RecordBatch, error], lookback int, targetSize int64) iter.Seq2[[]arrow.RecordBatch, error] {
	return func(yield func([]arrow.RecordBatch, error) bool) {
		var readErr error
		stopped := false
		input := func(yieldRecord func(arrow.RecordBatch) bool) {
			for record, err := range records {
				if err != nil {
					readErr = err

					return
				}
				record.Retain()
				if !yieldRecord(record) || stopped {
					return
				}
			}
		}

		for batch := range internal.PackingIterator(input, targetSize, lookback, recordNBytes, false) {
			if stopped || readErr != nil {
				releasePackedRecords(batch)

				continue
			}
			if !yield(batch, nil) {
				// Drain only retained bins. input stops before reading another
				// record once this yield returns to it.
				stopped = true
			}
		}
		if !stopped && readErr != nil {
			yield(nil, readErr)
		}
	}
}

// writeDeleteRecords publishes file descriptors only after all input and writes
// succeed. No returned descriptor can refer to a file removed on failure.
func (w *concurrentDataFileWriter) writeDeleteRecords(ctx context.Context, location string, meta *MetadataBuilder, schema *iceberg.Schema, args recordWritingArgs, targetSize int64) iter.Seq2[iceberg.DataFile, error] {
	return func(yield func(iceberg.DataFile, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(nil, err)

			return
		}
		nextCount, stopCount := iter.Pull(args.counter)
		defer stopCount()
		input := func(yield func(arrow.RecordBatch, error) bool) {
			for record, err := range args.itr {
				err = errors.Join(err, context.Cause(ctx))
				if !yield(record, err) || err != nil {
					return
				}
			}
		}
		var readErr error
		tasks := func(yield func(WriteTask) bool) {
			fileCount := 0
			for batch, err := range binPackRecords(input, defaultBinPackLookback, targetSize) {
				if err != nil {
					readErr = err

					return
				}
				count, _ := nextCount()
				fileCount++
				task := WriteTask{
					Uuid: *args.writeUUID, ID: count, PartitionID: iceberg.UnpartitionedSpec.ID(),
					FileCount: fileCount, Schema: schema, Batches: batch,
				}
				if !yield(task) {
					releasePackedRecords(batch)

					return
				}
			}
		}

		var files []iceberg.DataFile
		var writeErr error
		for file, err := range w.writeFiles(ctx, location, args.fs, meta, meta.props, nil, tasks) {
			if err != nil {
				writeErr = errors.Join(writeErr, err)
			} else {
				files = append(files, file)
			}
		}
		// MapExec has joined the task producer and workers before ending the
		// iterator, so readErr is no longer being written on another goroutine.
		if err := errors.Join(readErr, writeErr, context.Cause(ctx)); err != nil {
			cleanupCtx := context.WithoutCancel(ctx)
			for _, file := range files {
				err = errors.Join(err, args.fs.Remove(cleanupCtx, file.FilePath()))
			}
			yield(nil, err)

			return
		}
		for _, file := range files {
			if !yield(file, nil) {
				return
			}
		}
	}
}
