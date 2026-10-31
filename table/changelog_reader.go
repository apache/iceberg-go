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
	"maps"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
)

const (
	// ChangelogChangeTypeColumn is INSERT or DELETE. A copy-on-write overwrite
	// emits both for rows that did not change. UPDATE_BEFORE and UPDATE_AFTER
	// are not produced.
	ChangelogChangeTypeColumn = "_change_type"
	// ChangelogChangeOrdinalColumn is the zero-based ordinal of the snapshot
	// that produced the change, shared by every row from that snapshot. The
	// Arrow type is int32, matching Java MetadataColumns.CHANGE_ORDINAL.
	ChangelogChangeOrdinalColumn = "_change_ordinal"
	// ChangelogCommitSnapshotIDColumn is the snapshot ID that committed the change.
	ChangelogCommitSnapshotIDColumn = "_commit_snapshot_id"
)

// ToArrowRecords plans the changelog and reads it. The result is a raw
// file-level changelog: a copy-on-write overwrite emits a DELETE and an
// INSERT for every row in the rewritten files, including rows that did not
// change, and UPDATE_BEFORE and UPDATE_AFTER are not produced. Added and
// removed data files are read with the deletes already stored on their scan
// task. Row-level deletes emit the rows removed by delete files added in the
// range, after deletes that already applied have been excluded.
func (s *IncrementalChangelogScan) ToArrowRecords(ctx context.Context) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	tasks, err := s.PlanFiles(ctx)
	if err != nil {
		return nil, nil, err
	}

	return s.Read(ctx, tasks)
}

// Read materializes changelog rows for tasks. Each batch has the scan
// projection followed by _change_type, _change_ordinal, and
// _commit_snapshot_id. This is a raw file-level changelog: unchanged rows in
// a rewritten file are emitted as DELETE and INSERT, and UPDATE_BEFORE and
// UPDATE_AFTER are not produced. AddedRowsScanTask and DeletedDataFileScanTask
// are read with Scan.ReadTasks, so same-snapshot deletes and deletes that
// already applied to a removed file are applied before the changelog columns
// are added. DeletedRowsScanTask emits rows matched by AddedDeletes and skips
// rows matched by ExistingDeletes. Files are read one at a time, in task
// order, so the rows stay in that order. WithMaxConcurrency does not read
// changelog files in parallel. The returned iterator is single-use. The
// caller releases every batch.
func (s *IncrementalChangelogScan) Read(ctx context.Context, tasks []ChangelogScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	if s == nil || s.scan == nil {
		return nil, nil, fmt.Errorf("%w: incremental changelog scan is not initialized", ErrInvalidOperation)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	for _, task := range tasks {
		if err := readableChangelogTask(task); err != nil {
			return nil, nil, err
		}
	}

	schema, useLargeTypes, err := changelogArrowSchema(s.scan)
	if err != nil {
		return nil, nil, err
	}
	if s.scan.limit == 0 || len(tasks) == 0 {
		return schema, func(yield func(arrow.RecordBatch, error) bool) {}, nil
	}

	readScan := s.scan.clone()
	limit := s.scan.limit

	return schema, func(yield func(arrow.RecordBatch, error) bool) {
		var emitted int64
		for _, task := range tasks {
			if err := ctx.Err(); err != nil {
				yield(nil, err)

				return
			}
			if limit > 0 {
				// Safe because tasks are read sequentially on one clone.
				readScan.limit = limit - emitted
			}

			var records iter.Seq2[arrow.RecordBatch, error]
			var err error
			if deleted, ok := task.(DeletedRowsScanTask); ok {
				_, records, err = readScan.readDeletedRows(ctx, deleted)
			} else {
				_, records, err = readScan.ReadTasks(ctx, []FileScanTask{task.ScanTask()})
			}
			if err != nil {
				yield(nil, err)

				return
			}

			for rec, err := range records {
				if err != nil {
					yield(nil, err)

					return
				}

				stamped, rows, err := stampChangelogBatch(ctx, schema, useLargeTypes, rec, task, limit, emitted)
				rec.Release()
				if err != nil {
					yield(nil, err)

					return
				}
				if stamped == nil {
					continue
				}
				if !yield(stamped, nil) {
					return
				}
				emitted += rows
				if limit > 0 && emitted >= limit {
					return
				}
			}
		}
	}, nil
}

func readableChangelogTask(task ChangelogScanTask) error {
	switch task.(type) {
	case AddedRowsScanTask, DeletedDataFileScanTask, DeletedRowsScanTask:
		return nil
	default:
		return fmt.Errorf("%w: unsupported changelog scan task %T", iceberg.ErrInvalidArgument, task)
	}
}

func changelogArrowSchema(scan *Scan) (*arrow.Schema, bool, error) {
	if atomic.LoadUint32(&scan.closed) != 0 {
		return nil, false, fmt.Errorf("%w: scan is closed", ErrInvalidOperation)
	}
	if scan.selectorErr != nil {
		return nil, false, scan.selectorErr
	}

	projected, err := scan.Projection()
	if err != nil {
		return nil, false, err
	}

	useLargeTypes, err := strconv.ParseBool(scan.options.Get(ScanOptionArrowUseLargeTypes, "false"))
	if err != nil {
		useLargeTypes = false
	}

	props := scan.metadata.Properties()
	if scan.arrowBatchSize > 0 {
		props = maps.Clone(props)
		if props == nil {
			props = iceberg.Properties{}
		}
		props[ParquetBatchSizeKey] = strconv.Itoa(scan.arrowBatchSize)
	}

	dataSchema, err := SchemaToArrowSchemaWithOptions(projected, ArrowSchemaOptions{
		UseLargeTypes:   useLargeTypes,
		TableProperties: props,
	})
	if err != nil {
		return nil, false, err
	}

	if err := rejectChangelogColumnCollision(dataSchema); err != nil {
		return nil, false, err
	}

	schema := dataSchema
	for _, field := range changelogArrowFields(useLargeTypes) {
		schema, err = schema.AddField(schema.NumFields(), field)
		if err != nil {
			return nil, false, err
		}
	}

	return schema, useLargeTypes, nil
}

func rejectChangelogColumnCollision(schema *arrow.Schema) error {
	for _, name := range []string{
		ChangelogChangeTypeColumn,
		ChangelogChangeOrdinalColumn,
		ChangelogCommitSnapshotIDColumn,
	} {
		if len(schema.FieldIndices(name)) > 0 {
			return fmt.Errorf("%w: projected column %q conflicts with a changelog column",
				iceberg.ErrInvalidSchema, name)
		}
	}

	return nil
}

func changelogBatchMatches(out, data *arrow.Schema) error {
	want := out.NumFields() - 3
	if data.NumFields() != want {
		return fmt.Errorf("%w: read batch has %d columns, changelog schema expects %d",
			iceberg.ErrInvalidSchema, data.NumFields(), want)
	}
	for i := range want {
		if !data.Field(i).Equal(out.Field(i)) {
			return fmt.Errorf("%w: read column %q does not match the changelog schema",
				iceberg.ErrInvalidSchema, data.Field(i).Name)
		}
	}

	return nil
}

func changelogArrowFields(useLargeTypes bool) []arrow.Field {
	changeType := arrow.BinaryTypes.String
	if useLargeTypes {
		changeType = arrow.BinaryTypes.LargeString
	}

	return []arrow.Field{
		{Name: ChangelogChangeTypeColumn, Type: changeType, Nullable: false},
		{Name: ChangelogChangeOrdinalColumn, Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: ChangelogCommitSnapshotIDColumn, Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}
}

// stampChangelogBatch appends the changelog columns and applies any remaining
// row limit. A nil batch means the limit was already exhausted. rec is not
// released here.
func stampChangelogBatch(ctx context.Context, schema *arrow.Schema, useLargeTypes bool, rec arrow.RecordBatch, task ChangelogScanTask, limit, emitted int64) (arrow.RecordBatch, int64, error) {
	if limit > 0 && emitted >= limit {
		return nil, 0, nil
	}

	rows := rec.NumRows()
	source := rec
	if limit > 0 && emitted+rows > limit {
		source = rec.NewSlice(0, limit-emitted)
		defer source.Release()
		rows = source.NumRows()
	}
	if rows == 0 {
		return nil, 0, nil
	}

	stamped, err := appendChangelogColumns(ctx, schema, useLargeTypes, source, task)
	if err != nil {
		return nil, 0, err
	}

	return stamped, rows, nil
}

func appendChangelogColumns(ctx context.Context, schema *arrow.Schema, useLargeTypes bool, rec arrow.RecordBatch, task ChangelogScanTask) (arrow.RecordBatch, error) {
	if err := changelogBatchMatches(schema, rec.Schema()); err != nil {
		return nil, err
	}

	mem := allocatorFrom(ctx)
	n := int(rec.NumRows())
	var changeScalar scalar.Scalar = scalar.NewStringScalar(string(task.Operation()))
	if useLargeTypes {
		changeScalar = scalar.NewLargeStringScalar(string(task.Operation()))
	}
	changeType, err := scalar.MakeArrayFromScalar(changeScalar, n, mem)
	if err != nil {
		return nil, err
	}
	defer changeType.Release()

	ordinal, err := scalar.MakeArrayFromScalar(scalar.NewInt32Scalar(int32(task.ChangeOrdinal())), n, mem)
	if err != nil {
		return nil, err
	}
	defer ordinal.Release()
	snapshotID, err := scalar.MakeArrayFromScalar(scalar.NewInt64Scalar(task.CommitSnapshotID()), n, mem)
	if err != nil {
		return nil, err
	}
	defer snapshotID.Release()

	cols := make([]arrow.Array, 0, rec.NumCols()+3)
	cols = append(cols, rec.Columns()...)
	cols = append(cols, changeType, ordinal, snapshotID)

	return array.NewRecordBatch(schema, cols, rec.NumRows()), nil
}

func allocatorFrom(ctx context.Context) memory.Allocator {
	if mem := compute.GetAllocator(ctx); mem != nil {
		return mem
	}

	return memory.DefaultAllocator
}
