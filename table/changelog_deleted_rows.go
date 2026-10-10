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
	"bytes"
	"context"
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/dv"
)

// changelogFilePosColumn carries the data file position of each row so a
// later keep step can still see it after existing deletes shorten the batch.
const changelogFilePosColumn = "_changelog_file_pos"

func (scan *Scan) readDeletedRows(ctx context.Context, task DeletedRowsScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	base := task.ScanTask()
	added := base
	added.DeleteFiles = task.addedDeletes.pos
	added.EqualityDeleteFiles = task.addedDeletes.eq
	added.DeletionVectorFiles = task.addedDeletes.dv

	return scan.readFileTasks(ctx, []FileScanTask{base}, &added)
}

func (as *arrowScan) prepareAddedDeleteLoaders(invariants *arrowScanInvariants) error {
	tasks := []FileScanTask{*as.addedDeletes}
	as.addedPosLoader = newLazyPositionDeleteLoader(as.fs, tasks)

	dvLoader, err := newLazyDeletionVectorLoader(as.fs, tasks)
	if err != nil {
		as.addedPosLoader.release()
		as.addedPosLoader = nil

		return err
	}
	as.addedDVLoader = dvLoader

	eqLoader, err := newLazyEqualityDeleteLoader(
		as.fs, invariants.tableSchema, nil, invariants.nameMapping, tasks)
	if err != nil {
		as.addedPosLoader.release()
		as.addedPosLoader = nil

		return err
	}
	if eqLoader.needsSchemaHistory() {
		eqLoader.tableSchemas = as.metadata.Schemas()
	}
	eqLoader.addFieldIDs(invariants.projectedIDs)
	as.addedEqLoader = eqLoader

	return nil
}

func (as *arrowScan) loadAddedDeletes(ctx context.Context, task FileScanTask) (positionDeletes, *dv.RoaringPositionBitmap, []*equalityDeleteSet, error) {
	if as.addedDeletes == nil {
		return nil, nil, nil, nil
	}

	dataPath := ""
	if task.File != nil {
		dataPath = task.File.FilePath()
	}
	if as.addedDeletes.File == nil || as.addedDeletes.File.FilePath() != dataPath {
		return nil, nil, nil, fmt.Errorf("%w: added delete files do not match data file %s",
			iceberg.ErrInvalidArgument, dataPath)
	}

	pos, err := as.addedPosLoader.load(ctx, *as.addedDeletes)
	if err != nil {
		return nil, nil, nil, err
	}
	bitmap, err := as.addedDVLoader.load(ctx, dataPath)
	if err != nil {
		return nil, nil, nil, err
	}
	eq, err := as.addedEqLoader.load(ctx, *as.addedDeletes)
	if err != nil {
		return nil, nil, nil, err
	}

	return pos, bitmap, eq, nil
}

func stampChangelogFilePos(ctx context.Context, cursor *rowPositionCursor) recProcessFn {
	return func(in arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer in.Release()

		mem := compute.GetAllocator(ctx)
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		n := int(in.NumRows())
		bldr.Reserve(n)
		for range n {
			bldr.Append(cursor.next())
		}
		pos := bldr.NewArray()
		defer pos.Release()

		schema, err := in.Schema().AddField(int(in.NumCols()), arrow.Field{
			Name:     changelogFilePosColumn,
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		})
		if err != nil {
			return nil, err
		}

		cols := make([]arrow.Array, in.NumCols()+1)
		copy(cols, in.Columns())
		cols[in.NumCols()] = pos

		return array.NewRecordBatch(schema, cols, in.NumRows()), nil
	}
}

func dropChangelogFilePos(in arrow.RecordBatch) (arrow.RecordBatch, error) {
	defer in.Release()

	n := int(in.NumCols())
	if n == 0 {
		in.Retain()

		return in, nil
	}

	fields := in.Schema().Fields()
	schema := arrow.NewSchema(fields[:n-1], nil)
	cols := make([]arrow.Array, n-1)
	copy(cols, in.Columns()[:n-1])

	return array.NewRecordBatch(schema, cols, in.NumRows()), nil
}

// keepAddedDeleteRows keeps rows removed by the added deletes. Existing
// deletes have already dropped rows that were removed earlier. A deletion
// vector supersedes added position deletes. Equality deletes are combined
// with that position check: a row is kept when either kind matches.
func keepAddedDeleteRows(
	ctx context.Context,
	positions set[int64],
	hasPos bool,
	bitmap *dv.RoaringPositionBitmap,
	rowCount int64,
	eqSets []*equalityDeleteSet,
	fileSchema *iceberg.Schema,
	dataFilePath string,
	stampPos bool,
) recProcessFn {
	var keepBits []byte
	if bitmap != nil && !bitmap.IsEmpty() {
		keepBits = bitmap.KeepMaskBytes(rowCount)
		hasPos = false
	}

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		n := int(r.NumRows())
		if n == 0 {
			r.Retain()

			return r, nil
		}

		var posCol *array.Int64
		if stampPos {
			col, ok := r.Column(int(r.NumCols()) - 1).(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("%w: changelog file position column is %s",
					iceberg.ErrInvalidSchema, r.Column(int(r.NumCols())-1).DataType())
			}
			posCol = col
		}

		keep := make([]bool, n)
		switch {
		case keepBits != nil:
			for i := range n {
				pos := posCol.Value(i)
				keep[i] = pos >= 0 && pos < rowCount && !deletionVectorKeepsRow(keepBits, rowCount, pos)
			}
		case hasPos:
			for i := range n {
				_, keep[i] = positions[posCol.Value(i)]
			}
		}

		if len(eqSets) > 0 {
			matched, err := equalityRowsMatched(r, eqSets, fileSchema, dataFilePath)
			if err != nil {
				return nil, err
			}
			for i := range n {
				keep[i] = keep[i] || matched[i]
			}
		}

		mem := compute.GetAllocator(ctx)
		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()
		bldr.Reserve(n)
		bldr.AppendValues(keep, nil)
		mask := bldr.NewBooleanArray()
		defer mask.Release()

		return compute.FilterRecordBatch(ctx, r, mask, compute.DefaultFilterOptions())
	}
}

// equalityRowsMatched reports rows that match an added equality delete. Keys use
// the same encoding as the scan filter that drops equality-deleted rows.
func equalityRowsMatched(r arrow.RecordBatch, eqDeleteSets []*equalityDeleteSet, fileSchema *iceberg.Schema, dataFilePath string) ([]bool, error) {
	requestedFieldIDs := make([]int, 0)
	requestedFieldIDSet := make(map[int]struct{})
	for _, eqDel := range eqDeleteSets {
		if len(eqDel.fieldIDs) != len(eqDel.colNames) {
			return nil, fmt.Errorf("%w: equality delete set has %d field IDs and %d column names",
				iceberg.ErrInvalidArgument, len(eqDel.fieldIDs), len(eqDel.colNames))
		}
		for _, fieldID := range eqDel.fieldIDs {
			if _, ok := requestedFieldIDSet[fieldID]; !ok {
				requestedFieldIDSet[fieldID] = struct{}{}
				requestedFieldIDs = append(requestedFieldIDs, fieldID)
			}
		}
	}

	fieldRefsByID := resolveArrowFieldsByID(fileSchema, requestedFieldIDs)
	fieldRefs := make([][]arrowFieldRef, len(eqDeleteSets))
	for i, eqDel := range eqDeleteSets {
		fieldRefs[i] = make([]arrowFieldRef, len(eqDel.fieldIDs))
		for fieldIdx, fieldID := range eqDel.fieldIDs {
			ref, err := resolveArrowField(fieldRefsByID, fieldID, eqDel.colNames[fieldIdx], dataFilePath)
			if err != nil {
				return nil, err
			}
			fieldRefs[i][fieldIdx] = ref
		}
	}

	matched := make([]bool, r.NumRows())
	var keyBuf bytes.Buffer
	for setIdx, eqDel := range eqDeleteSets {
		encoders := make([]colEncoder, len(eqDel.colNames))
		for i, name := range eqDel.colNames {
			enc, err := makeArrowFieldEncoder(r, fieldRefs[setIdx][i], eqDel.fieldIDs[i], name, dataFilePath)
			if err != nil {
				return nil, err
			}
			encoders[i] = enc
		}

		for row := range int(r.NumRows()) {
			if matched[row] {
				continue
			}
			keyBuf.Reset()
			for _, enc := range encoders {
				enc(&keyBuf, row)
			}
			if _, ok := eqDel.keys[bufString(&keyBuf)]; ok {
				matched[row] = true
			}
		}
	}

	return matched, nil
}
