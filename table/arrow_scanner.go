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
	"io"
	"iter"
	"strconv"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/substrait-io/substrait-go/v8/expr"
	"golang.org/x/sync/errgroup"
)

const (
	ScanOptionArrowUseLargeTypes = "arrow.use_large_types"
	ScanOptionRowLineageEnabled  = "row_lineage.enabled"
)

var PositionalDeleteArrowSchema, _ = SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)

type (
	positionDeletes   = []*arrow.Chunked
	perFilePosDeletes = map[string]positionDeletes
)

func readAllDeleteFiles(ctx context.Context, fs iceio.IO, tasks []FileScanTask, concurrency int) (perFilePosDeletes, error) {
	deletesPerFile := make(perFilePosDeletes)
	uniqueDeletes := make(map[string]iceberg.DataFile)

	for _, t := range tasks {
		for _, d := range t.DeleteFiles {
			if d.ContentType() != iceberg.EntryContentPosDeletes {
				continue
			}

			if _, ok := uniqueDeletes[d.FilePath()]; !ok {
				uniqueDeletes[d.FilePath()] = d
			}
		}
	}

	if len(uniqueDeletes) == 0 {
		return deletesPerFile, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	perFileChan := make(chan map[string]*arrow.Chunked, concurrency)
	go func() {
		// Inner g.Wait() gates the deferred channel close so a late worker
		// can't send on a closed channel. Outer g.Wait() below collects the
		// error — no cross-goroutine shared err variable.
		defer close(perFileChan)
		for _, v := range uniqueDeletes {
			g.Go(func() error {
				deletes, err := readDeletes(gctx, fs, v)
				if err != nil {
					return err
				}
				if deletes == nil {
					return nil
				}
				select {
				case perFileChan <- deletes:
					return nil
				case <-gctx.Done():
					return gctx.Err()
				}
			})
		}
		_ = g.Wait()
	}()

	for deletes := range perFileChan {
		for file, arr := range deletes {
			deletesPerFile[file] = append(deletesPerFile[file], arr)
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return deletesPerFile, nil
}

// perFileDVBitmaps maps each data-file path to the deletion-vector bitmap
// that applies to it. Kept separate from perFilePosDeletes so the row-filter
// pipeline can use compute.Filter on a Boolean mask built from Contains()
// directly, instead of materializing positions into a set[int64] + Take.
type perFileDVBitmaps = map[string]*dv.RoaringPositionBitmap

// readAllDeletionVectors reads every deletion-vector puffin blob referenced
// by the input tasks and returns a perFileDVBitmaps map keyed by the
// referenced data-file path.
//
// Dedup is by referenced-data-file path, not by puffin file path: a single
// puffin file can carry multiple DV blobs (one per data file). Keying by the
// puffin path would silently drop all but the first blob. This matches Java's
// DeleteFileIndex.findDV, which keys by data-file path. As a side-effect we
// can detect spec violations: two distinct DV blobs targeting the same data
// file is rejected (mirrors Java's "Can't index multiple DVs for %s"
// ValidationException — over-deletion risk if silently unioned).
//
// Validation happens up front, before any goroutines are launched, so the
// goroutine fan-out has no early-exit path. (An early return after g.Go
// dispatches but before g.Wait would close resultsChan while in-flight
// workers were still sending, panicking with "send on closed channel".)
func readAllDeletionVectors(ctx context.Context, fs iceio.IO, tasks []FileScanTask, concurrency int) (perFileDVBitmaps, error) {
	out := make(perFileDVBitmaps)
	uniqueDVs := make(map[string]iceberg.DataFile)

	for _, t := range tasks {
		for _, d := range t.DeletionVectorFiles {
			ref := d.ReferencedDataFile()
			if ref == nil {
				return nil, fmt.Errorf("deletion vector %s missing referenced_data_file", d.FilePath())
			}
			if d.ContentOffset() == nil || d.ContentSizeInBytes() == nil {
				// Spec §Manifest Files: content_offset and content_size_in_
				// bytes are required for DV entries. Surface the missing-
				// field cause directly here — otherwise the dedup check
				// below would produce the misleading "multiple deletion
				// vectors" error when two equally-broken entries collide.
				return nil, fmt.Errorf("deletion vector %s missing content_offset/content_size_in_bytes", d.FilePath())
			}
			if existing, seen := uniqueDVs[*ref]; seen {
				if !sameDVBlob(existing, d) {
					return nil, fmt.Errorf(
						"multiple deletion vectors for data file %s: %s and %s",
						*ref, existing.FilePath(), d.FilePath())
				}

				continue
			}
			uniqueDVs[*ref] = d
		}
	}

	if len(uniqueDVs) == 0 {
		return out, nil
	}

	type dvResult struct {
		referencedDataFile string
		bitmap             *dv.RoaringPositionBitmap
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	resultsChan := make(chan dvResult, concurrency)
	go func() {
		// g.Wait() before the deferred close so workers finish sending
		// before the channel is closed — otherwise a late worker would
		// panic on send to a closed channel. The error is collected via
		// the outer g.Wait() below, not stored on a shared variable.
		defer close(resultsChan)
		for ref, dvFile := range uniqueDVs {
			g.Go(func() error {
				bitmap, err := dv.ReadDV(fs, dvFile)
				if err != nil {
					return fmt.Errorf("read deletion vector %s: %w", dvFile.FilePath(), err)
				}
				select {
				case resultsChan <- dvResult{referencedDataFile: ref, bitmap: bitmap}:
					return nil
				case <-gctx.Done():
					return gctx.Err()
				}
			})
		}
		_ = g.Wait()
	}()

	for r := range resultsChan {
		out[r.referencedDataFile] = r.bitmap
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return out, nil
}

// sameDVBlob reports whether two DV manifest entries point at the same puffin
// blob — identical puffin file path and content offset. Different of either
// means two distinct DVs for the same data file, the over-deletion case
// readAllDeletionVectors rejects.
//
// Java's DeleteFileIndex is stricter: any second DV for the same data file is
// rejected with ValidationException("Can't index multiple DVs for %s"), even
// when both entries reference the same underlying blob. Same-blob dedup here
// is a deliberate divergence — reading the same blob twice is wasteful, not
// incorrect. ContentOffset is required to be non-nil by the spec and by the
// pre-pass in readAllDeletionVectors, so the comparison below assumes both.
func sameDVBlob(a, b iceberg.DataFile) bool {
	if a.FilePath() != b.FilePath() {
		return false
	}

	return *a.ContentOffset() == *b.ContentOffset()
}

func readDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (_ map[string]*arrow.Chunked, err error) {
	src, err := internal.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	tbl, err = array.UnifyTableDicts(compute.GetAllocator(ctx), tbl)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	filePathCol := tbl.Column(tbl.Schema().FieldIndices("file_path")[0]).Data()
	posCol := tbl.Column(tbl.Schema().FieldIndices("pos")[0]).Data()
	dict := filePathCol.Chunk(0).(*array.Dictionary).Dictionary().(*array.String)

	results := make(map[string]*arrow.Chunked)
	for i := 0; i < dict.Len(); i++ {
		v := dict.Value(i)

		mask, err := compute.CallFunction(ctx, "equal", nil,
			compute.NewDatumWithoutOwning(filePathCol), compute.NewDatum(v))
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(posCol),
			mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		results[v] = filtered.(*compute.ChunkedDatum).Value
	}

	return results, nil
}

type set[T comparable] map[T]struct{}

func combinePositionalDeletes(mem memory.Allocator, deletes set[int64], start, end int64) arrow.Array {
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()

	for i := start; i < end; i++ {
		if _, ok := deletes[i]; !ok {
			bldr.Append(i)
		}
	}

	return bldr.NewArray()
}

type recProcessFn func(arrow.RecordBatch) (arrow.RecordBatch, error)

func processPositionalDeletes(ctx context.Context, deletes set[int64]) recProcessFn {
	nextIdx, mem := int64(0), compute.GetAllocator(ctx)

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		currentIdx := nextIdx
		nextIdx += r.NumRows()

		indices := combinePositionalDeletes(mem, deletes, currentIdx, nextIdx)
		defer indices.Release()

		out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			compute.NewDatumWithoutOwning(r), compute.NewDatumWithoutOwning(indices))
		if err != nil {
			return nil, err
		}

		return out.(*compute.RecordDatum).Value, nil
	}
}

// filterByDeletionVector returns a pipeline step that drops rows present in
// the bitmap by building a per-batch Arrow Boolean keep-mask and applying
// compute.Filter. Preferred over processPositionalDeletes for DV-sourced
// deletes because RoaringPositionBitmap.Contains is O(1) and Filter with a
// bit-packed boolean mask is more vectorized than Take with an int64 index
// array. Tracks absolute row position across batches via a closure-captured
// counter, identical pattern to processPositionalDeletes.
func filterByDeletionVector(ctx context.Context, bitmap *dv.RoaringPositionBitmap) recProcessFn {
	nextIdx, mem := int64(0), compute.GetAllocator(ctx)

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		currentIdx := nextIdx
		nextIdx += r.NumRows()

		maskBuilder := array.NewBooleanBuilder(mem)
		defer maskBuilder.Release()
		maskBuilder.Reserve(int(r.NumRows()))
		for i := int64(0); i < r.NumRows(); i++ {
			// mask[i] = keep row i? → row i of the batch is at absolute
			// position currentIdx+i in the source file; keep if NOT in
			// the deletion vector.
			maskBuilder.Append(!bitmap.Contains(uint64(currentIdx + i)))
		}
		mask := maskBuilder.NewBooleanArray()
		defer mask.Release()

		out, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(r),
			compute.NewDatumWithoutOwning(mask), *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		return out.(*compute.RecordDatum).Value, nil
	}
}

// enrichRecordsWithPosDeleteFields enriches a RecordBatch with the columns declared in the PositionalDeleteArrowSchema
// so that during the pipeline filtering stages that sheds filtered out records, we still have a way to
// preserve the original position of those records.
func enrichRecordsWithPosDeleteFields(ctx context.Context, filePath iceberg.DataFile) recProcessFn {
	nextIdx, mem := int64(0), compute.GetAllocator(ctx)

	return func(inData arrow.RecordBatch) (outData arrow.RecordBatch, err error) {
		defer inData.Release()

		schema := inData.Schema()
		fieldIdx := schema.NumFields()
		schema, err = schema.AddField(fieldIdx, PositionalDeleteArrowSchema.Field(0))
		if err != nil {
			return nil, err
		}
		schema, err = schema.AddField(fieldIdx+1, PositionalDeleteArrowSchema.Field(1))
		if err != nil {
			return nil, err
		}

		rb := array.NewRecordBuilder(mem, PositionalDeleteArrowSchema)
		defer rb.Release()

		filePathBldr, posBldr := rb.Field(0).(*array.StringBuilder), rb.Field(1).(*array.Int64Builder)

		startPos := nextIdx
		nextIdx += inData.NumRows()

		for i := startPos; i < nextIdx; i++ {
			filePathBldr.Append(filePath.FilePath())
			posBldr.Append(i)
		}

		newCols := rb.NewRecordBatch()
		defer newCols.Release()

		columns := append(inData.Columns(), newCols.Column(0), newCols.Column(1))
		outData = array.NewRecordBatch(schema, columns, inData.NumRows())

		return outData, err
	}
}

func filterRecords(ctx context.Context, recordFilter expr.Expression) recProcessFn {
	return func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer rec.Release()

		input := compute.NewDatumWithoutOwning(rec)
		mask, err := exprs.ExecuteScalarExpression(ctx, rec.Schema(), recordFilter, input)
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		result, err := compute.Filter(ctx, input, mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		return result.(*compute.RecordDatum).Value, nil
	}
}

type arrowScan struct {
	fs              iceio.IO
	metadata        Metadata
	projectedSchema *iceberg.Schema
	boundRowFilter  iceberg.BooleanExpression
	caseSensitive   bool
	rowLimit        int64
	options         iceberg.Properties

	useLargeTypes bool
	concurrency   int

	nameMapping iceberg.NameMapping
}

// collectLeafIDs recursively collects leaf field IDs from a type
func collectLeafIDs(typ iceberg.Type, fieldID int, idset set[int]) {
	switch t := typ.(type) {
	case *iceberg.MapType:
		// For maps, collect leaf IDs from both key and value
		collectLeafIDs(t.KeyType, t.KeyID, idset)
		collectLeafIDs(t.ValueType, t.ValueID, idset)
	case *iceberg.ListType:
		// For lists, collect leaf IDs from the element
		collectLeafIDs(t.Element, t.ElementID, idset)
	case *iceberg.StructType:
		// For structs, collect leaf IDs from all fields
		for _, field := range t.FieldList {
			collectLeafIDs(field.Type, field.ID, idset)
		}
	default:
		// Primitive type - this is a leaf
		idset[fieldID] = struct{}{}
	}
}

func (as *arrowScan) projectedFieldIDs() (set[int], error) {
	idset := set[int]{}
	// Collect leaf field IDs for column pruning.
	// For nested types (map, list, struct), we recursively descend to find
	// the actual leaf primitive fields, not the intermediate container nodes.
	for _, field := range as.projectedSchema.Fields() {
		collectLeafIDs(field.Type, field.ID, idset)
	}

	if as.boundRowFilter != nil {
		extracted, err := iceberg.ExtractFieldIDs(as.boundRowFilter)
		if err != nil {
			return nil, err
		}

		for _, id := range extracted {
			idset[id] = struct{}{}
		}
	}

	return idset, nil
}

type enumeratedRecord struct {
	Record internal.Enumerated[arrow.RecordBatch]
	Task   internal.Enumerated[FileScanTask]
	Err    error
}

func (as *arrowScan) prepareToRead(ctx context.Context, file iceberg.DataFile) (*iceberg.Schema, []int, internal.FileReader, error) {
	ids, err := as.projectedFieldIDs()
	if err != nil {
		return nil, nil, nil, err
	}

	src, err := internal.GetFile(ctx, as.fs, file, false)
	if err != nil {
		return nil, nil, nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	fileSchema, colIndices, err := rdr.PrunedSchema(ids, as.nameMapping)
	if err != nil {
		rdr.Close()

		return nil, nil, nil, err
	}

	iceSchema, err := ArrowSchemaToIceberg(fileSchema, false, as.nameMapping)
	if err != nil {
		rdr.Close()

		return nil, nil, nil, err
	}

	return iceSchema, colIndices, rdr, nil
}

func (as *arrowScan) getRecordFilter(ctx context.Context, fileSchema *iceberg.Schema) (recProcessFn, bool, error) {
	if as.boundRowFilter == nil || as.boundRowFilter.Equals(iceberg.AlwaysTrue{}) {
		return nil, false, nil
	}

	translatedFilter, err := iceberg.TranslateColumnNames(as.boundRowFilter, fileSchema)
	if err != nil {
		return nil, false, err
	}

	if translatedFilter.Equals(iceberg.AlwaysFalse{}) {
		return nil, true, nil
	}

	translatedFilter, err = iceberg.BindExpr(fileSchema, translatedFilter, as.caseSensitive)
	if err != nil {
		return nil, false, err
	}

	if !translatedFilter.Equals(iceberg.AlwaysTrue{}) {
		extSet, recordFilter, err := substrait.ConvertExpr(fileSchema, translatedFilter, as.caseSensitive)
		if err != nil {
			return nil, false, err
		}

		ctx = exprs.WithExtensionIDSet(ctx, exprs.NewExtensionSetDefault(*extSet))

		return filterRecords(ctx, recordFilter), false, nil
	}

	return nil, false, nil
}

// synthesizeRowLineageColumns fills _row_id and _last_updated_sequence_number from task constants
// when those columns are present in the batch (e.g. from ToRequestedSchema). Per the Iceberg v3
// row lineage spec: if the value is null in the file, it is inherited (synthesized) from the file's
// first_row_id and data_sequence_number; otherwise the value from the file is kept.
// rowOffset is the 0-based row index within the current file and is updated so _row_id stays
// correct across multiple batches from the same file (first_row_id + row_position).
func synthesizeRowLineageColumns(
	ctx context.Context,
	rowOffset *int64,
	task FileScanTask,
	batch arrow.RecordBatch,
) (arrow.RecordBatch, error) {
	alloc := compute.GetAllocator(ctx)
	schema := batch.Schema()
	nrows := batch.NumRows()

	// Start from the existing columns; we'll replace the row lineage columns in-place
	// when we need to synthesize values.
	newCols := append([]arrow.Array(nil), batch.Columns()...)

	// Resolve column indices by name; -1 if not present.
	rowIDIndices := schema.FieldIndices(iceberg.RowIDColumnName)
	seqNumIndices := schema.FieldIndices(iceberg.LastUpdatedSequenceNumberColumnName)
	rowIDColIdx := -1
	if len(rowIDIndices) > 0 {
		rowIDColIdx = rowIDIndices[0]
	}
	seqNumColIdx := -1
	if len(seqNumIndices) > 0 {
		seqNumColIdx = seqNumIndices[0]
	}

	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()

	// _row_id: inherit first_row_id + row_position when null; else keep value from file.
	if rowIDColIdx >= 0 && task.FirstRowID != nil {
		if col, ok := newCols[rowIDColIdx].(*array.Int64); ok {
			bldr.Reserve(int(nrows))
			first := *task.FirstRowID
			for k := range nrows {
				if col.IsNull(int(k)) {
					bldr.Append(first + *rowOffset + int64(k))
				} else {
					bldr.Append(col.Value(int(k)))
				}
			}

			arr := bldr.NewArray()
			newCols[rowIDColIdx] = arr
			defer arr.Release()
		}
	}

	// _last_updated_sequence_number: inherit file's data_sequence_number when null; else keep value from file.
	if seqNumColIdx >= 0 && task.DataSequenceNumber != nil {
		if col, ok := newCols[seqNumColIdx].(*array.Int64); ok {
			bldr.Reserve(int(nrows))
			seq := *task.DataSequenceNumber
			for k := range nrows {
				if col.IsNull(int(k)) {
					bldr.Append(seq)
				} else {
					bldr.Append(col.Value(int(k)))
				}
			}

			arr := bldr.NewArray()
			newCols[seqNumColIdx] = arr
			defer arr.Release()
		}
	}

	// Advance so the next batch from this file uses the correct row position for _row_id.
	*rowOffset += nrows

	rec := array.NewRecordBatch(schema, newCols, nrows)

	return rec, nil
}

func (as *arrowScan) processRecords(
	ctx context.Context,
	task internal.Enumerated[FileScanTask],
	fileSchema *iceberg.Schema,
	rdr internal.FileReader,
	columns []int,
	pipeline []recProcessFn,
	out chan<- enumeratedRecord,
) (err error) {
	var (
		testRowGroups any
		recRdr        array.RecordReader
	)

	switch task.Value.File.FileFormat() {
	case iceberg.ParquetFile:
		statsFn, err := newParquetRowGroupStatsEvaluator(fileSchema, as.boundRowFilter, false)
		if err != nil {
			return err
		}

		bloomPreds, err := newBloomFilterPredicates(as.boundRowFilter)
		if err != nil {
			return err
		}

		testRowGroups = &internal.ParquetRowGroupTester{
			StatsFn:    statsFn,
			BloomPreds: bloomPreds,
		}
	}

	recRdr, err = rdr.GetRecords(ctx, columns, testRowGroups)
	if err != nil {
		return err
	}
	defer recRdr.Release()

	var (
		idx  int
		prev arrow.RecordBatch
	)

	for recRdr.Next() {
		if prev != nil {
			out <- enumeratedRecord{Record: internal.Enumerated[arrow.RecordBatch]{
				Value: prev, Index: idx, Last: false,
			}, Task: task}
			idx++
		}

		prev = recRdr.RecordBatch()
		prev.Retain()

		for _, f := range pipeline {
			prev, err = f(prev)
			if err != nil {
				return err
			}
		}
	}

	if prev != nil {
		out <- enumeratedRecord{Record: internal.Enumerated[arrow.RecordBatch]{
			Value: prev, Index: idx, Last: true,
		}, Task: task}
	}

	if recRdr.Err() != nil && recRdr.Err() != io.EOF {
		err = recRdr.Err()
	}

	return err
}

func (as *arrowScan) recordsFromTask(ctx context.Context, task internal.Enumerated[FileScanTask], out chan<- enumeratedRecord, positionalDeletes positionDeletes, dvBitmap *dv.RoaringPositionBitmap, eqDeleteSets []*equalityDeleteSet) (err error) {
	defer func() {
		if err != nil {
			out <- enumeratedRecord{Task: task, Err: err}
		}
	}()

	var (
		rdr        internal.FileReader
		iceSchema  *iceberg.Schema
		colIndices []int
		filterFunc recProcessFn
		dropFile   bool
	)

	iceSchema, colIndices, rdr, err = as.prepareToRead(ctx, task.Value.File)
	if err != nil {
		return err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	pipeline := make([]recProcessFn, 0, 3)
	if len(positionalDeletes) > 0 {
		deletes := set[int64]{}
		for _, chunk := range positionalDeletes {
			for _, a := range chunk.Chunks() {
				for _, v := range a.(*array.Int64).Int64Values() {
					deletes[v] = struct{}{}
				}
			}
		}

		pipeline = append(pipeline, processPositionalDeletes(ctx, deletes))
	}

	// PlanFiles skips Parquet pos-delete matching for any data file that
	// has a DV (per spec), so in practice the two are mutually exclusive
	// per task. Append after the pos-delete step anyway so a manually-
	// constructed task with both sources still gets both filters applied.
	if dvBitmap != nil && !dvBitmap.IsEmpty() {
		pipeline = append(pipeline, filterByDeletionVector(ctx, dvBitmap))
	}

	if len(eqDeleteSets) > 0 {
		eqFn, eqErr := processEqualityDeletes(ctx, eqDeleteSets)
		if eqErr != nil {
			return eqErr
		}

		pipeline = append(pipeline, eqFn)
	}

	filterFunc, dropFile, err = as.getRecordFilter(ctx, iceSchema)
	if err != nil {
		return err
	}

	if dropFile {
		var emptySchema *arrow.Schema
		emptySchema, err = SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
		if err != nil {
			return err
		}
		out <- enumeratedRecord{Task: task, Record: internal.Enumerated[arrow.RecordBatch]{
			Value: array.NewRecordBatch(emptySchema, nil, 0), Index: 0, Last: true,
		}}

		return err
	}

	if filterFunc != nil {
		pipeline = append(pipeline, filterFunc)
	}

	pipeline = append(pipeline, func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		return ToRequestedSchema(ctx, as.projectedSchema, iceSchema, r, SchemaOptions{UseLargeTypes: as.useLargeTypes})
	})

	// Row lineage: optionally fill _row_id and _last_updated_sequence_number from task
	// constants when in projection.
	rowLineageEnabled, err := strconv.ParseBool(as.options.Get(ScanOptionRowLineageEnabled, "true"))
	if err != nil {
		rowLineageEnabled = true
	}
	if rowLineageEnabled && (task.Value.FirstRowID != nil || task.Value.DataSequenceNumber != nil) {
		var rowOffset int64
		taskVal := task.Value
		pipeline = append(pipeline, func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
			defer r.Release()

			return synthesizeRowLineageColumns(ctx, &rowOffset, taskVal, r)
		})
	}

	err = as.processRecords(ctx, task, iceSchema, rdr, colIndices, pipeline, out)

	return err
}

func (as *arrowScan) producePosDeletesFromTask(ctx context.Context, task internal.Enumerated[FileScanTask], positionalDeletes positionDeletes, out chan<- enumeratedRecord) (err error) {
	defer func() {
		if err != nil {
			out <- enumeratedRecord{Task: task, Err: err}
		}
	}()

	var (
		rdr        internal.FileReader
		iceSchema  *iceberg.Schema
		colIndices []int
		filterFunc recProcessFn
		dropFile   bool
	)

	iceSchema, colIndices, rdr, err = as.prepareToRead(ctx, task.Value.File)
	if err != nil {
		return err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	fields := append(iceSchema.Fields(), iceberg.PositionalDeleteSchema.Fields()...)
	enrichedIcebergSchema := iceberg.NewSchema(iceSchema.ID+1, fields...)

	pipeline := make([]recProcessFn, 0, 2)
	pipeline = append(pipeline, enrichRecordsWithPosDeleteFields(ctx, task.Value.File))
	if len(positionalDeletes) > 0 {
		deletes := set[int64]{}
		for _, chunk := range positionalDeletes {
			for _, a := range chunk.Chunks() {
				for _, v := range a.(*array.Int64).Int64Values() {
					deletes[v] = struct{}{}
				}
			}
		}

		pipeline = append(pipeline, processPositionalDeletes(ctx, deletes))
	}

	filterFunc, dropFile, err = as.getRecordFilter(ctx, iceSchema)
	if err != nil {
		return err
	}

	// Nothing to delete in a dropped file
	if dropFile {
		var emptySchema *arrow.Schema
		emptySchema, err = SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, false, as.useLargeTypes)
		if err != nil {
			return err
		}
		out <- enumeratedRecord{Task: task, Record: internal.Enumerated[arrow.RecordBatch]{
			Value: array.NewRecordBatch(emptySchema, nil, 0), Index: 0, Last: true,
		}}

		return err
	}

	if filterFunc != nil {
		pipeline = append(pipeline, filterFunc)
	}
	pipeline = append(pipeline, func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		return ToRequestedSchema(ctx, iceberg.PositionalDeleteSchema, enrichedIcebergSchema, r, SchemaOptions{IncludeFieldIDs: true, UseLargeTypes: as.useLargeTypes})
	})

	err = as.processRecords(ctx, task, iceSchema, rdr, colIndices, pipeline, out)

	return err
}

func createIterator(ctx context.Context, numWorkers uint, records <-chan enumeratedRecord, deletesPerFile perFilePosDeletes, cancel context.CancelCauseFunc, rowLimit int64) iter.Seq2[arrow.RecordBatch, error] {
	isBeforeAny := func(batch enumeratedRecord) bool {
		return batch.Task.Index < 0
	}

	sequenced := internal.MakeSequencedChan(uint(numWorkers), records,
		func(left, right *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*left):
				return true
			case isBeforeAny(*right):
				return false
			case left.Err != nil || right.Err != nil:
				return true
			case left.Task.Index == right.Task.Index:
				return left.Record.Index < right.Record.Index
			default:
				return left.Task.Index < right.Task.Index
			}
		}, func(prev, next *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*prev):
				return next.Task.Index == 0 && next.Record.Index == 0
			case next.Err != nil:
				return true
			case prev.Task.Index == next.Task.Index:
				return next.Record.Index == prev.Record.Index+1
			default:
				return next.Task.Index == prev.Task.Index+1 &&
					prev.Record.Last && next.Record.Index == 0
			}
		}, enumeratedRecord{Task: internal.Enumerated[FileScanTask]{Index: -1}})

	totalRowCount := int64(0)

	return func(yield func(arrow.RecordBatch, error) bool) {
		defer func() {
			for rec := range sequenced {
				if rec.Record.Value != nil {
					rec.Record.Value.Release()
				}
			}

			for _, v := range deletesPerFile {
				for _, chunk := range v {
					chunk.Release()
				}
			}
		}()

		defer cancel(nil)

		for {
			select {
			case <-ctx.Done():
				if err := context.Cause(ctx); err != nil {
					yield(nil, err)
				}

				return
			case enum, ok := <-sequenced:
				if !ok {
					return
				}

				if enum.Err != nil {
					yield(nil, enum.Err)

					return
				}

				rec := enum.Record.Value
				if rowLimit > 0 {
					if totalRowCount >= rowLimit {
						rec.Release()

						return
					} else if totalRowCount+rec.NumRows() > rowLimit {
						defer rec.Release()
						rec = rec.NewSlice(0, rowLimit-totalRowCount)
					}
				}

				if rec.NumRows() == 0 {
					// skip empty records
					continue
				}

				if !yield(rec, nil) {
					return
				}
				totalRowCount += rec.NumRows()
				if rowLimit > 0 && totalRowCount >= rowLimit {
					return
				}
			}
		}
	}
}

func (as *arrowScan) recordBatchesFromTasksAndDeletes(ctx context.Context, tasks []FileScanTask, deletesPerFile perFilePosDeletes, dvBitmaps perFileDVBitmaps, eqDeleteSets map[int][]*equalityDeleteSet) iter.Seq2[arrow.RecordBatch, error] {
	extSet := substrait.NewExtensionSet()
	as.nameMapping = as.metadata.NameMapping()

	ctx, cancel := context.WithCancelCause(exprs.WithExtensionIDSet(ctx, extSet))
	taskChan := make(chan internal.Enumerated[FileScanTask], len(tasks))

	// numWorkers := 1
	numWorkers := min(as.concurrency, len(tasks))
	records := make(chan enumeratedRecord, numWorkers)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}

					filePath := task.Value.File.FilePath()
					if err := as.recordsFromTask(ctx, task, records,
						deletesPerFile[filePath],
						dvBitmaps[filePath],
						eqDeleteSets[task.Index]); err != nil {
						cancel(err)

						return
					}
				}
			}
		}()
	}

	go func() {
		for i, t := range tasks {
			taskChan <- internal.Enumerated[FileScanTask]{
				Value: t, Index: i, Last: i == len(tasks)-1,
			}
		}
		close(taskChan)

		wg.Wait()
		close(records)
	}()

	return createIterator(ctx, uint(numWorkers), records, deletesPerFile,
		cancel, as.rowLimit)
}

func (as *arrowScan) GetRecords(ctx context.Context, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	var err error
	as.useLargeTypes, err = strconv.ParseBool(as.options.Get(ScanOptionArrowUseLargeTypes, "false"))
	if err != nil {
		as.useLargeTypes = false
	}

	ctx = internal.WithTableProperties(ctx, as.metadata.Properties())

	resultSchema, err := SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
	if err != nil {
		return nil, nil, err
	}

	if as.rowLimit == 0 {
		return resultSchema, func(yield func(arrow.RecordBatch, error) bool) {}, nil
	}

	deletesPerFile, err := readAllDeleteFiles(ctx, as.fs, tasks, as.concurrency)
	if err != nil {
		return nil, nil, err
	}

	// DV bitmaps stay in their native form rather than being materialized
	// into int64 positions and merged with the Parquet pos-delete map.
	// filterByDeletionVector applies the bitmap to each batch via a Boolean
	// keep-mask + compute.Filter — O(1) Contains lookups, vectorized Filter,
	// no intermediate position set.
	dvBitmaps, err := readAllDeletionVectors(ctx, as.fs, tasks, as.concurrency)
	if err != nil {
		return nil, nil, err
	}

	eqDeleteSets, err := readAllEqualityDeleteFiles(ctx, as.fs,
		as.metadata.CurrentSchema(), tasks, as.concurrency)
	if err != nil {
		return nil, nil, err
	}

	return resultSchema, as.recordBatchesFromTasksAndDeletes(ctx, tasks, deletesPerFile, dvBitmaps, eqDeleteSets), nil
}
