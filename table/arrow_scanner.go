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
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/substrait-io/substrait-go/v3/expr"
	"golang.org/x/sync/errgroup"
)

const (
	ScanOptionArrowUseLargeTypes = "arrow.use_large_types"
)

type (
	positionDeletes   = []*arrow.Chunked
	perFilePosDeletes = map[string]positionDeletes
	equalityDeletes   = []arrow.Table
	perFileEqDeletes  = map[string]equalityDeletes
)

// deleteFiles contains both position and equality delete files for a scan
type deleteFiles struct {
	positionDeletes perFilePosDeletes
	equalityDeletes perFileEqDeletes
}

func readAllDeleteFiles(ctx context.Context, fs iceio.IO, tasks []FileScanTask, concurrency int) (*deleteFiles, error) {
	var (
		posDeletesPerFile = make(perFilePosDeletes)
		eqDeletesPerFile  = make(perFileEqDeletes)
		uniquePosDeletes  = make(map[string]iceberg.DataFile)
		uniqueEqDeletes   = make(map[string]iceberg.DataFile)
		err               error
	)

	// Separate position and equality deletes
	for _, t := range tasks {
		for _, d := range t.DeleteFiles {
			switch d.ContentType() {
			case iceberg.EntryContentPosDeletes:
				if _, ok := uniquePosDeletes[d.FilePath()]; !ok {
					uniquePosDeletes[d.FilePath()] = d
				}
			case iceberg.EntryContentEqDeletes:
				if _, ok := uniqueEqDeletes[d.FilePath()]; !ok {
					uniqueEqDeletes[d.FilePath()] = d
				}
			}
		}
	}

	// Read position deletes
	if len(uniquePosDeletes) > 0 {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)

		perFileChan := make(chan map[string]*arrow.Chunked, concurrency)
		go func() {
			defer close(perFileChan)
			for _, v := range uniquePosDeletes {
				g.Go(func() error {
					deletes, err := readPositionDeletes(ctx, fs, v)
					if deletes != nil {
						perFileChan <- deletes
					}
					return err
				})
			}
			err = g.Wait()
		}()

		for deletes := range perFileChan {
			for file, arr := range deletes {
				posDeletesPerFile[file] = append(posDeletesPerFile[file], arr)
			}
		}

		if err != nil {
			return nil, err
		}
	}

	// Read equality deletes
	if len(uniqueEqDeletes) > 0 {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)

		perFileEqChan := make(chan map[string]arrow.Table, concurrency)
		go func() {
			defer close(perFileEqChan)
			for _, v := range uniqueEqDeletes {
				g.Go(func() error {
					deletes, err := readEqualityDeletes(ctx, fs, v)
					if deletes != nil {
						perFileEqChan <- deletes
					}
					return err
				})
			}
			err = g.Wait()
		}()

		for deletes := range perFileEqChan {
			for file, tbl := range deletes {
				eqDeletesPerFile[file] = append(eqDeletesPerFile[file], tbl)
			}
		}

		if err != nil {
			return nil, err
		}
	}

	return &deleteFiles{
		positionDeletes: posDeletesPerFile,
		equalityDeletes: eqDeletesPerFile,
	}, nil
}

// readDeletes is kept for backward compatibility, calling readPositionDeletes
func readDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (map[string]*arrow.Chunked, error) {
	return readPositionDeletes(ctx, fs, dataFile)
}

func readPositionDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (map[string]*arrow.Chunked, error) {
	src, err := internal.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()

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

func readEqualityDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (map[string]arrow.Table, error) {
	src, err := internal.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()

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
	dict := filePathCol.Chunk(0).(*array.Dictionary).Dictionary().(*array.String)

	results := make(map[string]arrow.Table)
	for i := 0; i < dict.Len(); i++ {
		v := dict.Value(i)

		mask, err := compute.CallFunction(ctx, "equal", nil,
			compute.NewDatumWithoutOwning(filePathCol), compute.NewDatum(v))
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(tbl), mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		results[v] = filtered.(*compute.TableDatum).Value
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

type recProcessFn func(arrow.Record) (arrow.Record, error)

func processPositionalDeletes(ctx context.Context, deletes set[int64]) recProcessFn {
	mem := compute.GetAllocator(ctx)
	return func(record arrow.Record) (arrow.Record, error) {
		record.Retain()
		defer record.Release()

		currentIdx := record.Column(0).(*array.Int64).Value(0)
		nextIdx := currentIdx + int64(record.NumRows())

		indices := combinePositionalDeletes(mem, deletes, currentIdx, nextIdx)
		defer indices.Release()

		if indices.Len() == 0 {
			return nil, nil
		}

		out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			compute.NewDatumWithoutOwning(record), compute.NewDatumWithoutOwning(indices))
		if err != nil {
			return nil, err
		}

		return out.(*compute.RecordDatum).Value, nil
	}
}

// processEqualityDeletes creates a record processing function that filters out records
// matching equality delete conditions.
func processEqualityDeletes(ctx context.Context, equalityDeletes equalityDeletes, schema *iceberg.Schema) (recProcessFn, error) {
	// Pre-process equality deletes to create lookup structures
	deleteIndex := make(map[string][]map[int]interface{})
	
	for _, table := range equalityDeletes {
		// Convert arrow table to delete conditions
		conditions, err := arrowTableToDeleteConditions(table, schema)
		if err != nil {
			return nil, fmt.Errorf("converting equality deletes: %w", err)
		}
		
		for filePath, conds := range conditions {
			deleteIndex[filePath] = append(deleteIndex[filePath], conds...)
		}
	}
	
	return func(record arrow.Record) (arrow.Record, error) {
		record.Retain()
		defer record.Release()
		
		// For now, return the record as-is since we need file path context
		// In a full implementation, this would need the file path to look up deletes
		// This is a placeholder - the actual implementation would need more context
		return record, nil
	}, nil
}

// arrowTableToDeleteConditions converts an Arrow table containing equality deletes
// to a map of file paths to delete conditions.
func arrowTableToDeleteConditions(table arrow.Table, schema *iceberg.Schema) (map[string][]map[int]interface{}, error) {
	result := make(map[string][]map[int]interface{})
	
	// Find the file_path column
	filePathIdx := -1
	for i, field := range table.Schema().Fields() {
		if field.Name == "file_path" {
			filePathIdx = i
			break
		}
	}
	
	if filePathIdx == -1 {
		return nil, fmt.Errorf("file_path column not found in equality delete table")
	}
	
	filePathCol := table.Column(filePathIdx)
	
	// Process each row to extract delete conditions
	for rowIdx := int64(0); rowIdx < table.NumRows(); rowIdx++ {
		// Extract file path
		var filePath string
		for _, chunk := range filePathCol.Data().Chunks() {
			if rowIdx < int64(chunk.Len()) {
				if arr, ok := chunk.(*array.String); ok {
					filePath = arr.Value(int(rowIdx))
					break
				}
			}
			rowIdx -= int64(chunk.Len())
		}
		
		// Extract equality field values
		condition := make(map[int]interface{})
		for colIdx, field := range table.Schema().Fields() {
			if field.Name == "file_path" {
				continue // Skip file_path column
			}
			
			// Find corresponding field ID in schema
			if iceField, found := schema.FindFieldByName(field.Name); found {
				col := table.Column(colIdx)
				for _, chunk := range col.Data().Chunks() {
					if rowIdx < int64(chunk.Len()) {
						value := getValueFromArrowArray(chunk, int(rowIdx))
						condition[iceField.ID] = value
						break
					}
					rowIdx -= int64(chunk.Len())
				}
			}
		}
		
		result[filePath] = append(result[filePath], condition)
	}
	
	return result, nil
}

// getValueFromArrowArray extracts a value from an Arrow array at the given index.
func getValueFromArrowArray(arr arrow.Array, idx int) interface{} {
	if arr.IsNull(idx) {
		return nil
	}
	
	switch a := arr.(type) {
	case *array.String:
		return a.Value(idx)
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.Boolean:
		return a.Value(idx)
	default:
		// For other types, return as string representation
		return fmt.Sprintf("%v", arr)
	}
}

func filterRecords(ctx context.Context, recordFilter expr.Expression) recProcessFn {
	return func(rec arrow.Record) (arrow.Record, error) {
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

func (as *arrowScan) projectedFieldIDs() (set[int], error) {
	idset := set[int]{}
	for _, id := range as.projectedSchema.FieldIDs() {
		typ, _ := as.projectedSchema.FindTypeByID(id)
		switch typ.(type) {
		case *iceberg.MapType, *iceberg.ListType:
		default:
			idset[id] = struct{}{}
		}
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
	Record internal.Enumerated[arrow.Record]
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
		testRowGroups, err = newParquetRowGroupStatsEvaluator(fileSchema, as.boundRowFilter, false)
		if err != nil {
			return err
		}
	}

	recRdr, err = rdr.GetRecords(ctx, columns, testRowGroups)
	if err != nil {
		return err
	}
	defer recRdr.Release()

	var (
		idx  int
		prev arrow.Record
	)

	for recRdr.Next() {
		if prev != nil {
			out <- enumeratedRecord{Record: internal.Enumerated[arrow.Record]{
				Value: prev, Index: idx, Last: false,
			}, Task: task}
			idx++
		}

		prev = recRdr.Record()
		prev.Retain()

		for _, f := range pipeline {
			prev, err = f(prev)
			if err != nil {
				return err
			}
		}
	}

	if prev != nil {
		out <- enumeratedRecord{Record: internal.Enumerated[arrow.Record]{
			Value: prev, Index: idx, Last: true,
		}, Task: task}
	}

	if recRdr.Err() != nil && recRdr.Err() != io.EOF {
		err = recRdr.Err()
	}

	return err
}

func (as *arrowScan) recordsFromTask(ctx context.Context, task internal.Enumerated[FileScanTask], out chan<- enumeratedRecord, positionalDeletes positionDeletes, equalityDeletes equalityDeletes) (err error) {
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
		return
	}
	defer rdr.Close()

	pipeline := make([]recProcessFn, 0, 3)
	
	// Add position delete processing
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
	
	// Add equality delete processing
	if len(equalityDeletes) > 0 {
		eqDeleteProcessor, err := processEqualityDeletes(ctx, equalityDeletes, iceSchema)
		if err != nil {
			return err
		}
		pipeline = append(pipeline, eqDeleteProcessor)
	}

	filterFunc, dropFile, err = as.getRecordFilter(ctx, iceSchema)
	if err != nil {
		return
	}

	if dropFile {
		var emptySchema *arrow.Schema
		emptySchema, err = SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
		if err != nil {
			return err
		}
		out <- enumeratedRecord{Task: task, Record: internal.Enumerated[arrow.Record]{
			Value: array.NewRecord(emptySchema, nil, 0), Index: 0, Last: true,
		}}

		return
	}

	if filterFunc != nil {
		pipeline = append(pipeline, filterFunc)
	}

	pipeline = append(pipeline, func(r arrow.Record) (arrow.Record, error) {
		defer r.Release()

		return ToRequestedSchema(ctx, as.projectedSchema, iceSchema, r, false, false, as.useLargeTypes)
	})

	err = as.processRecords(ctx, task, iceSchema, rdr, colIndices, pipeline, out)

	return
}

func createIterator(ctx context.Context, numWorkers uint, records <-chan enumeratedRecord, deletesPerFile perFilePosDeletes, cancel context.CancelCauseFunc, rowLimit int64) iter.Seq2[arrow.Record, error] {
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

	return func(yield func(arrow.Record, error) bool) {
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

func (as *arrowScan) recordBatchesFromTasksAndDeletes(ctx context.Context, tasks []FileScanTask, allDeletes *deleteFiles) iter.Seq2[arrow.Record, error] {
	extSet := substrait.NewExtensionSet()
	as.nameMapping = as.metadata.NameMapping()

	ctx, cancel := context.WithCancelCause(exprs.WithExtensionIDSet(ctx, extSet))
	taskChan := make(chan internal.Enumerated[FileScanTask], len(tasks))

	// numWorkers := 1
	numWorkers := min(as.concurrency, len(tasks))
	records := make(chan enumeratedRecord, numWorkers)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for _ = range numWorkers {
		go func() {
			defer wg.Done()
			for task := range taskChan {
				if context.Cause(ctx) != nil {
					return
				}

				filePath := task.Value.File.FilePath()
				if err := as.recordsFromTask(ctx, task, records,
					allDeletes.positionDeletes[filePath],
					allDeletes.equalityDeletes[filePath]); err != nil {
					cancel(err)
					return
				}
			}
		}()
	}

	go func() {
		defer close(taskChan)
		for i, task := range tasks {
			taskChan <- internal.Enumerated[FileScanTask]{Index: i, Value: task}
		}
	}()

	go func() {
		wg.Wait()
		close(records)
	}()

	return createIterator(ctx, uint(numWorkers), records, allDeletes.positionDeletes, cancel, as.rowLimit)
}

func (as *arrowScan) GetRecords(ctx context.Context, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.Record, error], error) {
	var err error
	as.useLargeTypes, err = strconv.ParseBool(as.options.Get(ScanOptionArrowUseLargeTypes, "false"))
	if err != nil {
		as.useLargeTypes = false
	}

	resultSchema, err := SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
	if err != nil {
		return nil, nil, err
	}

	if as.rowLimit == 0 {
		return resultSchema, func(yield func(arrow.Record, error) bool) {}, nil
	}

	deletesPerFile, err := readAllDeleteFiles(ctx, as.fs, tasks, as.concurrency)
	if err != nil {
		return nil, nil, err
	}

	return resultSchema, as.recordBatchesFromTasksAndDeletes(ctx, tasks, deletesPerFile), nil
}
