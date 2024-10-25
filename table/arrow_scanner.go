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
	"container/heap"
	"context"
	"io"
	"iter"
	"runtime"
	"strconv"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/substrait-io/substrait-go/expr"
	"golang.org/x/sync/errgroup"
)

const (
	ScanOptionArrowUseLargeTypes = "arrow.use_large_types"
)

func readAllDeleteFiles(ctx context.Context, fs iceio.IO, tasks []FileScanTask) (map[string][]*arrow.Chunked, error) {
	var (
		deletesPerFile = make(map[string][]*arrow.Chunked)
		uniqueDeletes  = make(map[string]iceberg.DataFile)
		err            error
	)

	for _, t := range tasks {
		for _, d := range t.DeleteFiles {
			if _, ok := uniqueDeletes[d.FilePath()]; !ok {
				uniqueDeletes[d.FilePath()] = d
			}
		}
	}

	if len(uniqueDeletes) == 0 {
		return deletesPerFile, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())

	perFileChan := make(chan map[string]*arrow.Chunked, runtime.NumCPU())
	go func() {
		defer close(perFileChan)
		for _, v := range uniqueDeletes {
			g.Go(func() error {
				deletes, err := readDeletes(ctx, fs, v)
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
			deletesPerFile[file] = append(deletesPerFile[file], arr)
		}
	}

	return deletesPerFile, err
}

func readDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (map[string]*arrow.Chunked, error) {
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

func processPositionalDeletes(ctx context.Context, deletes set[int64]) func(arrow.Record) (arrow.Record, error) {
	nextIdx, mem := int64(0), compute.GetAllocator(ctx)
	return func(r arrow.Record) (arrow.Record, error) {
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

func filterRecords(ctx context.Context, recordFilter expr.Expression) func(arrow.Record) (arrow.Record, error) {
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
}

func (as *arrowScan) projectedFieldIDs() (set[int], error) {
	idset := set[int]{}
	for _, field := range as.projectedSchema.Fields() {
		switch field.Type.(type) {
		case *iceberg.MapType, *iceberg.ListType:
		default:
			idset[field.ID] = struct{}{}
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

type enumerated[T any] struct {
	value T
	index int
	last  bool
}

type pqueue[T any] struct {
	queue   []*T
	compare func(a, b *T) bool
}

func (pq *pqueue[T]) Len() int { return len(pq.queue) }
func (pq *pqueue[T]) Less(i, j int) bool {
	return pq.compare(pq.queue[i], pq.queue[j])
}
func (pq *pqueue[T]) Swap(i, j int) {
	pq.queue[i], pq.queue[j] = pq.queue[j], pq.queue[i]
}

func (pq *pqueue[T]) Push(x any) {
	pq.queue = append(pq.queue, x.(*T))
}

func (pq *pqueue[T]) Pop() any {
	old := pq.queue
	n := len(old)

	item := old[n-1]
	old[n-1] = nil
	pq.queue = old[0 : n-1]
	return item
}

func makeSequencedChan[T any](bufferSize uint, source <-chan T, comesAfter, isNext func(a, b *T) bool, initial T) <-chan T {
	pq := pqueue[T]{queue: make([]*T, 0), compare: comesAfter}
	heap.Init(&pq)
	previous, out := &initial, make(chan T, bufferSize)
	go func() {
		defer close(out)
		for val := range source {
			heap.Push(&pq, &val)
			for pq.Len() > 0 && isNext(previous, pq.queue[0]) {
				previous = heap.Pop(&pq).(*T)
				out <- *previous
			}
		}
	}()
	return out
}

type enumeratedRecord struct {
	Record enumerated[arrow.Record]
	Task   enumerated[FileScanTask]
	Err    error
}

func (as *arrowScan) recordsFromTask(ctx context.Context, task enumerated[FileScanTask], out chan<- enumeratedRecord, positionalDeletes []*arrow.Chunked) (err error) {
	defer func() {
		if err != nil {
			out <- enumeratedRecord{Task: task, Err: err}
		}
	}()

	var (
		ids set[int]
		src internal.FileSource
		rdr internal.FileReader

		fileSchema *arrow.Schema
		iceSchema  *iceberg.Schema
		colIndices []int
	)

	ids, err = as.projectedFieldIDs()
	if err != nil {
		return
	}

	src, err = internal.GetFile(ctx, as.fs, task.value.File, false)
	if err != nil {
		return
	}

	rdr, err = src.GetReader(ctx)
	if err != nil {
		return
	}
	defer rdr.Close()

	fileSchema, colIndices, err = rdr.PrunedSchema(ids)
	if err != nil {
		return
	}

	iceSchema, err = ArrowSchemaToIceberg(fileSchema, false, nil)
	if err != nil {
		return
	}

	pipeline := make([]func(arrow.Record) (arrow.Record, error), 0, 2)
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

	if as.boundRowFilter != nil && !as.boundRowFilter.Equals(iceberg.AlwaysTrue{}) {
		var (
			recordFilter     expr.Expression
			extSet           *expr.ExtensionRegistry
			translatedFilter iceberg.BooleanExpression
		)

		translatedFilter, err = iceberg.TranslateColumnNames(as.boundRowFilter, iceSchema)
		if err != nil {
			return
		}

		translatedFilter, err = iceberg.BindExpr(iceSchema, translatedFilter, as.caseSensitive)
		if err != nil {
			return
		}

		if translatedFilter.Equals(iceberg.AlwaysFalse{}) {
			var emptySchema *arrow.Schema
			emptySchema, err = SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
			if err != nil {
				return err
			}
			out <- enumeratedRecord{Task: task, Record: enumerated[arrow.Record]{
				value: array.NewRecord(emptySchema, nil, 0), index: 0, last: true}}
			return
		}

		if !translatedFilter.Equals(iceberg.AlwaysTrue{}) {
			extSet, recordFilter, err = substrait.ConvertExpr(iceSchema, translatedFilter)
			if err != nil {
				return
			}

			ctx = exprs.WithExtensionIDSet(ctx, exprs.NewExtensionSetDefault(*extSet))
			pipeline = append(pipeline, filterRecords(ctx, recordFilter))
		}
	}

	pipeline = append(pipeline, func(r arrow.Record) (arrow.Record, error) {
		defer r.Release()
		return ToRequestedSchema(as.projectedSchema, iceSchema, r, false, false, as.useLargeTypes)
	})

	var (
		idx  int
		prev arrow.Record
	)

	var testRg func(*metadata.RowGroupMetaData, []int) (bool, error)
	testRg, err = newParquetRowGroupStatsEvaluator(iceSchema, as.boundRowFilter, as.caseSensitive, false)
	if err != nil {
		return
	}

	var recRdr array.RecordReader
	recRdr, err = rdr.GetRecords(ctx, colIndices, testRg)
	if err != nil {
		return
	}
	defer recRdr.Release()

	for recRdr.Next() {
		if prev != nil {
			out <- enumeratedRecord{Record: enumerated[arrow.Record]{
				value: prev, index: idx, last: false}, Task: task}
			idx++
		}

		prev = recRdr.Record()
		prev.Retain()

		for _, f := range pipeline {
			prev, err = f(prev)
			if err != nil {
				return
			}
		}
	}

	if prev != nil {
		out <- enumeratedRecord{Record: enumerated[arrow.Record]{
			value: prev, index: idx, last: true}, Task: task}
	}

	if recRdr.Err() != nil && recRdr.Err() != io.EOF {
		err = recRdr.Err()
	}
	return
}

func (as *arrowScan) recordBatchesFromTasksAndDeletes(ctx context.Context, tasks []FileScanTask, deletesPerFile map[string][]*arrow.Chunked) iter.Seq2[arrow.Record, error] {
	extSet := substrait.NewExtensionSet()

	ctx, cancel := context.WithCancel(exprs.WithExtensionIDSet(ctx, extSet))
	taskChan := make(chan enumerated[FileScanTask], len(tasks))

	// numWorkers := 1
	numWorkers := min(runtime.NumCPU(), len(tasks))
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

					if err := as.recordsFromTask(ctx, task, records,
						deletesPerFile[task.value.File.FilePath()]); err != nil {
						return
					}
				}
			}
		}()
	}

	go func() {
		for i, t := range tasks {
			taskChan <- enumerated[FileScanTask]{
				value: t, index: i, last: i == len(tasks)-1}
		}
		close(taskChan)

		wg.Wait()
		close(records)
	}()

	isBeforeAny := func(batch enumeratedRecord) bool {
		return batch.Task.index < 0
	}

	sequenced := makeSequencedChan(uint(numWorkers), records,
		func(left, right *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*left):
				return true
			case isBeforeAny(*right):
				return false
			case left.Err != nil || right.Err != nil:
				return true
			case left.Task.index == right.Task.index:
				return left.Record.index < right.Record.index
			default:
				return left.Task.index < right.Task.index
			}
		}, func(prev, next *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*prev):
				return next.Task.index == 0 && next.Record.index == 0
			case next.Err != nil:
				return true
			case prev.Task.index == next.Task.index:
				return next.Record.index == prev.Record.index+1
			default:
				return next.Task.index == prev.Task.index+1 &&
					prev.Record.last && next.Record.index == 0
			}
		}, enumeratedRecord{Task: enumerated[FileScanTask]{index: -1}})

	totalRowCount := int64(0)
	return func(yield func(arrow.Record, error) bool) {
		defer func() {
			for rec := range sequenced {
				if rec.Record.value != nil {
					rec.Record.value.Release()
				}
			}

			for _, v := range deletesPerFile {
				for _, chunk := range v {
					chunk.Release()
				}
			}
		}()

		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case enum, ok := <-sequenced:
				if !ok {
					return
				}

				if enum.Err != nil {
					yield(nil, enum.Err)
					return
				}

				rec := enum.Record.value
				if as.rowLimit > 0 {
					if totalRowCount >= as.rowLimit {
						rec.Release()
						return
					} else if totalRowCount+rec.NumRows() > as.rowLimit {
						defer rec.Release()
						rec = rec.NewSlice(0, as.rowLimit-totalRowCount)
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
				if as.rowLimit > 0 && totalRowCount >= as.rowLimit {
					return
				}
			}
		}
	}
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

	deletesPerFile, err := readAllDeleteFiles(ctx, as.fs, tasks)
	if err != nil {
		return nil, nil, err
	}

	return resultSchema, as.recordBatchesFromTasksAndDeletes(ctx, tasks, deletesPerFile), nil
}
