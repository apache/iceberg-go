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
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// sortStrategy writes a compaction group's rows sorted by order. Groups
// whose files are already sorted by order are merged; others are sorted
// in memory.
type sortStrategy struct {
	order     SortOrder
	meta      Metadata
	spec      iceberg.PartitionSpec
	schema    *iceberg.Schema
	batchRows int64
}

func newSortStrategy(tbl *Table, order SortOrder, batchRows int64) (sortStrategy, error) {
	if order.IsUnsorted() {
		return sortStrategy{}, fmt.Errorf("%w: sort compaction requires a sort order with at least one field", ErrInvalidOperation)
	}
	if err := order.CheckCompatibility(tbl.Schema()); err != nil {
		return sortStrategy{}, fmt.Errorf("sort compaction: %w", err)
	}

	return sortStrategy{
		order:     order,
		meta:      tbl.Metadata(),
		spec:      tbl.Spec(),
		schema:    tbl.Schema(),
		batchRows: batchRows,
	}, nil
}

func (s sortStrategy) records(ctx context.Context, scan *Scan, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	if parts, ok := presortedPartitions(s.meta, s.spec, s.order, tasks, scan.concurrency); ok {
		return mergedCompactionRecords(ctx, scan, s.schema, s.order, s.batchRows, parts)
	}

	sc, records, err := scan.ReadTasks(ctx, tasks)
	if err != nil {
		return nil, nil, err
	}

	return sc, sortedCompactionRecords(ctx, s.spec, s.schema, s.order, s.batchRows, records), nil
}

func (s sortStrategy) writeOptions() []WriteRecordOption {
	return []WriteRecordOption{withPresortedInput(claimableSortOrderID(s.meta, s.order))}
}

// sortKeyColumn is one field of a rowComparator.
type sortKeyColumn struct {
	col        int
	desc       bool
	nullsFirst bool
	values     valueComparator
}

// rowComparator orders the rows of record batches that share one schema
// by the source columns of a sort order, following the Iceberg spec: each
// field's null order places nulls whatever its direction, floating-point
// values order as -NaN < -Inf < -x < -0 < 0 < x < Inf < NaN, and strings
// and binary compare as unsigned bytes. A descending field reverses the
// order of its non-null values.
type rowComparator []sortKeyColumn

// newRowComparator resolves order against the scan output schema sc.
// Sort fields are matched by name through schema because the scan emits
// the table schema's top-level columns by name.
func newRowComparator(order SortOrder, schema *iceberg.Schema, sc *arrow.Schema) (rowComparator, error) {
	fields := schema.Fields()
	keys := make(rowComparator, 0, order.Len())
	for _, field := range order.fields {
		idx, ok := topLevelFieldIndex(fields, field.FirstSourceID())
		if !ok {
			return nil, fmt.Errorf("sort order %d: source id %d is not a top-level column in schema",
				order.OrderID(), field.FirstSourceID())
		}
		colIdx := sc.FieldIndices(fields[idx].Name)
		if len(colIdx) != 1 {
			return nil, fmt.Errorf("sort order %d: column %q not found in scan output",
				order.OrderID(), fields[idx].Name)
		}
		values, err := newValueComparator(sc.Field(colIdx[0]).Type)
		if err != nil {
			return nil, fmt.Errorf("sort order %d: column %q: %w", order.OrderID(), fields[idx].Name, err)
		}
		keys = append(keys, sortKeyColumn{
			col:        colIdx[0],
			desc:       field.Direction == SortDESC,
			nullsFirst: field.NullOrder == NullsFirst,
			values:     values,
		})
	}

	return keys, nil
}

// compareNulls orders a null against a non-null value. It reports false
// when neither is null.
func compareNulls(xNull, yNull, nullsFirst bool) (int, bool) {
	switch {
	case !xNull && !yNull:
		return 0, false
	case xNull && yNull:
		return 0, true
	case xNull == nullsFirst:
		return -1, true
	default:
		return 1, true
	}
}

func (c rowComparator) compare(a arrow.RecordBatch, i int, b arrow.RecordBatch, j int) int {
	for _, k := range c {
		x, y := a.Column(k.col), b.Column(k.col)
		r, isNull := compareNulls(x.IsNull(i), y.IsNull(j), k.nullsFirst)
		if !isNull {
			r = k.values.compare(x, i, y, j)
			if k.desc {
				r = -r
			}
		}
		if r != 0 {
			return r
		}
	}

	return 0
}

// bind returns compare specialized to rows of batches, which resolves each
// key column's typed values once instead of on every comparison.
func (c rowComparator) bind(batches []arrow.RecordBatch) func(x, y rowRef) int {
	type boundKey struct {
		cols       []arrow.Array
		hasNulls   bool
		desc       bool
		nullsFirst bool
		compare    func(xb, xi, yb, yi int) int
	}
	keys := make([]boundKey, len(c))
	for n, k := range c {
		cols := make([]arrow.Array, len(batches))
		hasNulls := false
		for b, rec := range batches {
			cols[b] = rec.Column(k.col)
			hasNulls = hasNulls || cols[b].NullN() > 0
		}
		keys[n] = boundKey{cols, hasNulls, k.desc, k.nullsFirst, k.values.bind(cols)}
	}

	return func(x, y rowRef) int {
		xb, xi, yb, yi := x.batch(), x.row(), y.batch(), y.row()
		for _, k := range keys {
			var r int
			isNull := false
			if k.hasNulls {
				r, isNull = compareNulls(k.cols[xb].IsNull(xi), k.cols[yb].IsNull(yi), k.nullsFirst)
			}
			if !isNull {
				r = k.compare(xb, xi, yb, yi)
				if k.desc {
					r = -r
				}
			}
			if r != 0 {
				return r
			}
		}

		return 0
	}
}

// valueComparator compares the non-null values of one Arrow type, either
// at any two positions of any two arrays, or bound to a fixed set of
// arrays and addressed by (array, row).
type valueComparator struct {
	compare func(a arrow.Array, i int, b arrow.Array, j int) int
	bind    func(cols []arrow.Array) func(xb, xi, yb, yi int) int
}

func orderedValues[T cmp.Ordered, A interface{ Values() []T }]() valueComparator {
	return fixedWidthValues[T, A](cmp.Compare[T])
}

func fixedWidthValues[T any, A interface{ Values() []T }](compare func(T, T) int) valueComparator {
	return valueComparator{
		compare: func(a arrow.Array, i int, b arrow.Array, j int) int {
			return compare(a.(A).Values()[i], b.(A).Values()[j])
		},
		bind: func(cols []arrow.Array) func(xb, xi, yb, yi int) int {
			values := make([][]T, len(cols))
			for n, col := range cols {
				values[n] = col.(A).Values()
			}

			return func(xb, xi, yb, yi int) int { return compare(values[xb][xi], values[yb][yi]) }
		},
	}
}

func accessorValues[T any, A interface{ Value(int) T }](compare func(T, T) int) valueComparator {
	return valueComparator{
		compare: func(a arrow.Array, i int, b arrow.Array, j int) int {
			return compare(a.(A).Value(i), b.(A).Value(j))
		},
		bind: func(cols []arrow.Array) func(xb, xi, yb, yi int) int {
			typed := make([]A, len(cols))
			for n, col := range cols {
				typed[n] = col.(A)
			}

			return func(xb, xi, yb, yi int) int { return compare(typed[xb].Value(xi), typed[yb].Value(yi)) }
		},
	}
}

func compareFloat[T float32 | float64](x, y T) int {
	xNaN, yNaN := x != x, y != y
	switch {
	case xNaN || yNaN:
		rank := func(nan bool, v T) int {
			switch {
			case !nan:
				return 0
			case math.Signbit(float64(v)):
				return -1
			default:
				return 1
			}
		}

		return cmp.Compare(rank(xNaN, x), rank(yNaN, y))
	case x == y:
		// Only -0 and 0 compare equal while differing in sign.
		return cmp.Compare(boolRank(!math.Signbit(float64(x))), boolRank(!math.Signbit(float64(y))))
	case x < y:
		return -1
	default:
		return 1
	}
}

func compareBool(x, y bool) int { return cmp.Compare(boolRank(x), boolRank(y)) }

func boolRank(v bool) int {
	if v {
		return 1
	}

	return 0
}

func compareDecimal128(x, y decimal128.Num) int { return x.Cmp(y) }

func newValueComparator(dt arrow.DataType) (valueComparator, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return accessorValues[bool, *array.Boolean](compareBool), nil
	case arrow.INT8:
		return orderedValues[int8, *array.Int8](), nil
	case arrow.INT16:
		return orderedValues[int16, *array.Int16](), nil
	case arrow.INT32:
		return orderedValues[int32, *array.Int32](), nil
	case arrow.INT64:
		return orderedValues[int64, *array.Int64](), nil
	case arrow.UINT8:
		return orderedValues[uint8, *array.Uint8](), nil
	case arrow.UINT16:
		return orderedValues[uint16, *array.Uint16](), nil
	case arrow.UINT32:
		return orderedValues[uint32, *array.Uint32](), nil
	case arrow.UINT64:
		return orderedValues[uint64, *array.Uint64](), nil
	case arrow.FLOAT32:
		return fixedWidthValues[float32, *array.Float32](compareFloat[float32]), nil
	case arrow.FLOAT64:
		return fixedWidthValues[float64, *array.Float64](compareFloat[float64]), nil
	case arrow.DATE32:
		return orderedValues[arrow.Date32, *array.Date32](), nil
	case arrow.DATE64:
		return orderedValues[arrow.Date64, *array.Date64](), nil
	case arrow.TIME32:
		return orderedValues[arrow.Time32, *array.Time32](), nil
	case arrow.TIME64:
		return orderedValues[arrow.Time64, *array.Time64](), nil
	case arrow.TIMESTAMP:
		return orderedValues[arrow.Timestamp, *array.Timestamp](), nil
	case arrow.STRING:
		return accessorValues[string, *array.String](strings.Compare), nil
	case arrow.LARGE_STRING:
		return accessorValues[string, *array.LargeString](strings.Compare), nil
	case arrow.STRING_VIEW:
		return accessorValues[string, *array.StringView](strings.Compare), nil
	case arrow.BINARY:
		return accessorValues[[]byte, *array.Binary](bytes.Compare), nil
	case arrow.LARGE_BINARY:
		return accessorValues[[]byte, *array.LargeBinary](bytes.Compare), nil
	case arrow.BINARY_VIEW:
		return accessorValues[[]byte, *array.BinaryView](bytes.Compare), nil
	case arrow.FIXED_SIZE_BINARY:
		return accessorValues[[]byte, *array.FixedSizeBinary](bytes.Compare), nil
	case arrow.DECIMAL128:
		return fixedWidthValues[decimal128.Num, *array.Decimal128](compareDecimal128), nil
	case arrow.EXTENSION:
		storage, err := newValueComparator(dt.(arrow.ExtensionType).StorageType())
		if err != nil {
			return valueComparator{}, err
		}
		unwrap := func(a arrow.Array) arrow.Array { return a.(array.ExtensionArray).Storage() }

		return valueComparator{
			compare: func(a arrow.Array, i int, b arrow.Array, j int) int {
				return storage.compare(unwrap(a), i, unwrap(b), j)
			},
			bind: func(cols []arrow.Array) func(xb, xi, yb, yi int) int {
				stored := make([]arrow.Array, len(cols))
				for n, col := range cols {
					stored[n] = unwrap(col)
				}

				return storage.bind(stored)
			},
		}, nil
	default:
		return valueComparator{}, fmt.Errorf("%w: cannot sort by values of type %s", iceberg.ErrNotImplemented, dt)
	}
}

// claimableSortOrderID returns the id to record as sort_order_id on files
// written sorted by order, or UnsortedSortOrderID when the claim would not
// hold. Rows are ordered by each field's first source column. That matches
// the spec's ordering only when every field but the last is identity and
// the last preserves order: ordering by a coarser transform's source
// column (day(ts)) also orders rows the transform ties on, which breaks
// the order of the fields after it. The id must also name the same order
// in the table metadata.
func claimableSortOrderID(meta Metadata, order SortOrder) int {
	for i, field := range order.fields {
		if len(field.SourceIDs) > 1 || field.Transform == nil || !field.Transform.PreservesOrder() {
			return UnsortedSortOrderID
		}
		if _, identity := field.Transform.(iceberg.IdentityTransform); !identity && i < len(order.fields)-1 {
			return UnsortedSortOrderID
		}
	}
	for _, existing := range meta.SortOrders() {
		if existing.Equals(order) {
			return order.OrderID()
		}
	}

	return UnsortedSortOrderID
}

type compactionPartition struct {
	rec     partitionRecord
	batches []arrow.RecordBatch
}

func releaseCompactionPartitions(parts []compactionPartition) {
	for _, p := range parts {
		for _, b := range p.batches {
			b.Release()
		}
	}
}

// sortedCompactionRecords materializes every batch of records, splits the
// rows by the table's current partition spec and yields each partition's
// rows globally sorted by order, one partition after another, in batches
// of at most batchRows rows. Partitions are emitted in order of first
// appearance, so the output stays clustered for the clustered writer even
// when the group's files were written under an older spec.
//
// The whole input is held in memory until the last partition is emitted.
func sortedCompactionRecords(
	ctx context.Context,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	order SortOrder,
	batchRows int64,
	records iter.Seq2[arrow.RecordBatch, error],
) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		parts, rows, err := collectCompactionPartitions(ctx, spec, schema, order, records)
		defer func() { releaseCompactionPartitions(parts) }()
		if err != nil {
			yield(nil, err)

			return
		}

		for i := range parts {
			if !yieldSortedPartition(ctx, parts[i].batches, rows, batchRows, yield) {
				return
			}
			releaseCompactionPartitions(parts[i : i+1])
			parts[i].batches = nil
		}
	}
}

// collectCompactionPartitions drains records into per-partition batches
// and resolves order against the scan output schema of the first batch.
func collectCompactionPartitions(
	ctx context.Context,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	order SortOrder,
	records iter.Seq2[arrow.RecordBatch, error],
) ([]compactionPartition, rowComparator, error) {
	var (
		parts  []compactionPartition
		rows   rowComparator
		plan   *partitionExtractionPlan
		takeFn = partitionBatchByKey(ctx)
	)

	for rec, err := range records {
		if err != nil {
			return parts, nil, err
		}
		if rec.NumRows() == 0 {
			rec.Release()

			continue
		}
		if rows == nil {
			if rows, err = newRowComparator(order, schema, rec.Schema()); err != nil {
				rec.Release()

				return parts, nil, err
			}
		}

		if spec.IsUnpartitioned() {
			if len(parts) == 0 {
				parts = append(parts, compactionPartition{})
			}
			parts[0].batches = append(parts[0].batches, rec)

			continue
		}

		if plan == nil {
			if plan, err = newPartitionExtractionPlan(spec, schema, rec.Schema()); err != nil {
				rec.Release()

				return parts, nil, err
			}
		}
		infos, err := plan.getRecordPartitions(rec)
		if err != nil {
			rec.Release()

			return parts, nil, err
		}
		slices.SortFunc(infos, func(a, b *partitionInfo) int {
			return cmp.Compare(a.rows[0], b.rows[0])
		})

		for _, info := range infos {
			sub, err := takeFn(rec, info.rows)
			if err != nil {
				rec.Release()

				return parts, nil, err
			}
			idx := slices.IndexFunc(parts, func(p compactionPartition) bool {
				return partitionRecordsEqual(p.rec, info.partitionRec)
			})
			if idx < 0 {
				parts = append(parts, compactionPartition{rec: info.partitionRec})
				idx = len(parts) - 1
			}
			parts[idx].batches = append(parts[idx].batches, sub)
		}
		rec.Release()
	}

	return parts, rows, nil
}

// rowRef addresses one row of a slice of batches: the batch index in the
// high 32 bits and the row within it in the low 32.
type rowRef uint64

func newRowRef(batch, row int) rowRef { return rowRef(uint64(batch)<<32 | uint64(row)) }
func (r rowRef) batch() int           { return int(r >> 32) }
func (r rowRef) row() int             { return int(r & math.MaxUint32) }

// yieldSortedPartition sorts batches as one table and yields the sorted
// rows. Ownership of each yielded batch passes to the consumer. It returns
// false when iteration must stop.
func yieldSortedPartition(
	ctx context.Context,
	batches []arrow.RecordBatch,
	rows rowComparator,
	batchRows int64,
	yield func(arrow.RecordBatch, error) bool,
) bool {
	if len(batches) == 0 {
		return true
	}

	sc := batches[0].Schema()
	var total int64
	for _, b := range batches {
		if !b.Schema().Equal(sc) {
			yield(nil, errors.New("sort compaction: scan produced batches with differing schemas"))

			return false
		}
		total += b.NumRows()
	}

	compare := rows.bind(batches)
	if partitionRowsAlreadySorted(batches, compare) {
		// Keep the scan's natural order. Every output owns its reference to
		// the original batch; no row references or Arrow Take arrays are needed.
		for _, batch := range batches {
			for off := int64(0); off < batch.NumRows(); off += batchRows {
				if err := ctx.Err(); err != nil {
					yield(nil, err)

					return false
				}
				end := min(off+batchRows, batch.NumRows())
				var out arrow.RecordBatch
				if off == 0 && end == batch.NumRows() {
					batch.Retain()
					out = batch
				} else {
					out = batch.NewSlice(off, end)
				}
				if !yield(out, nil) {
					return false
				}
			}
		}

		return true
	}

	refs := make([]rowRef, 0, total)
	for b, rec := range batches {
		for r := range int(rec.NumRows()) {
			refs = append(refs, newRowRef(b, r))
		}
	}
	slices.SortStableFunc(refs, compare)

	takeFn := partitionBatchByKey(ctx)
	mem := compute.GetAllocator(ctx)
	for off := 0; off < len(refs); off += int(batchRows) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)

			return false
		}

		end := min(off+int(batchRows), len(refs))
		out, err := gatherSortedRows(mem, takeFn, batches, refs[off:end])
		if err != nil {
			yield(nil, fmt.Errorf("sort compaction: gather sorted rows: %w", err))

			return false
		}
		if !yield(out, nil) {
			return false
		}
	}

	return true
}

// partitionRowsAlreadySorted also checks batch boundaries. It uses the same
// Iceberg comparator as the stable sort, including null order and signed NaNs.
// Equal keys retain their original input order without a re-sort.
func partitionRowsAlreadySorted(batches []arrow.RecordBatch, compare func(rowRef, rowRef) int) bool {
	var previous rowRef
	hasPrevious := false
	for batch, rec := range batches {
		for row := range int(rec.NumRows()) {
			current := newRowRef(batch, row)
			if hasPrevious && compare(previous, current) > 0 {
				return false
			}
			previous, hasPrevious = current, true
		}
	}

	return true
}

// gatherSortedRows builds one batch holding the rows refs of batches, in
// that order. Each source batch is taken from directly so no column is
// ever concatenated beyond one output batch.
func gatherSortedRows(
	mem memory.Allocator,
	takeFn partitionBatchFn,
	batches []arrow.RecordBatch,
	refs []rowRef,
) (arrow.RecordBatch, error) {
	local := make([][]int64, len(batches))
	pos := make([]int64, len(refs))
	for i, ref := range refs {
		b := ref.batch()
		pos[i] = int64(len(local[b]))
		local[b] = append(local[b], int64(ref.row()))
	}

	var pieces []arrow.RecordBatch
	defer func() {
		for _, p := range pieces {
			p.Release()
		}
	}()
	base := make([]int64, len(batches))
	var n int64
	for b, idx := range local {
		if len(idx) == 0 {
			continue
		}
		piece, err := takeFn(batches[b], idx)
		if err != nil {
			return nil, err
		}
		pieces = append(pieces, piece)
		base[b] = n
		n += int64(len(idx))
	}

	// A single source batch was taken in output order already.
	if len(pieces) == 1 {
		pieces[0].Retain()

		return pieces[0], nil
	}

	merged, err := concatRecordBatches(mem, pieces)
	if err != nil {
		return nil, err
	}
	defer merged.Release()

	order := make([]int64, len(refs))
	for i, ref := range refs {
		order[i] = base[ref.batch()] + pos[i]
	}

	return takeFn(merged, order)
}

func concatRecordBatches(mem memory.Allocator, batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	sc := batches[0].Schema()
	cols := make([]arrow.Array, sc.NumFields())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()

	var rows int64
	for _, b := range batches {
		rows += b.NumRows()
	}
	chunks := make([]arrow.Array, len(batches))
	for i := range cols {
		for j, b := range batches {
			chunks[j] = b.Column(i)
		}
		col, err := array.Concatenate(chunks, mem)
		if err != nil {
			return nil, fmt.Errorf("concatenate column %q: %w", sc.Field(i).Name, err)
		}
		cols[i] = col
	}

	return array.NewRecordBatch(sc, cols, rows), nil
}

// presortedPartitions reports whether tasks can be merged instead of
// sorted: every task's file records order's id as its sort_order_id, the
// id is one claimableSortOrderID accepts, every field of order is
// identity, every file was written under spec, so each file holds rows of
// a single partition of spec, and no partition has more than maxStreams
// files. The tasks are returned grouped by partition, in order of first
// appearance.
//
// The merge compares source values, so a transformed field rules it out:
// a file sorted by [day(ts)] is validly unsorted by ts within a day.
// maxStreams bounds the files the merge reads at once.
func presortedPartitions(meta Metadata, spec iceberg.PartitionSpec, order SortOrder, tasks []FileScanTask, maxStreams int) ([][]FileScanTask, bool) {
	id := claimableSortOrderID(meta, order)
	if id == UnsortedSortOrderID {
		return nil, false
	}
	for _, field := range order.fields {
		if _, identity := field.Transform.(iceberg.IdentityTransform); !identity {
			return nil, false
		}
	}

	var (
		keys  []partitionRecord
		parts [][]FileScanTask
	)
	for _, task := range tasks {
		sortOrderID := task.File.SortOrderID()
		if sortOrderID == nil || *sortOrderID != id || int(task.File.SpecID()) != spec.ID() {
			return nil, false
		}

		values := task.File.Partition()
		key := make(partitionRecord, 0, spec.NumFields())
		for _, field := range spec.Fields() {
			key = append(key, values[field.FieldID])
		}
		idx := slices.IndexFunc(keys, func(k partitionRecord) bool { return partitionRecordsEqual(k, key) })
		if idx < 0 {
			keys = append(keys, key)
			parts = append(parts, nil)
			idx = len(parts) - 1
		}
		parts[idx] = append(parts[idx], task)
		if len(parts[idx]) > maxStreams {
			return nil, false
		}
	}

	return parts, true
}

// mergedCompactionRecords reads each task of parts (see
// presortedPartitions) as its own stream and merges the streams of one
// partition after another into batches of at most batchRows rows sorted
// by order. Each stream of the partition being merged holds up to about
// five batches (its scan worker, the channels behind it and the merge
// cursor), instead of the whole group being held.
func mergedCompactionRecords(
	ctx context.Context,
	scan *Scan,
	schema *iceberg.Schema,
	order SortOrder,
	batchRows int64,
	parts [][]FileScanTask,
) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	tasks := slices.Concat(parts...)
	sc, streams, release, err := scan.readTasksPerTask(ctx, tasks)
	if err != nil {
		return nil, nil, err
	}
	rows, err := newRowComparator(order, schema, sc)
	if err != nil {
		release()

		return nil, nil, err
	}

	paths := make([]string, len(tasks))
	for i, task := range tasks {
		paths[i] = task.File.FilePath()
	}

	records := func(yield func(arrow.RecordBatch, error) bool) {
		defer release()
		start := 0
		for _, part := range parts {
			end := start + len(part)
			if !mergeSortedStreams(ctx, rows, paths[start:end], streams[start:end], batchRows, yield) {
				return
			}
			start = end
		}
	}

	return sc, records, nil
}

// mergeCursor walks one sorted stream of a k-way merge.
type mergeCursor struct {
	idx  int
	path string
	next func() (arrow.RecordBatch, error, bool)
	stop func()
	rec  arrow.RecordBatch
	row  int
	// held is rec's index in the pending output's source batches, or -1
	// while no pending row comes from rec.
	held int
}

// load moves the cursor to the first row of the stream's next non-empty
// batch and reports false at the end of the stream. It checks that the
// stream is sorted, since a merge of unsorted input would silently yield
// unsorted output.
func (c *mergeCursor) load(rows rowComparator) (bool, error) {
	for {
		rec, err, ok := c.next()
		switch {
		case !ok:
			c.releaseBatch()

			return false, nil
		case err != nil:
			return false, err
		case rec.NumRows() == 0:
			rec.Release()

			continue
		}

		sorted := c.rec == nil || rows.compare(c.rec, int(c.rec.NumRows())-1, rec, 0) <= 0
		for r := 1; sorted && r < int(rec.NumRows()); r++ {
			sorted = rows.compare(rec, r-1, rec, r) <= 0
		}
		c.releaseBatch()
		c.rec, c.row, c.held = rec, 0, -1
		if !sorted {
			return false, fmt.Errorf("sort compaction: data file %s records the sort order being merged but is not sorted by it", c.path)
		}

		return true, nil
	}
}

func (c *mergeCursor) releaseBatch() {
	if c.rec != nil {
		c.rec.Release()
		c.rec = nil
	}
}

// mergeHeap orders cursors by their current row, ties broken by stream
// index so the merge is deterministic.
type mergeHeap struct {
	cursors []*mergeCursor
	rows    rowComparator
}

func (h *mergeHeap) precedes(a *mergeCursor, row int, b *mergeCursor) bool {
	if c := h.rows.compare(a.rec, row, b.rec, b.row); c != 0 {
		return c < 0
	}

	return a.idx < b.idx
}

func (h *mergeHeap) Len() int { return len(h.cursors) }
func (h *mergeHeap) Less(i, j int) bool {
	return h.precedes(h.cursors[i], h.cursors[i].row, h.cursors[j])
}
func (h *mergeHeap) Swap(i, j int) { h.cursors[i], h.cursors[j] = h.cursors[j], h.cursors[i] }
func (h *mergeHeap) Push(x any)    { h.cursors = append(h.cursors, x.(*mergeCursor)) }
func (h *mergeHeap) Pop() any {
	last := h.cursors[len(h.cursors)-1]
	h.cursors = h.cursors[:len(h.cursors)-1]

	return last
}

// mergeRun is rows [start, end) of the pending output's source batch
// held[batch].
type mergeRun struct{ batch, start, end int }

// mergeSortedStreams k-way merges streams, each sorted by rows, and yields
// the result in batches of at most batchRows rows. Every step takes the
// longest run of the minimum stream's current batch that precedes the
// runner-up's head. An output batch that is one run is a zero-copy slice;
// otherwise its rows are gathered from their source batches, so
// interleaved streams cost a gather per output batch rather than a slice
// per run. Ownership of each yielded batch passes to the consumer. It
// returns false when iteration must stop.
func mergeSortedStreams(
	ctx context.Context,
	rows rowComparator,
	paths []string,
	streams []iter.Seq2[arrow.RecordBatch, error],
	batchRows int64,
	yield func(arrow.RecordBatch, error) bool,
) bool {
	h := &mergeHeap{rows: rows}
	cursors := make([]*mergeCursor, 0, len(streams))
	var (
		held        []arrow.RecordBatch
		runs        []mergeRun
		pendingRows int64
	)
	releaseHeld := func() {
		for _, b := range held {
			b.Release()
		}
		held, runs, pendingRows = held[:0], runs[:0], 0
		for _, c := range cursors {
			c.held = -1
		}
	}
	defer func() {
		for _, c := range cursors {
			c.releaseBatch()
			c.stop()
		}
		releaseHeld()
	}()

	for i, stream := range streams {
		next, stop := iter.Pull2(stream)
		c := &mergeCursor{idx: i, path: paths[i], next: next, stop: stop, held: -1}
		cursors = append(cursors, c)
		ok, err := c.load(rows)
		if err != nil {
			yield(nil, err)

			return false
		}
		if ok {
			h.cursors = append(h.cursors, c)
		}
	}
	heap.Init(h)

	mem := compute.GetAllocator(ctx)
	takeFn := partitionBatchByKey(ctx)
	flush := func() bool {
		out, err := gatherMergeRuns(mem, takeFn, held, runs, pendingRows)
		releaseHeld()
		if err != nil {
			yield(nil, fmt.Errorf("sort compaction: merge sorted rows: %w", err))

			return false
		}

		return yield(out, nil)
	}

	for h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			yield(nil, err)

			return false
		}

		c := heap.Pop(h).(*mergeCursor)
		end := int(c.rec.NumRows())
		if h.Len() > 0 {
			next := h.cursors[0]
			end = c.row + sort.Search(end-c.row, func(k int) bool {
				return !h.precedes(c, c.row+k, next)
			})
		}
		end = min(end, c.row+int(batchRows-pendingRows))

		if c.held < 0 {
			c.rec.Retain()
			held = append(held, c.rec)
			c.held = len(held) - 1
		}
		if last := len(runs) - 1; last >= 0 && runs[last].batch == c.held && runs[last].end == c.row {
			runs[last].end = end
		} else {
			runs = append(runs, mergeRun{batch: c.held, start: c.row, end: end})
		}
		pendingRows += int64(end - c.row)
		c.row = end
		if pendingRows == batchRows && !flush() {
			return false
		}

		if c.row < int(c.rec.NumRows()) {
			heap.Push(h, c)

			continue
		}
		ok, err := c.load(rows)
		if err != nil {
			yield(nil, err)

			return false
		}
		if ok {
			heap.Push(h, c)
		}
	}

	if pendingRows > 0 {
		return flush()
	}

	return true
}

// gatherMergeRuns builds one batch of the n rows of runs, in order.
func gatherMergeRuns(
	mem memory.Allocator,
	takeFn partitionBatchFn,
	held []arrow.RecordBatch,
	runs []mergeRun,
	n int64,
) (arrow.RecordBatch, error) {
	if len(runs) == 1 {
		r := runs[0]

		return held[r.batch].NewSlice(int64(r.start), int64(r.end)), nil
	}

	refs := make([]rowRef, 0, n)
	for _, r := range runs {
		for row := r.start; row < r.end; row++ {
			refs = append(refs, newRowRef(r.batch, row))
		}
	}

	return gatherSortedRows(mem, takeFn, held, refs)
}
