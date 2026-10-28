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
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// resolveCompactionSortKeys converts order into Arrow sort keys against
// the columns of the scan output schema sc. Sort fields are matched by
// name through schema because the scan emits the table schema's
// top-level columns by name.
func resolveCompactionSortKeys(order SortOrder, schema *iceberg.Schema, sc *arrow.Schema) ([]compute.SortKey, error) {
	fields := schema.Fields()
	keys := make([]compute.SortKey, 0, order.Len())
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

		key := compute.SortKey{ColumnIndex: colIdx[0], Order: compute.SortOrderAscending, NullPlacement: compute.SortNullsAtEnd}
		if field.Direction == SortDESC {
			key.Order = compute.SortOrderDescending
		}
		if field.NullOrder == NullsFirst {
			key.NullPlacement = compute.SortNullsAtStart
		}
		keys = append(keys, key)
	}

	return keys, nil
}

// claimableSortOrderID returns the id to record as sort_order_id on files
// written sorted by order, or UnsortedSortOrderID when the claim would not
// hold. Rows are ordered by each field's first source column, which only
// matches the spec's ordering for single-source, order-preserving
// transforms, and the id must name the same order in the table metadata.
func claimableSortOrderID(meta Metadata, order SortOrder) int {
	for _, field := range order.fields {
		if len(field.SourceIDs) > 1 || field.Transform == nil || !field.Transform.PreservesOrder() {
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
		parts, keys, err := collectCompactionPartitions(ctx, spec, schema, order, records)
		defer func() { releaseCompactionPartitions(parts) }()
		if err != nil {
			yield(nil, err)

			return
		}

		for i := range parts {
			if !yieldSortedPartition(ctx, parts[i].batches, keys, batchRows, yield) {
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
) ([]compactionPartition, []compute.SortKey, error) {
	var (
		parts  []compactionPartition
		keys   []compute.SortKey
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
		if keys == nil {
			if keys, err = resolveCompactionSortKeys(order, schema, rec.Schema()); err != nil {
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

	return parts, keys, nil
}

// yieldSortedPartition sorts batches as one table and yields the sorted
// rows. Ownership of each yielded batch passes to the consumer. It returns
// false when iteration must stop.
func yieldSortedPartition(
	ctx context.Context,
	batches []arrow.RecordBatch,
	keys []compute.SortKey,
	batchRows int64,
	yield func(arrow.RecordBatch, error) bool,
) bool {
	if len(batches) == 0 {
		return true
	}

	sc := batches[0].Schema()
	for _, b := range batches[1:] {
		if !b.Schema().Equal(sc) {
			yield(nil, errors.New("sort compaction: scan produced batches with differing schemas"))

			return false
		}
	}

	tbl := array.NewTableFromRecords(sc, batches)
	defer tbl.Release()

	indices, err := compute.SortIndicesTable(ctx, tbl, keys)
	if err != nil {
		yield(nil, fmt.Errorf("sort compaction: compute sort indices: %w", err))

		return false
	}
	defer indices.Release()

	perm := indices.(*array.Uint64).Uint64Values()
	starts := make([]int64, len(batches)+1)
	for i, b := range batches {
		starts[i+1] = starts[i] + b.NumRows()
	}

	takeFn := partitionBatchByKey(ctx)
	mem := compute.GetAllocator(ctx)
	for off := 0; off < len(perm); off += int(batchRows) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)

			return false
		}

		end := min(off+int(batchRows), len(perm))
		out, err := gatherSortedRows(mem, takeFn, batches, starts, perm[off:end])
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

// gatherSortedRows builds one batch holding the rows at the global
// positions rows (in that order) of the concatenation of batches, whose
// cumulative row offsets are starts. Each source batch is taken from
// directly so no column is ever concatenated beyond one output batch.
func gatherSortedRows(
	mem memory.Allocator,
	takeFn partitionBatchFn,
	batches []arrow.RecordBatch,
	starts []int64,
	rows []uint64,
) (arrow.RecordBatch, error) {
	local := make([][]int64, len(batches))
	src := make([]int, len(rows))
	pos := make([]int64, len(rows))
	for i, g := range rows {
		b := sort.Search(len(batches), func(j int) bool { return starts[j+1] > int64(g) })
		src[i] = b
		pos[i] = int64(len(local[b]))
		local[b] = append(local[b], int64(g)-starts[b])
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

	order := make([]int64, len(rows))
	for i := range rows {
		order[i] = base[src[i]] + pos[i]
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
