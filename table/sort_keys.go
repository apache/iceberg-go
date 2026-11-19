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
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
)

// resolveSortKeys converts an Iceberg SortOrder into Arrow compute sort keys
// against the given file schema. Each sort field's source column is used as
// the sort key directly: order-preserving transforms (identity, truncate,
// year/month/day/hour) sort the same as their source values. Float keys
// use FloatTotalOrder, which matches the spec's float order
// (-NaN < -Inf < -0 < 0 < Inf < NaN) and the row comparator's.
//
// Returns nil keys for an unsorted SortOrder. Returns an error when a sort
// field's source id is not a top-level column of fileSchema.
//
// Limitations:
//
//   - Per-batch, not per-file: keys are applied to each batch independently,
//     not merged into a globally sorted run across batches. Files therefore
//     record no sort_order_id, which asserts the whole file is sorted.
//   - Bucket transform falls back to source-column order, since the hash
//     bucket is not order-preserving.
//   - Multi-argument transform sort fields use only the first source column.
//   - This function accepts only top-level sources. That is a limitation of
//     arrow-go: a compute.SortKey addresses a column only by its top-level
//     index (ColumnIndex), so arrow-go's sort kernel cannot sort by a
//     column nested in a struct. This function rejects such a source with
//     an error. The writer checks [allTopLevelSortSources] first and sorts
//     orders with a nested source with sortRecordBatch instead.
func resolveSortKeys(order SortOrder, fileSchema *iceberg.Schema) ([]compute.SortKey, error) {
	if order.IsUnsorted() {
		return nil, nil
	}

	fields := fileSchema.Fields()
	keys := make([]compute.SortKey, 0, order.Len())
	for _, field := range order.fields {
		idx, ok := topLevelFieldIndex(fields, field.FirstSourceID())
		if !ok {
			return nil, fmt.Errorf("sort order %d: source id %d is not a top-level column in schema",
				order.OrderID(), field.FirstSourceID())
		}

		key := compute.SortKey{
			ColumnIndex: idx, Order: compute.SortOrderAscending, NullPlacement: compute.SortNullsAtEnd,
			FloatTotalOrder: true,
		}
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

func topLevelFieldIndex(fields []iceberg.NestedField, sourceID int) (int, bool) {
	for i, f := range fields {
		if f.ID == sourceID {
			return i, true
		}
	}

	return 0, false
}

// allTopLevelSortSources reports whether every source of order is a
// top-level column of fields. Only then can Arrow's sort kernel order a
// batch by it; otherwise the writer falls back to sortRecordBatch.
func allTopLevelSortSources(order SortOrder, fields []iceberg.NestedField) bool {
	for _, field := range order.fields {
		if _, ok := topLevelFieldIndex(fields, field.FirstSourceID()); !ok {
			return false
		}
	}

	return true
}

// sortFieldPath returns the struct-index path through fields, the top-level
// fields of schema, to the column sourceID.
func sortFieldPath(schema *iceberg.Schema, fields []iceberg.NestedField, sourceID, orderID int) ([]int, error) {
	if path, ok := structFieldPath(fields, sourceID); ok {
		return path, nil
	}
	if _, ok := schema.FindFieldByID(sourceID); ok {
		return nil, fmt.Errorf("%w: sort order %d: source id %d is nested under a list or map",
			iceberg.ErrInvalidArgument, orderID, sourceID)
	}

	return nil, fmt.Errorf("sort order %d: source id %d is not a column in schema", orderID, sourceID)
}

// structFieldPath returns the field-index path to sourceID, descending
// only into structs.
func structFieldPath(fields []iceberg.NestedField, sourceID int) ([]int, bool) {
	for i, f := range fields {
		if f.ID == sourceID {
			return []int{i}, true
		}
		st, ok := f.Type.(*iceberg.StructType)
		if !ok {
			continue
		}
		if inner, ok := structFieldPath(st.FieldList, sourceID); ok {
			return append([]int{i}, inner...), true
		}
	}

	return nil, false
}

// arrowColumnPath maps schemaPath, a struct-index path into fields, onto
// the path into sc that has the same field name at each level, and returns
// the Arrow field at its end. Names are matched instead of positions
// because a scan may order struct children differently from the schema.
func arrowColumnPath(fields []iceberg.NestedField, schemaPath []int, sc *arrow.Schema) ([]int, arrow.Field, error) {
	path := make([]int, len(schemaPath))
	names := make([]string, 0, len(schemaPath))
	var leaf arrow.Field
	for depth, idx := range schemaPath {
		f := fields[idx]
		names = append(names, f.Name)
		var candidates []int
		if depth == 0 {
			candidates = sc.FieldIndices(f.Name)
		} else {
			st, ok := leaf.Type.(*arrow.StructType)
			if !ok {
				return nil, arrow.Field{}, fmt.Errorf("column %q is %s in the record schema, not a struct",
					strings.Join(names[:depth], "."), leaf.Type)
			}
			candidates = st.FieldIndices(f.Name)
		}
		if len(candidates) != 1 {
			return nil, arrow.Field{}, fmt.Errorf("column %q not found in the record schema", strings.Join(names, "."))
		}
		path[depth] = candidates[0]
		if depth == 0 {
			leaf = sc.Field(candidates[0])
		} else {
			leaf = leaf.Type.(*arrow.StructType).Field(candidates[0])
		}
		if st, ok := f.Type.(*iceberg.StructType); ok {
			fields = st.FieldList
		}
	}

	return path, leaf, nil
}

// sortRecordBatch returns the rows of batch stably ordered by rows. When
// batch is already in order it is returned retained rather than copied.
//
// The writer uses it only for sort orders with a source nested in a
// struct, which Arrow's sort kernel cannot order by. It is slower than the
// kernel, so once Arrow supports nested sort keys this fallback can go.
func sortRecordBatch(ctx context.Context, batch arrow.RecordBatch, rows rowComparator) (arrow.RecordBatch, error) {
	// With a single batch, a row's rowRef is its row index.
	compare := rows.bind([]arrow.RecordBatch{batch})
	indices := make([]int64, batch.NumRows())
	for i := range indices {
		indices[i] = int64(i)
	}
	slices.SortStableFunc(indices, func(x, y int64) int { return compare(rowRef(x), rowRef(y)) })

	identity := true
	for i, idx := range indices {
		if idx != int64(i) {
			identity = false

			break
		}
	}
	if identity {
		batch.Retain()

		return batch, nil
	}

	return partitionBatchByKey(ctx)(batch, indices)
}
