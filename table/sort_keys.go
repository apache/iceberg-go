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
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
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
//   - Only top-level sources are accepted, because a compute.SortKey
//     addresses a column only by its top-level index (ColumnIndex). The
//     writer checks allTopLevelSortSources first and sorts orders with a
//     nested source with sortRecordBatch instead.
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
// batch by it directly; otherwise the writer uses sortRecordBatch.
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
	leaf := arrow.Field{Type: arrow.StructOf(sc.Fields()...)}
	for depth, idx := range schemaPath {
		f := fields[idx]
		st, ok := leaf.Type.(*arrow.StructType)
		if !ok {
			return nil, arrow.Field{}, fmt.Errorf("column %q is %s in the record schema, not a struct",
				strings.Join(names, "."), leaf.Type)
		}
		names = append(names, f.Name)
		candidates := st.FieldIndices(f.Name)
		if len(candidates) != 1 {
			return nil, arrow.Field{}, fmt.Errorf("column %q not found in the record schema", strings.Join(names, "."))
		}
		path[depth] = candidates[0]
		leaf = st.Field(candidates[0])
		if st, ok := f.Type.(*iceberg.StructType); ok {
			fields = st.FieldList
		}
	}

	return path, leaf, nil
}

// nestedSortKey is one field of a nestedSortKeys.
type nestedSortKey struct {
	// path is the struct-child index path from a top-level column down to
	// the sort source, as in sortKeyColumn.
	path []int
	// key orders the hoisted column; its ColumnIndex is the field's
	// position in nestedSortKeys.
	key compute.SortKey
}

// nestedSortKeys orders record batches by sort sources that may be nested
// in structs. Arrow's sort kernel addresses only top-level columns, so
// sortRecordBatch hoists each source into a top-level column of a
// zero-copy batch, sorts that with the kernel, and takes the original
// batch's rows in the resulting order.
type nestedSortKeys struct {
	fields []nestedSortKey
	// schema is the hoisted batch's schema.
	schema *arrow.Schema
}

// newNestedSortKeys resolves order against the record schema sc, finding
// sources as newRowComparator does. Order semantics match
// resolveSortKeys: float keys use FloatTotalOrder and each field orders by
// its first source column.
//
// As with resolveSortKeys, a source type the kernel cannot sort is
// reported by the first sortRecordBatch, not here. Extension sources are
// sorted by their storage.
func newNestedSortKeys(order SortOrder, schema *iceberg.Schema, sc *arrow.Schema) (nestedSortKeys, error) {
	fields := schema.FieldsRef(iceberginternal.SchemaRef{})
	keys := nestedSortKeys{fields: make([]nestedSortKey, 0, order.Len())}
	hoisted := make([]arrow.Field, 0, order.Len())
	for _, field := range order.fields {
		schemaPath, err := sortFieldPath(schema, fields, field.FirstSourceID(), order.OrderID())
		if err != nil {
			return nestedSortKeys{}, err
		}
		path, leaf, err := arrowColumnPath(fields, schemaPath, sc)
		if err != nil {
			return nestedSortKeys{}, fmt.Errorf("sort order %d: %w", order.OrderID(), err)
		}
		typ := leaf.Type
		if ext, ok := typ.(arrow.ExtensionType); ok {
			typ = ext.StorageType()
		}
		key := compute.SortKey{
			ColumnIndex: len(keys.fields), Order: compute.SortOrderAscending,
			NullPlacement: compute.SortNullsAtEnd, FloatTotalOrder: true,
		}
		if field.Direction == SortDESC {
			key.Order = compute.SortOrderDescending
		}
		if field.NullOrder == NullsFirst {
			key.NullPlacement = compute.SortNullsAtStart
		}
		keys.fields = append(keys.fields, nestedSortKey{path: path, key: key})
		hoisted = append(hoisted, arrow.Field{Name: strconv.Itoa(len(hoisted)), Type: typ, Nullable: true})
	}
	keys.schema = arrow.NewSchema(hoisted, nil)

	return keys, nil
}

// sortRecordBatch returns the rows of batch stably ordered by keys. When
// batch is already in order it is returned retained rather than copied.
//
// Hoisting shares the source columns' buffers; only a source under a
// struct with nulls gets a new validity bitmap (see hoistSortColumn). The
// kernel's indices and the reordered copy of batch are allocated as for
// compute.SortRecordBatch.
func sortRecordBatch(ctx context.Context, batch arrow.RecordBatch, keys nestedSortKeys) (arrow.RecordBatch, error) {
	mem := compute.GetAllocator(ctx)
	cols := make([]arrow.Array, 0, len(keys.fields))
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()
	sortKeys := make([]compute.SortKey, len(keys.fields))
	for i, f := range keys.fields {
		cols = append(cols, hoistSortColumn(mem, batch, f.path))
		sortKeys[i] = f.key
	}
	hoisted := array.NewRecordBatch(keys.schema, cols, batch.NumRows())
	defer hoisted.Release()

	indices, err := compute.SortIndicesRecordBatch(ctx, hoisted, sortKeys)
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	if isIdentityPermutation(indices.(*array.Uint64).Uint64Values()) {
		batch.Retain()

		return batch, nil
	}

	out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
		compute.NewDatumWithoutOwning(batch), compute.NewDatumWithoutOwning(indices))
	if err != nil {
		return nil, err
	}
	defer out.Release()
	sorted := out.(*compute.RecordDatum).Value
	sorted.Retain()

	return sorted, nil
}

func isIdentityPermutation(indices []uint64) bool {
	for i, idx := range indices {
		if idx != uint64(i) {
			return false
		}
	}

	return true
}

// hoistSortColumn returns the array at path in batch, retained, with
// extension arrays unwrapped to their storage. A struct child's validity
// ignores its parents', so where an enclosing struct has nulls the result
// is the child with a new validity bitmap: the AND of the child's and
// every enclosing struct's. Its other buffers are shared.
func hoistSortColumn(mem memory.Allocator, batch arrow.RecordBatch, path []int) arrow.Array {
	col := batch.Column(path[0])
	var nullParents []arrow.Array
	for _, idx := range path[1:] {
		if col.NullN() > 0 {
			nullParents = append(nullParents, col)
		}
		col = col.(*array.Struct).Field(idx)
	}
	if ext, ok := col.(array.ExtensionArray); ok {
		col = ext.Storage()
	}
	if len(nullParents) == 0 {
		col.Retain()

		return col
	}

	// The bitmap keeps the child's offset, since an ArrayData offset
	// applies to all of its buffers.
	data := col.Data()
	off, n := int64(data.Offset()), int64(data.Len())
	size := int(bitutil.BytesForBits(off + n))
	validity := memory.NewResizableBuffer(mem)
	validity.Resize(size)
	defer validity.Release()
	if col.NullN() > 0 {
		bitutil.CopyBitmap(data.Buffers()[0].Bytes(), int(off), int(n), validity.Bytes(), int(off))
	} else {
		bitutil.SetBitsTo(validity.Bytes(), off, n, true)
	}
	// AND into scratch and swap, rather than in place: BitmapAnd does not
	// document whether out may alias an input.
	scratch := memory.NewResizableBuffer(mem)
	scratch.Resize(size)
	defer scratch.Release()
	for _, parent := range nullParents {
		pd := parent.Data()
		bitutil.BitmapAnd(validity.Bytes(), pd.Buffers()[0].Bytes(), off, int64(pd.Offset()), scratch.Bytes(), off, n)
		validity, scratch = scratch, validity
	}

	buffers := slices.Clone(data.Buffers())
	buffers[0] = validity
	masked := array.NewData(data.DataType(), int(n), buffers, data.Children(), array.UnknownNullCount, int(off))
	defer masked.Release()

	return array.MakeFromData(masked)
}
