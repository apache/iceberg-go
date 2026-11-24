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
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

var resolveSortKeysBenchmarkSink []compute.SortKey

func BenchmarkResolveSortKeys(b *testing.B) {
	for _, tc := range []struct {
		fieldCount int
		sortFields int
	}{
		{fieldCount: 32, sortFields: 1},
		{fieldCount: 32, sortFields: 4},
		{fieldCount: 32, sortFields: 16},
		{fieldCount: 256, sortFields: 1},
		{fieldCount: 256, sortFields: 8},
		{fieldCount: 256, sortFields: 32},
		{fieldCount: 2_048, sortFields: 1},
		{fieldCount: 2_048, sortFields: 8},
		{fieldCount: 2_048, sortFields: 32},
	} {
		b.Run(fmt.Sprintf("fields=%d/sort-keys=%d", tc.fieldCount, tc.sortFields), func(b *testing.B) {
			schema := benchmarkSortKeysSchema(tc.fieldCount, false)
			order := benchmarkSortKeysOrder(tc.sortFields)

			b.Run("before", func(b *testing.B) {
				benchmarkResolveSortKeys(b, order, schema, resolveSortKeysBefore)
			})
			b.Run("after", func(b *testing.B) {
				benchmarkResolveSortKeys(b, order, schema, resolveSortKeys)
			})
		})
	}

	b.Run("fields=128/sort-keys=16/nested", func(b *testing.B) {
		schema := benchmarkSortKeysSchema(128, true)
		order := benchmarkSortKeysOrder(16)

		b.Run("before", func(b *testing.B) {
			benchmarkResolveSortKeys(b, order, schema, resolveSortKeysBefore)
		})
		b.Run("after", func(b *testing.B) {
			benchmarkResolveSortKeys(b, order, schema, resolveSortKeys)
		})
	})
}

type resolveSortKeysBenchmarkFunc func(SortOrder, *iceberg.Schema) ([]compute.SortKey, error)

func benchmarkResolveSortKeys(
	b *testing.B,
	order SortOrder,
	schema *iceberg.Schema,
	resolve resolveSortKeysBenchmarkFunc,
) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		keys, err := resolve(order, schema)
		if err != nil {
			b.Fatal(err)
		}
		resolveSortKeysBenchmarkSink = keys
	}
}

func benchmarkSortKeysSchema(fieldCount int, nested bool) *iceberg.Schema {
	fields := make([]iceberg.NestedField, fieldCount)
	nextID := fieldCount + 1
	for i := range fieldCount {
		field := iceberg.NestedField{
			ID:       i + 1,
			Name:     fmt.Sprintf("field_%d", i),
			Required: true,
			Type:     iceberg.PrimitiveTypes.Int64,
		}
		if nested {
			children := make([]iceberg.NestedField, 4)
			for child := range children {
				children[child] = iceberg.NestedField{
					ID:       nextID,
					Name:     fmt.Sprintf("child_%d", child),
					Required: true,
					Type:     iceberg.PrimitiveTypes.Int64,
				}
				nextID++
			}
			field.Type = iceberg.NewStructType(children...)
		}
		fields[i] = field
	}

	return iceberg.NewSchema(1, fields...)
}

func benchmarkSortKeysOrder(sortFieldCount int) SortOrder {
	fields := make([]SortField, sortFieldCount)
	for i := range sortFieldCount {
		fields[i] = SortField{
			SourceIDs: []int{i + 1}, Transform: iceberg.IdentityTransform{},
			Direction: SortASC, NullOrder: NullsLast,
		}
	}

	order, err := NewSortOrder(1, fields)
	if err != nil {
		panic(err)
	}

	return order
}

// resolveSortKeysBefore keeps the previous implementation available for
// before/after benchmark comparisons from a single checkout.
func resolveSortKeysBefore(order SortOrder, fileSchema *iceberg.Schema) ([]compute.SortKey, error) {
	if order.IsUnsorted() {
		return nil, nil
	}

	keys := make([]compute.SortKey, 0, order.Len())
	for _, field := range order.fields {
		idx, ok := topLevelFieldIndexBefore(fileSchema, field.FirstSourceID())
		if !ok {
			return nil, fmt.Errorf("sort order %d: source id %d is not a top-level column in schema",
				order.OrderID(), field.FirstSourceID())
		}

		key := compute.SortKey{ColumnIndex: idx, Order: compute.SortOrderAscending, NullPlacement: compute.SortNullsAtEnd}
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

func topLevelFieldIndexBefore(schema *iceberg.Schema, sourceID int) (int, bool) {
	for i, f := range schema.Fields() {
		if f.ID == sourceID {
			return i, true
		}
	}

	return 0, false
}

// BenchmarkSortRecordBatch compares sorting a batch with the row comparator
// (sortRecordBatchByComparator) and with the Arrow kernel, on one int64, int64-with-nulls, float64 or string key.
// Arrow radix-sorts a single non-null integer key, and with FloatTotalOrder
// a single non-null float key, so the int64 and int64-nulls cases take
// different Arrow code paths. The Arrow side uses resolveSortKeys, as the
// writer does.
func BenchmarkSortRecordBatch(b *testing.B) {
	const rows = 1 << 16
	mem := memory.NewGoAllocator()
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	rng := rand.New(rand.NewPCG(1, 2))
	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()
	for range rows {
		bldr.Field(0).(*array.Int64Builder).Append(rng.Int64N(1 << 20))
		bldr.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("key-%08d", rng.IntN(1<<20)))
		if rng.IntN(10) == 0 {
			bldr.Field(2).AppendNull()
		} else {
			bldr.Field(2).(*array.Int64Builder).Append(rng.Int64N(1 << 20))
		}
		bldr.Field(3).(*array.Float64Builder).Append(rng.NormFloat64())
	}
	batch := bldr.NewRecordBatch()
	defer batch.Release()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "i", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "s", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "n", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 4, Name: "f", Type: iceberg.PrimitiveTypes.Float64},
	)
	ctx := compute.WithAllocator(b.Context(), mem)
	for _, key := range []struct {
		name string
		col  int
	}{{"int64", 0}, {"int64-nulls", 2}, {"float64", 3}, {"string", 1}} {
		order, err := NewSortOrder(1, []SortField{{
			SourceIDs: []int{key.col + 1}, Transform: iceberg.IdentityTransform{},
			Direction: SortASC, NullOrder: NullsFirst,
		}})
		if err != nil {
			b.Fatal(err)
		}
		cmp, err := newRowComparator(order, schema, sc)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("key="+key.name+"/impl=rowComparator", func(b *testing.B) {
			for range b.N {
				out, err := sortRecordBatchByComparator(ctx, batch, cmp)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
		b.Run("key="+key.name+"/impl=arrow", func(b *testing.B) {
			keys, err := resolveSortKeys(order, schema)
			if err != nil {
				b.Fatal(err)
			}
			for range b.N {
				out, err := compute.SortRecordBatch(ctx, batch, keys)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
		})
	}
}

// BenchmarkSortRecordBatchNested compares the two ways to sort a batch by
// a key nested in a struct: the row comparator (sortRecordBatchByComparator)
// and hoisting the key for Arrow's kernel (sortRecordBatch). impl=top-level
// sorts the same values held in a top-level column with the kernel
// directly, as the floor hoisting can reach. parent-nulls makes 10% of the
// enclosing structs null, so hoisting must build a validity bitmap.
func BenchmarkSortRecordBatchNested(b *testing.B) {
	const rows = 1 << 16
	mem := memory.NewGoAllocator()
	leaves := []struct {
		name string
		ice  iceberg.Type
	}{
		{"int64", iceberg.PrimitiveTypes.Int64},
		{"int64-nulls", iceberg.PrimitiveTypes.Int64},
		{"float64", iceberg.PrimitiveTypes.Float64},
		{"string", iceberg.PrimitiveTypes.String},
	}
	// Columns: one top-level column per leaf, then struct "s" and struct
	// "sn" (with null parents), each holding a copy of every leaf.
	var fields []iceberg.NestedField
	id := 1
	for _, l := range leaves {
		fields = append(fields, iceberg.NestedField{ID: id, Name: l.name, Type: l.ice})
		id++
	}
	for _, parent := range []string{"s", "sn"} {
		children := make([]iceberg.NestedField, len(leaves))
		for i, l := range leaves {
			children[i] = iceberg.NestedField{ID: id + 1 + i, Name: l.name, Type: l.ice}
		}
		fields = append(fields, iceberg.NestedField{ID: id, Name: parent, Type: iceberg.NewStructType(children...)})
		id += 1 + len(leaves)
	}
	schema := iceberg.NewSchema(0, fields...)
	sc, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		b.Fatal(err)
	}

	rng := rand.New(rand.NewPCG(1, 2))
	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()
	appendLeaf := func(fb array.Builder, leaf int, v int64, f float64, null bool) {
		if null {
			fb.AppendNull()

			return
		}
		switch leaf {
		case 0, 1:
			fb.(*array.Int64Builder).Append(v)
		case 2:
			fb.(*array.Float64Builder).Append(f)
		case 3:
			fb.(*array.StringBuilder).Append(fmt.Sprintf("key-%08d", v))
		}
	}
	for range rows {
		v, f, null := rng.Int64N(1<<20), rng.NormFloat64(), rng.IntN(10) == 0
		for leaf := range leaves {
			leafNull := leaf == 1 && null
			appendLeaf(bldr.Field(leaf), leaf, v, f, leafNull)
			for p, valid := range []bool{true, rng.IntN(10) != 0} {
				st := bldr.Field(len(leaves) + p).(*array.StructBuilder)
				if leaf == 0 {
					st.AppendValues([]bool{valid})
				}
				appendLeaf(st.FieldBuilder(leaf), leaf, v, f, leafNull)
			}
		}
	}
	batch := bldr.NewRecordBatch()
	defer batch.Release()

	ctx := compute.WithAllocator(b.Context(), mem)
	run := func(b *testing.B, sort func() (arrow.RecordBatch, error)) {
		b.ReportAllocs()
		for range b.N {
			out, err := sort()
			if err != nil {
				b.Fatal(err)
			}
			out.Release()
		}
	}
	orderOn := func(sourceID int) SortOrder {
		order, err := NewSortOrder(1, []SortField{identSort(sourceID, SortASC, NullsFirst)})
		if err != nil {
			b.Fatal(err)
		}

		return order
	}
	for leaf, l := range leaves {
		topLevel := orderOn(leaf + 1)
		b.Run("key="+l.name+"/impl=top-level", func(b *testing.B) {
			keys, err := resolveSortKeys(topLevel, schema)
			if err != nil {
				b.Fatal(err)
			}
			run(b, func() (arrow.RecordBatch, error) { return compute.SortRecordBatch(ctx, batch, keys) })
		})
		for p, parent := range []string{"no-parent-nulls", "parent-nulls"} {
			parentID := len(leaves) + 1 + p*(1+len(leaves))
			order := orderOn(parentID + 1 + leaf)
			cmp, err := newRowComparator(order, schema, sc)
			if err != nil {
				b.Fatal(err)
			}
			keys, err := newNestedSortKeys(order, schema, sc)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("key="+l.name+"/"+parent+"/impl=rowComparator", func(b *testing.B) {
				run(b, func() (arrow.RecordBatch, error) { return sortRecordBatchByComparator(ctx, batch, cmp) })
			})
			b.Run("key="+l.name+"/"+parent+"/impl=hoisted", func(b *testing.B) {
				run(b, func() (arrow.RecordBatch, error) { return sortRecordBatch(ctx, batch, keys) })
			})
		}
	}
}
