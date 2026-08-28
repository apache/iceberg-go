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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/compute"
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
			field.Type = &iceberg.StructType{FieldList: children}
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
		idx, ok := topLevelFieldIndexBefore(fileSchema, field.SourceID())
		if !ok {
			return nil, fmt.Errorf("sort order %d: source id %d is not a top-level column in schema",
				order.OrderID(), field.SourceID())
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
