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
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

type orderedPartitionTestRow struct {
	key   int64
	valid bool
	label string
}

func orderedPartitionTestSetup(t testing.TB) (*arrow.Schema, rowComparator) {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "key", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "key", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String},
	)
	order, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
		Direction: SortASC, NullOrder: NullsFirst,
	}})
	if err != nil {
		t.Fatal(err)
	}
	cmp, err := newRowComparator(order, schema, sc)
	if err != nil {
		t.Fatal(err)
	}

	return sc, cmp
}

func makeOrderedPartitionTestBatch(mem memory.Allocator, sc *arrow.Schema, rows []orderedPartitionTestRow) arrow.RecordBatch {
	builder := array.NewRecordBuilder(mem, sc)
	defer builder.Release()
	keys := builder.Field(0).(*array.Int64Builder)
	labels := builder.Field(1).(*array.StringBuilder)
	for _, row := range rows {
		if row.valid {
			keys.Append(row.key)
		} else {
			keys.AppendNull()
		}
		labels.Append(row.label)
	}

	return builder.NewRecordBatch()
}

func TestYieldSortedPartitionFastPathPreservesTiesAndNulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	sc, cmp := orderedPartitionTestSetup(t)
	batches := []arrow.RecordBatch{
		makeOrderedPartitionTestBatch(mem, sc, []orderedPartitionTestRow{
			{label: "null"}, {key: 1, valid: true, label: "a"}, {key: 1, valid: true, label: "b"},
		}),
		makeOrderedPartitionTestBatch(mem, sc, []orderedPartitionTestRow{
			{key: 1, valid: true, label: "c"},
			{key: 2, valid: true, label: "d"},
			{key: 3, valid: true, label: "e"},
			{key: 3, valid: true, label: "f"},
			{key: 4, valid: true, label: "g"},
		}),
	}
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	var got []string
	var sizes []int64
	calls := 0
	ok := yieldSortedPartition(t.Context(), batches, cmp, 3, func(rec arrow.RecordBatch, err error) bool {
		if err != nil {
			t.Fatalf("yield: %v", err)
		}
		defer rec.Release()
		if calls == 0 && rec != batches[0] {
			t.Fatal("full input batch was copied")
		}
		calls++
		sizes = append(sizes, rec.NumRows())
		col := rec.Column(1).(*array.String)
		for i := range col.Len() {
			got = append(got, col.Value(i))
		}

		return true
	})
	if !ok {
		t.Fatal("iteration stopped early")
	}
	if want := []int64{3, 3, 2}; !slices.Equal(sizes, want) {
		t.Fatalf("batch sizes %v; want %v", sizes, want)
	}
	if want := []string{"null", "a", "b", "c", "d", "e", "f", "g"}; !slices.Equal(got, want) {
		t.Fatalf("row order %v; want %v", got, want)
	}
}

func TestYieldSortedPartitionFastPathFallsBack(t *testing.T) {
	tests := []struct {
		name  string
		input [][]orderedPartitionTestRow
		want  []string
	}{
		{name: "overlapping batches", input: [][]orderedPartitionTestRow{
			{{key: 1, valid: true, label: "a"}, {key: 4, valid: true, label: "b"}, {key: 4, valid: true, label: "c"}},
			{{key: 0, valid: true, label: "d"}, {key: 2, valid: true, label: "e"}, {key: 4, valid: true, label: "f"}},
		}, want: []string{"d", "a", "e", "b", "c", "f"}},
		{name: "unordered within batch", input: [][]orderedPartitionTestRow{
			{{key: 5, valid: true, label: "a"}, {key: 1, valid: true, label: "b"}, {key: 5, valid: true, label: "c"}},
			{{key: 2, valid: true, label: "d"}, {key: 3, valid: true, label: "e"}, {key: 4, valid: true, label: "f"}},
		}, want: []string{"b", "d", "e", "f", "a", "c"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { mem.AssertSize(t, 0) })
			sc, cmp := orderedPartitionTestSetup(t)
			batches := make([]arrow.RecordBatch, len(tc.input))
			for i, rows := range tc.input {
				batches[i] = makeOrderedPartitionTestBatch(mem, sc, rows)
			}
			defer func() {
				for _, batch := range batches {
					batch.Release()
				}
			}()
			var got []string
			ok := yieldSortedPartition(t.Context(), batches, cmp, 2, func(rec arrow.RecordBatch, err error) bool {
				if err != nil {
					t.Fatalf("yield: %v", err)
				}
				defer rec.Release()
				col := rec.Column(1).(*array.String)
				for i := range col.Len() {
					got = append(got, col.Value(i))
				}

				return true
			})
			if !ok || !slices.Equal(got, tc.want) {
				t.Fatalf("got %v (ok=%v), want %v", got, ok, tc.want)
			}
		})
	}
}

func TestYieldSortedPartitionFastPathCancellation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	sc, cmp := orderedPartitionTestSetup(t)
	batch := makeOrderedPartitionTestBatch(mem, sc, []orderedPartitionTestRow{{key: 1, valid: true, label: "a"}})
	defer batch.Release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	ok := yieldSortedPartition(ctx, []arrow.RecordBatch{batch}, cmp, 1, func(rec arrow.RecordBatch, err error) bool {
		calls++
		if rec != nil {
			rec.Release()
			t.Fatal("expected cancellation, got record")
		}
		if err != context.Canceled {
			t.Fatalf("got %v, want context.Canceled", err)
		}

		return true
	})
	if ok || calls != 1 {
		t.Fatalf("ok=%v calls=%d", ok, calls)
	}
}

// legacyYieldSortedPartition retains the pre-fast-path implementation to
// measure before and after on the same Go toolchain and Arrow version.
func legacyYieldSortedPartition(
	ctx context.Context, batches []arrow.RecordBatch, rows rowComparator, batchRows int64,
	yield func(arrow.RecordBatch, error) bool,
) bool {
	if len(batches) == 0 {
		return true
	}
	sc := batches[0].Schema()
	var total int64
	for _, rec := range batches {
		if !rec.Schema().Equal(sc) {
			yield(nil, errors.New("different schemas"))

			return false
		}
		total += rec.NumRows()
	}
	refs := make([]rowRef, 0, total)
	for batch, rec := range batches {
		for row := range int(rec.NumRows()) {
			refs = append(refs, newRowRef(batch, row))
		}
	}
	slices.SortStableFunc(refs, rows.bind(batches))
	takeFn := partitionBatchByKey(ctx)
	mem := compute.GetAllocator(ctx)
	for off := 0; off < len(refs); off += int(batchRows) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)

			return false
		}
		out, err := gatherSortedRows(mem, takeFn, batches, refs[off:min(off+int(batchRows), len(refs))])
		if err != nil {
			yield(nil, err)

			return false
		}
		if !yield(out, nil) {
			return false
		}
	}

	return true
}

func BenchmarkYieldSortedPartitionFastPath(b *testing.B) {
	const (
		batchCount   = 8
		rowsPerBatch = 4096
		columnCount  = 16
		outputRows   = 8192
	)
	for _, mode := range []string{"globally_sorted", "nearly_sorted", "interleaved", "shuffled"} {
		b.Run(mode, func(b *testing.B) {
			fields := make([]arrow.Field, columnCount)
			for i := range fields {
				fields[i] = arrow.Field{Name: "c" + strconv.Itoa(i), Type: arrow.PrimitiveTypes.Int64}
			}
			sc := arrow.NewSchema(fields, nil)
			schemaFields := make([]iceberg.NestedField, columnCount)
			for i := range fields {
				schemaFields[i] = iceberg.NestedField{ID: i + 1, Name: fields[i].Name, Type: iceberg.PrimitiveTypes.Int64, Required: true}
			}
			order, err := NewSortOrder(1, []SortField{{
				SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
				Direction: SortASC, NullOrder: NullsFirst,
			}})
			if err != nil {
				b.Fatal(err)
			}
			comparator, err := newRowComparator(order, iceberg.NewSchema(0, schemaFields...), sc)
			if err != nil {
				b.Fatal(err)
			}
			batches := make([]arrow.RecordBatch, batchCount)
			for batch := range batches {
				builder := array.NewRecordBuilder(memory.DefaultAllocator, sc)
				for row := range rowsPerBatch {
					var key int64
					switch mode {
					case "globally_sorted", "nearly_sorted":
						key = int64(batch*rowsPerBatch + row)
						if mode == "nearly_sorted" && batch == batchCount-1 && row == rowsPerBatch-1 {
							key = 0
						}
					case "interleaved":
						key = int64(row*batchCount + batch)
					case "shuffled":
						key = int64((row*37%rowsPerBatch)*batchCount + batch)
					}
					for col := range columnCount {
						builder.Field(col).(*array.Int64Builder).Append(key + int64(col))
					}
				}
				batches[batch] = builder.NewRecordBatch()
				builder.Release()
			}
			b.Cleanup(func() {
				for _, batch := range batches {
					batch.Release()
				}
			})
			for _, method := range []struct {
				name string
				fn   func(context.Context, []arrow.RecordBatch, rowComparator, int64, func(arrow.RecordBatch, error) bool) bool
			}{
				{"legacy", legacyYieldSortedPartition},
				{"fastpath", yieldSortedPartition},
			} {
				b.Run(method.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(batchCount*rowsPerBatch, "rows/op")
					b.ResetTimer()
					for range b.N {
						var count int64
						ok := method.fn(b.Context(), batches, comparator, outputRows, func(rec arrow.RecordBatch, err error) bool {
							if err != nil {
								b.Fatal(err)
							}
							count += rec.NumRows()
							rec.Release()

							return true
						})
						if !ok || count != batchCount*rowsPerBatch {
							b.Fatalf("got %d rows (ok=%v)", count, ok)
						}
					}
				})
			}
		})
	}
}
