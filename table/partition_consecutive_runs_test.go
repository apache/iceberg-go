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
	"reflect"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// This deliberately retains the old per-row extraction path as a benchmark
// baseline and as an independent oracle for consecutive-row reuse.
func partitionExtractionPerRowForTest(plan *partitionExtractionPlan, record arrow.RecordBatch) ([]*partitionInfo, error) {
	partitions := newPartitionMapNode()
	values := make(partitionRecord, len(plan.fields))
	for row := range record.NumRows() {
		for i, field := range plan.fields {
			col := record.Column(field.columnPath[0])
			if col.IsNull(int(row)) {
				values[i] = nil
				continue
			}

			value, err := field.valueAt(col, int(row))
			if err != nil {
				return nil, err
			}
			values[i] = value
		}
		part := partitions.getOrCreate(values, plan.fields, record.NumRows())
		part.rows = append(part.rows, row)
	}

	return partitions.collectPartitions(), nil
}

func assertPartitionExtractionEquivalent(t *testing.T, plan *partitionExtractionPlan, record arrow.RecordBatch) {
	t.Helper()
	want, err := partitionExtractionPerRowForTest(plan, record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.getRecordPartitions(record)
	if err != nil {
		t.Fatal(err)
	}
	sortByFirstRow := func(parts []*partitionInfo) {
		sort.Slice(parts, func(i, j int) bool { return parts[i].rows[0] < parts[j].rows[0] })
	}
	sortByFirstRow(want)
	sortByFirstRow(got)
	if len(got) != len(want) {
		t.Fatalf("partition count: got %d, want %d", len(got), len(want))
	}
	for i := range got {
		if !reflect.DeepEqual(got[i].rows, want[i].rows) ||
			!reflect.DeepEqual(got[i].partitionValues, want[i].partitionValues) ||
			!reflect.DeepEqual(got[i].partitionRec, want[i].partitionRec) {
			t.Fatalf("partition %d differs: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestPartitionExtractionConsecutiveRows(t *testing.T) {
	const microsPerDay = 86_400_000_000
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: tsType, Nullable: true},
		{Name: "hostname", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "timestamp", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 2, Name: "hostname", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "day", Transform: iceberg.DayTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "host_bucket", Transform: iceberg.BucketTransform{NumBuckets: 32}},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1002, Name: "id", Transform: iceberg.IdentityTransform{}},
	)
	rows := []struct {
		ts   *int64
		host *string
		id   *int64
	}{}
	at := func(v int64) *int64 { return &v }
	str := func(v string) *string { return &v }
	for _, item := range []struct {
		ts *int64
		host *string
		id *int64
	}{
		{at(0), str("alpha"), at(1)}, {at(0), str("alpha"), at(1)},
		{at(0), str("alpha"), at(1)}, {at(0), str("beta"), at(1)},
		{at(microsPerDay), str("beta"), at(1)}, {at(microsPerDay), str("beta"), at(1)},
		{nil, nil, nil}, {nil, nil, nil}, {nil, str("beta"), nil},
		{at(0), str("alpha"), at(1)}, {at(0), str("alpha"), at(1)},
	} {
		rows = append(rows, item)
	}

	tb := array.NewTimestampBuilder(memory.DefaultAllocator, tsType)
	hb := array.NewStringBuilder(memory.DefaultAllocator)
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	defer tb.Release()
	defer hb.Release()
	defer ib.Release()
	for _, item := range rows {
		if item.ts == nil {
			tb.AppendNull()
		} else {
			tb.Append(arrow.Timestamp(*item.ts))
		}
		if item.host == nil {
			hb.AppendNull()
		} else {
			hb.Append(*item.host)
		}
		if item.id == nil {
			ib.AppendNull()
		} else {
			ib.Append(*item.id)
		}
	}
	columns := []arrow.Array{tb.NewArray(), hb.NewArray(), ib.NewArray()}
	record := array.NewRecordBatch(schema, columns, int64(len(rows)))
	for _, col := range columns {
		col.Release()
	}
	defer record.Release()

	plan, err := newPartitionExtractionPlan(spec, icebergSchema, schema)
	if err != nil {
		t.Fatal(err)
	}
	assertPartitionExtractionEquivalent(t, plan, record)
}

func BenchmarkPartitionExtractionConsecutiveRows(b *testing.B) {
	const rows = 65_536
	for _, runSize := range []int{1, 8, 256, 4096} {
		b.Run(fmt.Sprintf("run_%d", runSize), func(b *testing.B) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "host", Type: arrow.BinaryTypes.String}}, nil)
			icebergSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "host", Type: iceberg.PrimitiveTypes.String},
			)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "bucket", Transform: iceberg.BucketTransform{NumBuckets: 32},
			})
			builder := array.NewStringBuilder(memory.DefaultAllocator)
			for row := range rows {
				builder.Append(fmt.Sprintf("host-%04d", (row/runSize)%128))
			}
			col := builder.NewArray()
			builder.Release()
			record := array.NewRecordBatch(schema, []arrow.Array{col}, rows)
			col.Release()
			defer record.Release()

			plan, err := newPartitionExtractionPlan(spec, icebergSchema, schema)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("per_row", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := partitionExtractionPerRowForTest(plan, record); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("reuse", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := plan.getRecordPartitions(record); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
