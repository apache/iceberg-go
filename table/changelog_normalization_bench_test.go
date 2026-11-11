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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkChangelogEmissionWide(b *testing.B) {
	const width, batchCount, batchRows = 16, 4, 1024
	fields := make([]arrow.Field, 0, width+3)
	for i := range width {
		fields = append(fields, arrow.Field{Name: fmt.Sprintf("c%d", i), Type: arrow.PrimitiveTypes.Int64})
	}
	fields = append(fields, changelogArrowFields(false)...)
	schema := arrow.NewSchema(fields, nil)
	batches := make([]arrow.RecordBatch, 0, batchCount)
	for j := range batchCount {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for row := range batchRows {
			for col := range width {
				builder.Field(col).(*array.Int64Builder).Append(int64(j*batchRows + row + col))
			}
			builder.Field(width).(*array.StringBuilder).Append("INSERT")
			builder.Field(width + 1).(*array.Int32Builder).Append(1)
			builder.Field(width + 2).(*array.Int64Builder).Append(7)
		}
		batches = append(batches, builder.NewRecordBatch())
		builder.Release()
	}
	defer func() {
		for _, rec := range batches {
			rec.Release()
		}
	}()
	refs := make([]rowRef, 0, batchCount*batchRows)
	ops := make([]ChangelogOperation, 0, cap(refs))
	for row := range batchRows {
		for batch := range batchCount {
			refs = append(refs, newRowRef(batch, row))
			ops = append(ops, ChangelogOpInsert)
		}
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec, err := gatherNormalizedChangelogRows(ctx, batches, refs, ops)
		if err != nil {
			b.Fatal(err)
		}
		rec.Release()
	}
}

func BenchmarkChangelogEmissionSparse(b *testing.B) {
	fields := append([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, changelogArrowFields(false)...)
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("INSERT")
	builder.Field(2).(*array.Int32Builder).Append(1)
	builder.Field(3).(*array.Int64Builder).Append(7)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	batches := make([]arrow.RecordBatch, 50000)
	for i := range batches {
		batches[i] = record
	}
	refs := []rowRef{newRowRef(len(batches)-1, 0)}
	ops := []ChangelogOperation{ChangelogOpInsert}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		out, err := gatherNormalizedChangelogRows(ctx, batches, refs, ops)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
