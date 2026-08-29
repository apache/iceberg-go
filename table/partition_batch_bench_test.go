// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

func BenchmarkPartitionBatchByKey(b *testing.B) {
	const rows = 65_536

	full := make([]int64, rows)
	contiguous := make([]int64, rows/2)
	scattered := make([]int64, rows/2)
	for row := range rows {
		full[row] = int64(row)
		if row < rows/2 {
			contiguous[row] = int64(rows/4 + row)
			scattered[row] = int64(row * 2)
		}
	}

	for _, columns := range []int{1, 8} {
		record := newPartitionBatchBenchmarkRecord(columns, rows)
		b.Run(fmt.Sprintf("%d_columns", columns), func(b *testing.B) {
			for _, test := range []struct {
				name    string
				indices []int64
			}{
				{name: "full", indices: full},
				{name: "contiguous_half", indices: contiguous},
				{name: "scattered_half", indices: scattered},
			} {
				b.Run(test.name, func(b *testing.B) {
					partitionBatch := partitionBatchByKey(context.Background())
					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						batch, err := partitionBatch(record, test.indices)
						if err != nil {
							b.Fatal(err)
						}
						batch.Release()
					}
				})
			}
		})
		record.Release()
	}
}

func BenchmarkMaterializeDictionaryColumns(b *testing.B) {
	const rows = 1024

	for _, columns := range []int{1, 8, 64} {
		record := newPartitionBatchBenchmarkRecord(columns, rows)
		b.Run(fmt.Sprintf("no_dictionaries_%d_columns", columns), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				record.Retain()
				materialized, err := materializeDictionaryColumns(context.Background(), record, arrow.Metadata{})
				if err != nil {
					b.Fatal(err)
				}
				materialized.Release()
			}
		})
		record.Release()
	}
}

func newPartitionBatchBenchmarkRecord(columns, rows int) arrow.RecordBatch {
	fields := make([]arrow.Field, columns)
	arrays := make([]arrow.Array, columns)
	for column := range columns {
		fields[column] = arrow.Field{Name: fmt.Sprintf("value_%d", column), Type: arrow.PrimitiveTypes.Int64}
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.Reserve(rows)
		for row := range rows {
			builder.Append(int64(row + column))
		}
		arrays[column] = builder.NewArray()
		builder.Release()
	}

	record := array.NewRecordBatch(arrow.NewSchema(fields, nil), arrays, int64(rows))
	for _, values := range arrays {
		values.Release()
	}

	return record
}
