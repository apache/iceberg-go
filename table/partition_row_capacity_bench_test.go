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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

func BenchmarkPartitionRowCapacity(b *testing.B) {
	const rows = 32_768

	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: arrow.PrimitiveTypes.Int32}}, nil)
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})

	tests := []struct {
		name  string
		value func(int) int32
	}{
		{name: "one_partition", value: func(int) int32 { return 0 }},
		{name: "16_partitions", value: func(row int) int32 { return int32(row % 16) }},
		{name: "1024_partitions", value: func(row int) int32 { return int32(row % 1024) }},
		{name: "one_partition_per_row", value: func(row int) int32 { return int32(row) }},
		{
			name: "90_percent_one_partition",
			value: func(row int) int32 {
				if row < rows*9/10 {
					return 0
				}

				return int32(row)
			},
		},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			builder := array.NewInt32Builder(memory.DefaultAllocator)
			builder.Reserve(rows)
			for row := range rows {
				builder.Append(test.value(row))
			}
			column := builder.NewArray()
			builder.Release()

			record := array.NewRecordBatch(arrowSchema, []arrow.Array{column}, rows)
			column.Release()
			defer record.Release()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := getRecordPartitions(spec, icebergSchema, record); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
