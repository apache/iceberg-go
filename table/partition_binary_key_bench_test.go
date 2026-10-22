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
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

func BenchmarkBinaryPartitionGrouping(b *testing.B) {
	const rows = 65_536

	tests := []struct {
		name       string
		keySize    int
		partitions int
	}{
		{name: "16B_16_partitions", keySize: 16, partitions: 16},
		{name: "16B_1024_partitions", keySize: 16, partitions: 1_024},
		{name: "64B_1024_partitions", keySize: 64, partitions: 1_024},
		{name: "16B_32768_partitions", keySize: 16, partitions: 32_768},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			arrowSchema := arrow.NewSchema([]arrow.Field{
				{Name: "part", Type: arrow.BinaryTypes.Binary},
			}, nil)
			icebergSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Binary},
			)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
			})

			values := make([][]byte, test.partitions)
			for i := range values {
				value := make([]byte, test.keySize)
				binary.LittleEndian.PutUint64(value, uint64(i))
				values[i] = value
			}

			builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			for row := range rows {
				builder.Append(values[row%test.partitions])
			}
			column := builder.NewArray()
			builder.Release()

			record := array.NewRecordBatch(arrowSchema, []arrow.Array{column}, rows)
			column.Release()
			defer record.Release()

			plan, err := newPartitionExtractionPlan(spec, icebergSchema, record.Schema())
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				partitions, err := plan.getRecordPartitions(record)
				if err != nil {
					b.Fatal(err)
				}
				if len(partitions) != test.partitions {
					b.Fatalf("got %d partitions, want %d", len(partitions), test.partitions)
				}
			}
		})
	}
}
