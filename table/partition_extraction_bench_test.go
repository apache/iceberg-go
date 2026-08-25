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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

func BenchmarkPartitionExtraction(b *testing.B) {
	const partitionFields = 4

	arrowFields := make([]arrow.Field, partitionFields)
	icebergFields := make([]iceberg.NestedField, partitionFields)
	specFields := make([]iceberg.PartitionField, partitionFields)
	for i := range partitionFields {
		name := fmt.Sprintf("part_%d", i)
		arrowFields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32}
		icebergFields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: iceberg.PrimitiveTypes.Int32}
		specFields[i] = iceberg.PartitionField{
			SourceIDs: []int{i + 1},
			FieldID:   1000 + i,
			Name:      name,
			Transform: iceberg.IdentityTransform{},
		}
	}

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	icebergSchema := iceberg.NewSchema(0, icebergFields...)
	spec := iceberg.NewPartitionSpec(specFields...)

	for _, rows := range []int{0, 1, 16, 1024} {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			columns := make([]arrow.Array, partitionFields)
			for i := range columns {
				builder := array.NewInt32Builder(memory.DefaultAllocator)
				for row := range rows {
					builder.Append(int32(row % 8))
				}
				columns[i] = builder.NewArray()
				builder.Release()
			}

			record := array.NewRecordBatch(arrowSchema, columns, int64(rows))
			for _, column := range columns {
				column.Release()
			}
			defer record.Release()
			writer := newPartitionedFanoutWriter(spec, icebergSchema, nil, nil)
			if _, err := writer.getPartitions(record); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := writer.getPartitions(record); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPartitionTransforms(b *testing.B) {
	const rows = 65_536

	tests := []struct {
		name        string
		arrowType   arrow.DataType
		icebergType iceberg.Type
		transform   iceberg.Transform
		appendRows  func(int) arrow.Array
	}{
		{
			name:        "identity_int64",
			arrowType:   arrow.PrimitiveTypes.Int64,
			icebergType: iceberg.PrimitiveTypes.Int64,
			transform:   iceberg.IdentityTransform{},
			appendRows: func(count int) arrow.Array {
				builder := array.NewInt64Builder(memory.DefaultAllocator)
				defer builder.Release()
				for row := range count {
					builder.Append(int64(row % 128))
				}

				return builder.NewArray()
			},
		},
		{
			name:        "bucket_string",
			arrowType:   arrow.BinaryTypes.String,
			icebergType: iceberg.PrimitiveTypes.String,
			transform:   iceberg.BucketTransform{NumBuckets: 64},
			appendRows: func(count int) arrow.Array {
				builder := array.NewStringBuilder(memory.DefaultAllocator)
				defer builder.Release()
				for row := range count {
					builder.Append(fmt.Sprintf("partition-value-%03d", row%128))
				}

				return builder.NewArray()
			},
		},
		{
			name:        "truncate_string",
			arrowType:   arrow.BinaryTypes.String,
			icebergType: iceberg.PrimitiveTypes.String,
			transform:   iceberg.TruncateTransform{Width: 19},
			appendRows: func(count int) arrow.Array {
				builder := array.NewStringBuilder(memory.DefaultAllocator)
				defer builder.Release()
				for row := range count {
					builder.Append(fmt.Sprintf("partition-value-%03d", row%128))
				}

				return builder.NewArray()
			},
		},
		{
			name:        "day_timestamp",
			arrowType:   &arrow.TimestampType{Unit: arrow.Microsecond},
			icebergType: iceberg.PrimitiveTypes.Timestamp,
			transform:   iceberg.DayTransform{},
			appendRows: func(count int) arrow.Array {
				builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond})
				defer builder.Release()
				for row := range count {
					builder.Append(arrow.Timestamp(int64(row%128) * 86_400_000_000))
				}

				return builder.NewArray()
			},
		},
		{
			name:        "hour_timestamp_ns",
			arrowType:   &arrow.TimestampType{Unit: arrow.Nanosecond},
			icebergType: iceberg.PrimitiveTypes.TimestampNs,
			transform:   iceberg.HourTransform{},
			appendRows: func(count int) arrow.Array {
				builder := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Nanosecond})
				defer builder.Release()
				for row := range count {
					builder.Append(arrow.Timestamp(int64(row%128) * 3_600_000_000_000))
				}

				return builder.NewArray()
			},
		},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: test.arrowType}}, nil)
			icebergSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "part", Type: test.icebergType},
			)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: test.transform,
			})
			column := test.appendRows(rows)
			record := array.NewRecordBatch(arrowSchema, []arrow.Array{column}, rows)
			column.Release()
			defer record.Release()

			writer := newPartitionedFanoutWriter(spec, icebergSchema, nil, nil)
			if _, err := writer.getPartitions(record); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := writer.getPartitions(record); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
