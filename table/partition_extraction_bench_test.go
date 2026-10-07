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

func BenchmarkPartitionMapCollection(b *testing.B) {
	for _, test := range []struct {
		fieldCount     int
		partitionCount int
	}{
		{fieldCount: 2, partitionCount: 64},
		{fieldCount: 2, partitionCount: 1_024},
		{fieldCount: 2, partitionCount: 16_384},
		{fieldCount: 4, partitionCount: 64},
		{fieldCount: 4, partitionCount: 1_024},
		{fieldCount: 4, partitionCount: 16_384},
	} {
		b.Run(fmt.Sprintf("fields_%d/partitions_%d", test.fieldCount, test.partitionCount), func(b *testing.B) {
			tree := benchmarkPartitionMap(b, test.fieldCount, test.partitionCount)
			b.ReportAllocs()
			b.ReportMetric(float64(test.partitionCount), "partitions/op")
			b.ResetTimer()
			for b.Loop() {
				partitionCollectionBenchmarkSink = len(tree.collectPartitions())
			}
		})
	}
}

var partitionCollectionBenchmarkSink int

func benchmarkPartitionMap(b *testing.B, fieldCount, partitionCount int) *partitionMapNode {
	b.Helper()

	base := 1
	for {
		combinations := 1
		for range fieldCount {
			combinations *= base
		}
		if combinations >= partitionCount {
			break
		}
		base++
	}

	fieldInfo := make([]partitionFieldInfo, fieldCount)
	for field := range fieldCount {
		fieldInfo[field].fieldID = 1000 + field
	}

	tree := newPartitionMapNode()
	for partition := range partitionCount {
		record := make(partitionRecord, fieldCount)
		value := partition
		for field := range fieldCount {
			record[field] = int32(value % base)
			value /= base
		}
		tree.getOrCreate(record, fieldInfo, int64(partitionCount))
	}

	return tree
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

func BenchmarkPartitionPathFormatting(b *testing.B) {
	for _, fieldCount := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("fields_%d", fieldCount), func(b *testing.B) {
			arrowFields := make([]arrow.Field, fieldCount)
			icebergFields := make([]iceberg.NestedField, fieldCount)
			specFields := make([]iceberg.PartitionField, fieldCount)
			values := make(partitionRecord, fieldCount)
			for i := range fieldCount {
				name := fmt.Sprintf("part #%d", i)
				arrowFields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
				icebergFields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: iceberg.PrimitiveTypes.String}
				specFields[i] = iceberg.PartitionField{
					SourceIDs: []int{i + 1}, FieldID: 1000 + i,
					Transform: iceberg.IdentityTransform{}, Name: name,
				}
				values[i] = fmt.Sprintf("value/%d", i)
			}

			schema := iceberg.NewSchema(0, icebergFields...)
			spec := iceberg.NewPartitionSpec(specFields...)
			arrowSchema := arrow.NewSchema(arrowFields, nil)
			plan, err := newPartitionExtractionPlan(spec, schema, arrowSchema)
			if err != nil {
				b.Fatal(err)
			}

			b.Run("partition_to_path", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_ = spec.PartitionToPath(values, schema)
				}
			})
			b.Run("reused_plan", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_ = plan.pathPlan.format(values)
				}
			})
		})
	}
}
