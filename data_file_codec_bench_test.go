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

package iceberg

import (
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/stretchr/testify/require"
)

func BenchmarkDataFileSchemaCache(b *testing.B) {
	schema := NewSchema(1,
		NestedField{ID: 1, Name: "id", Type: Int64Type{}},
		NestedField{ID: 2, Name: "category", Type: StringType{}},
		NestedField{ID: 3, Name: "ts", Type: TimestampType{}},
		NestedField{ID: 4, Name: "price", Type: DecimalTypeOf(10, 2)},
		NestedField{ID: 5, Name: "nested", Type: &StructType{FieldList: []NestedField{
			{ID: 6, Name: "value", Type: StringType{}},
		}}},
	)
	for _, tc := range []struct {
		name      string
		spec      PartitionSpec
		partition map[int]any
	}{
		{"unpartitioned", NewPartitionSpec(), nil},
		{"identity", NewPartitionSpec(PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: IdentityTransform{},
		}), map[int]any{1000: int64(42)}},
		{"mixed_4", NewPartitionSpec(
			PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "bucket", Transform: BucketTransform{NumBuckets: 16}},
			PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "category", Transform: TruncateTransform{Width: 4}},
			PartitionField{SourceIDs: []int{3}, FieldID: 1002, Name: "day", Transform: DayTransform{}},
			PartitionField{SourceIDs: []int{4}, FieldID: 1003, Name: "price", Transform: IdentityTransform{}},
		), map[int]any{1000: int32(3), 1001: "east", 1002: Date(123), 1003: Decimal{Val: decimal128.FromI64(4212), Scale: 2}}},
		{"nested_source", NewPartitionSpec(PartitionField{
			SourceIDs: []int{5}, FieldID: 1000, Name: "bucket", Transform: BucketTransform{NumBuckets: 16},
		}), map[int]any{1000: int32(3)}},
	} {
		for _, version := range []int{1, 2, 3} {
			b.Run(tc.name+"/v"+strconv.Itoa(version), func(b *testing.B) {
				builder, err := NewDataFileBuilder(tc.spec, EntryContentData,
					"s3://bucket/table/data.parquet", ParquetFile, tc.partition, nil, nil, 1024, 1024*1024)
				require.NoError(b, err)
				builder.ColumnSizes(map[int]int64{1: 512, 2: 256}).
					ValueCounts(map[int]int64{1: 1024, 2: 1024}).
					NullValueCounts(map[int]int64{1: 0, 2: 4}).
					LowerBoundValues(map[int][]byte{1: {0x01}, 2: []byte("a")}).
					UpperBoundValues(map[int][]byte{1: {0xff}, 2: []byte("z")}).
					SplitOffsets([]int64{0, 4096})
				df := builder.Build().(*dataFile)
				encoded, err := df.MarshalAvroEntry(tc.spec, schema, version)
				require.NoError(b, err)
				_, err = unmarshalAvroDataFileEntry(encoded, tc.spec, schema, version)
				require.NoError(b, err)

				b.Run("lookup", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, _, err := manifestEntrySchemaFor(tc.spec, schema, version); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("marshal", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, err := df.MarshalAvroEntry(tc.spec, schema, version); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("unmarshal", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, err := unmarshalAvroDataFileEntry(encoded, tc.spec, schema, version); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}
