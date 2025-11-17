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

package iceberg_test

import (
	"testing"

	"github.com/apache/iceberg-go"
)

// BenchmarkPartitionToPath benchmarks the optimized PartitionToPath function
// which uses cached URL-escaped field names and strings.Builder for efficient
// string concatenation.
func BenchmarkPartitionToPath(b *testing.B) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "other_str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "int", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 4, Name: "date", Type: iceberg.PrimitiveTypes.Date, Required: true},
		iceberg.NestedField{ID: 5, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)

	// Create a partition spec with fields that need URL escaping
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "my#str%bucket",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.IdentityTransform{}, Name: "other str+bucket",
		},
		iceberg.PartitionField{
			SourceID: 3, FieldID: 1002,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "my!int:bucket",
		},
		iceberg.PartitionField{
			SourceID: 4, FieldID: 1003,
			Transform: iceberg.DayTransform{}, Name: "date/day",
		},
		iceberg.PartitionField{
			SourceID: 5, FieldID: 1004,
			Transform: iceberg.HourTransform{}, Name: "ts/hour",
		},
	)

	record := partitionRecord{"my+str", "( )", int32(10), int32(18993), int64(1672531200000000)}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = spec.PartitionToPath(record, schema)
	}
}

// BenchmarkPartitionToPathManyFields benchmarks PartitionToPath with many partition fields
// to verify performance scales well with the number of fields.
func BenchmarkPartitionToPathManyFields(b *testing.B) {
	// Create a schema with many fields
	fields := make([]iceberg.NestedField, 0, 20)
	partitionFields := make([]iceberg.PartitionField, 0, 20)
	recordValues := make([]any, 0, 20)

	for i := 1; i <= 20; i++ {
		fields = append(fields, iceberg.NestedField{
			ID: i, Name: "field_" + string(rune('a'+i-1)),
			Type: iceberg.PrimitiveTypes.String, Required: false,
		})
		partitionFields = append(partitionFields, iceberg.PartitionField{
			SourceID: i, FieldID: 1000 + i,
			Transform: iceberg.IdentityTransform{}, Name: "part_field_" + string(rune('a'+i-1)),
		})
		recordValues = append(recordValues, "value_"+string(rune('a'+i-1)))
	}

	schema := iceberg.NewSchema(0, fields...)
	spec := iceberg.NewPartitionSpecID(1, partitionFields...)
	record := partitionRecord(recordValues)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = spec.PartitionToPath(record, schema)
	}
}

// BenchmarkPartitionType benchmarks the PartitionType function with caching.
// The first call should be slower (builds the type), subsequent calls should
// be faster (uses cache).
func BenchmarkPartitionType(b *testing.B) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "int", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "bool", Type: iceberg.PrimitiveTypes.Bool, Required: false},
	)

	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket",
		},
		iceberg.PartitionField{
			SourceID: 3, FieldID: 1002,
			Transform: iceberg.IdentityTransform{}, Name: "bool_identity",
		},
	)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = spec.PartitionType(schema)
	}
}

// BenchmarkPartitionTypeMultipleSchemas benchmarks PartitionType with multiple
// different schemas to verify caching works correctly per schema ID.
func BenchmarkPartitionTypeMultipleSchemas(b *testing.B) {
	schemas := make([]*iceberg.Schema, 10)
	for i := range schemas {
		schemas[i] = iceberg.NewSchema(i,
			iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
			iceberg.NestedField{ID: 2, Name: "int", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		)
	}

	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.IdentityTransform{}, Name: "str_identity",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 10}, Name: "int_bucket",
		},
	)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		schema := schemas[i%len(schemas)]
		_ = spec.PartitionType(schema)
	}
}

// BenchmarkPartitionToPathRepeated benchmarks repeated calls to PartitionToPath
// with the same spec to verify that cached escaped names provide performance benefits.
func BenchmarkPartitionToPathRepeated(b *testing.B) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "other_str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "int", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	// Use field names that require URL escaping to maximize the benefit of caching
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "my#str%bucket",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.IdentityTransform{}, Name: "other str+bucket",
		},
		iceberg.PartitionField{
			SourceID: 3, FieldID: 1002,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "my!int:bucket",
		},
	)

	// Create multiple records with different values
	records := []partitionRecord{
		{"my+str", "( )", int32(10)},
		{"another/value", "test data", int32(20)},
		{"third&record", "more data", int32(30)},
		{"fourth=record", "even more", int32(40)},
		{"fifth?record", "last one", int32(50)},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		record := records[i%len(records)]
		_ = spec.PartitionToPath(record, schema)
	}
}

// BenchmarkPartitionSpecInitialize benchmarks the initialization of a partition spec
// which now pre-computes URL-escaped field names.
func BenchmarkPartitionSpecInitialize(b *testing.B) {
	fields := make([]iceberg.PartitionField, 10)
	for i := range fields {
		fields[i] = iceberg.PartitionField{
			SourceID: i + 1, FieldID: 1000 + i,
			Transform: iceberg.IdentityTransform{}, Name: "field#with%special&chars=" + string(rune('a'+i)),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = iceberg.NewPartitionSpecID(1, fields...)
	}
}
