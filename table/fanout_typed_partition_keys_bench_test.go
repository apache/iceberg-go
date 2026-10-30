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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

func TestPartitionMapTypedKeyIsolation(t *testing.T) {
	fieldInfo := []partitionFieldInfo{{fieldID: 1000}}
	keys := []any{int32(1), int64(1), "1", []byte("1"), float32(1), float64(1), nil}
	root := newPartitionMapNode()
	parts := make([]*partitionInfo, len(keys))
	for i, key := range keys {
		parts[i] = root.getOrCreate(partitionRecord{key}, fieldInfo, int64(len(keys)))
		parts[i].rows = append(parts[i].rows, int64(i))
	}
	if got := len(root.collectPartitions()); got != len(keys) {
		t.Fatalf("got %d distinct typed partitions, want %d", got, len(keys))
	}
	for i, key := range keys {
		if got := root.getOrCreate(partitionRecord{key}, fieldInfo, int64(len(keys))); got != parts[i] {
			t.Fatalf("key %T %v changed partition", key, key)
		}
	}
	// The fallback must still canonicalize NaNs.
	nan1 := math.Float64frombits(0x7ff8000000000001)
	nan2 := math.Float64frombits(0x7ff8000000000002)
	if root.getOrCreate(partitionRecord{nan1}, fieldInfo, 8) != root.getOrCreate(partitionRecord{nan2}, fieldInfo, 8) {
		t.Fatal("NaN payloads must share one partition")
	}
	// Mixed primitive levels must survive the result-tree traversal.
	nested := newPartitionMapNode()
	nestedFields := []partitionFieldInfo{{fieldID: 1000}, {fieldID: 1001}}
	for _, key := range []partitionRecord{
		{int64(2), "a"}, {int64(2), "b"}, {int64(3), "a"},
	} {
		nested.getOrCreate(key, nestedFields, 3)
	}
	if got := len(nested.collectPartitions()); got != 3 {
		t.Fatalf("collected %d nested partitions, want 3", got)
	}
}

// BenchmarkFanoutTypedPartitionKeys measures the real Arrow-to-Iceberg
// extraction and fanout grouping path. Construct fixtures before timing.
func BenchmarkFanoutTypedPartitionKeys(b *testing.B) {
	const rows = 1 << 16
	cases := []struct {
		name        string
		kind        string
		cardinality int
		clustered   bool
	}{
		{"Int64_64", "int64", 64, false},
		{"Int64_16384", "int64", 16384, false},
		{"Int32_64", "int32", 64, false},
		{"String_1024", "string", 1024, false},
		{"Int64_Clustered", "int64", 64, true},
		{"Float64_Fallback", "float64", 64, false},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.StopTimer()
			mem := memory.NewGoAllocator()
			var column arrow.Array
			var typ iceberg.Type
			var arrowType arrow.DataType
			asString := make([]string, tc.cardinality)
			for i := range asString {
				asString[i] = fmt.Sprintf("part-%04d", i)
			}
			partitionAt := func(i int) int {
				if tc.clustered {
					return (i * tc.cardinality) / rows
				}

				return i % tc.cardinality
			}
			switch tc.kind {
			case "int32":
				builder := array.NewInt32Builder(mem)
				for i := range rows {
					builder.Append(int32(partitionAt(i)))
				}
				column = builder.NewArray()
				builder.Release()
				typ, arrowType = iceberg.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32
			case "int64":
				builder := array.NewInt64Builder(mem)
				for i := range rows {
					builder.Append(int64(partitionAt(i)))
				}
				column = builder.NewArray()
				builder.Release()
				typ, arrowType = iceberg.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64
			case "string":
				builder := array.NewStringBuilder(mem)
				for i := range rows {
					builder.Append(asString[partitionAt(i)])
				}
				column = builder.NewArray()
				builder.Release()
				typ, arrowType = iceberg.PrimitiveTypes.String, arrow.BinaryTypes.String
			case "float64":
				builder := array.NewFloat64Builder(mem)
				for i := range rows {
					builder.Append(float64(partitionAt(i)))
				}
				column = builder.NewArray()
				builder.Release()
				typ, arrowType = iceberg.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float64
			}
			arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: arrowType}}, nil)
			record := array.NewRecordBatch(arrowSchema, []arrow.Array{column}, rows)
			column.Release()
			defer record.Release()
			schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "part", Type: typ})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "partition",
				Transform: iceberg.IdentityTransform{},
			})
			plan, err := newPartitionExtractionPlan(spec, schema, record.Schema())
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.StartTimer()
			for range b.N {
				parts, err := plan.getRecordPartitions(record)
				if err != nil {
					b.Fatal(err)
				}
				if len(parts) != tc.cardinality {
					b.Fatalf("got %d partitions, want %d", len(parts), tc.cardinality)
				}
				typedPartitionKeyBenchmarkSink = len(parts)
			}
		})
	}
}

var typedPartitionKeyBenchmarkSink int
