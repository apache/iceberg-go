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
	"fmt"
	"testing"
)

func BenchmarkHighestFieldID(b *testing.B) {
	for _, tc := range []struct {
		name   string
		schema *Schema
		want   int
	}{
		{name: "flat/fields=1", schema: benchmarkHighestFlatSchema(1), want: 1},
		{name: "flat/fields=32", schema: benchmarkHighestFlatSchema(32), want: 32},
		{name: "flat/fields=256", schema: benchmarkHighestFlatSchema(256), want: 256},
		{name: "flat/fields=2048", schema: benchmarkHighestFlatSchema(2048), want: 2048},
		{name: "nested/depth=8", schema: benchmarkHighestNestedSchema(8), want: 9},
		{name: "nested/depth=32", schema: benchmarkHighestNestedSchema(32), want: 33},
		{name: "nested/depth=64", schema: benchmarkHighestNestedSchema(64), want: 65},
		{name: "mixed/groups=8", schema: benchmarkHighestMixedSchema(8), want: 40},
		{name: "mixed/groups=64", schema: benchmarkHighestMixedSchema(64), want: 320},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var got int
			for b.Loop() {
				got = tc.schema.HighestFieldID()
			}
			if got != tc.want {
				b.Fatalf("highest field ID = %d, want %d", got, tc.want)
			}
		})
	}
}

func benchmarkHighestFlatSchema(fieldCount int) *Schema {
	fields := make([]NestedField, fieldCount)
	for i := range fields {
		fields[i] = NestedField{
			ID:   i + 1,
			Name: fmt.Sprintf("field_%d", i),
			Type: PrimitiveTypes.Int64,
		}
	}

	return NewSchema(0, fields...)
}

func benchmarkHighestNestedSchema(depth int) *Schema {
	field := NestedField{ID: depth + 1, Name: "leaf", Type: PrimitiveTypes.Int64}
	for i := depth - 1; i >= 0; i-- {
		field = NestedField{
			ID:   i + 1,
			Name: fmt.Sprintf("level_%d", i),
			Type: &StructType{FieldList: []NestedField{field}},
		}
	}

	return NewSchema(0, field)
}

func benchmarkHighestMixedSchema(groups int) *Schema {
	fields := make([]NestedField, groups)
	nextID := 1
	for i := range fields {
		fields[i] = NestedField{
			ID:   nextID,
			Name: fmt.Sprintf("field_%d", i),
			Type: &MapType{
				KeyID:   nextID + 1,
				KeyType: PrimitiveTypes.String,
				ValueID: nextID + 2,
				ValueType: &ListType{
					ElementID: nextID + 3,
					Element: &StructType{FieldList: []NestedField{{
						ID:   nextID + 4,
						Name: "value",
						Type: PrimitiveTypes.Int64,
					}}},
				},
			},
		}
		nextID += 5
	}

	return NewSchema(0, fields...)
}
