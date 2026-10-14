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

package iceberg_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

func BenchmarkApplyNameMappingWideSchema(b *testing.B) {
	const fieldCount = 256

	fields := make([]iceberg.NestedField, fieldCount)
	mapping := make(iceberg.NameMapping, fieldCount)
	for i := range fieldCount {
		name := fmt.Sprintf("field_%d", i)
		fieldID := i + 1
		fields[i] = iceberg.NestedField{
			ID:   fieldID,
			Name: name,
			Type: iceberg.PrimitiveTypes.Int32,
		}
		mapping[i] = iceberg.MappedField{
			FieldID: &fieldID,
			Names:   []string{name},
		}
	}
	schema := iceberg.NewSchema(0, fields...)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := iceberg.ApplyNameMapping(schema, mapping); err != nil {
			b.Fatal(err)
		}
	}
}

var benchmarkUpdatedNameMapping iceberg.NameMapping

func benchmarkNameMappingFields(count int) iceberg.NameMapping {
	mapping := make(iceberg.NameMapping, count)
	for i := range count {
		mapping[i] = iceberg.MappedField{
			FieldID: new(i + 1),
			Names:   []string{fmt.Sprintf("field_%d", i), fmt.Sprintf("old_field_%d", i)},
		}
	}

	return mapping
}

func BenchmarkUpdateNameMapping(b *testing.B) {
	for _, size := range []int{2, 8, 256, 1024} {
		for _, operation := range []string{"no-updates", "rename", "rename-reused", "add-reused"} {
			b.Run(fmt.Sprintf("%s/%d", operation, size), func(b *testing.B) {
				mapping := benchmarkNameMappingFields(size)
				var updates map[int]iceberg.NestedField
				var adds map[int][]iceberg.NestedField
				switch operation {
				case "rename":
					updates = map[int]iceberg.NestedField{1: {ID: 1, Name: "renamed"}}
				case "rename-reused":
					updates = map[int]iceberg.NestedField{2: {ID: 2, Name: "field_0"}}
				case "add-reused":
					adds = map[int][]iceberg.NestedField{-1: {{ID: size + 1, Name: "field_0", Type: iceberg.PrimitiveTypes.Int32}}}
				}

				b.ReportAllocs()
				for b.Loop() {
					result, err := iceberg.UpdateNameMapping(mapping, updates, adds)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkUpdatedNameMapping = result
				}
			})
		}
	}
}

func BenchmarkUpdateNameMappingNested(b *testing.B) {
	for _, size := range []int{8, 256} {
		for _, operation := range []string{"rename-reused", "add-reused"} {
			b.Run(fmt.Sprintf("%s/%d", operation, size), func(b *testing.B) {
				parentID := size + 1
				mapping := iceberg.NameMapping{{
					FieldID: &parentID,
					Names:   []string{"parent"},
					Fields:  benchmarkNameMappingFields(size),
				}}
				var updates map[int]iceberg.NestedField
				var adds map[int][]iceberg.NestedField
				if operation == "rename-reused" {
					updates = map[int]iceberg.NestedField{2: {ID: 2, Name: "field_0"}}
				} else {
					adds = map[int][]iceberg.NestedField{parentID: {{ID: size + 2, Name: "field_0", Type: iceberg.PrimitiveTypes.Int32}}}
				}

				b.ReportAllocs()
				for b.Loop() {
					result, err := iceberg.UpdateNameMapping(mapping, updates, adds)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkUpdatedNameMapping = result
				}
			})
		}
	}
}
