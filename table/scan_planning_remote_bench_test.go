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
	"testing"

	"github.com/apache/iceberg-go"
)

var remoteWildcardProjectionBenchmarkSink int

func BenchmarkRemotePlanningSelectedFields(b *testing.B) {
	for _, fieldCount := range []int{100, 1_000, 5_000} {
		schema := remoteWildcardProjectionBenchmarkSchema(fieldCount)
		scan := &Scan{selectedFields: []string{"*"}, caseSensitive: true}
		if _, err := remotePlanningSelectedFields(scan, schema); err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("top_level_fields_%d", fieldCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				selected, err := remotePlanningSelectedFields(scan, schema)
				if err != nil {
					b.Fatal(err)
				}
				remoteWildcardProjectionBenchmarkSink = len(selected)
			}
		})
	}
}

func remoteWildcardProjectionBenchmarkSchema(fieldCount int) *iceberg.Schema {
	fields := make([]iceberg.NestedField, fieldCount)
	nextID := 1
	for i := range fieldCount {
		name := fmt.Sprintf("field_%05d", i)
		switch i % 3 {
		case 0:
			fields[i] = iceberg.NestedField{
				ID:   nextID,
				Name: name,
				Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: nextID + 1, Name: "city", Type: iceberg.PrimitiveTypes.String, InitialDefault: "default-city"},
					{ID: nextID + 2, Name: "zip", Type: iceberg.PrimitiveTypes.Int32, WriteDefault: int32(0)},
				}},
				InitialDefault: map[string]any{
					"city": "default-city",
					"zip":  int32(0),
				},
			}
			nextID += 3
		case 1:
			fields[i] = iceberg.NestedField{
				ID:   nextID,
				Name: name,
				Type: &iceberg.ListType{
					ElementID: nextID + 1,
					Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
						{ID: nextID + 2, Name: "city", Type: iceberg.PrimitiveTypes.String, InitialDefault: "default-city"},
						{ID: nextID + 3, Name: "zip", Type: iceberg.PrimitiveTypes.Int32, WriteDefault: int32(0)},
					}},
				},
				InitialDefault: []any{map[string]any{
					"city": "default-city",
					"zip":  int32(0),
				}},
			}
			nextID += 4
		default:
			fields[i] = iceberg.NestedField{
				ID:   nextID,
				Name: name,
				Type: &iceberg.MapType{
					KeyID:   nextID + 1,
					KeyType: iceberg.PrimitiveTypes.String,
					ValueID: nextID + 2,
					ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
						{ID: nextID + 3, Name: "city", Type: iceberg.PrimitiveTypes.String, InitialDefault: "default-city"},
						{ID: nextID + 4, Name: "zip", Type: iceberg.PrimitiveTypes.Int32, WriteDefault: int32(0)},
					}},
				},
				InitialDefault: map[string]any{
					"default-key": map[string]any{
						"city": "default-city",
						"zip":  int32(0),
					},
				},
			}
			nextID += 5
		}
	}

	return iceberg.NewSchema(0, fields...)
}
