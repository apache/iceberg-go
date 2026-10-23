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

var (
	benchmarkNameIndexByName map[string]int
	benchmarkNameIndexByID   map[int]string
)

func benchmarkWideNestedSchema(width, depth int) *Schema {
	nextID := 0
	leaves := make([]NestedField, width)
	for i := range leaves {
		nextID++
		leaves[i] = NestedField{
			ID: nextID, Name: fmt.Sprintf("column_%d", i), Type: PrimitiveTypes.Int64,
		}
	}
	for level := range depth {
		nextID++
		leaves = []NestedField{{
			ID: nextID, Name: fmt.Sprintf("nested_%d", level),
			Type: &StructType{FieldList: leaves},
		}}
	}

	return NewSchema(1, leaves...)
}

func BenchmarkSchemaNameIndexByName(b *testing.B) {
	for _, tc := range []struct{ width, depth int }{
		{width: 256, depth: 0},
		{width: 256, depth: 2},
		{width: 256, depth: 8},
		{width: 256, depth: 24},
	} {
		b.Run(fmt.Sprintf("width=%d/depth=%d", tc.width, tc.depth), func(b *testing.B) {
			schema := benchmarkWideNestedSchema(tc.width, tc.depth)
			b.ReportAllocs()
			for range b.N {
				index, err := IndexByName(schema)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkNameIndexByName = index
			}
		})
	}
}

func BenchmarkSchemaNameIndexByID(b *testing.B) {
	for _, tc := range []struct{ width, depth int }{
		{width: 256, depth: 0},
		{width: 256, depth: 2},
		{width: 256, depth: 8},
		{width: 256, depth: 24},
	} {
		b.Run(fmt.Sprintf("width=%d/depth=%d", tc.width, tc.depth), func(b *testing.B) {
			schema := benchmarkWideNestedSchema(tc.width, tc.depth)
			b.ReportAllocs()
			for range b.N {
				index, err := IndexNameByID(schema)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkNameIndexByID = index
			}
		})
	}
}
