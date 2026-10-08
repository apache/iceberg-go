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
	"strings"
	"testing"
)

var (
	benchmarkColumnPathSegmentsResult []string
	benchmarkColumnPathFilterResult   BooleanExpression
	benchmarkColumnPathExtractResult  []VariantExtractColumn
)

func BenchmarkColumnPathSegments(b *testing.B) {
	for _, depth := range []int{1, 2, 4, 8, 32, 64} {
		names := make([]string, depth)
		for i := range names {
			names[i] = fmt.Sprintf("level_%d", i)
		}
		schema := columnPathTestSchema(names)
		schema.columnPathSegments(depth)
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkColumnPathSegmentsResult = schema.columnPathSegments(depth)
			}
		})
	}

	b.Run("missing", func(b *testing.B) {
		schema := columnPathTestSchema([]string{"payload"})
		schema.columnPathSegments(1)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			benchmarkColumnPathSegmentsResult = schema.columnPathSegments(2)
		}
	})
}

func BenchmarkTranslateNestedVariantExtract(b *testing.B) {
	for _, depth := range []int{1, 2, 4, 8, 32, 64} {
		names := make([]string, depth)
		for i := range names {
			names[i] = fmt.Sprintf("level_%d", i)
		}
		schema := columnPathTestSchema(names)
		bound, err := EqualTo(Extract(Reference(strings.Join(names, ".")), "$.value", PrimitiveTypes.Int64), int64(7)).Bind(schema, true)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := TranslateColumnNamesForScan(bound, schema); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				benchmarkColumnPathFilterResult, benchmarkColumnPathExtractResult, err = TranslateColumnNamesForScan(bound, schema)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
