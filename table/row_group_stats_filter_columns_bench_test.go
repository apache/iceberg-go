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

var rowGroupStatsFilterColumnsBenchmarkSink int

func BenchmarkParquetRowGroupStatsFilterColumns(b *testing.B) {
	const rowGroups = 1024

	for _, columnCount := range []int{32, 128} {
		meta := buildRowGroupMetricsMetadata(b, rowGroups, columnCount, true)
		fields := make([]iceberg.NestedField, columnCount)
		readCols := make([]int, columnCount)
		for i := range columnCount {
			fields[i] = iceberg.NestedField{
				ID: i + 1, Name: fmt.Sprintf("field_%d", i),
				Type: iceberg.PrimitiveTypes.String, Required: true,
			}
			readCols[i] = i
		}
		schema := iceberg.NewSchema(0, fields...)

		filterCounts := []int{1, 4}
		if columnCount == 128 {
			filterCounts = append(filterCounts, columnCount)
		}
		for _, filterCount := range filterCounts {
			var filter iceberg.BooleanExpression
			for i := range filterCount {
				pred := iceberg.EqualTo(iceberg.Reference(fmt.Sprintf("field_%d", i)), "m")
				if filter == nil {
					filter = pred
				} else {
					filter = iceberg.NewAnd(filter, pred)
				}
			}
			bound, err := iceberg.BindExpr(schema, filter, true)
			if err != nil {
				b.Fatal(err)
			}

			for _, test := range []struct {
				name string
				cols []int
			}{
				{name: "all_read_columns", cols: readCols},
				{name: "filter_columns", cols: readCols[:filterCount]},
			} {
				b.Run(fmt.Sprintf("columns=%d/filter_columns=%d/%s", columnCount, filterCount, test.name), func(b *testing.B) {
					eval, err := newParquetRowGroupStatsEvaluator(schema, bound, false)
					if err != nil {
						b.Fatal(err)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						matched := 0
						for rowGroup := range meta.NumRowGroups() {
							keep, err := eval(meta.RowGroup(rowGroup), test.cols)
							if err != nil {
								b.Fatal(err)
							}
							if keep {
								matched++
							}
						}
						rowGroupStatsFilterColumnsBenchmarkSink = matched
					}
					b.StopTimer()

					if rowGroupStatsFilterColumnsBenchmarkSink != rowGroups {
						b.Fatalf("expected %d matches, got %d", rowGroups, rowGroupStatsFilterColumnsBenchmarkSink)
					}
				})
			}
		}
	}
}
