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

var partitionResidualBenchmarkSink iceberg.BooleanExpression

func BenchmarkPartitionResidualPlanning(b *testing.B) {
	for _, tc := range []struct {
		name      string
		transform iceberg.Transform
		filter    iceberg.BooleanExpression
		partition func(int) any
	}{
		{
			name:      "identity",
			transform: iceberg.IdentityTransform{},
			filter: iceberg.NewAnd(
				iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"),
				iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
			),
			partition: func(i int) any {
				if i%2 == 0 {
					return "acme"
				}

				return "other"
			},
		},
		{
			name:      "day-range",
			transform: iceberg.DayTransform{},
			filter: iceberg.NewAnd(
				iceberg.GreaterThanEqual(iceberg.Reference("event_ts"), "2022-11-27T10:00:00"),
				iceberg.LessThan(iceberg.Reference("event_ts"), "2022-11-30T10:00:00"),
			),
			partition: func(i int) any {
				return iceberg.Date(19323 + i%5)
			},
		},
	} {
		b.Run(tc.name+"/files=4096", func(b *testing.B) {
			var schema *iceberg.Schema
			if tc.name == "identity" {
				schema = partitionResidualTestSchema()
			} else {
				schema = iceberg.NewSchema(1, iceberg.NestedField{
					ID: 1, Name: "event_ts", Type: iceberg.PrimitiveTypes.Timestamp,
				})
			}
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: tc.transform,
			})
			bound, err := iceberg.BindExpr(schema, tc.filter, true)
			if err != nil {
				b.Fatal(err)
			}
			evaluator, err := newPartitionResidualEvaluator(schema, &spec, bound, true)
			if err != nil {
				b.Fatal(err)
			}
			if evaluator == nil {
				b.Fatal("expected partition residual evaluator")
			}

			partitions := make([]map[int]any, 4096)
			for i := range partitions {
				partitions[i] = map[int]any{1000: tc.partition(i)}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, partition := range partitions {
					residual, _, err := evaluator.residual(partition)
					if err != nil {
						b.Fatal(err)
					}
					partitionResidualBenchmarkSink = residual
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(partitions)), "files/op")
		})
	}
}

// BenchmarkPartitionResidualUnchangedSubtrees measures per-file residual
// evaluation when either no predicate simplifies or just one predicate does.
// The balanced tree keeps the unaffected branches representative of filters
// containing many conditions.
func BenchmarkPartitionResidualUnchangedSubtrees(b *testing.B) {
	schema := partitionResidualTestSchema()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})

	for _, predicateCount := range []int{8, 32} {
		predicates := make([]iceberg.BooleanExpression, 0, predicateCount)
		predicates = append(predicates, iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme"))
		for i := 1; i < predicateCount; i++ {
			predicates = append(predicates, iceberg.GreaterThan(iceberg.Reference("amount"), int64(i)))
		}

		bound, err := iceberg.BindExpr(schema, benchmarkBalancedPartitionResidualAnd(predicates), true)
		if err != nil {
			b.Fatal(err)
		}
		evaluator, err := newPartitionResidualEvaluator(schema, &spec, bound, true)
		if err != nil {
			b.Fatal(err)
		}
		if evaluator == nil {
			b.Fatal("expected partition residual evaluator")
		}

		for _, tc := range []struct {
			name    string
			value   any
			present bool
			mixed   bool
		}{
			{name: "missing"},
			{name: "null", present: true},
			{name: "one-simplified", value: "acme", present: true},
			{name: "mixed", mixed: true},
		} {
			b.Run(fmt.Sprintf("predicates=%d/%s/files=4096", predicateCount, tc.name), func(b *testing.B) {
				partitions := make([]map[int]any, 4096)
				for i := range partitions {
					partitions[i] = make(map[int]any, 1)
					if tc.mixed {
						switch i % 4 {
						case 1:
							partitions[i][1000] = nil
						case 2:
							partitions[i][1000] = "acme"
						case 3:
							partitions[i][1000] = "other"
						}
					} else if tc.present {
						partitions[i][1000] = tc.value
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					for _, partition := range partitions {
						residual, _, err := evaluator.residual(partition)
						if err != nil {
							b.Fatal(err)
						}
						partitionResidualBenchmarkSink = residual
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(partitions)), "files/op")
			})
		}
	}
}

func benchmarkBalancedPartitionResidualAnd(predicates []iceberg.BooleanExpression) iceberg.BooleanExpression {
	if len(predicates) == 1 {
		return predicates[0]
	}
	mid := len(predicates) / 2

	return iceberg.NewAnd(
		benchmarkBalancedPartitionResidualAnd(predicates[:mid]),
		benchmarkBalancedPartitionResidualAnd(predicates[mid:]),
	)
}
