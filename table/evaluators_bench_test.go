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

var manifestEvaluatorBenchmarkSink int

func BenchmarkManifestEvaluatorBuiltInPartitions(b *testing.B) {
	for _, manifestCount := range []int{1, 100, 1_000} {
		for _, fieldCount := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("manifests=%d/fields=%d", manifestCount, fieldCount), func(b *testing.B) {
				spec, schema := manifestEvaluatorBenchmarkSpec(fieldCount)
				filter := manifestEvaluatorBenchmarkFilter(fieldCount)
				eval, err := newManifestEvaluator(spec, schema, filter, true)
				if err != nil {
					b.Fatal(err)
				}

				summaries := manifestEvaluatorBenchmarkSummaries(fieldCount)
				manifests := make([]iceberg.ManifestFile, manifestCount)
				for i := range manifests {
					manifests[i] = iceberg.NewManifestFile(
						2, fmt.Sprintf("manifest-%d.avro", i), 0, int32(spec.ID()), 1,
					).Partitions(summaries).Build()
				}

				b.ReportAllocs()
				b.ReportMetric(float64(manifestCount), "manifests")
				b.ReportMetric(float64(fieldCount), "partition_fields")
				b.ResetTimer()
				for range b.N {
					matched := 0
					for _, manifest := range manifests {
						keep, err := eval(manifest)
						if err != nil {
							b.Fatal(err)
						}
						if keep {
							matched++
						}
					}
					manifestEvaluatorBenchmarkSink = matched
				}
			})
		}
	}
}

func manifestEvaluatorBenchmarkSpec(fieldCount int) (iceberg.PartitionSpec, *iceberg.Schema) {
	schemaFields := make([]iceberg.NestedField, fieldCount)
	partitionFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fieldCount {
		fieldID := i + 1
		fieldName := fmt.Sprintf("field_%d", i)
		schemaFields[i] = iceberg.NestedField{
			ID: fieldID, Name: fieldName, Type: iceberg.PrimitiveTypes.Int32, Required: true,
		}
		partitionFields[i] = iceberg.PartitionField{
			SourceIDs: []int{fieldID}, FieldID: 1000 + i,
			Name: fieldName, Transform: iceberg.IdentityTransform{},
		}
	}

	return iceberg.NewPartitionSpecID(1, partitionFields...), iceberg.NewSchema(1, schemaFields...)
}

func manifestEvaluatorBenchmarkFilter(fieldCount int) iceberg.BooleanExpression {
	var filter iceberg.BooleanExpression = iceberg.GreaterThanEqual(
		iceberg.Reference("field_0"), int32(0))
	for i := 1; i < fieldCount; i++ {
		filter = iceberg.NewAnd(filter,
			iceberg.GreaterThanEqual(iceberg.Reference(fmt.Sprintf("field_%d", i)), int32(0)))
	}

	return filter
}

func manifestEvaluatorBenchmarkSummaries(fieldCount int) []iceberg.FieldSummary {
	lower, err := iceberg.Int32Literal(0).MarshalBinary()
	if err != nil {
		panic(err)
	}
	upper, err := iceberg.Int32Literal(100).MarshalBinary()
	if err != nil {
		panic(err)
	}
	containsNaN := false

	summaries := make([]iceberg.FieldSummary, fieldCount)
	for i := range summaries {
		summaries[i] = iceberg.FieldSummary{
			ContainsNaN: &containsNaN,
			LowerBound:  &lower,
			UpperBound:  &upper,
		}
	}

	return summaries
}
