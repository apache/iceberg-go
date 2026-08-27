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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
)

var manifestInPredicateBenchmarkSink bool

func BenchmarkManifestEvaluatorInPredicate(b *testing.B) {
	for _, literalCount := range []int{2, 10, 50, 200} {
		b.Run("int32/in="+strconv.Itoa(literalCount), func(b *testing.B) {
			values := make([]int32, literalCount)
			for i := range values {
				values[i] = int32(i)
			}

			b.Run("pruned-lower", func(b *testing.B) {
				benchmarkManifestIn(b, values, int32(literalCount+1), int32(literalCount+1), iceberg.PrimitiveTypes.Int32)
			})
			b.Run("pruned-upper", func(b *testing.B) {
				benchmarkManifestIn(b, values, int32(-1), int32(-1), iceberg.PrimitiveTypes.Int32)
			})
			b.Run("no-prune", func(b *testing.B) {
				benchmarkManifestIn(b, values, int32(-1), int32(literalCount+1), iceberg.PrimitiveTypes.Int32)
			})
		})

		b.Run("string/in="+strconv.Itoa(literalCount), func(b *testing.B) {
			values := make([]string, literalCount)
			for i := range values {
				values[i] = "v" + strconv.Itoa(i)
			}

			b.Run("pruned-lower", func(b *testing.B) {
				benchmarkManifestIn(b, values, "z", "z", iceberg.PrimitiveTypes.String)
			})
			b.Run("pruned-upper", func(b *testing.B) {
				benchmarkManifestIn(b, values, "a", "a", iceberg.PrimitiveTypes.String)
			})
			b.Run("no-prune", func(b *testing.B) {
				benchmarkManifestIn(b, values, "a", "z", iceberg.PrimitiveTypes.String)
			})
		})

		b.Run("decimal/in="+strconv.Itoa(literalCount), func(b *testing.B) {
			values := make([]iceberg.Decimal, literalCount)
			for i := range values {
				values[i] = iceberg.Decimal{Val: decimal128.FromI64(int64(i * 100)), Scale: 2}
			}

			b.Run("pruned-lower", func(b *testing.B) {
				benchmarkManifestIn(b, values, iceberg.Decimal{Val: decimal128.FromI64(int64(literalCount+1) * 100), Scale: 2}, iceberg.Decimal{Val: decimal128.FromI64(int64(literalCount+1) * 100), Scale: 2}, iceberg.DecimalTypeOf(12, 2))
			})
			b.Run("pruned-upper", func(b *testing.B) {
				benchmarkManifestIn(b, values, iceberg.Decimal{Val: decimal128.FromI64(-100), Scale: 2}, iceberg.Decimal{Val: decimal128.FromI64(-100), Scale: 2}, iceberg.DecimalTypeOf(12, 2))
			})
			b.Run("no-prune", func(b *testing.B) {
				benchmarkManifestIn(b, values, iceberg.Decimal{Val: decimal128.FromI64(-100), Scale: 2}, iceberg.Decimal{Val: decimal128.FromI64(int64(literalCount+1) * 100), Scale: 2}, iceberg.DecimalTypeOf(12, 2))
			})
		})
	}
}

func benchmarkManifestIn[T iceberg.LiteralType](b *testing.B, values []T, lower, upper T, typ iceberg.Type) {
	b.Helper()
	lowerBytes, err := iceberg.NewLiteral(lower).MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	upperBytes, err := iceberg.NewLiteral(upper).MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: typ})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "value", Transform: iceberg.IdentityTransform{},
	})
	eval, err := newManifestEvaluator(spec, schema, iceberg.IsIn(iceberg.Reference("value"), values...), true)
	if err != nil {
		b.Fatal(err)
	}
	manifest := iceberg.NewManifestFile(2, "manifest.avro", 1, 0, 1).Partitions(
		[]iceberg.FieldSummary{{LowerBound: &lowerBytes, UpperBound: &upperBytes}},
	).Build()

	b.ReportAllocs()
	b.ReportMetric(float64(len(values)), "literals")
	b.ResetTimer()
	for range b.N {
		matched, err := eval(manifest)
		if err != nil {
			b.Fatal(err)
		}
		manifestInPredicateBenchmarkSink = matched
	}
}
