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

var (
	manifestInPredicateBenchmarkSink         bool
	inclusiveMetricsInPredicateBenchmarkSink int
)

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

func BenchmarkInclusiveMetricsEvalInPredicate(b *testing.B) {
	for _, literalCount := range []int{10, 50, 200} {
		values := make([]int32, literalCount)
		for i := range values {
			values[i] = int32(i * 2)
		}

		maxValue := values[len(values)-1]
		for _, bounds := range []struct {
			name  string
			lower int32
			upper int32
		}{
			{name: "below-set", lower: -2, upper: -2},
			{name: "above-set", lower: maxValue + 2, upper: maxValue + 2},
			{name: "overlap", lower: values[0], upper: maxValue},
			{name: "sparse-gap", lower: values[literalCount/2] + 1, upper: values[literalCount/2] + 1},
		} {
			b.Run("int32/in="+strconv.Itoa(literalCount)+"/"+bounds.name, func(b *testing.B) {
				benchmarkInclusiveMetricsIn(b, values, bounds.lower, bounds.upper, literalCount, bounds.name == "overlap")
			})
		}
	}
}

func benchmarkInclusiveMetricsIn(b *testing.B, values []int32, lower, upper int32, literalCount int, wantMatch bool) {
	b.Helper()

	lowerBytes, err := iceberg.NewLiteral(lower).MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	upperBytes, err := iceberg.NewLiteral(upper).MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32})
	eval, err := newInclusiveMetricsEvaluator(
		schema, iceberg.IsIn(iceberg.Reference("value"), values...), true, true,
	)
	if err != nil {
		b.Fatal(err)
	}

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, "file.parquet", iceberg.ParquetFile,
		nil, nil, nil, 10, 100,
	)
	if err != nil {
		b.Fatal(err)
	}
	file := builder.
		ValueCounts(map[int]int64{1: 10}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: lowerBytes}).
		UpperBoundValues(map[int][]byte{1: upperBytes}).
		Build()
	files := make([]iceberg.DataFile, 10_000)
	for i := range files {
		files[i] = file
	}

	b.ReportAllocs()
	b.ReportMetric(float64(len(files)), "files/op")
	b.ReportMetric(float64(literalCount), "literals/file")
	b.ResetTimer()
	for range b.N {
		matchedFiles := 0
		for _, file := range files {
			matched, err := eval(file)
			if err != nil {
				b.Fatal(err)
			}
			if matched {
				matchedFiles++
			}
		}
		wantFiles := 0
		if wantMatch {
			wantFiles = len(files)
		}
		if matchedFiles != wantFiles {
			b.Fatalf("matched %d files, want %d", matchedFiles, wantFiles)
		}
		inclusiveMetricsInPredicateBenchmarkSink = matchedFiles
	}
}
