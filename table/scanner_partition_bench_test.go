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

var partitionEvaluatorBenchmarkSink int

func BenchmarkPartitionEvaluator(b *testing.B) {
	for _, fieldCount := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("fields=%d/files=4096", fieldCount), func(b *testing.B) {
			materialized, borrowed, files := benchmarkPartitionEvaluator(b, fieldCount, 4096)
			benchmarkPartitionEvaluatorVariant(b, "materialized", files, materialized)
			benchmarkPartitionEvaluatorVariant(b, "borrowed", files, borrowed)
		})
	}
}

func benchmarkPartitionEvaluatorVariant(
	b *testing.B,
	name string,
	files []iceberg.DataFile,
	evaluate func(iceberg.DataFile) (bool, error),
) {
	b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			matched := 0
			for _, file := range files {
				matches, err := evaluate(file)
				if err != nil {
					b.Fatal(err)
				}
				if matches {
					matched++
				}
			}
			partitionEvaluatorBenchmarkSink = matched
		}
		b.StopTimer()

		if partitionEvaluatorBenchmarkSink != len(files)/2 {
			b.Fatalf("expected %d matches, got %d", len(files)/2, partitionEvaluatorBenchmarkSink)
		}
	})
}

func benchmarkPartitionEvaluator(
	b *testing.B,
	fieldCount, fileCount int,
) (func(iceberg.DataFile) (bool, error), func(iceberg.DataFile) (bool, error), []iceberg.DataFile) {
	b.Helper()

	schemaFields := make([]iceberg.NestedField, fieldCount)
	partitionFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fieldCount {
		sourceID := i + 1
		fieldID := 1000 + i
		name := fmt.Sprintf("partition_%d", fieldID)
		schemaFields[i] = iceberg.NestedField{
			ID: sourceID, Name: fmt.Sprintf("source_%d", sourceID),
			Type: iceberg.PrimitiveTypes.Int32, Required: true,
		}
		partitionFields[i] = iceberg.PartitionField{
			SourceIDs: []int{sourceID}, FieldID: fieldID,
			Name: name, Transform: iceberg.IdentityTransform{},
		}
	}

	schema := iceberg.NewSchema(0, schemaFields...)
	spec := iceberg.NewPartitionSpec(partitionFields...)

	filter := iceberg.EqualTo(iceberg.Reference("partition_1000"), int32(1))
	partType := spec.PartitionType(schema)
	partSchema := iceberg.NewSchema(0, partType.FieldList...)
	fn, err := iceberg.ExpressionEvaluator(partSchema, filter, true)
	if err != nil {
		b.Fatal(err)
	}
	metadata, err := NewMetadata(
		schema, &spec, UnsortedSortOrder, "s3://bucket/table", iceberg.Properties{},
	)
	if err != nil {
		b.Fatal(err)
	}
	partitionFilters := newKeyDefaultMapWrapErr(func(int) (iceberg.BooleanExpression, error) {
		return filter, nil
	})
	borrowed, err := buildPartitionEvaluator(spec.ID(), metadata, schema, partitionFilters, true)
	if err != nil {
		b.Fatal(err)
	}
	materialized := func(file iceberg.DataFile) (bool, error) {
		return fn(GetPartitionRecord(file, partType))
	}

	files := make([]iceberg.DataFile, fileCount)
	for i := range files {
		partition := make(map[int]any, fieldCount)
		for field := range partitionFields {
			partition[partitionFields[field].FieldID] = int32((i + field) % 2)
		}
		file, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, fmt.Sprintf("file-%d.parquet", i),
			iceberg.ParquetFile, partition, nil, nil, 1, 1,
		)
		if err != nil {
			b.Fatal(err)
		}
		files[i] = file.Build()
	}

	return materialized, borrowed, files
}
