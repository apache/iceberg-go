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

var (
	partitionEvaluatorBenchmarkSink  int
	partitionProjectionBenchmarkSink int
)

func BenchmarkPartitionProjectionPlanning(b *testing.B) {
	for _, specCount := range []int{8, 64, 256} {
		scan, schema := benchmarkPartitionProjectionScan(b, specCount)
		b.Run(fmt.Sprintf("specs=%d", specCount), func(b *testing.B) {
			b.Run("separate_caches", func(b *testing.B) {
				benchmarkPartitionProjectionPhases(b, scan, schema, specCount, false)
			})
			b.Run("shared_cache", func(b *testing.B) {
				benchmarkPartitionProjectionPhases(b, scan, schema, specCount, true)
			})
		})
	}
}

func benchmarkPartitionProjectionPhases(
	b *testing.B,
	scan *Scan,
	schema *iceberg.Schema,
	specCount int,
	shared bool,
) {
	b.ReportAllocs()
	b.ResetTimer()
	var projectionBuilds int64
	newPartitionFilters := func() *keyDefaultMapErr[int, iceberg.BooleanExpression] {
		return newKeyDefaultMapWrapErr(func(specID int) (iceberg.BooleanExpression, error) {
			projectionBuilds++

			return buildPartitionProjection(specID, scan.metadata, schema, scan.rowFilter, scan.caseSensitive)
		})
	}

	for b.Loop() {
		manifestFilters := newPartitionFilters()
		partitionFilters := manifestFilters
		if !shared {
			partitionFilters = newPartitionFilters()
		}

		manifestEvaluators := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
			return buildManifestEvaluator(specID, scan.metadata, schema, manifestFilters, scan.caseSensitive)
		})
		for specID := range specCount {
			if _, err := manifestEvaluators.Get(specID); err != nil {
				b.Fatal(err)
			}
		}

		partitionEvaluators := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.DataFile) (bool, error), error) {
			return buildPartitionEvaluator(specID, scan.metadata, schema, partitionFilters, scan.caseSensitive)
		})
		for specID := range specCount {
			if _, err := partitionEvaluators.Get(specID); err != nil {
				b.Fatal(err)
			}
		}

		partitionProjectionBenchmarkSink = len(manifestFilters.data) + len(partitionFilters.data)
	}

	b.StopTimer()
	b.ReportMetric(float64(projectionBuilds)/float64(b.N), "projection-builds/op")
}

func benchmarkPartitionProjectionScan(b *testing.B, specCount int) (*Scan, *iceberg.Schema) {
	b.Helper()

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	specs := make([]iceberg.PartitionSpec, specCount)
	for specID := range specCount {
		specs[specID] = iceberg.NewPartitionSpecID(specID, iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000 + specID,
			Name:      fmt.Sprintf("id_%d", specID),
			Transform: iceberg.IdentityTransform{},
		})
	}

	metadata := &metadataV2{commonMetadata: commonMetadata{
		SchemaList:      []*iceberg.Schema{schema},
		CurrentSchemaID: schema.ID,
		Specs:           specs,
	}}

	return &Scan{
		metadata: metadata,
		rowFilter: iceberg.NewAnd(
			iceberg.EqualTo(iceberg.Reference("id"), int32(7)),
			iceberg.GreaterThanEqual(iceberg.Reference("payload"), "a"),
		),
		caseSensitive: true,
	}, schema
}

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
