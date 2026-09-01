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

var equalityDeleteBenchmarkSink int

func BenchmarkEqualityDeleteIndex(b *testing.B) {
	const dataFileCount = 10_000

	for _, deleteFileCount := range []int{100, 1_000, 10_000} {
		for _, partitionCount := range []int{1, 10, 100} {
			name := fmt.Sprintf("deletes=%d/partitions=%d", deleteFileCount, partitionCount)
			b.Run(name, func(b *testing.B) {
				specs := equalityDeleteIndexTestSpecs()
				deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
				for i := range deleteEntries {
					deleteEntries[i] = newEqualityDeleteIndexTestEntry(
						fmt.Sprintf("delete-%d.parquet", i),
						1,
						map[int]any{1000: int32(i % partitionCount)},
						int64(i+1),
					)
				}

				dataEntries := make([]iceberg.ManifestEntry, dataFileCount)
				for i := range dataEntries {
					dataEntries[i] = newEqualityDeleteIndexTestEntry(
						fmt.Sprintf("data-%d.parquet", i),
						1,
						map[int]any{1000: int32(i % partitionCount)},
						int64(deleteFileCount-1),
					)
				}

				b.ReportAllocs()
				b.ReportMetric(dataFileCount, "data_files")
				b.ReportMetric(float64(deleteFileCount), "delete_files")
				b.ResetTimer()
				for range b.N {
					idx, err := buildEqualityDeleteIndex(deleteEntries, specs, nil)
					if err != nil {
						b.Fatal(err)
					}

					matched := 0
					for _, dataEntry := range dataEntries {
						files, err := idx.forDataFile(dataEntry)
						if err != nil {
							b.Fatal(err)
						}
						matched += len(files)
					}
					equalityDeleteBenchmarkSink = matched
				}
			})
		}
	}
}

func BenchmarkEqualityDeleteIndexMetrics(b *testing.B) {
	const (
		dataFileCount   = 10_000
		deleteFileCount = 10_000
		groupCount      = 100
		filesPerGroup   = deleteFileCount / groupCount
	)

	specs := equalityDeleteIndexTestSpecs()
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	partition := map[int]any{1000: int32(0)}

	lowerBounds := make([][]byte, groupCount)
	upperBounds := make([][]byte, groupCount)
	for group := range groupCount {
		lowerBounds[group], upperBounds[group] = equalityDeleteMetricsTestBounds(
			b, int32(group*filesPerGroup), int32((group+1)*filesPerGroup-1))
	}

	deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
	for i := range deleteEntries {
		group := i / filesPerGroup
		deleteEntries[i] = newEqualityDeleteMetricsTestEntry(
			fmt.Sprintf("delete-%d.parquet", i),
			1,
			partition,
			iceberg.EntryContentEqDeletes,
			int64(i+1),
			[]int{1},
			map[int]int64{1: 1},
			map[int]int64{1: 0},
			map[int]int64{1: 0},
			lowerBounds[group],
			upperBounds[group],
		)
	}

	dataEntries := make([]iceberg.ManifestEntry, dataFileCount)
	for i := range dataEntries {
		group := i / filesPerGroup
		dataEntries[i] = newEqualityDeleteMetricsTestEntry(
			fmt.Sprintf("data-%d.parquet", i),
			1,
			partition,
			iceberg.EntryContentData,
			0,
			nil,
			map[int]int64{1: 1},
			map[int]int64{1: 0},
			map[int]int64{1: 0},
			lowerBounds[group],
			upperBounds[group],
		)
	}

	benchmark := func(b *testing.B, schema *iceberg.Schema) {
		b.ReportAllocs()
		attached := 0
		b.ResetTimer()
		for range b.N {
			idx, err := buildEqualityDeleteIndex(deleteEntries, specs, schema)
			if err != nil {
				b.Fatal(err)
			}

			attached = 0
			for _, dataEntry := range dataEntries {
				files, err := idx.forDataFile(dataEntry)
				if err != nil {
					b.Fatal(err)
				}
				attached += len(files)
			}
			equalityDeleteBenchmarkSink = attached
		}
		b.StopTimer()
		b.ReportMetric(float64(attached)/float64(dataFileCount), "attached_deletes_per_data_file")
	}

	b.Run("baseline", func(b *testing.B) {
		benchmark(b, nil)
	})
	b.Run("metrics", func(b *testing.B) {
		benchmark(b, schema)
	})
}

func BenchmarkEqualityDeleteIndexOldData(b *testing.B) {
	const (
		deleteFileCount = 1_000
		dataFileCount   = 100
		partitionCount  = 10
	)

	specs := equalityDeleteIndexTestSpecs()
	deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
	for i := range deleteEntries {
		deleteEntries[i] = newEqualityDeleteIndexTestEntry(
			fmt.Sprintf("delete-%d.parquet", i),
			1,
			map[int]any{1000: int32(i % partitionCount)},
			int64(i+1),
		)
	}
	dataEntries := make([]iceberg.ManifestEntry, dataFileCount)
	for i := range dataEntries {
		dataEntries[i] = newEqualityDeleteIndexTestEntry(
			fmt.Sprintf("data-%d.parquet", i),
			1,
			map[int]any{1000: int32(i % partitionCount)},
			0,
		)
	}

	b.ReportAllocs()
	b.ReportMetric(dataFileCount, "data_files")
	b.ReportMetric(deleteFileCount, "delete_files")
	b.ResetTimer()
	for range b.N {
		idx, err := buildEqualityDeleteIndex(deleteEntries, specs, nil)
		if err != nil {
			b.Fatal(err)
		}

		matched := 0
		for _, dataEntry := range dataEntries {
			files, err := idx.forDataFile(dataEntry)
			if err != nil {
				b.Fatal(err)
			}
			matched += len(files)
		}
		equalityDeleteBenchmarkSink = matched
	}
}

func BenchmarkEqualityDeleteIndexNoDataFiles(b *testing.B) {
	const (
		deleteFileCount = 10_000
		partitionCount  = 100
	)

	specs := equalityDeleteIndexTestSpecs()
	deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
	for i := range deleteEntries {
		deleteEntries[i] = newEqualityDeleteIndexTestEntry(
			fmt.Sprintf("delete-%d.parquet", i),
			1,
			map[int]any{1000: int32(i % partitionCount)},
			int64(i+1),
		)
	}

	b.ReportAllocs()
	b.ReportMetric(0, "data_files")
	b.ReportMetric(deleteFileCount, "delete_files")
	b.ResetTimer()
	for range b.N {
		idx, err := buildEqualityDeleteIndex(deleteEntries, specs, nil)
		if err != nil {
			b.Fatal(err)
		}
		equalityDeleteBenchmarkSink = len(idx.global) + len(idx.byPartition)
	}
}

func BenchmarkEqualityDeleteIndexBuiltInPartition(b *testing.B) {
	for _, fieldCount := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("fields=%d", fieldCount), func(b *testing.B) {
			spec := equalityDeleteIndexBenchmarkSpec(fieldCount)
			specs := equalityDeleteIndexTestSpecLookup{1: spec}
			deleteEntry := newEqualityDeleteIndexBuiltInEntry(
				b, spec, iceberg.EntryContentEqDeletes, "delete.parquet",
				equalityDeleteIndexBenchmarkPartition(fieldCount, -1), 2,
			)
			dataEntry := newEqualityDeleteIndexBuiltInEntry(
				b, spec, iceberg.EntryContentData, "data.parquet",
				equalityDeleteIndexBenchmarkPartition(fieldCount, 1), 1,
			)

			b.Run("build", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					idx, err := buildEqualityDeleteIndex([]iceberg.ManifestEntry{deleteEntry}, specs, nil)
					if err != nil {
						b.Fatal(err)
					}
					equalityDeleteBenchmarkSink = len(idx.byPartition)
				}
			})

			idx, err := buildEqualityDeleteIndex([]iceberg.ManifestEntry{deleteEntry}, specs, nil)
			if err != nil {
				b.Fatal(err)
			}

			b.Run("lookup", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					matched, err := idx.forDataFile(dataEntry)
					if err != nil {
						b.Fatal(err)
					}
					equalityDeleteBenchmarkSink = len(matched)
				}
			})
		})
	}
}

func equalityDeleteIndexBenchmarkSpec(fieldCount int) iceberg.PartitionSpec {
	fields := make([]iceberg.PartitionField, fieldCount)
	for i := range fields {
		fields[i] = iceberg.PartitionField{
			SourceIDs: []int{i + 1},
			FieldID:   1000 + i,
			Name:      fmt.Sprintf("partition_%d", i),
			Transform: iceberg.IdentityTransform{},
		}
	}

	return iceberg.NewPartitionSpecID(1, fields...)
}

func equalityDeleteIndexBenchmarkPartition(fieldCount int, value int32) map[int]any {
	partition := make(map[int]any, fieldCount)
	for i := range fieldCount {
		partition[1000+int(i)] = value
	}

	return partition
}

func newEqualityDeleteIndexBuiltInEntry(
	b *testing.B,
	spec iceberg.PartitionSpec,
	content iceberg.ManifestEntryContent,
	path string,
	partition map[int]any,
	sequenceNumber int64,
) iceberg.ManifestEntry {
	b.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		content,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		1,
		1,
	)
	if err != nil {
		b.Fatal(err)
	}

	return iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED,
		nil,
		&sequenceNumber,
		nil,
		builder.Build(),
	)
}
