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
	"sort"
	"testing"

	"github.com/apache/iceberg-go"
)

var positionalDeleteBenchmarkSink int
var positionalDeletePartitionKeyBenchmarkSink string

func BenchmarkPositionalDeletePartitionKey(b *testing.B) {
	for _, benchmark := range []struct {
		name        string
		fieldCount  int
		binaryValue bool
	}{
		{name: "int32", fieldCount: 1},
		{name: "int32", fieldCount: 8},
		{name: "binary", fieldCount: 32, binaryValue: true},
	} {
		b.Run(fmt.Sprintf("%s/fields=%d/files=10000", benchmark.name, benchmark.fieldCount), func(b *testing.B) {
			files := positionalDeletePartitionKeyBenchmarkFiles(
				b, benchmark.fieldCount, benchmark.binaryValue, 10_000)

			b.Run("before", func(b *testing.B) {
				benchmarkPositionalDeletePartitionKeys(b, files, false)
			})
			b.Run("after", func(b *testing.B) {
				benchmarkPositionalDeletePartitionKeys(b, files, true)
			})
		})
	}
}

func BenchmarkPositionalDeleteIndexSparsePaths20KBy5K(b *testing.B) {
	const (
		dataFileCount   = 20_000
		deleteFileCount = 5_000
	)
	dataEntries, deleteEntries := positionalDeleteBenchmarkEntries(dataFileCount, deleteFileCount)

	b.ReportAllocs()
	b.ReportMetric(dataFileCount, "data_files")
	b.ReportMetric(deleteFileCount, "delete_files")
	b.ResetTimer()
	for range b.N {
		positionalDeleteBenchmarkSink = benchmarkPositionalDeleteIndex(b, dataEntries, deleteEntries)
	}
	if positionalDeleteBenchmarkSink != deleteFileCount {
		b.Fatalf("expected %d matches, got %d", deleteFileCount, positionalDeleteBenchmarkSink)
	}
}

func positionalDeletePartitionKeyBenchmarkFiles(
	b *testing.B,
	fieldCount int,
	binaryValue bool,
	fileCount int,
) []iceberg.DataFile {
	b.Helper()

	partitionFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fieldCount {
		fieldID := 1000 + i
		partitionFields[i] = iceberg.PartitionField{
			SourceIDs: []int{i + 1},
			FieldID:   fieldID,
			Name:      fmt.Sprintf("partition_%d", fieldID),
			Transform: iceberg.IdentityTransform{},
		}
	}
	spec := iceberg.NewPartitionSpecID(1, partitionFields...)
	files := make([]iceberg.DataFile, fileCount)
	for i := range files {
		partition := make(map[int]any, fieldCount)
		for field, partitionField := range partitionFields {
			if binaryValue {
				partition[partitionField.FieldID] = []byte(fmt.Sprintf(
					"partition-%02d-%02d", i%100, field))
			} else {
				partition[partitionField.FieldID] = int32(i % 100)
			}
		}

		builder, err := iceberg.NewDataFileBuilder(
			spec,
			iceberg.EntryContentData,
			fmt.Sprintf("data-%05d.parquet", i),
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
		files[i] = builder.Build()
	}

	return files
}

func benchmarkPositionalDeletePartitionKeys(
	b *testing.B,
	files []iceberg.DataFile,
	borrowed bool,
) {
	b.Helper()
	b.ReportAllocs()
	b.ReportMetric(float64(len(files)), "files/op")
	b.ResetTimer()

	var key string
	for range b.N {
		for _, file := range files {
			var partition map[int]any
			if borrowed {
				partition = dataFilePartition(file)
			} else {
				partition = file.Partition()
			}
			var err error
			key, err = canonicalPartitionKey(file.SpecID(), partition)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	positionalDeletePartitionKeyBenchmarkSink = key
}

func BenchmarkPositionalDeletePlanningSparsePaths(b *testing.B) {
	const (
		dataFileCount   = 2_000
		deleteFileCount = 500
	)
	dataEntries, deleteEntries := positionalDeleteBenchmarkEntries(dataFileCount, deleteFileCount)
	benchmarkPositionalDeletePlanningVariants(b, dataEntries, deleteEntries, deleteFileCount)
}

func BenchmarkPositionalDeletePlanningPartitionScopedMetrics(b *testing.B) {
	const (
		dataFileCount   = 2_000
		deleteFileCount = 500
	)
	dataEntries, deleteEntries := positionalDeletePartitionBenchmarkEntries(
		dataFileCount, deleteFileCount)
	benchmarkPositionalDeletePlanningVariants(b, dataEntries, deleteEntries, dataFileCount)
}

func BenchmarkPositionalDeletePlanningSelectivePartition(b *testing.B) {
	const (
		dataFileCount   = 2_000
		deleteFileCount = 500
		partitionCount  = 100
	)
	dataEntries, deleteEntries := positionalDeleteSelectivePartitionBenchmarkEntries(
		dataFileCount, deleteFileCount, partitionCount)
	expectedMatches := dataFileCount * (deleteFileCount / partitionCount)
	benchmarkPositionalDeletePlanningPartitionVariants(
		b, dataEntries, deleteEntries, expectedMatches)
}

func BenchmarkPositionalDeletePlanningMixed(b *testing.B) {
	const (
		dataFileCount   = 2_000
		deleteFileCount = 500
	)
	dataEntries, deleteEntries, expectedMatches := positionalDeleteMixedBenchmarkEntries(
		dataFileCount, deleteFileCount)
	benchmarkPositionalDeletePlanningVariants(b, dataEntries, deleteEntries, expectedMatches)
}

func benchmarkPositionalDeletePlanningVariants(
	b *testing.B,
	dataEntries, deleteEntries []iceberg.ManifestEntry,
	expectedMatches int,
) {
	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			positionalDeleteBenchmarkSink = benchmarkPositionalDeleteIndex(b, dataEntries, deleteEntries)
		}
		if positionalDeleteBenchmarkSink != expectedMatches {
			b.Fatalf("expected %d matches, got %d", expectedMatches, positionalDeleteBenchmarkSink)
		}
	})

	b.Run("metrics_suffix", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			matched := 0
			for _, dataEntry := range dataEntries {
				files, err := matchPositionalDeletesByMetrics(dataEntry, deleteEntries)
				if err != nil {
					b.Fatal(err)
				}
				matched += len(files)
			}
			positionalDeleteBenchmarkSink = matched
		}
		if positionalDeleteBenchmarkSink != expectedMatches {
			b.Fatalf("expected %d matches, got %d", expectedMatches, positionalDeleteBenchmarkSink)
		}
	})
}

func benchmarkPositionalDeletePlanningPartitionVariants(
	b *testing.B,
	dataEntries, deleteEntries []iceberg.ManifestEntry,
	expectedMatches int,
) {
	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			positionalDeleteBenchmarkSink = benchmarkPositionalDeleteIndex(b, dataEntries, deleteEntries)
		}
		if positionalDeleteBenchmarkSink != expectedMatches {
			b.Fatalf("expected %d matches, got %d", expectedMatches, positionalDeleteBenchmarkSink)
		}
	})

	b.Run("partition_suffix", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			matched := 0
			for _, dataEntry := range dataEntries {
				files, err := matchPositionalDeletesByPartition(dataEntry, deleteEntries)
				if err != nil {
					b.Fatal(err)
				}
				matched += len(files)
			}
			positionalDeleteBenchmarkSink = matched
		}
		if positionalDeleteBenchmarkSink != expectedMatches {
			b.Fatalf("expected %d matches, got %d", expectedMatches, positionalDeleteBenchmarkSink)
		}
	})
}

func positionalDeleteBenchmarkEntries(
	dataFileCount, deleteFileCount int,
) ([]iceberg.ManifestEntry, []iceberg.ManifestEntry) {
	dataEntries := positionalDeleteBenchmarkDataEntries(dataFileCount)

	deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
	for i := range deleteEntries {
		path := positionalDeleteBenchmarkPath(i * (dataFileCount / deleteFileCount))
		deleteEntries[i] = newPositionalDeleteIndexTestEntry(
			"delete-"+path, 0, nil, 2, &path, path)
	}

	return dataEntries, deleteEntries
}

func positionalDeletePartitionBenchmarkEntries(
	dataFileCount, deleteFileCount int,
) ([]iceberg.ManifestEntry, []iceberg.ManifestEntry) {
	dataEntries := positionalDeleteBenchmarkDataEntries(dataFileCount)
	deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
	filesPerDelete := dataFileCount / deleteFileCount
	for i := range deleteEntries {
		first := i * filesPerDelete
		last := first + filesPerDelete - 1
		deleteEntries[i] = newPositionalDeleteIndexTestEntryWithBounds(
			fmt.Sprintf("partition-delete-%06d.parquet", i),
			0,
			nil,
			2,
			nil,
			positionalDeleteBenchmarkPath(first),
			positionalDeleteBenchmarkPath(last),
		)
	}

	return dataEntries, deleteEntries
}

func positionalDeleteSelectivePartitionBenchmarkEntries(
	dataFileCount, deleteFileCount, partitionCount int,
) ([]iceberg.ManifestEntry, []iceberg.ManifestEntry) {
	dataEntries := make([]iceberg.ManifestEntry, dataFileCount)
	for i := range dataEntries {
		partition := map[int]any{1000: int32(i % partitionCount)}
		dataEntries[i] = newPositionalDeleteIndexDataEntry(
			positionalDeleteBenchmarkPath(i), 0, partition, 1)
	}

	deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
	for i := range deleteEntries {
		partition := map[int]any{1000: int32(i % partitionCount)}
		deleteEntries[i] = newPositionalDeleteIndexTestEntry(
			fmt.Sprintf("partition-delete-%06d.parquet", i),
			0,
			partition,
			2,
			nil,
			"",
		)
	}

	return dataEntries, deleteEntries
}

func positionalDeleteMixedBenchmarkEntries(
	dataFileCount, deleteFileCount int,
) ([]iceberg.ManifestEntry, []iceberg.ManifestEntry, int) {
	dataEntries := positionalDeleteBenchmarkDataEntries(dataFileCount)
	fileScopedCount := deleteFileCount / 2
	partitionScopedCount := deleteFileCount - fileScopedCount
	deleteEntries := make([]iceberg.ManifestEntry, 0, deleteFileCount)

	fileScopedDataCount := dataFileCount / 2
	fileStride := fileScopedDataCount / fileScopedCount
	for i := range fileScopedCount {
		path := positionalDeleteBenchmarkPath(i * fileStride)
		deleteEntries = append(deleteEntries, newPositionalDeleteIndexTestEntry(
			"file-delete-"+path, 0, nil, 2, &path, path))
	}

	partitionDataCount := dataFileCount - fileScopedDataCount
	partitionStride := partitionDataCount / partitionScopedCount
	for i := range partitionScopedCount {
		first := fileScopedDataCount + i*partitionStride
		last := first + partitionStride - 1
		deleteEntries = append(deleteEntries, newPositionalDeleteIndexTestEntryWithBounds(
			fmt.Sprintf("partition-delete-%06d.parquet", i),
			0,
			nil,
			2,
			nil,
			positionalDeleteBenchmarkPath(first),
			positionalDeleteBenchmarkPath(last),
		))
	}

	return dataEntries, deleteEntries, fileScopedCount + partitionDataCount
}

func positionalDeleteBenchmarkDataEntries(dataFileCount int) []iceberg.ManifestEntry {
	dataEntries := make([]iceberg.ManifestEntry, dataFileCount)
	for i := range dataEntries {
		path := positionalDeleteBenchmarkPath(i)
		dataEntries[i] = newPositionalDeleteIndexDataEntry(path, 0, nil, 1)
	}

	return dataEntries
}

func benchmarkPositionalDeleteIndex(
	b *testing.B,
	dataEntries, deleteEntries []iceberg.ManifestEntry,
) int {
	idx, err := buildPositionalDeleteIndex(deleteEntries)
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

	return matched
}

func matchPositionalDeletesByMetrics(
	dataEntry iceberg.ManifestEntry,
	deleteEntries []iceberg.ManifestEntry,
) ([]iceberg.DataFile, error) {
	start := sort.Search(len(deleteEntries), func(i int) bool {
		return deleteEntries[i].SequenceNum() >= dataEntry.SequenceNum()
	})
	evaluator, err := newInclusiveMetricsEvaluator(iceberg.PositionalDeleteSchema,
		iceberg.EqualTo(iceberg.Reference("file_path"), dataEntry.DataFile().FilePath()),
		true, false)
	if err != nil {
		return nil, err
	}

	var out []iceberg.DataFile
	for _, deleteEntry := range deleteEntries[start:] {
		matches, err := evaluator(deleteEntry.DataFile())
		if err != nil {
			return nil, err
		}
		if matches {
			out = append(out, deleteEntry.DataFile())
		}
	}

	return out, nil
}

func matchPositionalDeletesByPartition(
	dataEntry iceberg.ManifestEntry,
	deleteEntries []iceberg.ManifestEntry,
) ([]iceberg.DataFile, error) {
	dataFile := dataEntry.DataFile()
	dataPartitionKey, err := canonicalPartitionKey(dataFile.SpecID(), dataFile.Partition())
	if err != nil {
		return nil, err
	}

	start := sort.Search(len(deleteEntries), func(i int) bool {
		return deleteEntries[i].SequenceNum() >= dataEntry.SequenceNum()
	})
	evaluator, err := newInclusiveMetricsEvaluator(iceberg.PositionalDeleteSchema,
		iceberg.EqualTo(iceberg.Reference("file_path"), dataFile.FilePath()),
		true, false)
	if err != nil {
		return nil, err
	}

	var out []iceberg.DataFile
	for _, deleteEntry := range deleteEntries[start:] {
		deleteFile := deleteEntry.DataFile()
		deletePartitionKey, err := canonicalPartitionKey(
			deleteFile.SpecID(), deleteFile.Partition())
		if err != nil {
			return nil, err
		}
		if deletePartitionKey != dataPartitionKey {
			continue
		}

		matches, err := evaluator(deleteFile)
		if err != nil {
			return nil, err
		}
		if matches {
			out = append(out, deleteFile)
		}
	}

	return out, nil
}

func positionalDeleteBenchmarkPath(i int) string {
	return fmt.Sprintf("data-%06d.parquet", i)
}
