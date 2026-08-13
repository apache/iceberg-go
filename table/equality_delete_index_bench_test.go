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

	"github.com/DataDog/iceberg-go"
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
					idx, err := buildEqualityDeleteIndex(deleteEntries, specs)
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
		idx, err := buildEqualityDeleteIndex(deleteEntries, specs)
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
		idx, err := buildEqualityDeleteIndex(deleteEntries, specs)
		if err != nil {
			b.Fatal(err)
		}
		equalityDeleteBenchmarkSink = len(idx.global) + len(idx.byPartition)
	}
}
