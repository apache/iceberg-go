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
	"runtime"
	"testing"

	"github.com/apache/iceberg-go"
)

func BenchmarkDeleteIndexRetainsOnlyRequiredStats(b *testing.B) {
	const (
		deleteFileCount = 100
		fieldCount      = 512
	)

	for _, kind := range []string{"positional", "equality"} {
		b.Run(fmt.Sprintf("%s/files=%d/fields=%d", kind, deleteFileCount, fieldCount), func(b *testing.B) {
			entries := wideDeleteIndexBenchmarkEntries(b, kind, deleteFileCount, fieldCount)
			sourceStatsPerFile := countDataFileStats(entries[0].DataFile())
			var schema *iceberg.Schema
			if kind == "equality" {
				// Include schema-driven metric decoding in the equality benchmark.
				schema = equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
			}

			b.ReportAllocs()
			b.ResetTimer()

			var index any
			for range b.N {
				if kind == "positional" {
					built, err := buildPositionalDeleteIndex(entries)
					if err != nil {
						b.Fatal(err)
					}
					index = built
				} else {
					built, err := buildEqualityDeleteIndex(entries, equalityDeleteIndexTestSpecs(), schema)
					if err != nil {
						b.Fatal(err)
					}
					index = built
				}
			}

			b.StopTimer()
			var retainedStatsPerFile int
			if kind == "positional" {
				retainedStatsPerFile = indexedPositionalStatsPerFile(index.(*positionalDeleteIndex))
			} else {
				retainedStatsPerFile = indexedEqualityStatsPerFile(index.(*equalityDeleteIndex))
			}
			b.ReportMetric(float64(sourceStatsPerFile), "source_stats_fields/file")
			b.ReportMetric(float64(retainedStatsPerFile), "retained_stats_fields/file")
			runtime.KeepAlive(index)
		})
	}
}

func wideDeleteIndexBenchmarkEntries(
	b *testing.B,
	kind string,
	fileCount, fieldCount int,
) []iceberg.ManifestEntry {
	b.Helper()

	values := make(map[int]int64, fieldCount+1)
	nulls := make(map[int]int64, fieldCount+1)
	nans := make(map[int]int64, fieldCount+1)
	lower := make(map[int][]byte, fieldCount+1)
	upper := make(map[int][]byte, fieldCount+1)
	for fieldID := 1; fieldID <= fieldCount; fieldID++ {
		values[fieldID] = 1
		nulls[fieldID] = 0
		nans[fieldID] = 0
		lower[fieldID] = []byte(fmt.Sprintf("lower-%03d", fieldID))
		upper[fieldID] = []byte(fmt.Sprintf("upper-%03d", fieldID))
	}

	var spec iceberg.PartitionSpec
	var partition map[int]any
	var content iceberg.ManifestEntryContent
	var equalityFieldIDs []int
	if kind == "positional" {
		spec = iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: iceberg.IdentityTransform{},
		})
		partition = map[int]any{1000: "partition"}
		content = iceberg.EntryContentPosDeletes
		values[filePathFieldID] = 1
		nulls[filePathFieldID] = 0
		nans[filePathFieldID] = 0
		lower[filePathFieldID] = []byte("data-a.parquet")
		upper[filePathFieldID] = []byte("data-z.parquet")
	} else {
		spec = *iceberg.UnpartitionedSpec
		content = iceberg.EntryContentEqDeletes
		equalityFieldIDs = []int{1, 2}
	}

	entries := make([]iceberg.ManifestEntry, fileCount)
	for i := range entries {
		file := newDeleteIndexStatsFile(
			b,
			spec,
			content,
			iceberg.ParquetFile,
			fmt.Sprintf("delete-%d.parquet", i),
			partition,
			1,
			100,
			values,
			nulls,
			nans,
			lower,
			upper,
			equalityFieldIDs,
		)
		entries[i] = iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, nil, int64Ptr(int64(i+1)), nil, file)
	}

	return entries
}

func indexedPositionalStatsPerFile(index *positionalDeleteIndex) int {
	for _, entries := range index.byPartition {
		if len(entries) == 0 {
			continue
		}

		return countDataFileStats(entries[0].file)
	}

	return 0
}

func indexedEqualityStatsPerFile(index *equalityDeleteIndex) int {
	if len(index.global) > 0 {
		return countDataFileStats(index.global[0].file)
	}
	for _, entries := range index.byPartition {
		if len(entries) > 0 {
			return countDataFileStats(entries[0].file)
		}
	}

	return 0
}

func countDataFileStats(file iceberg.DataFile) int {
	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(file)

	return len(valueCounts) + len(nullCounts) + len(nanCounts) + len(lowerBounds) + len(upperBounds)
}
