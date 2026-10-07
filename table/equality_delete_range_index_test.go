// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEqualityDeleteRangeIndexMatchesLinearReference(t *testing.T) {
	const deleteFileCount = 512

	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	partition := map[int]any{1000: int32(0)}
	deleteEntries := make([]iceberg.ManifestEntry, deleteFileCount)
	for i := range deleteEntries {
		var lower, upper []byte
		if i%31 != 0 {
			lower, upper = equalityDeleteMetricsTestBounds(
				t, int32(i*10), int32(i*10+4))
		}
		deleteEntries[i] = newEqualityDeleteMetricsTestEntry(
			fmt.Sprintf("delete-%d.parquet", i),
			1,
			partition,
			iceberg.EntryContentEqDeletes,
			int64(i+2),
			[]int{1},
			map[int]int64{1: 1},
			map[int]int64{1: 0},
			map[int]int64{1: 0},
			lower,
			upper,
		)
	}

	idx, err := buildEqualityDeleteIndex(
		deleteEntries, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)

	key, err := newEqualityDeletePartitionKey(1, partition)
	require.NoError(t, err)
	require.NotNil(t, idx.rangeIndexesByPartition[key])
	entries := idx.byPartition[key]

	tests := []struct {
		name     string
		sequence int64
		lower    int32
		upper    int32
	}{
		{name: "selective middle", sequence: 0, lower: 2000, upper: 2099},
		{name: "selective with sequence pruning", sequence: 200, lower: 4000, upper: 4099},
		{name: "no ranged overlap keeps fallback", sequence: 0, lower: 9000, upper: 9100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper := equalityDeleteMetricsTestBounds(t, tt.lower, tt.upper)
			dataEntry := newEqualityDeleteMetricsTestEntry(
				"data.parquet",
				1,
				partition,
				iceberg.EntryContentData,
				tt.sequence,
				nil,
				map[int]int64{1: 1},
				map[int]int64{1: 0},
				map[int]int64{1: 0},
				lower,
				upper,
			)

			got, err := idx.forDataFile(dataEntry)
			require.NoError(t, err)

			stats := equalityDeleteDataFileStatsFor(dataEntry.DataFile())
			want := appendEqualityDeletesAfter(
				nil, entries, dataEntry.SequenceNum(), &stats)
			assert.Equal(t, equalityDeleteMetricPaths(want), equalityDeleteMetricPaths(got))
		})
	}
}

func BenchmarkEqualityDeleteRangeIndexLookup(b *testing.B) {
	const (
		dataFileCount   = 10_000
		deleteFileCount = 10_000
		groupCount      = 100
		filesPerGroup   = deleteFileCount / groupCount
	)

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

	idx, err := buildEqualityDeleteIndex(
		deleteEntries, equalityDeleteIndexTestSpecs(), schema)
	if err != nil {
		b.Fatal(err)
	}
	key, err := newEqualityDeletePartitionKey(1, partition)
	if err != nil {
		b.Fatal(err)
	}
	entries := idx.byPartition[key]
	rangeIndex := idx.rangeIndexesByPartition[key]
	if rangeIndex == nil {
		b.Fatal("range index was not built")
	}

	benchmark := func(b *testing.B, indexed bool) {
		b.ReportAllocs()
		matched := 0
		b.ResetTimer()
		for range b.N {
			matched = 0
			for _, dataEntry := range dataEntries {
				stats := equalityDeleteDataFileStatsFor(dataEntry.DataFile())
				var files []iceberg.DataFile
				if indexed {
					files = appendEqualityDeletesAfterIndexed(
						nil, entries, dataEntry.SequenceNum(), &stats, rangeIndex)
				} else {
					files = appendEqualityDeletesAfter(
						nil, entries, dataEntry.SequenceNum(), &stats)
				}
				matched += len(files)
			}
			equalityDeleteBenchmarkSink = matched
		}
		b.StopTimer()
		b.ReportMetric(float64(matched)/float64(dataFileCount), "attached_deletes_per_data_file")
	}

	b.Run("linear", func(b *testing.B) {
		benchmark(b, false)
	})
	b.Run("range_index", func(b *testing.B) {
		benchmark(b, true)
	})
}
