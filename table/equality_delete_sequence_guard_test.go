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
	"github.com/stretchr/testify/require"
)

// This wrapper does not implement DataFileStatsRef. Any statistics access
// falls back to the getters below, so a skipped lookup must not call them.
type equalityDeleteStatsForbiddenFile struct {
	iceberg.DataFile
}

func (equalityDeleteStatsForbiddenFile) ValueCounts() map[int]int64 {
	panic("unused value counts")
}

func (equalityDeleteStatsForbiddenFile) NullValueCounts() map[int]int64 {
	panic("unused null counts")
}

func (equalityDeleteStatsForbiddenFile) NaNValueCounts() map[int]int64 {
	panic("unused NaN counts")
}

func (equalityDeleteStatsForbiddenFile) LowerBoundValues() map[int][]byte {
	panic("unused lower bounds")
}

func (equalityDeleteStatsForbiddenFile) UpperBoundValues() map[int][]byte {
	panic("unused upper bounds")
}

func TestEqualityDeleteIndexSkipsUnusedDataFileStatistics(t *testing.T) {
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	partA := map[int]any{1000: int32(1)}
	partB := map[int]any{1000: int32(2)}
	newDelete := func(path string, specID int32, partition map[int]any, seq int64) iceberg.ManifestEntry {
		return newEqualityDeleteMetricsTestEntry(path, specID, partition,
			iceberg.EntryContentEqDeletes, seq, []int{1}, nil, nil, nil, nil, nil)
	}
	idx, err := buildEqualityDeleteIndex([]iceberg.ManifestEntry{
		newDelete("delete-a.parquet", 1, partA, 2),
		newDelete("delete-b.parquet", 1, partB, 10),
	}, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)

	for _, tc := range []struct {
		name      string
		partition map[int]any
		seq       int64
		skipStats bool
		want      []string
	}{
		{name: "same sequence", partition: partA, seq: 2, skipStats: true},
		{name: "newer than own partition", partition: partA, seq: 5, skipStats: true},
		{name: "newer than all deletes", partition: partA, seq: 11, skipStats: true},
		{name: "other partition", partition: map[int]any{1000: int32(3)}, seq: 1, skipStats: true},
		{name: "older data", partition: partA, seq: 1, want: []string{"delete-a.parquet"}},
		{name: "later delete in own partition", partition: partB, seq: 5, want: []string{"delete-b.parquet"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := newEqualityDeleteMetricsTestEntry("data.parquet", 1, tc.partition,
				iceberg.EntryContentData, tc.seq, nil, nil, nil, nil, nil, nil)
			if tc.skipStats {
				seq := entry.DataSequenceNumber()
				entry = iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, &seq, nil,
					equalityDeleteStatsForbiddenFile{entry.DataFile()})
			}

			matched, err := idx.forDataFile(entry)
			require.NoError(t, err)
			if tc.want == nil {
				require.Empty(t, matched)
			} else {
				require.Equal(t, tc.want, equalityDeleteMetricPaths(matched))
			}
		})
	}

	t.Run("global delete still applies", func(t *testing.T) {
		globalIdx, err := buildEqualityDeleteIndex([]iceberg.ManifestEntry{
			newDelete("delete-a.parquet", 1, partA, 2),
			newDelete("global.parquet", 0, nil, 8),
		}, equalityDeleteIndexTestSpecs(), schema)
		require.NoError(t, err)
		data := newEqualityDeleteMetricsTestEntry("data.parquet", 1, partA,
			iceberg.EntryContentData, 5, nil, nil, nil, nil, nil, nil)
		matched, err := globalIdx.forDataFile(data)
		require.NoError(t, err)
		require.Equal(t, []string{"global.parquet"}, equalityDeleteMetricPaths(matched))
	})

	t.Run("global delete older than data skips stats", func(t *testing.T) {
		globalIdx, err := buildEqualityDeleteIndex([]iceberg.ManifestEntry{
			newDelete("delete-a.parquet", 1, partA, 2),
			newDelete("global.parquet", 0, nil, 3),
		}, equalityDeleteIndexTestSpecs(), schema)
		require.NoError(t, err)

		data := newEqualityDeleteMetricsTestEntry("data.parquet", 1, partA,
			iceberg.EntryContentData, 5, nil, nil, nil, nil, nil, nil)
		seq := data.DataSequenceNumber()
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, &seq, nil,
			equalityDeleteStatsForbiddenFile{data.DataFile()})
		matched, err := globalIdx.forDataFile(entry)
		require.NoError(t, err)
		require.Empty(t, matched)
	})

	t.Run("invalid partition is still rejected", func(t *testing.T) {
		data := newEqualityDeleteMetricsTestEntry("data.parquet", 1,
			map[int]any{1000: make(chan struct{})}, iceberg.EntryContentData,
			11, nil, nil, nil, nil, nil, nil)
		_, err := idx.forDataFile(data)
		require.Error(t, err)
	})
}

func BenchmarkEqualityDeleteIndexSequenceSkip(b *testing.B) {
	const fileCount = 1024
	partA := map[int]any{1000: int32(1)}
	partB := map[int]any{1000: int32(2)}
	specs := equalityDeleteIndexTestSpecs()
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	deletes := []iceberg.ManifestEntry{
		newEqualityDeleteMetricsTestEntry("delete-a.parquet", 1, partA,
			iceberg.EntryContentEqDeletes, 2, []int{1}, nil, nil, nil, nil, nil),
		newEqualityDeleteMetricsTestEntry("delete-b.parquet", 1, partB,
			iceberg.EntryContentEqDeletes, 10, []int{1}, nil, nil, nil, nil, nil),
	}
	idx, err := buildEqualityDeleteIndex(deletes, specs, schema)
	if err != nil {
		b.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		seq  int64
	}{
		{name: "no applicable deletes", seq: 5},
		{name: "same delete sequence", seq: 2},
		{name: "applicable delete", seq: 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			data := make([]iceberg.ManifestEntry, fileCount)
			for i := range data {
				data[i] = newEqualityDeleteMetricsTestEntry(
					fmt.Sprintf("data-%d.parquet", i), 1, partA,
					iceberg.EntryContentData, tc.seq, nil,
					map[int]int64{1: 1}, map[int]int64{1: 0},
					nil, nil, nil)
			}
			b.ReportAllocs()
			b.ReportMetric(fileCount, "data_files")
			b.ResetTimer()
			for range b.N {
				matched := 0
				for _, entry := range data {
					files, err := idx.forDataFile(entry)
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
