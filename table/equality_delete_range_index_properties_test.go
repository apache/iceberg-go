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
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func equalityDeleteRangeTestSpecs() equalityDeleteIndexTestSpecLookup {
	specs := equalityDeleteIndexTestSpecs()
	for _, id := range []int{1, 2} {
		// Partition on a separate field so equality bounds may vary within it.
		specs[id] = iceberg.NewPartitionSpecID(id, iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1000, Name: "partition",
			Transform: iceberg.IdentityTransform{},
		})
	}

	return specs
}

func equalityDeleteRangeTestBucket(t testing.TB, count int, specID int32, partition map[int]any) []iceberg.ManifestEntry {
	t.Helper()
	entries := make([]iceberg.ManifestEntry, count)
	for i := range entries {
		lower, upper := equalityDeleteMetricsTestBounds(t, int32(i*10), int32(i*10+4))
		entries[i] = newEqualityDeleteMetricsTestEntry(
			fmt.Sprintf("delete-%d.parquet", i), specID, partition, iceberg.EntryContentEqDeletes, int64(i+2),
			[]int{1}, map[int]int64{1: 1}, map[int]int64{1: 0}, nil, lower, upper)
	}

	return entries
}

func TestEqualityDeleteRangeIndexCandidateLimit(t *testing.T) {
	entries := equalityDeleteRangeTestBucket(t, equalityDeleteRangeIndexMinEntries, 0, nil)
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	idx, err := buildEqualityDeleteIndex(entries, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)
	limit := len(entries) / 2
	for _, matches := range []int{limit - 1, limit} {
		lower, upper := equalityDeleteMetricsTestBounds(t, int32(0), int32((matches-1)*10+4))
		data := newEqualityDeleteMetricsTestEntry("data.parquet", 0, nil, iceberg.EntryContentData, 0,
			nil, nil, nil, nil, lower, upper)
		stats := equalityDeleteDataFileStatsFor(data.DataFile())
		candidates, ok := idx.globalRangeIndex.candidates(0, 0, &stats)
		assert.Equal(t, matches < limit, ok, "candidate count %d, limit %d", matches, limit)
		if ok {
			assert.Len(t, candidates, matches)
		}
		got, err := idx.forDataFile(data)
		require.NoError(t, err)
		assert.Len(t, got, matches, "falling back must preserve the matching deletes")
	}
}

func TestEqualityDeleteRangeIndexDefersUnusableLookups(t *testing.T) {
	count := equalityDeleteRangeIndexMinEntries
	entries := equalityDeleteRangeTestBucket(t, count, 0, nil)
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	for _, remaining := range []int{
		equalityDeleteRangeIndexMinLookupEntries - 1,
		equalityDeleteRangeIndexMinLookupEntries,
		equalityDeleteRangeIndexMinLookupEntries + 1,
	} {
		for _, bounds := range []bool{false, true} {
			t.Run(fmt.Sprintf("remaining=%d/bounds=%t", remaining, bounds), func(t *testing.T) {
				idx, err := buildEqualityDeleteIndex(entries, equalityDeleteIndexTestSpecs(), schema)
				require.NoError(t, err)
				require.Empty(t, idx.globalRangeIndex.groups, "construction must defer the tree")
				var lower, upper []byte
				if bounds {
					lower, upper = equalityDeleteMetricsTestBounds(t, int32((count-1)*10), int32((count-1)*10+4))
				}
				seq := int64(count - remaining + 1)
				data := newEqualityDeleteMetricsTestEntry("data.parquet", 0, nil, iceberg.EntryContentData, seq,
					nil, nil, nil, nil, lower, upper)
				got, err := idx.forDataFile(data)
				require.NoError(t, err)
				stats := equalityDeleteDataFileStatsFor(data.DataFile())
				want := appendEqualityDeletesAfterLinear(nil, idx.global, seq, &stats)
				assert.Equal(t, equalityDeleteMetricPaths(want), equalityDeleteMetricPaths(got))
				assert.Equal(t, bounds && remaining >= equalityDeleteRangeIndexMinLookupEntries,
					len(idx.globalRangeIndex.groups) > 0, "only an eligible lookup should build the tree")
			})
		}
	}
}

func TestEqualityDeleteRangeIndexMultipleFieldsAndFallback(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "small_group", Type: iceberg.PrimitiveTypes.Int32})
	count := equalityDeleteRangeIndexMinEntries
	groupSize := equalityDeleteRangeIndexMinGroupEntries
	entries := equalityDeleteRangeTestBucket(t, count, 0, nil)
	for i, entry := range entries {
		file := entry.DataFile().(*equalityDeleteMetricsTestFile)
		switch {
		case i < count-2*groupSize:
		case i < count-groupSize:
			file.equalityFieldIDs = []int{1, 2}
			file.lowerBounds[2], file.upperBounds[2] = file.lowerBounds[1], file.upperBounds[1]
			delete(file.lowerBounds, 1)
			file.nullCounts[2] = 0
		case i < count-1:
			file.equalityFieldIDs = []int{3}
			file.lowerBounds[3], file.upperBounds[3] = file.lowerBounds[1], file.upperBounds[1]
			file.nullCounts[3] = 0
		default:
			file.equalityFieldIDs = []int{99}
		}
	}
	idx, err := buildEqualityDeleteIndex(entries, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)
	data := entries[0].DataFile().(*equalityDeleteMetricsTestFile)
	stats := equalityDeleteDataFileStatsFor(data)
	stats.lowerBounds = map[int][]byte{1: data.lowerBounds[1], 2: data.lowerBounds[1]}
	stats.upperBounds = map[int][]byte{1: data.upperBounds[1], 2: data.upperBounds[1]}
	candidates, ok := idx.globalRangeIndex.candidates(0, 0, &stats)
	require.True(t, ok)
	require.Len(t, idx.globalRangeIndex.groups, 2)
	assert.Equal(t, 1, idx.globalRangeIndex.groups[0].field.fieldID)
	assert.Equal(t, 2, idx.globalRangeIndex.groups[1].field.fieldID,
		"a composite key must skip an unusable first field")
	assert.Len(t, idx.globalRangeIndex.fallback, groupSize,
		"a sub-threshold group and an unknown field must stay in the fallback list")
	assert.Len(t, candidates, groupSize+1)
	got := appendEqualityDeletesAfterIndexed(nil, idx.global, 0, &stats, idx.globalRangeIndex)
	want := appendEqualityDeletesAfterLinear(nil, idx.global, 0, &stats)
	assert.Equal(t, equalityDeleteMetricPaths(want), equalityDeleteMetricPaths(got))
}

func equalityDeleteRandomRangeFile(t testing.TB, rng *rand.Rand, fields []iceberg.NestedField, safe bool) *equalityDeleteMetricsTestFile {
	t.Helper()
	file := &equalityDeleteMetricsTestFile{mockDataFile: &mockDataFile{
		format: iceberg.ParquetFile, count: 100,
		valueCounts: make(map[int]int64), nullCounts: make(map[int]int64), nanCounts: make(map[int]int64),
		lowerBounds: make(map[int][]byte), upperBounds: make(map[int][]byte),
	}}
	for _, field := range fields {
		id := field.ID
		lo := rng.IntN(4096)
		hi := lo + rng.IntN(8)
		var lower, upper []byte
		switch field.Type.(type) {
		case iceberg.Int32Type:
			lower, upper = equalityDeleteMetricsTestBounds(t, int32(lo), int32(hi))
		case iceberg.Int64Type:
			lower, upper = equalityDeleteMetricsTestBounds(t, int64(lo), int64(hi))
		case iceberg.Float32Type:
			lower, upper = equalityDeleteMetricsTestBounds(t, float32(lo), float32(hi))
		case iceberg.Float64Type:
			lower, upper = equalityDeleteMetricsTestBounds(t, float64(lo), float64(hi))
		case iceberg.StringType:
			lower, upper = equalityDeleteMetricsTestBounds(t, fmt.Sprintf("%04d", lo), fmt.Sprintf("%04d", hi))
		case iceberg.BinaryType:
			lower, upper = equalityDeleteMetricsTestBounds(t, []byte(fmt.Sprintf("%04d", lo)), []byte(fmt.Sprintf("%04d", hi)))
		default:
			t.Fatalf("unsupported test type %s", field.Type)
		}
		file.valueCounts[id], file.nullCounts[id], file.nanCounts[id] = 100, 0, 0
		file.lowerBounds[id], file.upperBounds[id] = lower, upper
		if safe {
			continue
		}
		switch rng.IntN(40) {
		case 0:
			delete(file.lowerBounds, id)
		case 1:
			delete(file.upperBounds, id)
		case 2:
			file.lowerBounds[id], file.upperBounds[id] = upper, lower
		case 3:
			delete(file.nullCounts, id)
		case 4:
			file.nullCounts[id] = 1
		case 5:
			delete(file.nanCounts, id)
		case 6:
			file.nanCounts[id] = 1
		case 7:
			delete(file.valueCounts, id)
		case 8:
			file.nullCounts[id] = 100
		}
	}

	return file
}

func TestEqualityDeleteRangeIndexRandomizedMatchesLinearReference(t *testing.T) {
	for _, types := range [][2]iceberg.Type{
		{iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64},
		{iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64},
		{iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary},
	} {
		t.Run(fmt.Sprintf("%s-%s", types[0], types[1]), func(t *testing.T) {
			fields := []iceberg.NestedField{
				{ID: 1, Name: "first", Type: types[0], Required: true},
				{ID: 2, Name: "second", Type: types[1]},
			}
			schema := iceberg.NewSchema(1, fields[0], fields[1], iceberg.NestedField{
				ID: 3, Name: "partition", Type: iceberg.PrimitiveTypes.Int32, Required: true,
			})
			fastLookups := 0
			for _, count := range []int{63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513} {
				rng := rand.New(rand.NewPCG(uint64(count), 2148))
				var entries []iceberg.ManifestEntry
				for bucket := range 3 {
					for i := range count {
						file := equalityDeleteRandomRangeFile(t, rng, fields, false)
						file.path = fmt.Sprintf("bucket-%d/delete-%d.parquet", bucket, i)
						file.contentType = iceberg.EntryContentEqDeletes
						if bucket > 0 {
							file.specid = 1
							file.partition = map[int]any{1000: int32(bucket - 1)}
						}
						file.equalityFieldIDs = []int{1 + rng.IntN(2)}
						if i%4 == 0 {
							file.equalityFieldIDs = []int{1, 2}
							if i%16 == 0 {
								delete(file.lowerBounds, 1)
							}
						}
						if i%37 == 0 {
							file.equalityFieldIDs = []int{99}
						}
						seq := int64(1 + rng.IntN(8))
						entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, &seq, nil, file))
					}
				}
				// Input order must not supply hidden sequence or range ordering.
				rng.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
				idx, err := buildEqualityDeleteIndex(entries, equalityDeleteRangeTestSpecs(), schema)
				require.NoError(t, err)
				for query := range 64 {
					file := equalityDeleteRandomRangeFile(t, rng, fields, query%8 == 0)
					file.path, file.contentType = "data.parquet", iceberg.EntryContentData
					file.specid = int32(query % 3)
					if file.specid != 0 {
						file.partition = map[int]any{1000: int32(rng.IntN(3))}
					}
					seq := int64(rng.IntN(9))
					if query%8 == 0 {
						seq = 0
					}
					data := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, &seq, nil, file)
					got, err := idx.forDataFile(data)
					require.NoError(t, err)
					key, err := newEqualityDeletePartitionKey(file.specid, file.partition)
					require.NoError(t, err)
					stats := equalityDeleteDataFileStatsFor(file)
					want := appendEqualityDeletesAfterLinear(nil, idx.global, seq, &stats)
					want = appendEqualityDeletesAfterLinear(want, idx.byPartition[key], seq, &stats)
					require.Equal(t, equalityDeleteMetricPaths(want), equalityDeleteMetricPaths(got),
						"seed=%d/2148 query=%d sequence=%d", count, query, seq)
					for _, index := range []*equalityDeleteRangeIndex{idx.globalRangeIndex, idx.rangeIndexesByPartition[key]} {
						if index == nil {
							continue
						}
						start := firstEqualityDeleteAfter(index.entries, seq)
						if len(index.entries)-start < equalityDeleteRangeIndexMinLookupEntries {
							continue
						}
						if candidates, ok := index.candidates(start, seq, &stats); ok {
							fastLookups++
							assert.Less(t, len(candidates), len(index.entries)-start)
						}
					}
				}
			}
			assert.Positive(t, fastLookups, "differential coverage must exercise the range index")
		})
	}
}

func TestEqualityDeleteRangeIndexConcurrentFirstLookups(t *testing.T) {
	partition := map[int]any{1000: int32(0)}
	otherPartition := map[int]any{1000: int32(1)}
	entries := equalityDeleteRangeTestBucket(t, 512, 0, nil)
	entries = append(entries, equalityDeleteRangeTestBucket(t, 512, 1, partition)...)
	entries = append(entries, equalityDeleteRangeTestBucket(t, 512, 1, otherPartition)...)
	for i, entry := range entries {
		entry.DataFile().(*equalityDeleteMetricsTestFile).path = fmt.Sprintf("delete-%d.parquet", i)
	}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "partition", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	idx, err := buildEqualityDeleteIndex(entries, equalityDeleteRangeTestSpecs(), schema)
	require.NoError(t, err)
	lower, upper := equalityDeleteMetricsTestBounds(t, int32(2000), int32(2004))
	data := newEqualityDeleteMetricsTestEntry("data.parquet", 1, partition, iceberg.EntryContentData, 0,
		nil, nil, nil, nil, lower, upper)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			<-start
			got, err := idx.forDataFile(data)
			if assert.NoError(t, err) {
				assert.Equal(t, []string{"delete-200.parquet", "delete-712.parquet"}, equalityDeleteMetricPaths(got))
			}
		})
	}
	close(start)
	wg.Wait()
	otherKey, err := newEqualityDeletePartitionKey(1, otherPartition)
	require.NoError(t, err)
	assert.Empty(t, idx.rangeIndexesByPartition[otherKey].groups, "unprobed partitions must not allocate a tree")
	assert.Len(t, idx.globalRangeIndex.groups[0].nodes, 512)
}

func TestEqualityDeleteRangeIndexFloatNaNGuards(t *testing.T) {
	for _, typ := range []iceberg.Type{iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64} {
		for _, deleteNaNs := range []int64{-1, 0, 1} {
			for _, dataNaNs := range []int64{-1, 0, 1} {
				t.Run(fmt.Sprintf("%s/deleteNaNs=%d/dataNaNs=%d", typ, deleteNaNs, dataNaNs), func(t *testing.T) {
					fields := []iceberg.NestedField{{ID: 1, Name: "id", Type: typ, Required: true}}
					rng := rand.New(rand.NewPCG(2148, 1))
					file := equalityDeleteRandomRangeFile(t, rng, fields, true)
					file.equalityFieldIDs = []int{1}
					file.nanCounts[1] = deleteNaNs
					if deleteNaNs < 0 {
						delete(file.nanCounts, 1)
					}
					var dataLower, dataUpper []byte
					if typ.Equals(iceberg.PrimitiveTypes.Float32) {
						file.lowerBounds[1], file.upperBounds[1] = equalityDeleteMetricsTestBounds(t, float32(20), float32(30))
						dataLower, dataUpper = equalityDeleteMetricsTestBounds(t, float32(0), float32(10))
					} else {
						file.lowerBounds[1], file.upperBounds[1] = equalityDeleteMetricsTestBounds(t, float64(20), float64(30))
						dataLower, dataUpper = equalityDeleteMetricsTestBounds(t, float64(0), float64(10))
					}
					seq := int64(2)
					entries := make([]iceberg.ManifestEntry, equalityDeleteRangeIndexMinEntries)
					for i := range entries {
						deleteFile := *file
						deleteFile.path = fmt.Sprintf("delete-%d.parquet", i)
						deleteFile.contentType = iceberg.EntryContentEqDeletes
						entries[i] = iceberg.NewManifestEntry(iceberg.EntryStatusADDED, nil, &seq, nil, &deleteFile)
					}
					idx, err := buildEqualityDeleteIndex(entries, equalityDeleteIndexTestSpecs(), iceberg.NewSchema(1, fields...))
					require.NoError(t, err)
					nanCounts := map[int]int64{1: dataNaNs}
					if dataNaNs < 0 {
						nanCounts = nil
					}
					data := newEqualityDeleteMetricsTestEntry("data.parquet", 0, nil, iceberg.EntryContentData, 0,
						nil, nil, nil, nanCounts, dataLower, dataUpper)
					stats := equalityDeleteDataFileStatsFor(data.DataFile())
					candidates, indexed := idx.globalRangeIndex.candidates(0, 0, &stats)
					assert.Equal(t, deleteNaNs == 0 && dataNaNs == 0, indexed)
					got, err := idx.forDataFile(data)
					require.NoError(t, err)
					if indexed {
						assert.Empty(t, candidates)
						assert.Empty(t, got)
					} else {
						assert.Len(t, got, len(entries), "unknown NaNs cannot prove disjointness")
					}
				})
			}
		}
	}
}

func TestEqualityDeleteRangeIndexOptionalParent(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "parent", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 1, Name: "child", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		}},
	})
	entries := equalityDeleteRangeTestBucket(t, equalityDeleteRangeIndexMinEntries, 0, nil)
	for _, entry := range entries {
		delete(entry.DataFile().(*equalityDeleteMetricsTestFile).nullCounts, 1)
	}
	idx, err := buildEqualityDeleteIndex(entries, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)
	lower, upper := equalityDeleteMetricsTestBounds(t, int32(20_000), int32(20_004))
	data := newEqualityDeleteMetricsTestEntry("data.parquet", 0, nil, iceberg.EntryContentData, 0,
		nil, map[int]int64{1: 1}, map[int]int64{1: 1}, nil, lower, upper)
	got, err := idx.forDataFile(data)
	require.NoError(t, err)
	assert.Len(t, got, len(entries), "a required child may be null under an optional parent")
	assert.Empty(t, idx.globalRangeIndex.groups, "unknown null keys must stay off the range tree")
}

// BenchmarkEqualityDeleteRangeIndexThreshold includes range-tree construction
// and repeated lookups over already-decoded delete metrics. A single lookup
// exposes cold-start cost; repeated lookups show when that cost is amortized.
func BenchmarkEqualityDeleteRangeIndexThreshold(b *testing.B) {
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	for _, count := range []int{255, 256, 257, 511, 512} {
		manifestEntries := equalityDeleteRangeTestBucket(b, count, 0, nil)
		entries := make([]equalityDeleteIndexEntry, count)
		for i, entry := range manifestEntries {
			entries[i] = newEqualityDeleteIndexEntry(entry, schema)
		}
		for _, lookups := range []int{1, 16, 128} {
			for _, broad := range []bool{false, true} {
				lower, upper := equalityDeleteMetricsTestBounds(b, int32(100), int32(104))
				if broad {
					lower, upper = equalityDeleteMetricsTestBounds(b, int32(0), int32(count*10))
				}
				data := newEqualityDeleteMetricsTestEntry("data.parquet", 0, nil, iceberg.EntryContentData, 0,
					nil, nil, nil, nil, lower, upper)
				for _, indexed := range []bool{false, true} {
					b.Run(fmt.Sprintf("entries=%d/lookups=%d/broad=%t/indexed=%t", count, lookups, broad, indexed), func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							var rangeIndex *equalityDeleteRangeIndex
							if indexed {
								rangeIndex = newEqualityDeleteRangeIndex(entries)
							}
							for range lookups {
								stats := equalityDeleteDataFileStatsFor(data.DataFile())
								files := appendEqualityDeletesAfterIndexed(nil, entries, 0, &stats, rangeIndex)
								equalityDeleteBenchmarkSink = len(files)
							}
							want := 1
							if broad {
								want = count
							}
							if equalityDeleteBenchmarkSink != want {
								b.Fatalf("expected %d deletes, got %d", want, equalityDeleteBenchmarkSink)
							}
						}
					})
				}
			}
		}
	}
}

var equalityDeleteRangeUnprobedSink *equalityDeleteRangeIndex

func BenchmarkEqualityDeleteRangeIndexUnprobed(b *testing.B) {
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	for _, count := range []int{256, 512, 4096} {
		manifestEntries := equalityDeleteRangeTestBucket(b, count, 0, nil)
		entries := make([]equalityDeleteIndexEntry, count)
		for i, entry := range manifestEntries {
			entries[i] = newEqualityDeleteIndexEntry(entry, schema)
		}
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				equalityDeleteRangeUnprobedSink = newEqualityDeleteRangeIndex(entries)
			}
		})
	}
}
