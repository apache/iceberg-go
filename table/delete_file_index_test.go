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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDeleteIndexStatsFile(
	t testing.TB,
	spec iceberg.PartitionSpec,
	content iceberg.ManifestEntryContent,
	format iceberg.FileFormat,
	path string,
	partition map[int]any,
	count, size int64,
	valueCounts, nullCounts, nanCounts map[int]int64,
	lowerBounds, upperBounds map[int][]byte,
	equalityFieldIDs []int,
) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec, content, path, format, partition, nil, nil, count, size)
	require.NoError(t, err)
	if valueCounts != nil {
		builder.ValueCounts(valueCounts)
	}
	if nullCounts != nil {
		builder.NullValueCounts(nullCounts)
	}
	if nanCounts != nil {
		builder.NaNValueCounts(nanCounts)
	}
	if lowerBounds != nil {
		builder.LowerBoundValues(lowerBounds)
	}
	if upperBounds != nil {
		builder.UpperBoundValues(upperBounds)
	}
	if equalityFieldIDs != nil {
		builder.EqualityFieldIDs(equalityFieldIDs)
	}

	return builder.Build()
}

func TestCompactDeleteFileForIndexRetainsSelectedStats(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "partition",
		Transform: iceberg.IdentityTransform{},
	})
	partition := map[int]any{1000: "us"}
	file := newDeleteIndexStatsFile(
		t,
		spec,
		iceberg.EntryContentPosDeletes,
		iceberg.ParquetFile,
		"delete.parquet",
		partition,
		11,
		1234,
		map[int]int64{filePathFieldID: 11, 1: 11, 2: 11},
		map[int]int64{filePathFieldID: 1, 1: 2, 2: 3},
		map[int]int64{filePathFieldID: 0, 1: 1, 2: 1},
		map[int][]byte{filePathFieldID: []byte("a"), 1: []byte("ignored"), 2: []byte("ignored")},
		map[int][]byte{filePathFieldID: []byte("z"), 1: []byte("ignored"), 2: []byte("ignored")},
		[]int{1, 2},
	)

	compacted, err := compactDeleteFileForIndex(file, partition, []int{filePathFieldID})
	require.NoError(t, err)
	assert.NotSame(t, file, compacted)

	assert.Equal(t, partition, compacted.Partition())
	assert.Equal(t, spec.ID(), int(compacted.SpecID()))
	assert.Equal(t, int64(11), compacted.Count())
	assert.Equal(t, int64(1234), compacted.FileSizeBytes())
	assert.Equal(t, map[int]int64{filePathFieldID: 11}, compacted.ValueCounts())
	assert.Equal(t, map[int]int64{filePathFieldID: 1}, compacted.NullValueCounts())
	assert.Equal(t, map[int]int64{filePathFieldID: 0}, compacted.NaNValueCounts())
	assert.Equal(t, map[int][]byte{filePathFieldID: []byte("a")}, compacted.LowerBoundValues())
	assert.Equal(t, map[int][]byte{filePathFieldID: []byte("z")}, compacted.UpperBoundValues())
	assert.Nil(t, compacted.ColumnSizes())
	assert.Nil(t, compacted.DistinctValueCounts())
	assert.Equal(t, []int{1, 2}, compacted.EqualityFieldIDs())

	compacted, err = compactDeleteFileForIndex(file, partition, []int{1, 2})
	require.NoError(t, err)
	assert.Equal(t, map[int]int64{1: 11, 2: 11}, compacted.ValueCounts())
	assert.Equal(t, map[int]int64{1: 2, 2: 3}, compacted.NullValueCounts())
	assert.Equal(t, map[int]int64{1: 1, 2: 1}, compacted.NaNValueCounts())
	assert.Equal(t, map[int][]byte{
		1: []byte("ignored"), 2: []byte("ignored"),
	}, compacted.LowerBoundValues())
	assert.Equal(t, map[int][]byte{
		1: []byte("ignored"), 2: []byte("ignored"),
	}, compacted.UpperBoundValues())
}

func TestCompactDeleteFileForIndexPreservesReadMetadata(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: iceberg.IdentityTransform{},
	})
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentPosDeletes,
		"delete.puffin",
		iceberg.PuffinFile,
		map[int]any{1000: "us"},
		nil,
		nil,
		11,
		1234,
	)
	require.NoError(t, err)
	builder.ColumnSizes(map[int]int64{1: 22})
	builder.DistinctValueCounts(map[int]int64{1: 2}) //nolint:staticcheck // SA1019: deliberate use of deprecated setter to verify it is not retained by compacted index files
	builder.KeyMetadata([]byte("key"))
	builder.SplitOffsets([]int64{4, 8})
	builder.EqualityFieldIDs([]int{1, 2})
	builder.SortOrderID(3)
	builder.FirstRowID(10)
	builder.ReferencedDataFile("data.parquet")
	builder.ContentOffset(20)
	builder.ContentSizeInBytes(30)

	compacted, err := compactDeleteFileForIndex(
		builder.Build(), map[int]any{1000: "us"}, nil)
	require.NoError(t, err)

	assert.Equal(t, iceberg.EntryContentPosDeletes, compacted.ContentType())
	assert.Equal(t, "delete.puffin", compacted.FilePath())
	assert.Equal(t, iceberg.PuffinFile, compacted.FileFormat())
	assert.Equal(t, int64(11), compacted.Count())
	assert.Equal(t, int64(1234), compacted.FileSizeBytes())
	assert.Equal(t, []byte("key"), compacted.KeyMetadata())
	assert.Equal(t, []int64{4, 8}, compacted.SplitOffsets())
	assert.Equal(t, []int{1, 2}, compacted.EqualityFieldIDs())
	assert.Equal(t, 3, *compacted.SortOrderID())
	assert.Equal(t, int64(10), *compacted.FirstRowID())
	assert.Equal(t, "data.parquet", *compacted.ReferencedDataFile())
	assert.Equal(t, int64(20), *compacted.ContentOffset())
	assert.Equal(t, int64(30), *compacted.ContentSizeInBytes())
	assert.Nil(t, compacted.ColumnSizes())
	assert.Nil(t, compacted.DistinctValueCounts())
}

func TestCompactDeleteFileForIndexKeepsExternalFallback(t *testing.T) {
	file := &mockDataFile{
		path:        "delete.parquet",
		contentType: iceberg.EntryContentPosDeletes,
		partition:   map[int]any{1000: "us"},
		count:       1,
		filesize:    1,
		valueCounts: map[int]int64{1: 1},
	}

	compacted, err := compactDeleteFileForIndex(file, file.partition, []int{1})
	require.NoError(t, err)
	assert.Same(t, file, compacted)
}

func TestDeleteIndexesRetainOnlyRequiredStats(t *testing.T) {
	partitionedSpec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: iceberg.IdentityTransform{},
	})
	partition := map[int]any{1000: "us"}
	pathLower, pathUpper := []byte("data-a.parquet"), []byte("data-z.parquet")
	positionDelete := newDeleteIndexStatsFile(
		t,
		partitionedSpec,
		iceberg.EntryContentPosDeletes,
		iceberg.ParquetFile,
		"position-delete.parquet",
		partition,
		1,
		100,
		map[int]int64{filePathFieldID: 1, 1: 1, 2: 1},
		map[int]int64{filePathFieldID: 0, 1: 0, 2: 0},
		map[int]int64{filePathFieldID: 0, 1: 0, 2: 0},
		map[int][]byte{filePathFieldID: pathLower, 1: []byte("unused")},
		map[int][]byte{filePathFieldID: pathUpper, 1: []byte("unused")},
		nil,
	)
	positionEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, int64Ptr(2), nil, positionDelete)
	positionIndex, err := buildPositionalDeleteIndex([]iceberg.ManifestEntry{positionEntry})
	require.NoError(t, err)
	dataEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, int64Ptr(1), nil,
		newDeleteIndexStatsFile(t, partitionedSpec, iceberg.EntryContentData, iceberg.ParquetFile,
			"data-m.parquet", partition, 1, 100, nil, nil, nil, nil, nil, nil))
	matched, err := positionIndex.forDataFile(dataEntry)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, map[int]int64{filePathFieldID: 1}, matched[0].ValueCounts())
	assert.Nil(t, matched[0].ColumnSizes())

	path := "data-m.parquet"
	pathBuilder, err := iceberg.NewDataFileBuilder(
		partitionedSpec,
		iceberg.EntryContentPosDeletes,
		"path-delete.parquet",
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		1,
		100,
	)
	require.NoError(t, err)
	pathBuilder.ValueCounts(map[int]int64{1: 1})
	pathBuilder.LowerBoundValues(map[int][]byte{1: []byte("unused")})
	pathBuilder.ReferencedDataFile(path)
	pathIndex, err := buildPositionalDeleteIndex([]iceberg.ManifestEntry{
		iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, nil, int64Ptr(2), nil, pathBuilder.Build()),
	})
	require.NoError(t, err)
	matched, err = pathIndex.forDataFile(dataEntry)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, path, *matched[0].ReferencedDataFile())
	assert.Nil(t, matched[0].ValueCounts())
	assert.Nil(t, matched[0].LowerBoundValues())

	equalitySpec := equalityDeleteIndexTestSpecs()[1]
	equalityDelete := newDeleteIndexStatsFile(
		t,
		equalitySpec,
		iceberg.EntryContentEqDeletes,
		iceberg.ParquetFile,
		"equality-delete.parquet",
		partition,
		1,
		100,
		map[int]int64{1: 1, 2: 1, 3: 1},
		map[int]int64{1: 0, 2: 0, 3: 0},
		map[int]int64{1: 0, 2: 0, 3: 0},
		map[int][]byte{1: []byte("a"), 2: []byte("b"), 3: []byte("c")},
		map[int][]byte{1: []byte("z"), 2: []byte("y"), 3: []byte("x")},
		[]int{1, 2},
	)
	equalityEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, int64Ptr(2), nil, equalityDelete)
	equalityIndex, err := buildEqualityDeleteIndex(
		[]iceberg.ManifestEntry{equalityEntry}, equalityDeleteIndexTestSpecs())
	require.NoError(t, err)
	equalityDataEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, int64Ptr(1), nil,
		newDeleteIndexStatsFile(t, equalitySpec, iceberg.EntryContentData, iceberg.ParquetFile,
			"data.parquet", partition, 1, 100, nil, nil, nil, nil, nil, nil))
	matched, err = equalityIndex.forDataFile(equalityDataEntry)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, map[int]int64{1: 1, 2: 1}, matched[0].ValueCounts())
	assert.NotContains(t, matched[0].ValueCounts(), 3)

	dvBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes,
		"deletion-vector.puffin",
		iceberg.PuffinFile,
		nil,
		nil,
		nil,
		1,
		100,
	)
	require.NoError(t, err)
	dvBuilder.ValueCounts(map[int]int64{1: 1})
	dvBuilder.ReferencedDataFile("data.parquet")
	dvBuilder.ContentOffset(10)
	dvBuilder.ContentSizeInBytes(20)
	dvEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, int64Ptr(2), nil, dvBuilder.Build())
	dvIndex, err := buildDVIndex([]iceberg.ManifestEntry{dvEntry})
	require.NoError(t, err)
	dvDataEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, int64Ptr(1), nil,
		newDeleteIndexStatsFile(t, *iceberg.UnpartitionedSpec, iceberg.EntryContentData,
			iceberg.ParquetFile, "data.parquet", nil, 1, 100, nil, nil, nil, nil, nil, nil))
	matched = matchDVToData(dvDataEntry, dvIndex)
	require.Len(t, matched, 1)
	assert.Nil(t, matched[0].ValueCounts())
	assert.Equal(t, "data.parquet", *matched[0].ReferencedDataFile())
	assert.Equal(t, int64(10), *matched[0].ContentOffset())
	assert.Equal(t, int64(20), *matched[0].ContentSizeInBytes())
}
