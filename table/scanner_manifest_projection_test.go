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
	"bytes"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenManifestWithProjectionDropsStatsAfterFiltering(t *testing.T) {
	spec := partitionedSpec()
	schema := simpleSchema()
	snapshotID := int64(1)
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"mem://default/table/data/file.parquet",
		iceberg.ParquetFile,
		map[int]any{1000: int32(7)},
		nil,
		nil,
		1,
		100,
	)
	require.NoError(t, err)
	builder.
		ValueCounts(map[int]int64{1: 1}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}}).
		UpperBoundValues(map[int][]byte{1: {0x01}})

	manifestPath := "mem://default/table/metadata/manifest.avro"
	var manifestBytes bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath,
		&manifestBytes,
		2,
		spec,
		schema,
		snapshotID,
		[]iceberg.ManifestEntry{iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, builder.Build(),
		)},
	)
	require.NoError(t, err)

	fs := iceio.NewMemFS()
	require.NoError(t, fs.WriteFile(manifestPath, manifestBytes.Bytes()))

	projection := iceberg.ManifestEntryProjection{IncludeColumnStats: true}
	entries, err := openManifestWithProjection(
		fs,
		manifest,
		func(file iceberg.DataFile) (bool, error) {
			assert.Equal(t, int32(7), file.Partition()[1000])

			return true, nil
		},
		func(file iceberg.DataFile) (bool, error) {
			assert.Equal(t, map[int]int64{1: 1}, file.ValueCounts())
			assert.Equal(t, map[int][]byte{1: {0x01}}, file.LowerBoundValues())

			return true, nil
		},
		&projection,
		true,
	)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "mem://default/table/data/file.parquet", entries[0].DataFile().FilePath())
	assert.Empty(t, entries[0].DataFile().ValueCounts())
	assert.Empty(t, entries[0].DataFile().LowerBoundValues())
}

func TestManifestProjectionRetainsDataFileStatsForDeleteScans(t *testing.T) {
	scan := &Scan{rowFilter: iceberg.AlwaysTrue{}}
	dataManifest := iceberg.NewManifestFile(
		2, "data-manifest.avro", 100, 0, 1,
	).Build()
	deleteManifest := iceberg.NewManifestFile(
		2, "delete-manifest.avro", 100, 0, 1,
	).Content(iceberg.ManifestContentDeletes).Build()

	projection, dropStats := scan.manifestProjectionForManifest(dataManifest, false)
	assert.False(t, projection.IncludeColumnStats)
	assert.False(t, dropStats)

	retainDataStats := scan.manifestProjectionRetainsDataStats(
		[]iceberg.ManifestFile{dataManifest, deleteManifest})
	assert.True(t, retainDataStats)

	projection, dropStats = scan.manifestProjectionForManifest(dataManifest, retainDataStats)
	assert.True(t, projection.IncludeColumnStats)
	assert.False(t, dropStats,
		"data-file stats must survive planning for equality-delete pruning")

	projection, dropStats = scan.manifestProjectionForManifest(deleteManifest, retainDataStats)
	assert.True(t, projection.IncludeColumnStats)
	assert.False(t, dropStats)

	filteredScan := &Scan{
		rowFilter: iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
	}
	projection, dropStats = filteredScan.manifestProjectionForManifest(dataManifest, false)
	assert.True(t, projection.IncludeColumnStats)
	assert.True(t, dropStats,
		"row-filter stats should be dropped after filtering when no delete scan needs them")
}

func TestDataFilesWithoutColumnStatsReusesSharedFiles(t *testing.T) {
	dataFile, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentEqDeletes,
		"mem://default/table/delete.parquet",
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		128,
	)
	require.NoError(t, err)
	dataFile.ValueCounts(map[int]int64{1: 1})
	file := dataFile.Build()
	memo := make(map[iceberg.DataFile]iceberg.DataFile)

	projected := dataFilesWithoutColumnStats([]iceberg.DataFile{file, file}, memo)

	require.Len(t, projected, 2)
	assert.NotSame(t, file, projected[0])
	assert.Same(t, projected[0], projected[1])
	assert.Empty(t, projected[0].ValueCounts())
}

func TestPlanDataManifestTasksWithProjectionRetainsEqualityDeletePruning(t *testing.T) {
	fs := iceio.NewMemFS()
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://projection-planning", nil)
	require.NoError(t, err)

	bound := func(value int64) []byte {
		encoded, err := iceberg.Int64Literal(value).MarshalBinary()
		require.NoError(t, err)

		return encoded
	}
	dataEntry := func(path string, lower, upper int64) iceberg.ManifestEntry {
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec,
			iceberg.EntryContentData,
			path,
			iceberg.ParquetFile,
			nil,
			nil,
			nil,
			2,
			128,
		)
		require.NoError(t, err)
		builder.
			ValueCounts(map[int]int64{1: 2}).
			NullValueCounts(map[int]int64{1: 0}).
			NaNValueCounts(map[int]int64{1: 0}).
			LowerBoundValues(map[int][]byte{1: bound(lower)}).
			UpperBoundValues(map[int][]byte{1: bound(upper)})
		sequenceNumber := int64(1)

		return iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, nil, &sequenceNumber, nil, builder.Build())
	}

	dataEntries := []iceberg.ManifestEntry{
		dataEntry("mem://projection-planning/data-1.parquet", 1, 2),
		dataEntry("mem://projection-planning/data-2.parquet", 3, 4),
	}
	manifestPath := "mem://projection-planning/data-manifest.avro"
	var manifestBytes bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath,
		&manifestBytes,
		2,
		*iceberg.UnpartitionedSpec,
		schema,
		1,
		dataEntries,
	)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(manifestPath, manifestBytes.Bytes()))

	deleteEntry := func(path string, value int64) iceberg.ManifestEntry {
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec,
			iceberg.EntryContentEqDeletes,
			path,
			iceberg.ParquetFile,
			nil,
			nil,
			nil,
			1,
			128,
		)
		require.NoError(t, err)
		builder.
			EqualityFieldIDs([]int{1}).
			ValueCounts(map[int]int64{1: 1}).
			NullValueCounts(map[int]int64{1: 0}).
			NaNValueCounts(map[int]int64{1: 0}).
			LowerBoundValues(map[int][]byte{1: bound(value)}).
			UpperBoundValues(map[int][]byte{1: bound(value)})
		sequenceNumber := int64(2)

		return iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, nil, &sequenceNumber, nil, builder.Build())
	}
	deleteEntries := []iceberg.ManifestEntry{
		deleteEntry("mem://projection-planning/delete-2.parquet", 2),
		deleteEntry("mem://projection-planning/delete-3.parquet", 3),
	}
	eqDeleteIndex, err := buildEqualityDeleteIndex(deleteEntries, meta, schema)
	require.NoError(t, err)
	posDeleteIndex, err := buildPositionalDeleteIndex(nil)
	require.NoError(t, err)
	dvIndex, err := buildDVIndex(nil)
	require.NoError(t, err)

	scan := &Scan{
		metadata:      meta,
		ioF:           testFSF(fs),
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
		concurrency:   2,
	}
	tasks, err := scan.planDataManifestTasksWithOptions(
		t.Context(),
		[]iceberg.ManifestFile{manifest},
		schema,
		0,
		posDeleteIndex,
		dvIndex,
		eqDeleteIndex,
		true,
		true,
	)
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	for _, task := range tasks {
		require.Len(t, task.EqualityDeleteFiles, 1)
		assert.Empty(t, task.File.ValueCounts())
		assert.Empty(t, task.EqualityDeleteFiles[0].ValueCounts())
	}
	assert.Equal(t, "mem://projection-planning/delete-2.parquet", tasks[0].EqualityDeleteFiles[0].FilePath())
	assert.Equal(t, "mem://projection-planning/delete-3.parquet", tasks[1].EqualityDeleteFiles[0].FilePath())

	posDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes,
		"mem://projection-planning/position-delete.parquet",
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		128,
	)
	require.NoError(t, err)
	posDeleteBuilder.
		ReferencedDataFile("mem://projection-planning/data-1.parquet").
		ValueCounts(map[int]int64{1: 1}).
		LowerBoundValues(map[int][]byte{1: bound(1)}).
		UpperBoundValues(map[int][]byte{1: bound(1)})
	posSequenceNumber := int64(2)
	posDeleteEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, &posSequenceNumber, nil, posDeleteBuilder.Build())
	posDeleteIndex, err = buildPositionalDeleteIndex([]iceberg.ManifestEntry{posDeleteEntry})
	require.NoError(t, err)
	emptyEqDeleteIndex, err := buildEqualityDeleteIndex(nil, meta, schema)
	require.NoError(t, err)
	tasks, err = scan.planDataManifestTasksWithOptions(
		t.Context(),
		[]iceberg.ManifestFile{manifest},
		schema,
		0,
		posDeleteIndex,
		dvIndex,
		emptyEqDeleteIndex,
		true,
		false,
	)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Len(t, tasks[0].DeleteFiles, 1)
	assert.Empty(t, tasks[0].File.ValueCounts())
	assert.Empty(t, tasks[0].DeleteFiles[0].ValueCounts())
}
