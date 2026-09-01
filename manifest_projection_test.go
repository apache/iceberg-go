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

package iceberg

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestEntryProjectionDropsTransientStats(t *testing.T) {
	schema := NewSchema(1, NestedField{
		ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true,
	})
	builder, err := NewDataFileBuilder(
		*UnpartitionedSpec,
		EntryContentData,
		"data.parquet",
		ParquetFile,
		nil,
		nil,
		nil,
		3,
		128,
	)
	require.NoError(t, err)
	builder.
		ColumnSizes(map[int]int64{1: 10}).
		ValueCounts(map[int]int64{1: 3}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}}).
		UpperBoundValues(map[int][]byte{1: {0x03}}).
		KeyMetadata([]byte{0x04}).
		SplitOffsets([]int64{4, 64}).
		SortOrderID(7).
		FirstRowID(10).
		ReferencedDataFile("data.parquet").
		ContentOffset(12).
		ContentSizeInBytes(20)

	snapshotID := int64(5)
	sequenceNumber := int64(6)
	fileSequenceNumber := int64(7)
	entry := NewManifestEntry(
		EntryStatusADDED,
		&snapshotID,
		&sequenceNumber,
		&fileSequenceNumber,
		builder.Build(),
	)
	var manifestBytes bytes.Buffer
	manifest, err := WriteManifest(
		"manifest.avro", &manifestBytes, 3, *UnpartitionedSpec, schema, snapshotID,
		[]ManifestEntry{entry},
	)
	require.NoError(t, err)

	reader, err := NewManifestReaderWithProjection(
		manifest, bytes.NewReader(manifestBytes.Bytes()), ManifestEntryProjection{},
	)
	require.NoError(t, err)
	projected, err := reader.ReadEntry()
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	assert.Equal(t, entry.Status(), projected.Status())
	assert.Equal(t, entry.SnapshotID(), projected.SnapshotID())
	assert.Equal(t, entry.SequenceNum(), projected.SequenceNum())
	assert.Equal(t, entry.FileSequenceNum(), projected.FileSequenceNum())
	assert.Equal(t, "data.parquet", projected.DataFile().FilePath())
	assert.Equal(t, int64(3), projected.DataFile().Count())
	assert.Equal(t, int64(128), projected.DataFile().FileSizeBytes())
	assert.Equal(t, []int64{4, 64}, projected.DataFile().SplitOffsets())
	assert.Equal(t, 7, *projected.DataFile().SortOrderID())
	assert.Equal(t, int64(10), *projected.DataFile().FirstRowID())
	assert.Equal(t, "data.parquet", *projected.DataFile().ReferencedDataFile())
	assert.Equal(t, int64(12), *projected.DataFile().ContentOffset())
	assert.Equal(t, int64(20), *projected.DataFile().ContentSizeInBytes())
	assert.Equal(t, []byte{0x04}, projected.DataFile().KeyMetadata())
	assert.Empty(t, projected.DataFile().ColumnSizes())
	assert.Empty(t, projected.DataFile().ValueCounts())
	assert.Empty(t, projected.DataFile().NullValueCounts())
	assert.Empty(t, projected.DataFile().NaNValueCounts())
	assert.Empty(t, projected.DataFile().LowerBoundValues())
	assert.Empty(t, projected.DataFile().UpperBoundValues())

	statsReader, err := NewManifestReaderWithProjection(
		manifest, bytes.NewReader(manifestBytes.Bytes()), ManifestEntryProjection{IncludeColumnStats: true},
	)
	require.NoError(t, err)
	withStats, err := statsReader.ReadEntry()
	require.NoError(t, err)
	require.NoError(t, statsReader.Close())
	assert.Equal(t, map[int]int64{1: 3}, withStats.DataFile().ValueCounts())
	assert.Equal(t, map[int][]byte{1: {0x01}}, withStats.DataFile().LowerBoundValues())
	assert.Equal(t, map[int][]byte{1: {0x03}}, withStats.DataFile().UpperBoundValues())
	assert.Empty(t, withStats.DataFile().ColumnSizes())
}

func TestManifestEntryProjectionSupportsManifestVersionsAndDeletes(t *testing.T) {
	tests := []struct {
		name            string
		version         int
		manifestContent ManifestContent
		entryContent    ManifestEntryContent
		format          FileFormat
	}{
		{name: "v1 data", version: 1, manifestContent: ManifestContentData, entryContent: EntryContentData, format: ParquetFile},
		{name: "v2 data", version: 2, manifestContent: ManifestContentData, entryContent: EntryContentData, format: ParquetFile},
		{name: "v3 data", version: 3, manifestContent: ManifestContentData, entryContent: EntryContentData, format: ParquetFile},
		{name: "v2 equality delete", version: 2, manifestContent: ManifestContentDeletes, entryContent: EntryContentEqDeletes, format: ParquetFile},
		{name: "v3 deletion vector", version: 3, manifestContent: ManifestContentDeletes, entryContent: EntryContentPosDeletes, format: PuffinFile},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := NewSchema(1, NestedField{
				ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true,
			})
			builder, err := NewDataFileBuilder(
				*UnpartitionedSpec,
				tt.entryContent,
				tt.name+".data",
				tt.format,
				nil,
				nil,
				nil,
				1,
				128,
			)
			require.NoError(t, err)
			builder.
				ValueCounts(map[int]int64{1: 1}).
				NullValueCounts(map[int]int64{1: 0}).
				NaNValueCounts(map[int]int64{1: 0}).
				LowerBoundValues(map[int][]byte{1: {0x01}}).
				UpperBoundValues(map[int][]byte{1: {0x01}})
			if tt.entryContent == EntryContentEqDeletes {
				builder.EqualityFieldIDs([]int{1})
			}
			if tt.entryContent == EntryContentPosDeletes && tt.format == PuffinFile {
				builder.ReferencedDataFile("data.parquet").ContentOffset(4).ContentSizeInBytes(8)
			}

			snapshotID := int64(5)
			sequenceNumber := int64(6)
			entry := NewManifestEntry(
				EntryStatusADDED,
				&snapshotID,
				&sequenceNumber,
				nil,
				builder.Build(),
			)
			var manifestBytes bytes.Buffer
			writer, err := NewManifestWriter(
				tt.version,
				&manifestBytes,
				*UnpartitionedSpec,
				schema,
				snapshotID,
				WithManifestWriterContent(tt.manifestContent),
			)
			require.NoError(t, err)
			require.NoError(t, writer.Add(entry))
			manifest, err := writer.ToManifestFile(
				tt.name+".manifest.avro", int64(manifestBytes.Len()))
			require.NoError(t, err)

			reader, err := NewManifestReaderWithProjection(
				manifest, bytes.NewReader(manifestBytes.Bytes()), ManifestEntryProjection{},
			)
			require.NoError(t, err)
			projected, err := reader.ReadEntry()
			require.NoError(t, err)
			require.NoError(t, reader.Close())

			assert.Equal(t, tt.entryContent, projected.DataFile().ContentType())
			assert.Equal(t, entry.DataFile().FilePath(), projected.DataFile().FilePath())
			assert.Empty(t, projected.DataFile().ValueCounts())
			assert.Empty(t, projected.DataFile().LowerBoundValues())
			if tt.entryContent == EntryContentPosDeletes && tt.format == PuffinFile {
				require.NotNil(t, projected.DataFile().ReferencedDataFile())
				assert.Equal(t, "data.parquet", *projected.DataFile().ReferencedDataFile())
				assert.Equal(t, int64(4), *projected.DataFile().ContentOffset())
				assert.Equal(t, int64(8), *projected.DataFile().ContentSizeInBytes())
			}
		})
	}
}

func TestManifestEntryWithoutColumnStatsPreservesEntry(t *testing.T) {
	builder, err := NewDataFileBuilder(
		*UnpartitionedSpec,
		EntryContentData,
		"data.parquet",
		ParquetFile,
		nil,
		nil,
		nil,
		1,
		10,
	)
	require.NoError(t, err)
	builder.ValueCounts(map[int]int64{1: 1})

	snapshotID := int64(3)
	entry := NewManifestEntry(EntryStatusEXISTING, &snapshotID, nil, nil, builder.Build())
	projected := ManifestEntryWithoutColumnStats(entry)

	assert.NotSame(t, entry, projected)
	assert.Equal(t, entry.Status(), projected.Status())
	assert.Equal(t, entry.SnapshotID(), projected.SnapshotID())
	assert.Equal(t, entry.DataFile().FilePath(), projected.DataFile().FilePath())
	assert.Empty(t, projected.DataFile().ValueCounts())
	assert.Equal(t, map[int]int64{1: 1}, entry.DataFile().ValueCounts())
}

func TestDataFileWithoutColumnStatsConcurrentPartitionInitialization(t *testing.T) {
	for range 32 {
		file := &dataFile{
			PartitionData: map[string]any{"partition": int32(7)},
			fieldNameToID: map[string]int{"partition": 1000},
		}
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() {
				<-start
				assert.Equal(t, map[int]any{1000: int32(7)}, file.Partition())
			})
			workers.Go(func() {
				<-start
				projected := DataFileWithoutColumnStats(file)
				assert.Equal(t, map[int]any{1000: int32(7)}, projected.Partition())
			})
		}
		close(start)
		workers.Wait()
	}
}
