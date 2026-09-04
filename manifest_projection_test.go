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
	"reflect"
	"sync"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestEntryProjectionWhitelistCoversDataFileSchema(t *testing.T) {
	schema := NewSchema(1, NestedField{
		ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true,
	})
	dataFileType := reflect.TypeOf(dataFile{})
	dataFileFields := make(map[string]struct{}, len(dataFileAvroFieldIndexes))
	for _, index := range dataFileAvroFieldIndexes {
		name := dataFileType.Field(index).Tag.Get("avro")
		dataFileFields[name] = struct{}{}
	}
	optionalFields := map[string]struct{}{
		"column_sizes":      {},
		"distinct_counts":   {},
		"value_counts":      {},
		"null_value_counts": {},
		"nan_value_counts":  {},
		"lower_bounds":      {},
		"upper_bounds":      {},
	}
	statsFields := map[string]struct{}{
		"value_counts":      {},
		"null_value_counts": {},
		"nan_value_counts":  {},
		"lower_bounds":      {},
		"upper_bounds":      {},
	}

	for version := 1; version <= 3; version++ {
		writerSchema, _, err := manifestEntrySchemaFor(*UnpartitionedSpec, schema, version)
		require.NoError(t, err)

		var foundDataFile bool
		for _, field := range writerSchema.Root().Fields {
			if field.Name != "data_file" {
				continue
			}
			foundDataFile = true
			for _, dataField := range field.Type.Fields {
				_, tagged := dataFileFields[dataField.Name]
				assert.True(t, tagged, "v%d field %q is missing from dataFile", version, dataField.Name)
				if !manifestScanDataFileField(dataField.Name, false) {
					_, optional := optionalFields[dataField.Name]
					assert.True(t, optional, "v%d field %q is silently omitted by projection", version, dataField.Name)
				}
				if _, stats := statsFields[dataField.Name]; stats {
					assert.True(t, manifestScanDataFileField(dataField.Name, true),
						"v%d optional stats field %q is not retained when requested", version, dataField.Name)
				}
			}
		}
		assert.True(t, foundDataFile, "v%d manifest schema has no data_file field", version)
	}

	for name := range dataFileFields {
		if manifestScanDataFileField(name, false) {
			continue
		}
		_, optional := optionalFields[name]
		assert.True(t, optional, "dataFile field %q is not classified by projection", name)
	}
}

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
		manifest, bytes.NewReader(manifestBytes.Bytes()), ManifestEntryProjection{IncludePruningStats: true},
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
				BlockSizeInBytes(64 * 1024).
				ColumnSizes(map[int]int64{1: 64}).
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
			assert.Equal(t, tt.version == 1, reader.isFallback)
			projected, err := reader.ReadEntry()
			require.NoError(t, err)
			require.NoError(t, reader.Close())

			assert.Equal(t, tt.entryContent, projected.DataFile().ContentType())
			assert.Equal(t, entry.DataFile().FilePath(), projected.DataFile().FilePath())
			assert.Empty(t, projected.DataFile().ValueCounts())
			assert.Empty(t, projected.DataFile().LowerBoundValues())
			if tt.version == 1 {
				assert.Equal(t, int64(64*1024), projected.DataFile().(*dataFile).BlockSizeInBytes)
			}
			if tt.entryContent == EntryContentPosDeletes && tt.format == PuffinFile {
				require.NotNil(t, projected.DataFile().ReferencedDataFile())
				assert.Equal(t, "data.parquet", *projected.DataFile().ReferencedDataFile())
				assert.Equal(t, int64(4), *projected.DataFile().ContentOffset())
				assert.Equal(t, int64(8), *projected.DataFile().ContentSizeInBytes())
			}

			statsReader, err := NewManifestReaderWithProjection(
				manifest, bytes.NewReader(manifestBytes.Bytes()), ManifestEntryProjection{IncludePruningStats: true},
			)
			require.NoError(t, err)
			withStats, err := statsReader.ReadEntry()
			require.NoError(t, err)
			require.NoError(t, statsReader.Close())
			assert.Equal(t, map[int]int64{1: 1}, withStats.DataFile().ValueCounts())
			assert.Equal(t, map[int]int64{1: 0}, withStats.DataFile().NullValueCounts())
			assert.Equal(t, map[int]int64{1: 0}, withStats.DataFile().NaNValueCounts())
			assert.Equal(t, map[int][]byte{1: {0x01}}, withStats.DataFile().LowerBoundValues())
			assert.Equal(t, map[int][]byte{1: {0x01}}, withStats.DataFile().UpperBoundValues())
			assert.Empty(t, withStats.DataFile().ColumnSizes())

			fullReader, err := NewManifestReader(manifest, bytes.NewReader(manifestBytes.Bytes()))
			require.NoError(t, err)
			full, err := fullReader.ReadEntry()
			require.NoError(t, err)
			require.NoError(t, fullReader.Close())
			assert.Equal(t, map[int]int64{1: 64}, full.DataFile().ColumnSizes())

			fs := iceio.NewMemFS()
			require.NoError(t, fs.WriteFile(manifest.FilePath(), manifestBytes.Bytes()))
			projectedEntries := map[string]ManifestEntry{
				"reader":                    projected,
				"reader with pruning stats": withStats,
				"entry without stats":       ManifestEntryWithoutColumnStats(full),
				"data file without stats": NewManifestEntry(EntryStatusADDED, &snapshotID,
					&sequenceNumber, nil, DataFileWithoutColumnStats(full.DataFile())),
				"repeated projection": ManifestEntryWithoutColumnStats(projected),
			}
			for entry, err := range EntriesWithProjection(fs, manifest, false, ManifestEntryProjection{}) {
				require.NoError(t, err)
				projectedEntries["iterator"] = entry
			}
			require.Len(t, projectedEntries, 6)
			for name, projectedEntry := range projectedEntries {
				t.Run(name, func(t *testing.T) {
					for _, operation := range []string{"add", "existing", "delete"} {
						t.Run(operation, func(t *testing.T) {
							var output bytes.Buffer
							writer, err := NewManifestWriter(tt.version, &output, *UnpartitionedSpec,
								schema, snapshotID, WithManifestWriterContent(tt.manifestContent))
							require.NoError(t, err)
							writeEntry := map[string]func(ManifestEntry) error{
								"add": writer.Add, "existing": writer.Existing, "delete": writer.Delete,
							}[operation]
							err = writeEntry(projectedEntry)
							require.ErrorIs(t, err, ErrInvalidArgument)
							require.ErrorContains(t, err, "projected data file")

							// Rejection must leave the writer usable and its counts unchanged.
							require.NoError(t, writer.Add(full))
							rewritten, err := writer.ToManifestFile("rewritten.avro", int64(output.Len()))
							require.NoError(t, err)
							assert.EqualValues(t, 1, rewritten.AddedDataFiles())
							assert.Zero(t, rewritten.ExistingDataFiles())
							assert.Zero(t, rewritten.DeletedDataFiles())
							reader, err := NewManifestReader(rewritten, bytes.NewReader(output.Bytes()))
							require.NoError(t, err)
							roundTrip, err := reader.ReadEntry()
							require.NoError(t, err)
							require.NoError(t, reader.Close())
							assert.Equal(t, full.DataFile().ColumnSizes(), roundTrip.DataFile().ColumnSizes())
						})
					}
					if tt.manifestContent == ManifestContentData {
						var output bytes.Buffer
						_, err := WriteManifest("rewritten.avro", &output, tt.version,
							*UnpartitionedSpec, schema, snapshotID, []ManifestEntry{projectedEntry})
						require.ErrorIs(t, err, ErrInvalidArgument)
						if tt.version == 3 {
							_, _, err = WriteManifestV3("rewritten-v3.avro", &output, 0,
								*UnpartitionedSpec, schema, snapshotID, []ManifestEntry{projectedEntry})
							require.ErrorIs(t, err, ErrInvalidArgument)
						}
					}
					_, err := projectedEntry.DataFile().(AvroEntryMarshaler).MarshalAvroEntry(
						*UnpartitionedSpec, schema, tt.version)
					require.ErrorIs(t, err, ErrInvalidArgument)
				})
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

func TestDataFileWithoutColumnStatsDoesNotInitializeSourcePartition(t *testing.T) {
	file := &dataFile{
		PartitionData: map[string]any{"partition": int32(7)},
		fieldNameToID: map[string]int{"partition": 1000},
	}

	projected := DataFileWithoutColumnStats(file)

	assert.Nil(t, file.fieldIDToPartitionData)
	assert.Equal(t, map[int]any{1000: int32(7)}, projected.Partition())
}
