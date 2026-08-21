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
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func writePositionDeleteManifestList(
	t *testing.T,
	fs iceio.WriteFileIO,
	path string,
	snapshotID int64,
	parentSnapshotID *int64,
	sequenceNumber int64,
	manifests []iceberg.ManifestFile,
) {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(
		2, &buf, snapshotID, parentSnapshotID, &sequenceNumber, 0, manifests))
	require.NoError(t, fs.WriteFile(path, buf.Bytes()))
}

func TestInspectPositionDeletesV3MixesParquetAndDeletionVector(t *testing.T) {
	memFS := iceio.NewMemFS()
	parquetDeletePath := "mem://position-deletes/table/data/delete.parquet"
	parquetDataPath := "mem://position-deletes/table/data/parquet-data.parquet"
	dvDataPath := "mem://position-deletes/table/data/dv-data.parquet"
	dvPath := "mem://position-deletes/table/data/deletes.puffin"

	writePosDeleteParquetToMemFS(t, memFS, parquetDeletePath, `[
		{"file_path": "`+parquetDataPath+`", "pos": 2},
		{"file_path": "`+parquetDataPath+`", "pos": 4}
	]`)
	parquetFile := newPosDeleteFile(t, parquetDeletePath, 2, 128)

	dvWriter := dv.NewDVWriter(memFS, func(id int32) *iceberg.PartitionSpec {
		if id == 0 {
			return iceberg.UnpartitionedSpec
		}

		return nil
	})
	require.NoError(t, dvWriter.Add(dvDataPath, []int64{1, 3}, 0, nil))
	dvFiles, err := dvWriter.Flush(context.Background(), dvPath)
	require.NoError(t, err)
	require.Len(t, dvFiles, 1)

	tbl := inspectPositionDeletesTable(
		t, 3, newInspectPositionDeletesMetadata(t, 3), memFS,
		[]iceberg.DataFile{parquetFile, dvFiles[0]},
	)
	rr, err := tbl.Inspect().PositionDeletes(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 4, record.NumRows())
	filePaths := record.Column(0).(*array.String)
	require.Equal(t, parquetDataPath, filePaths.Value(0))
	require.Equal(t, parquetDataPath, filePaths.Value(1))
	require.Equal(t, dvDataPath, filePaths.Value(2))
	require.Equal(t, dvDataPath, filePaths.Value(3))
	require.Equal(t, []int64{2, 4, 1, 3}, record.Column(1).(*array.Int64).Int64Values())
	require.EqualValues(t, 4, record.Column(2).NullN())

	deleteFilePaths := record.Column(4).(*array.String)
	require.Equal(t, parquetDeletePath, deleteFilePaths.Value(0))
	require.Equal(t, parquetDeletePath, deleteFilePaths.Value(1))
	require.Equal(t, dvPath, deleteFilePaths.Value(2))
	require.Equal(t, dvPath, deleteFilePaths.Value(3))

	offsets := record.Column(5).(*array.Int64)
	require.True(t, offsets.IsNull(0))
	require.True(t, offsets.IsNull(1))
	require.Equal(t, *dvFiles[0].ContentOffset(), offsets.Value(2))
	require.Equal(t, *dvFiles[0].ContentOffset(), offsets.Value(3))
	sizes := record.Column(6).(*array.Int64)
	require.True(t, sizes.IsNull(0))
	require.True(t, sizes.IsNull(1))
	require.Equal(t, *dvFiles[0].ContentSizeInBytes(), sizes.Value(2))
	require.Equal(t, *dvFiles[0].ContentSizeInBytes(), sizes.Value(3))
}

func TestInspectPositionDeletesUsesLiveEntriesFromCurrentSnapshot(t *testing.T) {
	memFS := iceio.NewMemFS()
	tableSchema := simpleSchema()
	oldSnapshotID := int64(1)
	currentSnapshotID := int64(2)
	oldSequenceNumber := int64(1)
	currentSequenceNumber := int64(2)
	oldDeletePath := "mem://position-deletes/table/data/expired.parquet"
	currentDeletePath := "mem://position-deletes/table/data/current.parquet"
	currentDataPath := "mem://position-deletes/table/data/current-data.parquet"

	writePosDeleteParquetToMemFS(t, memFS, currentDeletePath, `[
		{"file_path": "`+currentDataPath+`", "pos": 7}
	]`)
	oldDeleteFile := newPosDeleteFile(t, oldDeletePath, 1, 128)
	currentDeleteFile := newPosDeleteFile(t, currentDeletePath, 1, 128)
	deletedDeleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/removed.parquet", 1, 128)
	equalityDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"mem://position-deletes/table/data/equality.parquet", iceberg.ParquetFile,
		nil, nil, nil, 1, 128)
	require.NoError(t, err)
	equalityDeleteFile := equalityDeleteBuilder.EqualityFieldIDs([]int{1}).Build()

	entry := func(status iceberg.ManifestEntryStatus, snapshotID, sequenceNumber int64, file iceberg.DataFile) iceberg.ManifestEntry {
		return iceberg.NewManifestEntry(status, &snapshotID, &sequenceNumber, &sequenceNumber, file)
	}
	oldManifest := writeInspectManifest(
		t, memFS, "mem://position-deletes/table/metadata/expired.avro",
		*iceberg.UnpartitionedSpec, tableSchema, oldSnapshotID, iceberg.ManifestContentDeletes,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, oldSnapshotID, oldSequenceNumber, oldDeleteFile)},
	)
	currentDeleteManifest := writeInspectManifest(
		t, memFS, "mem://position-deletes/table/metadata/current-deletes.avro",
		*iceberg.UnpartitionedSpec, tableSchema, currentSnapshotID, iceberg.ManifestContentDeletes,
		[]iceberg.ManifestEntry{
			entry(iceberg.EntryStatusADDED, currentSnapshotID, currentSequenceNumber, currentDeleteFile),
			entry(iceberg.EntryStatusDELETED, currentSnapshotID, currentSequenceNumber, deletedDeleteFile),
			entry(iceberg.EntryStatusADDED, currentSnapshotID, currentSequenceNumber, equalityDeleteFile),
		},
	)
	dataFile := newTestDataFile(t, *iceberg.UnpartitionedSpec,
		"mem://position-deletes/table/data/data-file.parquet", nil)
	dataManifest := writeInspectManifest(
		t, memFS, "mem://position-deletes/table/metadata/data.avro",
		*iceberg.UnpartitionedSpec, tableSchema, currentSnapshotID, iceberg.ManifestContentData,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, currentSnapshotID, currentSequenceNumber, dataFile)},
	)

	oldManifestListPath := "mem://position-deletes/table/metadata/snap-1.avro"
	currentManifestListPath := "mem://position-deletes/table/metadata/snap-2.avro"
	writePositionDeleteManifestList(
		t, memFS, oldManifestListPath, oldSnapshotID, nil, oldSequenceNumber,
		[]iceberg.ManifestFile{oldManifest})
	writePositionDeleteManifestList(
		t, memFS, currentManifestListPath, currentSnapshotID, &oldSnapshotID, currentSequenceNumber,
		[]iceberg.ManifestFile{currentDeleteManifest, dataManifest})

	mb := newInspectPositionDeletesMetadataWithSchema(t, 2, tableSchema)
	schemaID := tableSchema.ID
	timestamp := time.Now().UnixMilli()
	require.NoError(t, mb.AddSnapshot(&Snapshot{
		SnapshotID: oldSnapshotID, SequenceNumber: oldSequenceNumber,
		TimestampMs: timestamp, ManifestList: oldManifestListPath, SchemaID: &schemaID,
	}))
	require.NoError(t, mb.AddSnapshot(&Snapshot{
		SnapshotID: currentSnapshotID, ParentSnapshotID: &oldSnapshotID,
		SequenceNumber: currentSequenceNumber, TimestampMs: timestamp + 1,
		ManifestList: currentManifestListPath, SchemaID: &schemaID,
	}))
	require.NoError(t, mb.SetSnapshotRef(MainBranch, currentSnapshotID, BranchRef))
	metadata, err := mb.Build()
	require.NoError(t, err)
	tbl := New(
		Identifier{"db", "position_deletes"}, metadata, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memFS, nil }, nil,
	)

	rr, err := tbl.Inspect().PositionDeletes(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 1, record.NumRows())
	require.Equal(t, currentDataPath, record.Column(0).(*array.String).Value(0))
	require.EqualValues(t, 7, record.Column(1).(*array.Int64).Value(0))
}

func TestPositionDeletesPartitionTypeAvoidsHistoricalSchemaFieldIDs(t *testing.T) {
	currentSchema := simpleSchema()
	historicalSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "historical", Type: iceberg.PrimitiveTypes.String, Required: true,
	})
	spec := partitionedSpec()
	lastPartitionID := 1000
	metadata := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:   2,
		UUID:            uuid.New(),
		LastColumnId:    2,
		SchemaList:      []*iceberg.Schema{currentSchema, historicalSchema},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{spec},
		DefaultSpecID:   0,
		LastPartitionID: &lastPartitionID,
	}}

	partitionType, partitionIDs, err := positionDeletesPartitionType(metadata)
	require.NoError(t, err)
	require.Equal(t, map[int]int{1000: 3}, partitionIDs)
	require.Equal(t, 3, partitionType.FieldList[0].ID)
}
