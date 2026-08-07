// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

package table

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestIncrementalAppendScanSnapshotBoundaries(t *testing.T) {
	scan := snapshotsTestTable().NewIncrementalAppendScan()
	inclusive, err := scan.FromSnapshotInclusive(101)
	require.NoError(t, err)
	inclusive, err = inclusive.ToSnapshot(102)
	require.NoError(t, err)

	snapshots, err := inclusive.snapshotsBetween(102)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 101, snapshots[0].SnapshotID)

	exclusive := scan.FromSnapshotExclusive(101)
	exclusive, err = exclusive.ToSnapshot(102)
	require.NoError(t, err)
	snapshots, err = exclusive.snapshotsBetween(102)
	require.NoError(t, err)
	require.Empty(t, snapshots, "the only snapshot after 101 is not an append")
}

func TestIncrementalAppendScanRejectsUnknownStart(t *testing.T) {
	_, err := snapshotsTestTable().NewIncrementalAppendScan().FromSnapshotInclusive(999)
	require.Error(t, err)
}

func TestIncrementalAppendScanPlansEachInheritedManifestOnce(t *testing.T) {
	tbl := incrementalAppendTestTable(t)

	scan, err := tbl.NewIncrementalAppendScan().ToSnapshot(2)
	require.NoError(t, err)
	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, "mem://default/table-location/data-1.parquet", tasks[0].File.FilePath())
	require.Equal(t, "mem://default/table-location/data-2.parquet", tasks[1].File.FilePath())

	filtered, err := tbl.NewIncrementalAppendScan(WithRowFilter(
		iceberg.EqualTo(iceberg.Reference("id"), int32(2)),
	)).ToSnapshot(2)
	require.NoError(t, err)
	tasks, err = filtered.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "mem://default/table-location/data-2.parquet", tasks[0].File.FilePath())
}

func TestIncrementalAppendScanHonorsSnapshotOptions(t *testing.T) {
	tbl := incrementalAppendTestTable(t)

	byID, err := tbl.NewIncrementalAppendScan(WithSnapshotID(1)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, byID, 1)
	require.Equal(t, "mem://default/table-location/data-1.parquet", byID[0].File.FilePath())

	byTime, err := tbl.NewIncrementalAppendScan(WithSnapshotAsOf(1000)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, byTime, 1)
	require.Equal(t, "mem://default/table-location/data-1.parquet", byTime[0].File.FilePath())
}

func incrementalAppendTestTable(t *testing.T) *Table {
	t.Helper()
	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()

	writeManifest := func(snapshotID int64, manifestPath, dataPath string, partition map[int]any) iceberg.ManifestFile {
		df := newTestDataFile(t, spec, dataPath, partition)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, df)
		var manifestBuf bytes.Buffer
		manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(manifestPath, manifestBuf.Bytes()))

		return manifest
	}

	manifest1 := writeManifest(1,
		"mem://default/table-location/metadata/manifest-1.avro",
		"mem://default/table-location/data-1.parquet", map[int]any{1000: int32(1)})
	manifest2 := writeManifest(2,
		"mem://default/table-location/metadata/manifest-2.avro",
		"mem://default/table-location/data-2.parquet", map[int]any{1000: int32(2)})

	writeManifestList := func(snapshotID int64, path string, manifests []iceberg.ManifestFile) {
		var listBuf bytes.Buffer
		seqNum := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0, manifests))
		require.NoError(t, fs.WriteFile(path, listBuf.Bytes()))
	}
	manifestList1Path := "mem://default/table-location/metadata/snap-1.avro"
	writeManifestList(1, manifestList1Path, []iceberg.ManifestFile{manifest1})
	listFile, err := fs.Open(manifestList1Path)
	require.NoError(t, err)
	manifestList1, err := iceberg.ReadManifestList(listFile)
	require.NoError(t, err)
	require.NoError(t, listFile.Close())
	writeManifestList(2, "mem://default/table-location/metadata/snap-2.avro", []iceberg.ManifestFile{manifestList1[0], manifest2})

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: 1, TimestampMs: 1000, ManifestList: "mem://default/table-location/metadata/snap-1.avro", SequenceNumber: 1, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: int64Ptr(1), TimestampMs: 2000, ManifestList: "mem://default/table-location/metadata/snap-2.avro", SequenceNumber: 2, Summary: &Summary{Operation: OpAppend}},
	}
	txn.meta.snapshotLog = []SnapshotLogEntry{
		{SnapshotID: 1, TimestampMs: 1000},
		{SnapshotID: 2, TimestampMs: 2000},
	}
	currentSnapshotID := int64(2)
	txn.meta.currentSnapshotID = &currentSnapshotID
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
}
