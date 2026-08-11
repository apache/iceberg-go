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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestIncrementalAppendScanSnapshotBoundaries(t *testing.T) {
	scan := snapshotsTestTable().NewIncrementalAppendScan()
	inclusive := scan.FromSnapshotInclusive(101).ToSnapshot(102)

	snapshots, err := inclusive.snapshotsBetween(102)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 101, snapshots[0].SnapshotID)

	exclusive := scan.FromSnapshotExclusive(101).ToSnapshot(102)
	snapshots, err = exclusive.snapshotsBetween(102)
	require.NoError(t, err)
	require.Empty(t, snapshots, "the only snapshot after 101 is not an append")
}

func TestIncrementalAppendScanRejectsUnknownStart(t *testing.T) {
	const nonExistentSnapshotID = int64(999)

	scan := snapshotsTestTable().NewIncrementalAppendScan().
		FromSnapshotInclusive(nonExistentSnapshotID)
	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "starting snapshot not found")
}

func TestIncrementalAppendScanRejectsUnknownEnd(t *testing.T) {
	const nonExistentSnapshotID = int64(999)

	scan := snapshotsTestTable().NewIncrementalAppendScan().ToSnapshot(nonExistentSnapshotID)
	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "ending snapshot not found")
}

func TestIncrementalAppendScanEqualBoundaries(t *testing.T) {
	tbl := incrementalAppendTestTable(t)

	inclusive := tbl.NewIncrementalAppendScan().FromSnapshotInclusive(1).ToSnapshot(1)
	tasks, err := inclusive.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "mem://default/table-location/data-1.parquet", tasks[0].File.FilePath())

	exclusive := tbl.NewIncrementalAppendScan().FromSnapshotExclusive(1).ToSnapshot(1)
	_, err = exclusive.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "must be a parent ancestor")
}

func TestIncrementalAppendScanRejectsUnsupportedPlanningModes(t *testing.T) {
	scan := incrementalAppendTestTable(t).NewIncrementalAppendScan(
		WithScanPlanningMode(ScanPlanningRemote),
	)

	tasks, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "do not support remote planning")
	require.Nil(t, tasks)
}

func TestIncrementalAppendScanAutoFallsBackToLocal(t *testing.T) {
	scan := incrementalAppendTestTable(t).NewIncrementalAppendScan(
		WithScanPlanningMode(ScanPlanningAuto),
	)

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
}

func TestIncrementalAppendScanPlansEachInheritedManifestOnce(t *testing.T) {
	tbl := incrementalAppendTestTable(t)

	scan := tbl.NewIncrementalAppendScan().ToSnapshot(2)
	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, "mem://default/table-location/data-1.parquet", tasks[0].File.FilePath())
	require.Equal(t, "mem://default/table-location/data-2.parquet", tasks[1].File.FilePath())

	filter := iceberg.EqualTo(iceberg.Reference("id"), int32(2))
	filtered := tbl.NewIncrementalAppendScan(WithRowFilter(filter)).ToSnapshot(2)
	tasks, err = filtered.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "mem://default/table-location/data-2.parquet", tasks[0].File.FilePath())
	require.NotNil(t, tasks[0].Residual)
	require.True(t, tasks[0].Residual.Equals(filter))
}

func TestIncrementalAppendScanSkipsOverwriteSnapshots(t *testing.T) {
	tbl := incrementalAppendMixedOperationTable(t)

	tasks, err := tbl.NewIncrementalAppendScan().
		FromSnapshotInclusive(1).
		ToSnapshot(3).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, "mem://default/mixed/data-a.parquet", tasks[0].File.FilePath())
	require.Equal(t, "mem://default/mixed/data-c.parquet", tasks[1].File.FilePath())
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

func TestIncrementalAppendScanAllowsExpiredExclusiveParent(t *testing.T) {
	const expiredSnapshotID = int64(1)
	tbl := incrementalAppendExpiredExclusiveTable(t)

	scan := tbl.NewIncrementalAppendScan().FromSnapshotExclusive(expiredSnapshotID)
	scan = scan.ToSnapshot(3)
	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, "mem://default/table-location/data-b.parquet", tasks[0].File.FilePath())
	require.Equal(t, "mem://default/table-location/data-c.parquet", tasks[1].File.FilePath())
}

func TestIncrementalAppendScanRejectsDivergentStart(t *testing.T) {
	tbl := incrementalAppendDivergentTable(t)

	inclusive := tbl.NewIncrementalAppendScan().FromSnapshotInclusive(10).ToSnapshot(22)
	_, err := inclusive.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "not an ancestor")

	exclusive := tbl.NewIncrementalAppendScan().FromSnapshotExclusive(10)
	exclusive = exclusive.ToSnapshot(22)
	_, err = exclusive.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "not an ancestor")
}

func TestIncrementalAppendScanUsesLiveSchemaForImplicitCurrent(t *testing.T) {
	tbl := incrementalAppendSchemaEvolutionTable(t)
	filter := iceberg.EqualTo(iceberg.Reference("category"), "new")

	normalTasks, err := tbl.Scan(WithRowFilter(filter)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, normalTasks, 2)

	incrementalTasks, err := tbl.NewIncrementalAppendScan(WithRowFilter(filter)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, incrementalTasks, 2)
}

func TestIncrementalAppendScanUsesSnapshotSchemaForExplicitEnd(t *testing.T) {
	tbl := incrementalAppendSchemaEvolutionTable(t)
	filter := iceberg.EqualTo(iceberg.Reference("category"), "new")

	scan := tbl.NewIncrementalAppendScan(WithRowFilter(filter)).ToSnapshot(2)
	_, err := scan.PlanFiles(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "category")
}

func TestIncrementalAppendScanRejectsMissingImplicitEnd(t *testing.T) {
	tbl := incrementalAppendTableWithoutCurrentSnapshot(t)

	inclusive := tbl.NewIncrementalAppendScan().FromSnapshotInclusive(1)
	_, err := inclusive.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "no ending snapshot")

	exclusive := tbl.NewIncrementalAppendScan().FromSnapshotExclusive(1)
	_, err = exclusive.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "no ending snapshot")
}

func TestIncrementalAppendScanPrunesManifestSummariesBeforeOpening(t *testing.T) {
	const (
		matchingManifestPath   = "mem://default/incremental/metadata/manifest-matching.avro"
		irrelevantManifestPath = "mem://default/incremental/metadata/manifest-irrelevant.avro"
		manifestListPath       = "mem://default/incremental/metadata/snap-1.avro"
		matchingDataPath       = "mem://default/incremental/data-matching.parquet"
		irrelevantDataPath     = "mem://default/incremental/data-irrelevant.parquet"
	)
	spec := partitionedSpec()
	schema := simpleSchema()
	fs := newTrackingCallsIO()

	writeManifest := func(manifestPath, dataPath string, partitionValue int32) iceberg.ManifestFile {
		dataFile := newTestDataFile(t, spec, dataPath, map[int]any{1000: partitionValue})
		snapshotID := int64(1)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dataFile)
		var manifestBuf bytes.Buffer
		manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID,
			[]iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(manifestPath, manifestBuf.Bytes()))

		return manifest
	}

	matchingManifest := writeManifest(matchingManifestPath, matchingDataPath, 1)
	irrelevantManifest := writeManifest(irrelevantManifestPath, irrelevantDataPath, 2)
	var manifestListBuf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &manifestListBuf, 1, nil, &seqNum, 0,
		[]iceberg.ManifestFile{matchingManifest, irrelevantManifest}))
	require.NoError(t, fs.WriteFile(manifestListPath, manifestListBuf.Bytes()))

	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://default/incremental", nil)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	schemaID := schema.ID
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		SequenceNumber: 1,
		SchemaID:       &schemaID,
		Summary:        &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 1, BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"incremental-pruning"}, meta, "metadata.json", testFSF(fs), nil)
	scan := tbl.NewIncrementalAppendScan(WithRowFilter(
		iceberg.EqualTo(iceberg.Reference("id"), int32(1)),
	))
	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, matchingDataPath, tasks[0].File.FilePath())
	require.Equal(t, 1, fs.openCount[matchingManifestPath])
	require.Zero(t, fs.openCount[irrelevantManifestPath])
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
		{SnapshotID: 1, TimestampMs: 1000, ManifestList: "mem://default/table-location/metadata/snap-1.avro", SequenceNumber: 1, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: int64Ptr(1), TimestampMs: 2000, ManifestList: "mem://default/table-location/metadata/snap-2.avro", SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
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

func incrementalAppendMixedOperationTable(t *testing.T) *Table {
	t.Helper()
	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()

	writeManifest := func(snapshotID int64, suffix string) iceberg.ManifestFile {
		dataPath := fmt.Sprintf("mem://default/mixed/data-%s.parquet", suffix)
		manifestPath := fmt.Sprintf("mem://default/mixed/metadata/manifest-%s.avro", suffix)
		file := newTestDataFile(t, spec, dataPath, map[int]any{1000: int32(snapshotID)})
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, file)
		var manifestBuf bytes.Buffer
		manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema,
			snapshotID, []iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(manifestPath, manifestBuf.Bytes()))

		return manifest
	}

	writeManifestList := func(snapshotID int64, manifests []iceberg.ManifestFile) (string, []iceberg.ManifestFile) {
		path := fmt.Sprintf("mem://default/mixed/metadata/snap-%d.avro", snapshotID)
		var listBuf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil,
			&sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(path, listBuf.Bytes()))
		listFile, err := fs.Open(path)
		require.NoError(t, err)
		manifestList, err := iceberg.ReadManifestList(listFile)
		require.NoError(t, err)
		require.NoError(t, listFile.Close())

		return path, manifestList
	}

	manifestA := writeManifest(1, "a")
	listAPath, listA := writeManifestList(1, []iceberg.ManifestFile{manifestA})
	manifestB := writeManifest(2, "b")
	listBPath, listB := writeManifestList(2, append(listA, manifestB))
	manifestC := writeManifest(3, "c")
	listCPath, _ := writeManifestList(3, append(listB, manifestC))

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: 1, TimestampMs: 1000, ManifestList: listAPath, SequenceNumber: 1, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: int64Ptr(1), TimestampMs: 2000, ManifestList: listBPath, SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpOverwrite}},
		{SnapshotID: 3, ParentSnapshotID: int64Ptr(2), TimestampMs: 3000, ManifestList: listCPath, SequenceNumber: 3, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
	}
	txn.meta.snapshotLog = []SnapshotLogEntry{
		{SnapshotID: 1, TimestampMs: 1000},
		{SnapshotID: 2, TimestampMs: 2000},
		{SnapshotID: 3, TimestampMs: 3000},
	}
	currentSnapshotID := int64(3)
	txn.meta.currentSnapshotID = &currentSnapshotID
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental-mixed"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
}

func incrementalAppendSchemaEvolutionTable(t *testing.T) *Table {
	t.Helper()
	tbl := incrementalAppendTestTable(t)
	builder, err := MetadataBuilderFromBase(tbl.metadata, "")
	require.NoError(t, err)

	evolvedSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String},
	)
	require.NoError(t, builder.AddSchema(evolvedSchema))
	require.NoError(t, builder.SetCurrentSchemaID(evolvedSchema.ID))

	tbl.metadata, err = builder.Build()
	require.NoError(t, err)

	return tbl
}

func incrementalAppendTableWithoutCurrentSnapshot(t *testing.T) *Table {
	t.Helper()
	schema := simpleSchema()
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://default/no-current", nil)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	schemaID := schema.ID
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		SequenceNumber: 1,
		SchemaID:       &schemaID,
		Summary:        &Summary{Operation: OpAppend},
	}))
	meta, err = builder.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental-no-current"}, meta, "metadata.json", nil, nil)
}

func incrementalAppendDivergentTable(t *testing.T) *Table {
	t.Helper()
	base, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://default/divergent", nil)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)

	baseTimestamp := base.LastUpdatedMillis()
	snapshots := []*Snapshot{
		{SnapshotID: 10, TimestampMs: baseTimestamp + 1, SequenceNumber: 1, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 20, TimestampMs: baseTimestamp + 2, SequenceNumber: 2, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 21, ParentSnapshotID: int64Ptr(20), TimestampMs: baseTimestamp + 3, SequenceNumber: 3, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 22, ParentSnapshotID: int64Ptr(21), TimestampMs: baseTimestamp + 4, SequenceNumber: 4, Summary: &Summary{Operation: OpAppend}},
	}
	for _, snapshot := range snapshots {
		require.NoError(t, builder.AddSnapshot(snapshot))
	}
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 22, BranchRef))
	meta, err := builder.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental-divergent"}, meta, "metadata.json", nil, nil)
}

func incrementalAppendExpiredExclusiveTable(t *testing.T) *Table {
	t.Helper()
	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()

	writeManifest := func(snapshotID int64, manifestPath, dataPath string) iceberg.ManifestFile {
		file := newTestDataFile(t, spec, dataPath, map[int]any{1000: int32(snapshotID)})
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, file)
		var manifestBuf bytes.Buffer
		manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID,
			[]iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(manifestPath, manifestBuf.Bytes()))

		return manifest
	}

	manifestB := writeManifest(2,
		"mem://default/table-location/metadata/manifest-b.avro",
		"mem://default/table-location/data-b.parquet")
	manifestC := writeManifest(3,
		"mem://default/table-location/metadata/manifest-c.avro",
		"mem://default/table-location/data-c.parquet")

	writeManifestList := func(snapshotID int64, path string, manifests []iceberg.ManifestFile) {
		var listBuf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(path, listBuf.Bytes()))
	}
	manifestListBPath := "mem://default/table-location/metadata/snap-b.avro"
	writeManifestList(2, manifestListBPath, []iceberg.ManifestFile{manifestB})
	listFile, err := fs.Open(manifestListBPath)
	require.NoError(t, err)
	manifestListB, err := iceberg.ReadManifestList(listFile)
	require.NoError(t, err)
	require.NoError(t, listFile.Close())

	manifestListCPath := "mem://default/table-location/metadata/snap-c.avro"
	writeManifestList(3, manifestListCPath, []iceberg.ManifestFile{manifestListB[0], manifestC})

	expiredParentID := int64(1)
	currentSnapshotID := int64(3)
	txn.meta.snapshotList = []Snapshot{
		{
			SnapshotID:       2,
			ParentSnapshotID: &expiredParentID,
			TimestampMs:      2000,
			ManifestList:     manifestListBPath,
			SequenceNumber:   2,
			SchemaID:         &schema.ID,
			Summary:          &Summary{Operation: OpAppend},
		},
		{
			SnapshotID:       3,
			ParentSnapshotID: int64Ptr(2),
			TimestampMs:      3000,
			ManifestList:     manifestListCPath,
			SequenceNumber:   3,
			SchemaID:         &schema.ID,
			Summary:          &Summary{Operation: OpAppend},
		},
	}
	txn.meta.snapshotLog = []SnapshotLogEntry{
		{SnapshotID: 2, TimestampMs: 2000},
		{SnapshotID: 3, TimestampMs: 3000},
	}
	txn.meta.currentSnapshotID = &currentSnapshotID
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental-expired"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
}
