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
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/stretchr/testify/require"
)

func TestIncrementalChangelogScanPlansAddedAndDeletedEntries(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 4)

	expected := []struct {
		path             string
		operation        ChangelogOperation
		ordinal          int
		commitSnapshotID int64
	}{
		{"mem://default/changelog/data-old.parquet", ChangelogOpInsert, 0, 1},
		{"mem://default/changelog/data-old.parquet", ChangelogOpDelete, 1, 2},
		{"mem://default/changelog/data-new.parquet", ChangelogOpInsert, 1, 2},
		{"mem://default/changelog/data-later.parquet", ChangelogOpInsert, 2, 4},
	}
	for i, want := range expected {
		fileTask := tasks[i].ScanTask()
		require.Equal(t, want.path, fileTask.File.FilePath())
		require.Equal(t, want.operation, tasks[i].Operation())
		require.Equal(t, want.ordinal, tasks[i].ChangeOrdinal())
		require.Equal(t, want.commitSnapshotID, tasks[i].CommitSnapshotID())
		require.Zero(t, fileTask.DeleteFiles)
		require.Zero(t, fileTask.EqualityDeleteFiles)
		require.Zero(t, fileTask.DeletionVectorFiles)
		require.NotNil(t, fileTask.DataSequenceNumber)
	}
}

func TestIncrementalChangelogScanSkipsManifestsWithoutChanges(t *testing.T) {
	reporter := &metrics.InMemoryReporter{}
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan(
		WithReporter(reporter),
	).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 4)

	reports := reporter.Reports()
	require.Len(t, reports, 1)
	report, ok := reports[0].(metrics.ScanReport)
	require.True(t, ok)
	require.Equal(t, int64(3), report.Metrics.TotalDataManifests.Value)
	require.Equal(t, int64(3), report.Metrics.ScannedDataManifests.Value)
}

func TestOpenManifestWithOptionsCanDiscardExistingEntries(t *testing.T) {
	spec := partitionedSpec()
	schema := simpleSchema()
	_, fs := createTestTransactionWithMemIO(t, spec)

	existingFile := newTestDataFile(t, spec,
		"mem://default/changelog/existing.parquet", map[int]any{1000: int32(1)})
	addedFile := newTestDataFile(t, spec,
		"mem://default/changelog/added.parquet", map[int]any{1000: int32(2)})
	snapshotID := int64(1)
	sequenceNumber := int64(1)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING, &snapshotID, &sequenceNumber, &sequenceNumber, existingFile),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &sequenceNumber, &sequenceNumber, addedFile),
	}
	manifestPath := "mem://default/changelog/metadata/mixed-manifest.avro"
	var buf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &buf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(manifestPath, buf.Bytes()))

	partitionCalls := 0
	metricsCalls := 0
	got, err := openManifestWithOptions(
		fs,
		manifest,
		func(iceberg.DataFile) (bool, error) {
			partitionCalls++

			return true, nil
		},
		func(iceberg.DataFile) (bool, error) {
			metricsCalls++

			return true, nil
		},
		false,
		true,
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, iceberg.EntryStatusADDED, got[0].Status())
	require.Equal(t, 1, partitionCalls)
	require.Equal(t, 1, metricsCalls)
}

func TestIncrementalChangelogScanHonorsSnapshotBoundaries(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(1).
		ToSnapshot(4).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	require.Equal(t, "mem://default/changelog/data-old.parquet", tasks[0].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpDelete, tasks[0].Operation())
	require.Equal(t, 0, tasks[0].ChangeOrdinal())
	require.Equal(t, "mem://default/changelog/data-new.parquet", tasks[1].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[1].Operation())
	require.Equal(t, 0, tasks[1].ChangeOrdinal())
	require.Equal(t, "mem://default/changelog/data-later.parquet", tasks[2].ScanTask().File.FilePath())
	require.Equal(t, 1, tasks[2].ChangeOrdinal())

	tasks, err = tbl.NewIncrementalChangelogScan().
		FromSnapshotInclusive(2).
		ToSnapshot(4).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	for _, task := range tasks {
		require.GreaterOrEqual(t, task.CommitSnapshotID(), int64(2))
	}
}

func TestIncrementalChangelogScanSkipsReplaceSnapshots(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotInclusive(3).
		ToSnapshot(3).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)
}

func TestIncrementalChangelogScanPreservesChangesAndSortsByFilePath(t *testing.T) {
	tbl := incrementalChangelogManifestRewriteTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 5)

	require.Equal(t, "mem://default/changelog-rewrite/data-a.parquet", tasks[0].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[0].Operation())
	require.Equal(t, 0, tasks[0].ChangeOrdinal())
	require.Equal(t, int64(1), tasks[0].CommitSnapshotID())

	require.Equal(t, "mem://default/changelog-rewrite/data-b.parquet", tasks[1].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[1].Operation())
	require.Equal(t, 1, tasks[1].ChangeOrdinal())
	require.Equal(t, int64(2), tasks[1].CommitSnapshotID())

	require.Equal(t, "mem://default/changelog-rewrite/data-m.parquet", tasks[2].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[2].Operation())
	require.Equal(t, 1, tasks[2].ChangeOrdinal())
	require.Equal(t, int64(2), tasks[2].CommitSnapshotID())

	require.Equal(t, "mem://default/changelog-rewrite/data-z.parquet", tasks[3].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[3].Operation())
	require.Equal(t, 1, tasks[3].ChangeOrdinal())
	require.Equal(t, int64(2), tasks[3].CommitSnapshotID())

	require.Equal(t, "mem://default/changelog-rewrite/data-c.parquet", tasks[4].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[4].Operation())
	require.Equal(t, 2, tasks[4].ChangeOrdinal())
	require.Equal(t, int64(4), tasks[4].CommitSnapshotID())
}

func TestIncrementalChangelogScanUsesLiveSchemaForImplicitCurrent(t *testing.T) {
	tbl := incrementalAppendSchemaEvolutionTable(t)
	filter := iceberg.EqualTo(iceberg.Reference("category"), "new")

	normalTasks, err := tbl.Scan(WithRowFilter(filter)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, normalTasks, 2)

	incrementalTasks, err := tbl.NewIncrementalChangelogScan(WithRowFilter(filter)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, incrementalTasks, 2)
}

func TestIncrementalChangelogScanUsesSnapshotSchemaForExplicitEnd(t *testing.T) {
	tbl := incrementalAppendSchemaEvolutionTable(t)
	filter := iceberg.EqualTo(iceberg.Reference("category"), "new")

	scan := tbl.NewIncrementalChangelogScan(WithRowFilter(filter)).ToSnapshot(2)
	_, err := scan.PlanFiles(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "category")
}

func TestIncrementalChangelogScanAppliesRowFilters(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)
	filter := iceberg.EqualTo(iceberg.Reference("id"), int32(2))

	tasks, err := tbl.NewIncrementalChangelogScan(
		WithRowFilter(filter),
	).ToSnapshot(4).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "mem://default/changelog/data-new.parquet", tasks[0].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[0].Operation())
	require.NotNil(t, tasks[0].ScanTask().Residual)
}

func TestIncrementalChangelogScanEmitsScanReport(t *testing.T) {
	reporter := &metrics.InMemoryReporter{}
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan(
		WithSelectedFields("id"),
		WithReporter(reporter),
	).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 4)

	reports := reporter.Reports()
	require.Len(t, reports, 1)
	report, ok := reports[0].(metrics.ScanReport)
	require.True(t, ok)
	require.Equal(t, int64(4), report.SnapshotID)
	require.Equal(t, []string{"id"}, report.ProjectedFieldNames)
	require.Equal(t, int64(4), report.Metrics.ResultDataFiles.Value)
	require.Equal(t, int64(3), report.Metrics.TotalDataManifests.Value)
	require.Equal(t, int64(3), report.Metrics.ScannedDataManifests.Value)
}

func TestIncrementalChangelogScanRejectsDeleteManifests(t *testing.T) {
	tbl := incrementalChangelogDeleteManifestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "scan range references a delete manifest")
	require.Nil(t, tasks)
}

func TestIncrementalChangelogScanRejectsCarriedForwardDeleteManifests(t *testing.T) {
	tbl := incrementalChangelogDeleteManifestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(2).
		ToSnapshot(3).
		PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "scan range references a delete manifest")
	require.ErrorContains(t, err, "snapshot 2")
	require.Nil(t, tasks)
}

func TestIncrementalChangelogScanRejectsMissingSnapshotOperation(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)
	setIncrementalSnapshotSummary(t, tbl, 1, nil)

	_, err := tbl.NewIncrementalChangelogScan().ToSnapshot(1).PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrMissingOperation)
	require.ErrorContains(t, err, "cannot determine operation for snapshot 1")
}

func TestIncrementalChangelogScanAllowsUnknownSnapshotOperation(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)
	setIncrementalSnapshotSummary(t, tbl, 2, &Summary{Operation: Operation("unknown")})

	tasks, err := tbl.NewIncrementalChangelogScan().ToSnapshot(4).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 4)
}

func TestIncrementalChangelogScanRejectsUnknownManifestEntryStatus(t *testing.T) {
	_, err := changelogOperation(iceberg.ManifestEntryStatus(99))
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "unknown manifest entry status 99")
}

func TestIncrementalChangelogScanRejectsUnknownStart(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	_, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotInclusive(999).
		ToSnapshot(4).
		PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "starting snapshot not found")
}

func TestIncrementalChangelogScanRejectsUnknownEnd(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	_, err := tbl.NewIncrementalChangelogScan().ToSnapshot(999).PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "ending snapshot not found")
}

func TestIncrementalChangelogScanAllowsExpiredExclusiveParent(t *testing.T) {
	tbl := incrementalAppendExpiredExclusiveTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(1).
		ToSnapshot(3).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, "mem://default/table-location/data-b.parquet", tasks[0].ScanTask().File.FilePath())
	require.Equal(t, "mem://default/table-location/data-c.parquet", tasks[1].ScanTask().File.FilePath())
}

func TestIncrementalChangelogScanRejectsDivergentStart(t *testing.T) {
	tbl := incrementalChangelogDivergentTable(t)

	inclusive := tbl.NewIncrementalChangelogScan().
		FromSnapshotInclusive(3).
		ToSnapshot(2)
	tasks, err := inclusive.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "starting snapshot 3 is not an ancestor of ending snapshot 2")
	require.Nil(t, tasks)

	exclusive := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(3).
		ToSnapshot(2)
	tasks, err = exclusive.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "starting snapshot 3 is not an ancestor of ending snapshot 2")
	require.Nil(t, tasks)
}

func TestChangelogOperationOrderPlacesDeletesBeforeInserts(t *testing.T) {
	operations := []ChangelogOperation{
		ChangelogOpDelete,
		ChangelogOpInsert,
		ChangelogOpUpdateBefore,
		ChangelogOpUpdateAfter,
	}
	for expected, operation := range operations {
		require.Equal(t, expected, changelogOperationOrder(operation))
	}
}

func TestIncrementalChangelogScanAutoFallsBackToLocal(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan(
		WithScanPlanningMode(ScanPlanningAuto),
	).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 4)
}

func TestIncrementalChangelogScanHonorsSnapshotOptions(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan(WithSnapshotID(2)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	for _, task := range tasks {
		require.LessOrEqual(t, task.CommitSnapshotID(), int64(2))
	}
}

func TestIncrementalChangelogScanHonorsContextCancellation(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)
	var manifestListOpens atomic.Int64
	originalFSF := tbl.fsF
	tbl.fsF = func(ctx context.Context) (iceio.IO, error) {
		fs, err := originalFSF(ctx)
		if err != nil {
			return nil, err
		}

		return &countingOpenIO{IO: fs, opens: &manifestListOpens}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := tbl.NewIncrementalChangelogScan().PlanFiles(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, manifestListOpens.Load())
}

func TestIncrementalChangelogScanChecksContextBetweenSnapshots(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)
	var manifestListOpens atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	originalFSF := tbl.fsF
	tbl.fsF = func(ctx context.Context) (iceio.IO, error) {
		fs, err := originalFSF(ctx)
		if err != nil {
			return nil, err
		}

		return &countingOpenIO{
			IO:        fs,
			opens:     &manifestListOpens,
			afterOpen: cancel,
		}, nil
	}

	_, err := tbl.NewIncrementalChangelogScan().PlanFiles(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), manifestListOpens.Load())
}

func TestIncrementalChangelogScanZeroValueReturnsError(t *testing.T) {
	var scan IncrementalChangelogScan

	tasks, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "not initialized")
	require.Nil(t, tasks)
}

func TestIncrementalChangelogScanRejectsUnsupportedPlanningModes(t *testing.T) {
	tbl := incrementalChangelogTestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan(
		WithScanPlanningMode(ScanPlanningRemote),
	).PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "do not support remote planning")
	require.Nil(t, tasks)
}

func incrementalChangelogDivergentTable(t *testing.T) *Table {
	t.Helper()

	base, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://default/changelog-divergent", nil)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)

	baseTimestamp := base.LastUpdatedMillis()
	snapshots := []*Snapshot{
		{SnapshotID: 1, TimestampMs: baseTimestamp + 1, SequenceNumber: 1, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: int64Ptr(1), TimestampMs: baseTimestamp + 2, SequenceNumber: 2, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 3, ParentSnapshotID: int64Ptr(1), TimestampMs: baseTimestamp + 3, SequenceNumber: 3, Summary: &Summary{Operation: OpAppend}},
	}
	for _, snapshot := range snapshots {
		require.NoError(t, builder.AddSnapshot(snapshot))
	}
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 2, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("feature", 3, BranchRef))
	meta, err := builder.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental-changelog-divergent"}, meta, "metadata.json", nil, nil)
}

func incrementalChangelogTestTable(t *testing.T) *Table {
	t.Helper()

	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()

	oldFile := newTestDataFile(t, spec,
		"mem://default/changelog/data-old.parquet", map[int]any{1000: int32(1)})
	newFile := newTestDataFile(t, spec,
		"mem://default/changelog/data-new.parquet", map[int]any{1000: int32(2)})
	replaceFile := newTestDataFile(t, spec,
		"mem://default/changelog/data-replace.parquet", map[int]any{1000: int32(3)})
	laterFile := newTestDataFile(t, spec,
		"mem://default/changelog/data-later.parquet", map[int]any{1000: int32(4)})

	entry := func(status iceberg.ManifestEntryStatus, snapshotID, sequenceNumber int64, file iceberg.DataFile) iceberg.ManifestEntry {
		return iceberg.NewManifestEntry(status, &snapshotID, &sequenceNumber, &sequenceNumber, file)
	}
	writeManifest := func(path string, snapshotID int64, entries []iceberg.ManifestEntry) iceberg.ManifestFile {
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, snapshotID, entries)
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(path, buf.Bytes()))

		return manifest
	}
	writeManifestList := func(path string, snapshotID int64, manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
		var buf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil,
			&sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(path, buf.Bytes()))

		listFile, err := fs.Open(path)
		require.NoError(t, err)
		list, err := iceberg.ReadManifestList(listFile)
		require.NoError(t, err)
		require.NoError(t, listFile.Close())

		return list
	}

	manifestOne := writeManifest(
		"mem://default/changelog/metadata/manifest-1.avro", 1,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, 1, 1, oldFile)})
	listOnePath := "mem://default/changelog/metadata/snap-1.avro"
	listOne := writeManifestList(listOnePath, 1, []iceberg.ManifestFile{manifestOne})

	manifestTwo := writeManifest(
		"mem://default/changelog/metadata/manifest-2.avro", 2,
		[]iceberg.ManifestEntry{
			entry(iceberg.EntryStatusDELETED, 2, 1, oldFile),
			entry(iceberg.EntryStatusADDED, 2, 2, newFile),
		})
	listTwoPath := "mem://default/changelog/metadata/snap-2.avro"
	listTwo := writeManifestList(listTwoPath, 2, append(listOne, manifestTwo))

	manifestThree := writeManifest(
		"mem://default/changelog/metadata/manifest-3.avro", 3,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, 3, 3, replaceFile)})
	listThreePath := "mem://default/changelog/metadata/snap-3.avro"
	listThree := writeManifestList(listThreePath, 3, append(listTwo, manifestThree))

	manifestFour := writeManifest(
		"mem://default/changelog/metadata/manifest-4.avro", 4,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, 4, 4, laterFile)})
	existingOnlyFile := newTestDataFile(t, spec,
		"mem://default/changelog/existing-only.parquet", map[int]any{1000: int32(5)})
	existingOnlyManifest := writeManifest(
		"mem://default/changelog/metadata/manifest-existing-only.avro", 4,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusEXISTING, 1, 1, existingOnlyFile)},
	)
	listFourPath := "mem://default/changelog/metadata/snap-4.avro"
	writeManifestList(listFourPath, 4, append(listThree, manifestFour, existingOnlyManifest))

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: 1, TimestampMs: 1000, ManifestList: listOnePath, SequenceNumber: 1, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: int64Ptr(1), TimestampMs: 2000, ManifestList: listTwoPath, SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpOverwrite}},
		{SnapshotID: 3, ParentSnapshotID: int64Ptr(2), TimestampMs: 3000, ManifestList: listThreePath, SequenceNumber: 3, SchemaID: &schema.ID, Summary: &Summary{Operation: OpReplace}},
		{SnapshotID: 4, ParentSnapshotID: int64Ptr(3), TimestampMs: 4000, ManifestList: listFourPath, SequenceNumber: 4, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
	}
	txn.meta.snapshotLog = []SnapshotLogEntry{
		{SnapshotID: 1, TimestampMs: 1000},
		{SnapshotID: 2, TimestampMs: 2000},
		{SnapshotID: 3, TimestampMs: 3000},
		{SnapshotID: 4, TimestampMs: 4000},
	}
	currentSnapshotID := int64(4)
	txn.meta.currentSnapshotID = &currentSnapshotID
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental-changelog"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
}

func incrementalChangelogManifestRewriteTable(t *testing.T) *Table {
	t.Helper()

	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()

	fileA := newTestDataFile(t, spec,
		"mem://default/changelog-rewrite/data-a.parquet", map[int]any{1000: int32(1)})
	fileB := newTestDataFile(t, spec,
		"mem://default/changelog-rewrite/data-b.parquet", map[int]any{1000: int32(2)})
	fileM := newTestDataFile(t, spec,
		"mem://default/changelog-rewrite/data-m.parquet", map[int]any{1000: int32(13)})
	fileZ := newTestDataFile(t, spec,
		"mem://default/changelog-rewrite/data-z.parquet", map[int]any{1000: int32(26)})
	fileC := newTestDataFile(t, spec,
		"mem://default/changelog-rewrite/data-c.parquet", map[int]any{1000: int32(3)})

	entry := func(status iceberg.ManifestEntryStatus, snapshotID, sequenceNumber int64, file iceberg.DataFile) iceberg.ManifestEntry {
		return iceberg.NewManifestEntry(status, &snapshotID, &sequenceNumber, &sequenceNumber, file)
	}
	writeManifest := func(path string, snapshotID int64, entries []iceberg.ManifestEntry) iceberg.ManifestFile {
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, snapshotID, entries)
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(path, buf.Bytes()))

		return manifest
	}
	writeManifestList := func(path string, snapshotID int64, manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
		var buf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil,
			&sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(path, buf.Bytes()))

		listFile, err := fs.Open(path)
		require.NoError(t, err)
		list, err := iceberg.ReadManifestList(listFile)
		require.NoError(t, err)
		require.NoError(t, listFile.Close())

		return list
	}

	manifestA := writeManifest(
		"mem://default/changelog-rewrite/metadata/manifest-a.avro", 1,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, 1, 1, fileA)})
	listOnePath := "mem://default/changelog-rewrite/metadata/snap-1.avro"
	listOne := writeManifestList(listOnePath, 1, []iceberg.ManifestFile{manifestA})

	manifestB := writeManifest(
		"mem://default/changelog-rewrite/metadata/manifest-b.avro", 2,
		[]iceberg.ManifestEntry{
			// This is the merged-manifest shape produced by append commits:
			// an EXISTING entry from an earlier in-range snapshot followed by
			// entries added by the manifest's owning snapshot. The added entries
			// are deliberately reverse-sorted to exercise the task path tiebreak.
			entry(iceberg.EntryStatusEXISTING, 1, 1, fileA),
			entry(iceberg.EntryStatusADDED, 2, 2, fileZ),
			entry(iceberg.EntryStatusADDED, 2, 2, fileM),
			entry(iceberg.EntryStatusADDED, 2, 2, fileB),
		})
	listTwoPath := "mem://default/changelog-rewrite/metadata/snap-2.avro"
	writeManifestList(listTwoPath, 2, append(listOne, manifestB))

	manifestRewrite := writeManifest(
		"mem://default/changelog-rewrite/metadata/manifest-rewrite.avro", 3,
		[]iceberg.ManifestEntry{
			entry(iceberg.EntryStatusEXISTING, 1, 1, fileA),
			entry(iceberg.EntryStatusEXISTING, 2, 2, fileB),
			entry(iceberg.EntryStatusEXISTING, 2, 2, fileZ),
		})
	listThreePath := "mem://default/changelog-rewrite/metadata/snap-3.avro"
	listThree := writeManifestList(listThreePath, 3, []iceberg.ManifestFile{manifestRewrite})

	manifestC := writeManifest(
		"mem://default/changelog-rewrite/metadata/manifest-c.avro", 4,
		[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, 4, 4, fileC)})
	listFourPath := "mem://default/changelog-rewrite/metadata/snap-4.avro"
	writeManifestList(listFourPath, 4, append(listThree, manifestC))

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: 1, TimestampMs: 1000, ManifestList: listOnePath, SequenceNumber: 1, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: int64Ptr(1), TimestampMs: 2000, ManifestList: listTwoPath, SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 3, ParentSnapshotID: int64Ptr(2), TimestampMs: 3000, ManifestList: listThreePath, SequenceNumber: 3, SchemaID: &schema.ID, Summary: &Summary{Operation: OpReplace}},
		{SnapshotID: 4, ParentSnapshotID: int64Ptr(3), TimestampMs: 4000, ManifestList: listFourPath, SequenceNumber: 4, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
	}
	txn.meta.snapshotLog = []SnapshotLogEntry{
		{SnapshotID: 1, TimestampMs: 1000},
		{SnapshotID: 2, TimestampMs: 2000},
		{SnapshotID: 3, TimestampMs: 3000},
		{SnapshotID: 4, TimestampMs: 4000},
	}
	currentSnapshotID := int64(4)
	txn.meta.currentSnapshotID = &currentSnapshotID
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"incremental-changelog-rewrite"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
}

func incrementalChangelogDeleteManifestTable(t *testing.T) *Table {
	t.Helper()

	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	dataFileOne := newTestDataFile(t, spec,
		"mem://default/changelog-delete/data-1.parquet", map[int]any{1000: int32(1)})
	dataFileThree := newTestDataFile(t, spec,
		"mem://default/changelog-delete/data-3.parquet", map[int]any{1000: int32(3)})
	deletePath := "mem://default/changelog-delete/delete.parquet"

	deleteFile := newTestPosDeleteFileForSpec(t, spec, deletePath, map[int]any{1000: int32(1)}, dataFileOne.FilePath())
	entry := func(status iceberg.ManifestEntryStatus, snapshotID, sequenceNumber int64, file iceberg.DataFile) iceberg.ManifestEntry {
		return iceberg.NewManifestEntry(status, &snapshotID, &sequenceNumber, &sequenceNumber, file)
	}
	writeDataManifest := func(path string, snapshotID int64, file iceberg.DataFile) iceberg.ManifestFile {
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, snapshotID,
			[]iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED, snapshotID, snapshotID, file)})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(path, buf.Bytes()))

		return manifest
	}
	writeManifestList := func(path string, snapshotID int64, manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
		var buf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil,
			&sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(path, buf.Bytes()))

		listFile, err := fs.Open(path)
		require.NoError(t, err)
		list, err := iceberg.ReadManifestList(listFile)
		require.NoError(t, err)
		require.NoError(t, listFile.Close())

		return list
	}

	manifestOnePath := "mem://default/changelog-delete/metadata/manifest-1.avro"
	manifestOne := writeDataManifest(manifestOnePath, 1, dataFileOne)
	listOnePath := "mem://default/changelog-delete/metadata/snap-1.avro"
	listOne := writeManifestList(listOnePath, 1, []iceberg.ManifestFile{manifestOne})

	deleteManifestPath := "mem://default/changelog-delete/metadata/delete-manifest.avro"
	deleteSnapshotID := int64(2)
	deleteSequenceNumber := int64(2)
	deleteEntry := entry(iceberg.EntryStatusADDED, deleteSnapshotID, deleteSequenceNumber, deleteFile)
	var deleteManifestBuf bytes.Buffer
	writer, err := iceberg.NewManifestWriter(2, &deleteManifestBuf, spec, schema, deleteSnapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, writer.Add(deleteEntry))
	require.NoError(t, writer.Close())
	deleteManifest, err := writer.ToManifestFile(deleteManifestPath, int64(deleteManifestBuf.Len()),
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(deleteManifestPath, deleteManifestBuf.Bytes()))

	listTwoPath := "mem://default/changelog-delete/metadata/snap-2.avro"
	listTwo := writeManifestList(listTwoPath, 2, append(listOne, deleteManifest))

	manifestThreePath := "mem://default/changelog-delete/metadata/manifest-3.avro"
	manifestThree := writeDataManifest(manifestThreePath, 3, dataFileThree)
	listThreePath := "mem://default/changelog-delete/metadata/snap-3.avro"
	writeManifestList(listThreePath, 3, append(listTwo, manifestThree))

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: 1, TimestampMs: 1000, ManifestList: listOnePath, SequenceNumber: 1, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: int64Ptr(1), TimestampMs: 2000, ManifestList: listTwoPath, SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpDelete}},
		{SnapshotID: 3, ParentSnapshotID: int64Ptr(2), TimestampMs: 3000, ManifestList: listThreePath, SequenceNumber: 3, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
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

	return New(Identifier{"incremental-changelog-delete"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
}

type countingOpenIO struct {
	iceio.IO
	opens     *atomic.Int64
	afterOpen func()
}

func (io *countingOpenIO) Open(name string) (iceio.File, error) {
	io.opens.Add(1)
	if io.afterOpen != nil {
		io.afterOpen()
	}

	return io.IO.Open(name)
}
