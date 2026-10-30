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
	"strconv"
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
	require.NoError(t, fs.WriteFile(context.Background(), manifestPath, buf.Bytes()))

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

func TestIncrementalChangelogScanPlansPositionDeletes(t *testing.T) {
	reporter := &metrics.InMemoryReporter{}
	tbl := incrementalChangelogDeleteManifestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan(WithReporter(reporter)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)

	require.Equal(t, ChangelogOpInsert, tasks[0].Operation())
	require.Equal(t, "mem://default/changelog-delete/data-1.parquet", tasks[0].ScanTask().File.FilePath())
	require.Equal(t, 0, tasks[0].ChangeOrdinal())
	require.Empty(t, tasks[0].ScanTask().DeleteFiles)

	rows, ok := tasks[1].(DeletedRowsScanTask)
	require.True(t, ok)
	require.Equal(t, ChangelogOpDelete, rows.Operation())
	require.Equal(t, 1, rows.ChangeOrdinal())
	require.Equal(t, int64(2), rows.CommitSnapshotID())
	require.Equal(t, "mem://default/changelog-delete/data-1.parquet", rows.ScanTask().File.FilePath())
	require.Equal(t, []string{"mem://default/changelog-delete/delete.parquet"}, changelogFilePaths(rows.AddedDeletes()))
	require.Empty(t, rows.ExistingDeletes())
	require.NotNil(t, rows.ScanTask().DataSequenceNumber)
	require.Equal(t, int64(1), *rows.ScanTask().DataSequenceNumber)

	require.Equal(t, ChangelogOpInsert, tasks[2].Operation())
	require.Equal(t, "mem://default/changelog-delete/data-3.parquet", tasks[2].ScanTask().File.FilePath())
	require.Equal(t, 2, tasks[2].ChangeOrdinal())

	onlyDelete, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotInclusive(2).
		ToSnapshot(2).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, onlyDelete, 1)
	_, ok = onlyDelete[0].(DeletedRowsScanTask)
	require.True(t, ok)

	reports := reporter.Reports()
	require.Len(t, reports, 1)
	report, ok := reports[0].(metrics.ScanReport)
	require.True(t, ok)
	require.Equal(t, int64(3), report.Metrics.ResultDataFiles.Value)
	require.Equal(t, int64(1), report.Metrics.ResultDeleteFiles.Value)
	require.Equal(t, int64(1), report.Metrics.PositionalDeleteFiles.Value)
	require.Equal(t, int64(1), report.Metrics.TotalDeleteManifests.Value)
	require.Equal(t, int64(1), report.Metrics.ScannedDeleteManifests.Value)
}

func TestIncrementalChangelogScanIgnoresCarriedForwardDeleteManifests(t *testing.T) {
	tbl := incrementalChangelogDeleteManifestTable(t)

	tasks, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(2).
		ToSnapshot(3).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, ChangelogOpInsert, tasks[0].Operation())
	require.Equal(t, "mem://default/changelog-delete/data-3.parquet", tasks[0].ScanTask().File.FilePath())
	require.Empty(t, tasks[0].ScanTask().DeleteFiles)
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
		{SnapshotID: 2, ParentSnapshotID: new(int64(1)), TimestampMs: baseTimestamp + 2, SequenceNumber: 2, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 3, ParentSnapshotID: new(int64(1)), TimestampMs: baseTimestamp + 3, SequenceNumber: 3, Summary: &Summary{Operation: OpAppend}},
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
		require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

		return manifest
	}
	writeManifestList := func(path string, snapshotID int64, manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
		var buf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil,
			&sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

		listFile, err := fs.Open(context.Background(), path)
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
		{SnapshotID: 2, ParentSnapshotID: new(int64(1)), TimestampMs: 2000, ManifestList: listTwoPath, SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpOverwrite}},
		{SnapshotID: 3, ParentSnapshotID: new(int64(2)), TimestampMs: 3000, ManifestList: listThreePath, SequenceNumber: 3, SchemaID: &schema.ID, Summary: &Summary{Operation: OpReplace}},
		{SnapshotID: 4, ParentSnapshotID: new(int64(3)), TimestampMs: 4000, ManifestList: listFourPath, SequenceNumber: 4, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
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
		require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

		return manifest
	}
	writeManifestList := func(path string, snapshotID int64, manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
		var buf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil,
			&sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

		listFile, err := fs.Open(context.Background(), path)
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
		{SnapshotID: 2, ParentSnapshotID: new(int64(1)), TimestampMs: 2000, ManifestList: listTwoPath, SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 3, ParentSnapshotID: new(int64(2)), TimestampMs: 3000, ManifestList: listThreePath, SequenceNumber: 3, SchemaID: &schema.ID, Summary: &Summary{Operation: OpReplace}},
		{SnapshotID: 4, ParentSnapshotID: new(int64(3)), TimestampMs: 4000, ManifestList: listFourPath, SequenceNumber: 4, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
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
		require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

		return manifest
	}
	writeManifestList := func(path string, snapshotID int64, manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
		var buf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil,
			&sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

		listFile, err := fs.Open(context.Background(), path)
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
	require.NoError(t, fs.WriteFile(context.Background(), deleteManifestPath, deleteManifestBuf.Bytes()))

	listTwoPath := "mem://default/changelog-delete/metadata/snap-2.avro"
	listTwo := writeManifestList(listTwoPath, 2, append(listOne, deleteManifest))

	manifestThreePath := "mem://default/changelog-delete/metadata/manifest-3.avro"
	manifestThree := writeDataManifest(manifestThreePath, 3, dataFileThree)
	listThreePath := "mem://default/changelog-delete/metadata/snap-3.avro"
	writeManifestList(listThreePath, 3, append(listTwo, manifestThree))

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: 1, TimestampMs: 1000, ManifestList: listOnePath, SequenceNumber: 1, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 2, ParentSnapshotID: new(int64(1)), TimestampMs: 2000, ManifestList: listTwoPath, SequenceNumber: 2, SchemaID: &schema.ID, Summary: &Summary{Operation: OpDelete}},
		{SnapshotID: 3, ParentSnapshotID: new(int64(2)), TimestampMs: 3000, ManifestList: listThreePath, SequenceNumber: 3, SchemaID: &schema.ID, Summary: &Summary{Operation: OpAppend}},
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

func TestIncrementalChangelogScanAttachesSameSnapshotDeletes(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-same/data.parquet", map[int]any{1000: int32(1)})
	posDelete := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-same/pos.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	eqDelete := newChangelogEqDelete(t, spec,
		"mem://default/changelog-same/eq.parquet", map[int]any{1000: int32(1)})
	tbl := writeChangelogScanTable(t, "changelog-same", []changelogScanStep{{
		operation: OpAppend,
		data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, data)},
		deletes: []iceberg.ManifestEntry{
			changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, posDelete),
			changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, eqDelete),
		},
	}})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	added, ok := tasks[0].(AddedRowsScanTask)
	require.True(t, ok)
	require.Equal(t, []string{posDelete.FilePath()}, changelogFilePaths(added.Deletes()))
	require.Empty(t, added.ScanTask().EqualityDeleteFiles)
}

func TestIncrementalChangelogScanSeparatesAddedAndExistingDeletes(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-prior/data.parquet", map[int]any{1000: int32(1)})
	first := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-prior/first.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	second := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-prior/second.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	tbl := writeChangelogScanTable(t, "changelog-prior", []changelogScanStep{
		{
			operation: OpAppend,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, data)},
		},
		{
			operation: OpDelete,
			deletes:   []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, first)},
		},
		{
			operation: OpDelete,
			deletes:   []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 3, 3, second)},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)

	firstRows, ok := tasks[1].(DeletedRowsScanTask)
	require.True(t, ok)
	require.Equal(t, []string{first.FilePath()}, changelogFilePaths(firstRows.AddedDeletes()))
	require.Empty(t, firstRows.ExistingDeletes())

	secondRows, ok := tasks[2].(DeletedRowsScanTask)
	require.True(t, ok)
	require.Equal(t, 2, secondRows.ChangeOrdinal())
	require.Equal(t, []string{second.FilePath()}, changelogFilePaths(secondRows.AddedDeletes()))
	require.Equal(t, []string{first.FilePath()}, changelogFilePaths(secondRows.ExistingDeletes()))
}

func TestIncrementalChangelogScanPlansEqualityDeletes(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-eq/data.parquet", map[int]any{1000: int32(1)})
	eqDelete := newChangelogEqDelete(t, spec,
		"mem://default/changelog-eq/eq.parquet", map[int]any{1000: int32(1)})
	tbl := writeChangelogScanTable(t, "changelog-eq", []changelogScanStep{
		{
			operation: OpAppend,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, data)},
		},
		{
			operation: OpDelete,
			deletes:   []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, eqDelete)},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	rows, ok := tasks[1].(DeletedRowsScanTask)
	require.True(t, ok)
	require.Equal(t, []string{eqDelete.FilePath()}, changelogFilePaths(rows.AddedDeletes()))
	require.Empty(t, rows.ScanTask().EqualityDeleteFiles)
	require.Equal(t, iceberg.EntryContentEqDeletes, rows.AddedDeletes()[0].ContentType())
}

func TestIncrementalChangelogScanPlansDeletionVectors(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-dv/data.parquet", map[int]any{1000: int32(1)})
	posDelete := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-dv/pos.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	dv := newChangelogDeletionVector(t, spec, "mem://default/changelog-dv/dv.puffin", data.FilePath())
	tbl := writeChangelogScanTable(t, "changelog-dv", []changelogScanStep{
		{
			operation: OpAppend,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, data)},
		},
		{
			operation: OpDelete,
			deletes:   []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, posDelete)},
		},
		{
			operation:       OpDelete,
			manifestVersion: 3,
			deletes:         []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 3, 3, dv)},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	rows, ok := tasks[2].(DeletedRowsScanTask)
	require.True(t, ok)
	require.Equal(t, []string{dv.FilePath()}, changelogFilePaths(rows.AddedDeletes()))
	require.Equal(t, []string{posDelete.FilePath()}, changelogFilePaths(rows.ExistingDeletes()))
	require.Empty(t, rows.ScanTask().DeletionVectorFiles)
}

func TestIncrementalChangelogScanAppliesExistingDeletesToRemovedFiles(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-removed/data.parquet", map[int]any{1000: int32(1)})
	posDelete := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-removed/pos.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	tbl := writeChangelogScanTable(t, "changelog-removed", []changelogScanStep{
		{
			operation: OpAppend,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, data)},
		},
		{
			operation: OpDelete,
			deletes:   []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, posDelete)},
		},
		{
			operation: OpOverwrite,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusDELETED, 3, 1, data)},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	removed, ok := tasks[2].(DeletedDataFileScanTask)
	require.True(t, ok)
	require.Equal(t, ChangelogOpDelete, removed.Operation())
	require.Equal(t, 2, removed.ChangeOrdinal())
	require.Equal(t, []string{posDelete.FilePath()}, changelogFilePaths(removed.ExistingDeletes()))

	bounded, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(2).
		ToSnapshot(3).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, bounded, 1)
	removed, ok = bounded[0].(DeletedDataFileScanTask)
	require.True(t, ok)
	require.Equal(t, []string{posDelete.FilePath()}, changelogFilePaths(removed.ExistingDeletes()))
}

func TestIncrementalChangelogScanFiltersDeleteBackedTasks(t *testing.T) {
	tbl := incrementalChangelogDeleteManifestTable(t)
	filter := iceberg.EqualTo(iceberg.Reference("id"), int32(3))

	tasks, err := tbl.NewIncrementalChangelogScan(WithRowFilter(filter)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, "mem://default/changelog-delete/data-3.parquet", tasks[0].ScanTask().File.FilePath())
	require.Equal(t, ChangelogOpInsert, tasks[0].Operation())
}

func TestIncrementalChangelogScanFiltersDeletedRowsByPartition(t *testing.T) {
	spec := partitionedSpec()
	partOne := newTestDataFile(t, spec, "mem://default/changelog-part/data-1.parquet", map[int]any{1000: int32(1)})
	partTwo := newTestDataFile(t, spec, "mem://default/changelog-part/data-2.parquet", map[int]any{1000: int32(2)})
	deleteOne := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-part/delete-1.parquet", map[int]any{1000: int32(1)}, partOne.FilePath())
	deleteTwo := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-part/delete-2.parquet", map[int]any{1000: int32(2)}, partTwo.FilePath())
	tbl := writeChangelogScanTable(t, "changelog-part", []changelogScanStep{
		{
			operation: OpAppend,
			data: []iceberg.ManifestEntry{
				changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, partOne),
				changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, partTwo),
			},
		},
		{
			operation: OpDelete,
			deletes: []iceberg.ManifestEntry{
				changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, deleteOne),
				changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, deleteTwo),
			},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan(
		WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int32(1))),
	).PlanFiles(context.Background())
	require.NoError(t, err)

	var deleted []DeletedRowsScanTask
	for _, task := range tasks {
		require.NotEqual(t, partTwo.FilePath(), task.ScanTask().File.FilePath())
		rows, ok := task.(DeletedRowsScanTask)
		if !ok {
			continue
		}
		deleted = append(deleted, rows)
	}
	require.Len(t, deleted, 1)
	require.Equal(t, partOne.FilePath(), deleted[0].ScanTask().File.FilePath())
	require.Equal(t, []string{deleteOne.FilePath()}, changelogFilePaths(deleted[0].AddedDeletes()))
}

func TestIncrementalChangelogScanAppliesDeletesBySequence(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-seq/data.parquet", map[int]any{1000: int32(1)})
	dataSeq := int64(5)

	tests := []struct {
		name      string
		kind      string
		deleteSeq int64
		want      bool
	}{
		{name: "position delete below data sequence", kind: "pos", deleteSeq: 4, want: false},
		{name: "position delete at data sequence", kind: "pos", deleteSeq: 5, want: true},
		{name: "position delete above data sequence", kind: "pos", deleteSeq: 6, want: true},
		{name: "equality delete below data sequence", kind: "eq", deleteSeq: 4, want: false},
		{name: "equality delete at data sequence", kind: "eq", deleteSeq: 5, want: false},
		{name: "equality delete above data sequence", kind: "eq", deleteSeq: 6, want: true},
		{name: "deletion vector below data sequence", kind: "dv", deleteSeq: 4, want: false},
		{name: "deletion vector at data sequence", kind: "dv", deleteSeq: 5, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var deleteFile iceberg.DataFile
			version := 2
			switch tt.kind {
			case "pos":
				deleteFile = newTestPosDeleteFileForSpec(t, spec,
					"mem://default/changelog-seq/pos.parquet", map[int]any{1000: int32(1)}, data.FilePath())
			case "eq":
				deleteFile = newChangelogEqDelete(t, spec,
					"mem://default/changelog-seq/eq.parquet", map[int]any{1000: int32(1)})
			default:
				deleteFile = newChangelogDeletionVector(t, spec, "mem://default/changelog-seq/dv.puffin", data.FilePath())
				version = 3
			}
			tbl := writeChangelogScanTable(t, "changelog-seq", []changelogScanStep{
				{
					operation: OpAppend,
					data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, dataSeq, data)},
				},
				{
					operation:       OpDelete,
					manifestVersion: version,
					deletes:         []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 2, tt.deleteSeq, deleteFile)},
				},
			})

			tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
			require.NoError(t, err)
			if !tt.want {
				require.Len(t, tasks, 1)
				require.Equal(t, ChangelogOpInsert, tasks[0].Operation())

				return
			}
			require.Len(t, tasks, 2)
			rows, ok := tasks[1].(DeletedRowsScanTask)
			require.True(t, ok)
			require.Equal(t, []string{deleteFile.FilePath()}, changelogFilePaths(rows.AddedDeletes()))
		})
	}
}

func TestIncrementalChangelogScanAppliesEveryDeleteInOneSnapshot(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-multi/data.parquet", map[int]any{1000: int32(1)})
	first := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-multi/first.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	second := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-multi/second.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	tbl := writeChangelogScanTable(t, "changelog-multi", []changelogScanStep{
		{
			operation: OpAppend,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, data)},
		},
		{
			operation: OpDelete,
			deletes: []iceberg.ManifestEntry{
				changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, first),
				changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, second),
			},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	rows, ok := tasks[1].(DeletedRowsScanTask)
	require.True(t, ok)
	require.ElementsMatch(t, []string{first.FilePath(), second.FilePath()}, changelogFilePaths(rows.AddedDeletes()))
}

func TestIncrementalChangelogScanFoldsReplaceDeleteRewrites(t *testing.T) {
	spec := partitionedSpec()
	data := newTestDataFile(t, spec, "mem://default/changelog-replace-del/data.parquet", map[int]any{1000: int32(1)})
	oldDelete := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-replace-del/old.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	newDelete := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-replace-del/new.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	tbl := writeChangelogScanTable(t, "changelog-replace-del", []changelogScanStep{
		{
			operation: OpAppend,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, data)},
		},
		{
			operation: OpDelete,
			deletes:   []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, oldDelete)},
		},
		{
			operation: OpReplace,
			deletes: []iceberg.ManifestEntry{
				changelogManifestEntry(iceberg.EntryStatusDELETED, 3, 2, oldDelete),
				changelogManifestEntry(iceberg.EntryStatusADDED, 3, 3, newDelete),
			},
		},
		{
			operation: OpOverwrite,
			data:      []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusDELETED, 4, 1, data)},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	for _, task := range tasks {
		require.NotEqual(t, int64(3), task.CommitSnapshotID())
	}
	removed, ok := tasks[2].(DeletedDataFileScanTask)
	require.True(t, ok)
	require.Equal(t, []string{newDelete.FilePath()}, changelogFilePaths(removed.ExistingDeletes()))
}

func TestIncrementalChangelogScanKeepsSiblingDeletionVector(t *testing.T) {
	spec := partitionedSpec()
	dataA := newTestDataFile(t, spec, "mem://default/changelog-puffin/a.parquet", map[int]any{1000: int32(1)})
	dataB := newTestDataFile(t, spec, "mem://default/changelog-puffin/b.parquet", map[int]any{1000: int32(2)})
	puffin := "mem://default/changelog-puffin/both.puffin"
	dvA := newSharedDeletionVector(t, spec, puffin, dataA.FilePath(), 0)
	dvB := newSharedDeletionVector(t, spec, puffin, dataB.FilePath(), 64)
	rewrittenA := newSharedDeletionVector(t, spec, "mem://default/changelog-puffin/a.puffin", dataA.FilePath(), 0)
	later := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-puffin/later.parquet", map[int]any{1000: int32(2)}, dataB.FilePath())
	tbl := writeChangelogScanTable(t, "changelog-puffin", []changelogScanStep{
		{
			operation: OpAppend,
			data: []iceberg.ManifestEntry{
				changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, dataA),
				changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, dataB),
			},
		},
		{
			operation:       OpDelete,
			manifestVersion: 3,
			deletes: []iceberg.ManifestEntry{
				changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, dvA),
				changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, dvB),
			},
		},
		{
			operation:       OpDelete,
			manifestVersion: 3,
			deletes: []iceberg.ManifestEntry{
				changelogManifestEntry(iceberg.EntryStatusDELETED, 3, 2, dvA),
				changelogManifestEntry(iceberg.EntryStatusADDED, 3, 3, rewrittenA),
			},
		},
		{
			operation:       OpDelete,
			manifestVersion: 3,
			deletes:         []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 4, 4, later)},
		},
	})

	tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(context.Background())
	require.NoError(t, err)
	var got DeletedRowsScanTask
	var found bool
	for _, task := range tasks {
		rows, ok := task.(DeletedRowsScanTask)
		if !ok || rows.ScanTask().File.FilePath() != dataB.FilePath() || rows.CommitSnapshotID() != 4 {
			continue
		}
		got = rows
		found = true
	}
	require.True(t, found)
	require.Equal(t, []string{later.FilePath()}, changelogFilePaths(got.AddedDeletes()))
	require.Equal(t, []string{dvB.FilePath()}, changelogFilePaths(got.ExistingDeletes()))
	require.Equal(t, dataB.FilePath(), referencedDataFilePath(got.ExistingDeletes()[0]))
}

func TestIncrementalChangelogScanPlansOverwriteWithExpiredParent(t *testing.T) {
	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	data := newTestDataFile(t, spec, "mem://default/changelog-expired-overwrite/data.parquet", map[int]any{1000: int32(1)})
	snapshotID := int64(2)
	dataManifest := writeChangelogDataManifest(t, fs, spec, schema,
		"mem://default/changelog-expired-overwrite/metadata/data.avro", snapshotID, 2,
		[]iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusDELETED, snapshotID, 1, data)})
	listPath := "mem://default/changelog-expired-overwrite/metadata/snap-2.avro"
	writeChangelogManifestList(t, fs, listPath, snapshotID, 2, []iceberg.ManifestFile{dataManifest})

	expiredParent := int64(1)
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:       snapshotID,
		ParentSnapshotID: &expiredParent,
		TimestampMs:      2000,
		ManifestList:     listPath,
		SequenceNumber:   snapshotID,
		SchemaID:         &schema.ID,
		Summary:          &Summary{Operation: OpOverwrite},
	}}
	txn.meta.snapshotLog = []SnapshotLogEntry{{SnapshotID: snapshotID, TimestampMs: 2000}}
	txn.meta.currentSnapshotID = &snapshotID
	meta, err := txn.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"changelog-expired-overwrite"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)

	tasks, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(expiredParent).
		ToSnapshot(snapshotID).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	_, ok := tasks[0].(DeletedDataFileScanTask)
	require.True(t, ok)
}

func TestIncrementalChangelogScanKeepsCarriedDeleteWhenParentExpired(t *testing.T) {
	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	data := newTestDataFile(t, spec, "mem://default/changelog-expired-delete/data.parquet", map[int]any{1000: int32(1)})
	pos := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/changelog-expired-delete/pos.parquet", map[int]any{1000: int32(1)}, data.FilePath())
	snapshotID := int64(2)
	expiredParent := int64(1)
	dataManifest := writeChangelogDataManifest(t, fs, spec, schema,
		"mem://default/changelog-expired-delete/metadata/data.avro", snapshotID, 2,
		[]iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusDELETED, snapshotID, 1, data)})
	deleteManifest := writeChangelogDeleteManifest(t, fs, spec, schema,
		"mem://default/changelog-expired-delete/metadata/deletes.avro", snapshotID, 2,
		[]iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusEXISTING, expiredParent, 1, pos)})
	listPath := "mem://default/changelog-expired-delete/metadata/snap-2.avro"
	writeChangelogManifestList(t, fs, listPath, snapshotID, 2, []iceberg.ManifestFile{dataManifest, deleteManifest})

	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:       snapshotID,
		ParentSnapshotID: &expiredParent,
		TimestampMs:      2000,
		ManifestList:     listPath,
		SequenceNumber:   snapshotID,
		SchemaID:         &schema.ID,
		Summary:          &Summary{Operation: OpOverwrite},
	}}
	txn.meta.snapshotLog = []SnapshotLogEntry{{SnapshotID: snapshotID, TimestampMs: 2000}}
	txn.meta.currentSnapshotID = &snapshotID
	meta, err := txn.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"changelog-expired-delete"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)

	tasks, err := tbl.NewIncrementalChangelogScan().
		FromSnapshotExclusive(expiredParent).
		ToSnapshot(snapshotID).
		PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	removed, ok := tasks[0].(DeletedDataFileScanTask)
	require.True(t, ok)
	require.Equal(t, []string{pos.FilePath()}, changelogFilePaths(removed.ExistingDeletes()))
}

func TestLiveManifestEntriesUsesTheLatestStatus(t *testing.T) {
	file := changelogTestDataFile(t, "data.parquet", iceberg.EntryContentData, iceberg.ParquetFile)
	// Snapshot IDs are not chronological. The delete is snapshot 3, whose
	// sequence number is 2. The add is snapshot 20, whose sequence number is 1.
	added := changelogManifestEntry(iceberg.EntryStatusADDED, 20, 1, file)
	deleted := changelogManifestEntry(iceberg.EntryStatusDELETED, 3, 1, file)
	readded := changelogManifestEntry(iceberg.EntryStatusADDED, 2, 1, file)
	sameSnapshotDeleted := changelogManifestEntry(iceberg.EntryStatusDELETED, 20, 1, file)
	sequences := func(snapshotID int64) (int64, bool) {
		switch snapshotID {
		case 20:
			return 1, true
		case 3:
			return 2, true
		case 2:
			return 3, true
		default:
			return 0, false
		}
	}

	for _, entries := range [][]iceberg.ManifestEntry{
		{added, deleted},
		{deleted, added},
		{added, sameSnapshotDeleted},
		{sameSnapshotDeleted, added},
	} {
		require.Empty(t, liveManifestEntries(entries, sequences))
	}

	for _, entries := range [][]iceberg.ManifestEntry{
		{added, deleted, readded},
		{readded, deleted, added},
	} {
		got := liveManifestEntries(entries, sequences)
		require.Len(t, got, 1)
		require.Equal(t, iceberg.EntryStatusADDED, got[0].Status())
		require.Equal(t, int64(2), got[0].SnapshotID())
	}
}

func TestEntriesBeforeDropsRemovedDeleteFiles(t *testing.T) {
	keep := changelogTestDataFile(t, "keep.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)
	drop := changelogTestDataFile(t, "drop.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)
	state := &changelogDeleteState{
		snapshots: []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}, {SnapshotID: 3}},
		preRange:  []iceberg.ManifestEntry{changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, drop)},
		added: map[int64][]iceberg.ManifestEntry{
			1: {changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, keep)},
		},
		removed: map[int64]map[string]struct{}{
			2: {drop.FilePath(): {}},
		},
	}

	require.Equal(t, []string{keep.FilePath()}, changelogEntryPaths(state.entriesBefore(3)))

	readded := &changelogDeleteState{
		snapshots: state.snapshots,
		preRange:  state.preRange,
		added: map[int64][]iceberg.ManifestEntry{
			1: {changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, keep)},
			2: {changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, drop)},
		},
		removed: state.removed,
	}
	require.Equal(t, []string{keep.FilePath(), drop.FilePath()}, changelogEntryPaths(readded.entriesBefore(3)))
}

func TestKeepNewestDeletionVector(t *testing.T) {
	spec := partitionedSpec()
	older := newChangelogDeletionVector(t, spec, "older.puffin", "data.parquet")
	newer := newChangelogDeletionVector(t, spec, "newer.puffin", "data.parquet")
	pos := changelogTestDataFile(t, "pos.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)
	got := keepNewestDeletionVector([]iceberg.ManifestEntry{
		changelogManifestEntry(iceberg.EntryStatusADDED, 1, 1, older),
		changelogManifestEntry(iceberg.EntryStatusADDED, 2, 2, pos),
		changelogManifestEntry(iceberg.EntryStatusADDED, 3, 3, newer),
	})
	require.Equal(t, []string{pos.FilePath(), newer.FilePath()}, changelogEntryPaths(got))
}

func changelogFilePaths(files []iceberg.DataFile) []string {
	out := make([]string, len(files))
	for i, file := range files {
		out[i] = file.FilePath()
	}

	return out
}

func changelogEntryPaths(entries []iceberg.ManifestEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = entry.DataFile().FilePath()
	}

	return out
}

func changelogManifestEntry(status iceberg.ManifestEntryStatus, snapshotID, sequence int64, file iceberg.DataFile) iceberg.ManifestEntry {
	return iceberg.NewManifestEntry(status, &snapshotID, &sequence, &sequence, file)
}

func newChangelogEqDelete(t *testing.T, spec iceberg.PartitionSpec, path string, partition map[int]any) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes, path, iceberg.ParquetFile, partition, nil, nil, 1, 1)
	require.NoError(t, err)

	return builder.EqualityFieldIDs([]int{1}).Build()
}

func newChangelogDeletionVector(t *testing.T, spec iceberg.PartitionSpec, path, dataPath string) iceberg.DataFile {
	t.Helper()

	return newSharedDeletionVector(t, spec, path, dataPath, 0)
}

func newSharedDeletionVector(t *testing.T, spec iceberg.PartitionSpec, path, dataPath string, offset int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, path, iceberg.PuffinFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)

	return builder.ReferencedDataFile(dataPath).ContentOffset(offset).ContentSizeInBytes(16).Build()
}

type changelogScanStep struct {
	operation       Operation
	manifestVersion int
	data            []iceberg.ManifestEntry
	deletes         []iceberg.ManifestEntry
}

func writeChangelogScanTable(t *testing.T, location string, steps []changelogScanStep) *Table {
	t.Helper()

	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	var manifestList []iceberg.ManifestFile
	listVersion := 2
	snapshots := make([]Snapshot, 0, len(steps))
	log := make([]SnapshotLogEntry, 0, len(steps))
	var parent *int64
	for i, step := range steps {
		snapshotID := int64(i + 1)
		version := step.manifestVersion
		if version == 0 {
			version = 2
		}
		if version > listVersion {
			listVersion = version
		}
		var written []iceberg.ManifestFile
		if len(step.data) > 0 {
			path := "mem://default/" + location + "/metadata/data-" + fmtSnapshot(snapshotID) + ".avro"
			written = append(written, writeChangelogDataManifest(t, fs, spec, schema, path, snapshotID, version, step.data))
		}
		if len(step.deletes) > 0 {
			path := "mem://default/" + location + "/metadata/deletes-" + fmtSnapshot(snapshotID) + ".avro"
			written = append(written, writeChangelogDeleteManifest(t, fs, spec, schema, path, snapshotID, version, step.deletes))
		}
		listPath := "mem://default/" + location + "/metadata/snap-" + fmtSnapshot(snapshotID) + ".avro"
		manifestList = writeChangelogManifestList(t, fs, listPath, snapshotID, listVersion, append(manifestList, written...))
		snapshots = append(snapshots, Snapshot{
			SnapshotID:       snapshotID,
			ParentSnapshotID: parent,
			TimestampMs:      snapshotID * 1000,
			ManifestList:     listPath,
			SequenceNumber:   snapshotID,
			SchemaID:         &schema.ID,
			Summary:          &Summary{Operation: step.operation},
		})
		log = append(log, SnapshotLogEntry{SnapshotID: snapshotID, TimestampMs: snapshotID * 1000})
		id := snapshotID
		parent = &id
	}
	txn.meta.snapshotList = snapshots
	txn.meta.snapshotLog = log
	if len(snapshots) > 0 {
		current := snapshots[len(snapshots)-1].SnapshotID
		txn.meta.currentSnapshotID = &current
	}
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{location}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return fs, nil
	}, nil)
}

func fmtSnapshot(snapshotID int64) string {
	return strconv.FormatInt(snapshotID, 10)
}

func writeChangelogDataManifest(
	t *testing.T,
	fs iceio.WriteFileIO,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	path string,
	snapshotID int64,
	version int,
	entries []iceberg.ManifestEntry,
) iceberg.ManifestFile {
	t.Helper()

	var buf bytes.Buffer
	manifest, err := iceberg.WriteManifest(path, &buf, version, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

	return manifest
}

func writeChangelogDeleteManifest(
	t *testing.T,
	fs iceio.WriteFileIO,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	path string,
	snapshotID int64,
	version int,
	entries []iceberg.ManifestEntry,
) iceberg.ManifestFile {
	t.Helper()

	var buf bytes.Buffer
	writer, err := iceberg.NewManifestWriter(version, &buf, spec, schema, snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	for _, entry := range entries {
		switch entry.Status() {
		case iceberg.EntryStatusDELETED:
			require.NoError(t, writer.Delete(entry))
		case iceberg.EntryStatusEXISTING:
			require.NoError(t, writer.Existing(entry))
		default:
			require.NoError(t, writer.Add(entry))
		}
	}
	require.NoError(t, writer.Close())
	manifest, err := writer.ToManifestFile(path, int64(buf.Len()),
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

	return manifest
}

func writeChangelogManifestList(
	t *testing.T,
	fs iceio.WriteFileIO,
	path string,
	snapshotID int64,
	version int,
	manifests []iceberg.ManifestFile,
) []iceberg.ManifestFile {
	t.Helper()

	var buf bytes.Buffer
	sequenceNumber := snapshotID
	require.NoError(t, iceberg.WriteManifestList(version, &buf, snapshotID, nil, &sequenceNumber, 0, manifests))
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))
	listFile, err := fs.Open(context.Background(), path)
	require.NoError(t, err)
	list, err := iceberg.ReadManifestList(listFile)
	require.NoError(t, err)
	require.NoError(t, listFile.Close())

	return list
}

type countingOpenIO struct {
	iceio.IO
	opens     *atomic.Int64
	afterOpen func()
}

func (io *countingOpenIO) Open(_ context.Context, name string) (iceio.File, error) {
	io.opens.Add(1)
	if io.afterOpen != nil {
		io.afterOpen()
	}

	return io.IO.Open(context.Background(), name)
}
