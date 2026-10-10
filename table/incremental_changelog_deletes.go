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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
)

// changelogTaskPlan is the snapshot range and the data-file changes already
// read for one changelog plan.
type changelogTaskPlan struct {
	scan                *Scan
	fs                  io.IO
	snapshots           []Snapshot
	manifestsBySnapshot map[int64][]iceberg.ManifestFile
	deleteManifests     map[int64][]iceberg.ManifestFile
	dataEntries         []iceberg.ManifestEntry
	snapshotOrdinals    map[int64]int
	schema              *iceberg.Schema
	partitionFilters    *keyDefaultMapErr[int, iceberg.BooleanExpression]
	residual            iceberg.BooleanExpression
	metrics             *scanMetricsAccumulator
}

// changelogDeleteIndex matches delete files to a data file with the same
// sequence rules as a normal scan.
type changelogDeleteIndex struct {
	pos *positionalDeleteIndex
	dv  map[string]iceberg.ManifestEntry
	eq  *equalityDeleteIndex
}

// changelogDeleteState tracks delete files added and removed inside the
// changelog range, plus delete files that were already live before it.
type changelogDeleteState struct {
	active    bool
	snapshots []Snapshot
	preRange  []iceberg.ManifestEntry
	added     map[int64][]iceberg.ManifestEntry
	removed   map[int64]map[string]struct{}
	specs     partitionSpecLookup
	schema    *iceberg.Schema

	addedCache  map[int64]*changelogDeleteIndex
	beforeCache map[int64]*changelogDeleteIndex
}

func (s *IncrementalChangelogScan) planChangelogTasks(ctx context.Context, plan changelogTaskPlan) ([]plannedChangelogTask, error) {
	state, err := s.loadChangelogDeleteState(ctx, plan)
	if err != nil {
		return nil, err
	}

	changedPaths := make(map[int64]map[string]struct{})
	tasks := make([]plannedChangelogTask, 0, len(plan.dataEntries))
	for _, entry := range plan.dataEntries {
		ordinal, ok := plan.snapshotOrdinals[entry.SnapshotID()]
		if !ok {
			continue
		}
		paths := changedPaths[entry.SnapshotID()]
		if paths == nil {
			paths = make(map[string]struct{})
			changedPaths[entry.SnapshotID()] = paths
		}
		paths[entry.DataFile().FilePath()] = struct{}{}

		deletes, err := state.deletesFor(entry)
		if err != nil {
			return nil, fmt.Errorf("incremental changelog scan snapshot %d: %w", entry.SnapshotID(), err)
		}
		task, err := newChangelogScanTask(entry, ordinal, plan.residual, deletes)
		if err != nil {
			return nil, fmt.Errorf("incremental changelog scan snapshot %d: %w", entry.SnapshotID(), err)
		}
		tasks = append(tasks, plannedChangelogTask{task: task})
	}

	rowTasks, err := s.planDeletedRowTasks(ctx, plan, state, changedPaths)
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, rowTasks...)
	sortChangelogTasks(tasks)

	return tasks, nil
}

func (s *IncrementalChangelogScan) loadChangelogDeleteState(ctx context.Context, plan changelogTaskPlan) (*changelogDeleteState, error) {
	state := &changelogDeleteState{
		snapshots: plan.snapshots,
		added:     make(map[int64][]iceberg.ManifestEntry),
		removed:   make(map[int64]map[string]struct{}),
		specs:     plan.scan.metadata,
		schema:    plan.schema,
	}
	for _, snapshot := range plan.snapshots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		manifests := plan.deleteManifests[snapshot.SnapshotID]
		if len(manifests) == 0 {
			continue
		}
		filtered, err := plan.scan.filterManifestsWithSchemaOptions(
			manifests, plan.schema, plan.metrics, plan.partitionFilters, true)
		if err != nil {
			return nil, err
		}
		entries, err := readManifestEntries(ctx, plan.fs, filtered, false, false)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.SnapshotID() != snapshot.SnapshotID {
				continue
			}
			if entry.Status() == iceberg.EntryStatusDELETED {
				if state.removed[snapshot.SnapshotID] == nil {
					state.removed[snapshot.SnapshotID] = make(map[string]struct{})
				}
				state.removed[snapshot.SnapshotID][entry.DataFile().FilePath()] = struct{}{}

				continue
			}
			state.added[snapshot.SnapshotID] = append(state.added[snapshot.SnapshotID], entry)
		}
	}

	deletedData := false
	for _, entry := range plan.dataEntries {
		if entry.Status() == iceberg.EntryStatusDELETED {
			deletedData = true

			break
		}
	}
	for _, entries := range state.added {
		if len(entries) > 0 {
			state.active = true

			break
		}
	}
	if len(state.removed) > 0 || deletedData {
		state.active = true
	}
	if !state.active {
		return state, nil
	}

	preRange, err := s.liveDeletesBeforeRange(ctx, plan.fs, plan.snapshots)
	if err != nil {
		return nil, err
	}
	state.preRange = preRange

	return state, nil
}

// liveDeletesBeforeRange returns delete files that were already live on the
// snapshot immediately before the changelog range.
func (s *IncrementalChangelogScan) liveDeletesBeforeRange(ctx context.Context, fs io.IO, snapshots []Snapshot) ([]iceberg.ManifestEntry, error) {
	parent := s.snapshotBeforeRange(snapshots)
	if parent == nil {
		return nil, nil
	}
	manifests, err := parent.Manifests(fs)
	if err != nil {
		return nil, err
	}
	deleteManifests := make([]iceberg.ManifestFile, 0)
	for _, manifest := range manifests {
		if manifest.ManifestContent() == iceberg.ManifestContentDeletes {
			deleteManifests = append(deleteManifests, manifest)
		}
	}
	entries, err := readManifestEntries(ctx, fs, deleteManifests, false, false)
	if err != nil {
		return nil, err
	}

	return liveManifestEntries(entries), nil
}

func (s *IncrementalChangelogScan) snapshotBeforeRange(snapshots []Snapshot) *Snapshot {
	if s.fromSnapshotID != nil && !s.fromInclusive {
		return s.scan.metadata.SnapshotByID(*s.fromSnapshotID)
	}
	if len(snapshots) == 0 || snapshots[0].ParentSnapshotID == nil {
		return nil
	}

	return s.scan.metadata.SnapshotByID(*snapshots[0].ParentSnapshotID)
}

func (s *IncrementalChangelogScan) planDeletedRowTasks(
	ctx context.Context,
	plan changelogTaskPlan,
	state *changelogDeleteState,
	changedPaths map[int64]map[string]struct{},
) ([]plannedChangelogTask, error) {
	if state == nil || !state.active {
		return nil, nil
	}
	matcher, err := plan.scan.dataFileMatcher(plan.schema, plan.partitionFilters)
	if err != nil {
		return nil, err
	}

	var tasks []plannedChangelogTask
	for _, snapshot := range plan.snapshots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(state.added[snapshot.SnapshotID]) == 0 {
			continue
		}
		addedIndex, err := state.addedIndex(snapshot.SnapshotID)
		if err != nil {
			return nil, fmt.Errorf("incremental changelog scan snapshot %d: %w", snapshot.SnapshotID, err)
		}
		live, err := liveDataEntries(ctx, plan.fs, plan.manifestsBySnapshot[snapshot.SnapshotID])
		if err != nil {
			return nil, err
		}
		ordinal := plan.snapshotOrdinals[snapshot.SnapshotID]
		seen := make(map[string]struct{})
		for _, entry := range live {
			path := entry.DataFile().FilePath()
			if _, changed := changedPaths[snapshot.SnapshotID][path]; changed {
				continue
			}
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}

			keep, err := matcher(entry.DataFile())
			if err != nil {
				return nil, err
			}
			if !keep {
				continue
			}
			added, err := addedIndex.forDataFile(entry)
			if err != nil {
				return nil, fmt.Errorf("incremental changelog scan snapshot %d: %w", snapshot.SnapshotID, err)
			}
			if len(added) == 0 {
				continue
			}
			var existing []iceberg.DataFile
			before, err := state.beforeIndex(snapshot.SnapshotID)
			if err != nil {
				return nil, fmt.Errorf("incremental changelog scan snapshot %d: %w", snapshot.SnapshotID, err)
			}
			if before != nil {
				existing, err = before.forDataFile(entry)
				if err != nil {
					return nil, fmt.Errorf("incremental changelog scan snapshot %d: %w", snapshot.SnapshotID, err)
				}
			}

			task, err := NewDeletedRowsScanTask(entry.DataFile(), added, existing, ordinal, snapshot.SnapshotID)
			if err != nil {
				return nil, fmt.Errorf("incremental changelog scan snapshot %d: %w", snapshot.SnapshotID, err)
			}
			configureChangelogFileTask(&task.FileScanTask, entry, plan.residual)
			tasks = append(tasks, plannedChangelogTask{task: task})
		}
	}

	return tasks, nil
}

func (st *changelogDeleteState) deletesFor(entry iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	if st == nil || !st.active {
		return nil, nil
	}

	var (
		idx *changelogDeleteIndex
		err error
	)
	switch entry.Status() {
	case iceberg.EntryStatusADDED:
		idx, err = st.addedIndex(entry.SnapshotID())
	case iceberg.EntryStatusDELETED:
		idx, err = st.beforeIndex(entry.SnapshotID())
	default:
		return nil, fmt.Errorf("%w: unknown manifest entry status %d", ErrInvalidMetadata, entry.Status())
	}
	if err != nil || idx == nil {
		return nil, err
	}

	return idx.forDataFile(entry)
}

func (st *changelogDeleteState) addedIndex(snapshotID int64) (*changelogDeleteIndex, error) {
	if st.addedCache == nil {
		st.addedCache = make(map[int64]*changelogDeleteIndex)
	}
	if idx, ok := st.addedCache[snapshotID]; ok {
		return idx, nil
	}
	idx, err := buildChangelogDeleteIndex(st.added[snapshotID], st.specs, st.schema)
	if err != nil {
		return nil, err
	}
	st.addedCache[snapshotID] = idx

	return idx, nil
}

func (st *changelogDeleteState) beforeIndex(snapshotID int64) (*changelogDeleteIndex, error) {
	if st.beforeCache == nil {
		st.beforeCache = make(map[int64]*changelogDeleteIndex)
	}
	if idx, ok := st.beforeCache[snapshotID]; ok {
		return idx, nil
	}
	idx, err := buildChangelogDeleteIndex(st.entriesBefore(snapshotID), st.specs, st.schema)
	if err != nil {
		return nil, err
	}
	st.beforeCache[snapshotID] = idx

	return idx, nil
}

// entriesBefore returns delete-file entries that applied strictly before
// snapshotID. Removals drop a path from the running set. A path removed and
// added again in one snapshot stays live as the added entry.
func (st *changelogDeleteState) entriesBefore(snapshotID int64) []iceberg.ManifestEntry {
	removed := make(map[string]struct{})
	var accumulated []iceberg.ManifestEntry
	for _, snapshot := range st.snapshots {
		if snapshot.SnapshotID == snapshotID {
			break
		}
		currentRemoved := st.removed[snapshot.SnapshotID]
		if len(currentRemoved) > 0 {
			for path := range currentRemoved {
				removed[path] = struct{}{}
			}
			accumulated = dropManifestEntries(accumulated, currentRemoved)
		}
		accumulated = append(accumulated, st.added[snapshot.SnapshotID]...)
	}

	out := make([]iceberg.ManifestEntry, 0, len(st.preRange)+len(accumulated))
	for _, entry := range st.preRange {
		if _, gone := removed[entry.DataFile().FilePath()]; gone {
			continue
		}
		out = append(out, entry)
	}
	out = append(out, accumulated...)

	return keepNewestDeletionVector(out)
}

func (idx *changelogDeleteIndex) forDataFile(entry iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	if idx == nil {
		return nil, nil
	}
	task, err := fileScanTaskForDataEntry(entry, idx.pos, idx.dv, idx.eq)
	if err != nil {
		return nil, err
	}
	files := allDeleteFiles(task)
	if len(files) == 0 {
		return nil, nil
	}

	return files, nil
}

func buildChangelogDeleteIndex(entries []iceberg.ManifestEntry, specs partitionSpecLookup, schema *iceberg.Schema) (*changelogDeleteIndex, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	entries = keepNewestDeletionVector(entries)
	classified, err := classifyManifestEntries(entries)
	if err != nil {
		return nil, err
	}
	if len(classified.dataEntries) > 0 {
		return nil, fmt.Errorf("%w: expected delete file, got %s",
			ErrInvalidMetadata, classified.dataEntries[0].DataFile().FilePath())
	}

	pos, err := buildPositionalDeleteIndex(classified.positionalDeleteEntries)
	if err != nil {
		return nil, err
	}
	dv, err := buildDVIndex(classified.dvEntries)
	if err != nil {
		return nil, err
	}
	eq, err := buildEqualityDeleteIndex(classified.equalityDeleteEntries, specs, schema)
	if err != nil {
		return nil, err
	}

	return &changelogDeleteIndex{pos: pos, dv: dv, eq: eq}, nil
}

func (scan *Scan) dataFileMatcher(
	schema *iceberg.Schema,
	partitionFilters *keyDefaultMapErr[int, iceberg.BooleanExpression],
) (func(iceberg.DataFile) (bool, error), error) {
	metricsEval, err := newInclusiveMetricsEvaluator(
		schema,
		scan.rowFilter,
		scan.caseSensitive,
		scan.options["include_empty_files"] == "true",
	)
	if err != nil {
		return nil, err
	}
	partitionEvaluators := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.DataFile) (bool, error), error) {
		return buildPartitionEvaluator(specID, scan.metadata, schema, partitionFilters, scan.caseSensitive)
	})

	return func(file iceberg.DataFile) (bool, error) {
		partEval, err := partitionEvaluators.Get(int(file.SpecID()))
		if err != nil {
			return false, fmt.Errorf("failed to build partition evaluator for spec %d: %w", file.SpecID(), err)
		}
		use, err := partEval(file)
		if err != nil || !use {
			return use, err
		}

		return metricsEval(file)
	}, nil
}

func liveDataEntries(ctx context.Context, fs io.IO, manifests []iceberg.ManifestFile) ([]iceberg.ManifestEntry, error) {
	dataManifests := make([]iceberg.ManifestFile, 0, len(manifests))
	for _, manifest := range manifests {
		if manifest.ManifestContent() == iceberg.ManifestContentData {
			dataManifests = append(dataManifests, manifest)
		}
	}
	entries, err := readManifestEntries(ctx, fs, dataManifests, false, false)
	if err != nil {
		return nil, err
	}
	dataEntries := make([]iceberg.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.DataFile().ContentType() == iceberg.EntryContentData {
			dataEntries = append(dataEntries, entry)
		}
	}

	return liveManifestEntries(dataEntries), nil
}

// liveManifestEntries keeps the last entry for each path and drops paths whose
// last entry is a deletion. Manifest order is oldest to newest, so a later
// add brings a path back.
func liveManifestEntries(entries []iceberg.ManifestEntry) []iceberg.ManifestEntry {
	latest := make(map[string]iceberg.ManifestEntry, len(entries))
	order := make([]string, 0, len(entries))
	for _, entry := range entries {
		path := entry.DataFile().FilePath()
		if _, ok := latest[path]; !ok {
			order = append(order, path)
		}
		latest[path] = entry
	}
	out := make([]iceberg.ManifestEntry, 0, len(latest))
	for _, path := range order {
		entry := latest[path]
		if entry.Status() == iceberg.EntryStatusDELETED {
			continue
		}
		out = append(out, entry)
	}

	return out
}

func readManifestEntries(
	ctx context.Context,
	fs io.IO,
	manifests []iceberg.ManifestFile,
	discardDeleted, discardExisting bool,
) ([]iceberg.ManifestEntry, error) {
	if len(manifests) == 0 {
		return nil, nil
	}
	keep := func(iceberg.DataFile) (bool, error) { return true, nil }
	out := make([]iceberg.ManifestEntry, 0)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err := openManifestWithOptions(fs, manifest, keep, keep, discardDeleted, discardExisting)
		if err != nil {
			return nil, err
		}
		out = append(out, entries...)
	}

	return out, nil
}

func dropManifestEntries(entries []iceberg.ManifestEntry, paths map[string]struct{}) []iceberg.ManifestEntry {
	if len(paths) == 0 {
		return entries
	}
	out := make([]iceberg.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		if _, gone := paths[entry.DataFile().FilePath()]; gone {
			continue
		}
		out = append(out, entry)
	}

	return out
}

// keepNewestDeletionVector leaves one deletion vector per referenced data
// file. Later entries win. A deletion vector is cumulative, so the newest one
// already contains positions deleted by the ones it replaced.
func keepNewestDeletionVector(entries []iceberg.ManifestEntry) []iceberg.ManifestEntry {
	latest := make(map[string]int)
	drop := make(map[int]struct{})
	for i, entry := range entries {
		file := entry.DataFile()
		if !IsDeletionVector(file) {
			continue
		}
		ref := referencedDataFilePath(file)
		if ref == "" {
			continue
		}
		if prev, ok := latest[ref]; ok {
			drop[prev] = struct{}{}
		}
		latest[ref] = i
	}
	if len(drop) == 0 {
		return entries
	}
	out := make([]iceberg.ManifestEntry, 0, len(entries)-len(drop))
	for i, entry := range entries {
		if _, ok := drop[i]; ok {
			continue
		}
		out = append(out, entry)
	}

	return out
}

func sortChangelogTasks(tasks []plannedChangelogTask) {
	slices.SortFunc(tasks, func(left, right plannedChangelogTask) int {
		if ordinal := cmp.Compare(left.task.ChangeOrdinal(), right.task.ChangeOrdinal()); ordinal != 0 {
			return ordinal
		}
		if operation := cmp.Compare(changelogOperationOrder(left.task.Operation()), changelogOperationOrder(right.task.Operation())); operation != 0 {
			return operation
		}
		if path := cmp.Compare(left.task.ScanTask().File.FilePath(), right.task.ScanTask().File.FilePath()); path != 0 {
			return path
		}

		return cmp.Compare(changelogTaskKindOrder(left.task), changelogTaskKindOrder(right.task))
	})
}

func changelogTaskKindOrder(task ChangelogScanTask) int {
	switch task.(type) {
	case DeletedDataFileScanTask:
		return 0
	case DeletedRowsScanTask:
		return 1
	case AddedRowsScanTask:
		return 2
	default:
		return 3
	}
}

// changelogMetricsTask counts delete files that produce a row-level change as
// well as deletes already stored on the scan task. Each delete is counted once
// per task, matching a normal scan report.
func changelogMetricsTask(task ChangelogScanTask) FileScanTask {
	file := task.ScanTask()
	rows, ok := task.(DeletedRowsScanTask)
	if !ok {
		return file
	}
	file.DeleteFiles = slices.Clone(file.DeleteFiles)
	file.EqualityDeleteFiles = slices.Clone(file.EqualityDeleteFiles)
	file.DeletionVectorFiles = slices.Clone(file.DeletionVectorFiles)
	for _, deleteFile := range rows.AddedDeletes() {
		switch {
		case IsDeletionVector(deleteFile):
			file.DeletionVectorFiles = append(file.DeletionVectorFiles, deleteFile)
		case deleteFile.ContentType() == iceberg.EntryContentEqDeletes:
			file.EqualityDeleteFiles = append(file.EqualityDeleteFiles, deleteFile)
		default:
			file.DeleteFiles = append(file.DeleteFiles, deleteFile)
		}
	}

	return file
}
