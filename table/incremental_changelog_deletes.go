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
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
)

// changelogTaskPlan is the snapshot range and the data-file changes already
// read for one changelog plan.
type changelogTaskPlan struct {
	scan                *Scan
	fs                  io.IO
	snapshots           []Snapshot
	timeline            []Snapshot
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
	// timeline includes replace snapshots so their delete-file adds and
	// removals update later tasks. Task emission uses plan.snapshots, which
	// omits those replace snapshots.
	timeline := plan.timeline
	if len(timeline) == 0 {
		timeline = plan.snapshots
	}
	state := &changelogDeleteState{
		snapshots: timeline,
		added:     make(map[int64][]iceberg.ManifestEntry),
		removed:   make(map[int64]map[string]struct{}),
		specs:     plan.scan.metadata,
		schema:    plan.schema,
	}
	for _, snapshot := range timeline {
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
				state.removed[snapshot.SnapshotID][deleteEntryKey(entry.DataFile())] = struct{}{}

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

	preRange, err := s.liveDeletesBeforeRange(ctx, plan)
	if err != nil {
		return nil, err
	}
	state.preRange = preRange

	return state, nil
}

// liveDeletesBeforeRange returns delete files that were already live on the
// snapshot immediately before the changelog range. When that snapshot has been
// expired, the first in-range snapshot's own delete manifests are used instead.
func (s *IncrementalChangelogScan) liveDeletesBeforeRange(ctx context.Context, plan changelogTaskPlan) ([]iceberg.ManifestEntry, error) {
	snapshots := timelineSnapshots(plan)
	parent := s.snapshotBeforeRange(snapshots)
	if parent == nil {
		if len(snapshots) == 0 || snapshots[0].ParentSnapshotID == nil {
			return nil, nil
		}

		return s.deletesLiveBeforeExpiredParent(ctx, plan, snapshots[0])
	}
	manifests, err := parent.Manifests(plan.fs)
	if err != nil {
		return nil, err
	}
	entries, err := s.readDeleteManifestEntries(ctx, plan, manifests)
	if err != nil {
		return nil, err
	}

	return liveDeleteEntries(entries, snapshotSequences(plan.scan.metadata)), nil
}

// deletesLiveBeforeExpiredParent rebuilds deletes that applied before the
// range from the first timeline snapshot. This is exact when that snapshot
// records a DELETED entry, under its own snapshot id, for every delete file
// it removed, and carried entries keep the snapshot id that added them. A
// writer that drops a fully removed delete manifest, instead of recording a
// DELETED entry, cannot be reconstructed. When the snapshot summary reports
// more removed-delete-files than the DELETED delete entries in its manifests,
// planning returns an error instead of dropping those deletes. That count is
// taken before partition pruning, so a filtered scan does not look like a
// missing delete file.
func (s *IncrementalChangelogScan) deletesLiveBeforeExpiredParent(
	ctx context.Context, plan changelogTaskPlan, snapshot Snapshot,
) ([]iceberg.ManifestEntry, error) {
	manifests, err := snapshot.Manifests(plan.fs)
	if err != nil {
		return nil, err
	}
	raw, err := readManifestEntries(ctx, plan.fs, deleteContentManifests(manifests), false, false)
	if err != nil {
		return nil, err
	}
	if err := checkRemovedDeleteFiles(snapshot, raw); err != nil {
		return nil, err
	}
	entries, err := s.readDeleteManifestEntries(ctx, plan, manifests)
	if err != nil {
		return nil, err
	}

	return entriesLiveBefore(entries, snapshot.SnapshotID), nil
}

func deleteContentManifests(manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
	out := make([]iceberg.ManifestFile, 0)
	for _, manifest := range manifests {
		if manifest.ManifestContent() == iceberg.ManifestContentDeletes {
			out = append(out, manifest)
		}
	}

	return out
}

// checkRemovedDeleteFiles reports an error when the snapshot summary claims
// more removed delete files than the DELETED delete entries recorded under
// that snapshot id. Entries must be the unfiltered delete-manifest contents.
func checkRemovedDeleteFiles(snapshot Snapshot, entries []iceberg.ManifestEntry) error {
	if snapshot.Summary == nil || len(snapshot.Summary.Properties) == 0 {
		return nil
	}
	raw, ok := snapshot.Summary.Properties[removedDeleteFilesKey]
	if !ok {
		return nil
	}
	reported, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("%w: snapshot %d removed-delete-files %q", iceberg.ErrInvalidArgument, snapshot.SnapshotID, raw)
	}
	recorded := 0
	for _, entry := range entries {
		if entry.Status() != iceberg.EntryStatusDELETED || entry.SnapshotID() != snapshot.SnapshotID {
			continue
		}
		if entry.DataFile().ContentType() == iceberg.EntryContentData {
			continue
		}
		recorded++
	}
	if reported > recorded {
		return fmt.Errorf("%w: snapshot %d %s is %d but recorded %d deleted delete entries",
			iceberg.ErrInvalidArgument, snapshot.SnapshotID, removedDeleteFilesKey, reported, recorded)
	}

	return nil
}

func (s *IncrementalChangelogScan) readDeleteManifestEntries(
	ctx context.Context, plan changelogTaskPlan, manifests []iceberg.ManifestFile,
) ([]iceberg.ManifestEntry, error) {
	deleteManifests := deleteContentManifests(manifests)
	// Prune with the scan partition filters. A local accumulator keeps this
	// pass from counting the same delete manifests in the changelog report.
	var pruned scanMetricsAccumulator
	deleteManifests, err := plan.scan.filterManifestsWithSchemaOptions(
		deleteManifests, plan.schema, &pruned, plan.partitionFilters, true)
	if err != nil {
		return nil, err
	}

	return readManifestEntries(ctx, plan.fs, deleteManifests, false, false)
}

// snapshotBeforeRange returns the snapshot whose live delete files already
// applied before the range. An exclusive start that is still present is that
// snapshot. When it has been expired, the parent of the first snapshot in the
// range is used instead. A nil result with a recorded parent means that parent
// has expired.
func (s *IncrementalChangelogScan) snapshotBeforeRange(snapshots []Snapshot) *Snapshot {
	if s.fromSnapshotID != nil && !s.fromInclusive {
		if snapshot := s.scan.metadata.SnapshotByID(*s.fromSnapshotID); snapshot != nil {
			return snapshot
		}
	}
	if len(snapshots) == 0 || snapshots[0].ParentSnapshotID == nil {
		return nil
	}

	return s.scan.metadata.SnapshotByID(*snapshots[0].ParentSnapshotID)
}

func timelineSnapshots(plan changelogTaskPlan) []Snapshot {
	if len(plan.timeline) > 0 {
		return plan.timeline
	}

	return plan.snapshots
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
	// Manifest files are immutable, so entries decoded for one snapshot can be
	// reused when a later snapshot carries the same manifest forward.
	decodedManifests := make(map[string][]iceberg.ManifestEntry)
	sequences := snapshotSequences(plan.scan.metadata)
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
		manifests := plan.manifestsBySnapshot[snapshot.SnapshotID]
		dataManifests := make([]iceberg.ManifestFile, 0, len(manifests))
		for _, manifest := range manifests {
			if manifest.ManifestContent() == iceberg.ManifestContentData {
				dataManifests = append(dataManifests, manifest)
			}
		}
		// Prune with the scan partition filters before opening manifests.
		// A local accumulator keeps carried-forward data manifests out of the
		// changelog scan report, which already counted them.
		var pruned scanMetricsAccumulator
		dataManifests, err = plan.scan.filterManifestsWithSchemaOptions(
			dataManifests, plan.schema, &pruned, plan.partitionFilters, true)
		if err != nil {
			return nil, err
		}
		// Each snapshot that adds deletes reads that snapshot's live data
		// manifests. Equality deletes and position deletes without a referenced
		// data file cannot be narrowed to one path, so those snapshots scan the
		// manifests the partition filter keeps. Decoded entries are cached by
		// manifest path across snapshots.
		live, err := liveDataEntries(ctx, plan.fs, dataManifests, decodedManifests, sequences)
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
// snapshotID. Removals drop that delete identity from the running set. A
// delete removed and added again in one snapshot stays live as the added entry.
func (st *changelogDeleteState) entriesBefore(snapshotID int64) []iceberg.ManifestEntry {
	removed := make(map[string]struct{})
	var accumulated []iceberg.ManifestEntry
	for _, snapshot := range st.snapshots {
		if snapshot.SnapshotID == snapshotID {
			break
		}
		currentRemoved := st.removed[snapshot.SnapshotID]
		if len(currentRemoved) > 0 {
			for key := range currentRemoved {
				removed[key] = struct{}{}
			}
			accumulated = dropManifestEntries(accumulated, currentRemoved)
		}
		accumulated = append(accumulated, st.added[snapshot.SnapshotID]...)
	}

	out := make([]iceberg.ManifestEntry, 0, len(st.preRange)+len(accumulated))
	for _, entry := range st.preRange {
		if _, gone := removed[deleteEntryKey(entry.DataFile())]; gone {
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

// buildChangelogDeleteIndex groups delete files with the scan's sequence
// rules. Equality deletes are resolved against schema, which is the schema at
// the end of the changelog range. An equality delete whose columns were
// dropped before that schema cannot be resolved.
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

func liveDataEntries(
	ctx context.Context,
	fs io.IO,
	manifests []iceberg.ManifestFile,
	decoded map[string][]iceberg.ManifestEntry,
	sequences snapshotSequenceLookup,
) ([]iceberg.ManifestEntry, error) {
	dataManifests := make([]iceberg.ManifestFile, 0, len(manifests))
	for _, manifest := range manifests {
		if manifest.ManifestContent() == iceberg.ManifestContentData {
			dataManifests = append(dataManifests, manifest)
		}
	}
	entries, err := readCachedManifestEntries(ctx, fs, dataManifests, decoded)
	if err != nil {
		return nil, err
	}
	dataEntries := make([]iceberg.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.DataFile().ContentType() == iceberg.EntryContentData {
			dataEntries = append(dataEntries, entry)
		}
	}

	return liveManifestEntries(dataEntries, sequences), nil
}

// snapshotSequenceLookup returns the sequence number of the snapshot that
// wrote an entry. Snapshot IDs are random, so they are not an order. The
// boolean is false when that snapshot is not in table metadata.
type snapshotSequenceLookup func(snapshotID int64) (sequence int64, ok bool)

func snapshotSequences(metadata Metadata) snapshotSequenceLookup {
	return func(snapshotID int64) (int64, bool) {
		snapshot := metadata.SnapshotByID(snapshotID)
		if snapshot == nil {
			return 0, false
		}

		return snapshot.SequenceNumber, true
	}
}

// liveManifestEntries keeps the winning entry for each data-file path and
// drops paths whose winning entry is a deletion. A higher entry sequence
// number wins. Equal entry sequence numbers use the sequence number of the
// snapshot that wrote each entry. When those are equal or unknown, a deletion
// wins.
func liveManifestEntries(entries []iceberg.ManifestEntry, sequences snapshotSequenceLookup) []iceberg.ManifestEntry {
	return liveEntriesByKey(entries, func(entry iceberg.ManifestEntry) string {
		return entry.DataFile().FilePath()
	}, sequences)
}

// liveDeleteEntries is liveManifestEntries for delete files. Deletion vectors
// that share a Puffin path stay distinct, keyed by that path plus the
// referenced data file and content offset.
func liveDeleteEntries(entries []iceberg.ManifestEntry, sequences snapshotSequenceLookup) []iceberg.ManifestEntry {
	return liveEntriesByKey(entries, func(entry iceberg.ManifestEntry) string {
		return deleteEntryKey(entry.DataFile())
	}, sequences)
}

func liveEntriesByKey(entries []iceberg.ManifestEntry, key func(iceberg.ManifestEntry) string, sequences snapshotSequenceLookup) []iceberg.ManifestEntry {
	latest := make(map[string]iceberg.ManifestEntry, len(entries))
	order := make([]string, 0, len(entries))
	for _, entry := range entries {
		id := key(entry)
		current, seen := latest[id]
		if !seen {
			order = append(order, id)
			latest[id] = entry

			continue
		}
		if changelogEntrySupersedes(entry, current, sequences) {
			latest[id] = entry
		}
	}
	out := make([]iceberg.ManifestEntry, 0, len(latest))
	for _, id := range order {
		entry := latest[id]
		if entry.Status() == iceberg.EntryStatusDELETED {
			continue
		}
		out = append(out, entry)
	}

	return out
}

// changelogEntrySupersedes reports whether next replaces current. The entry
// sequence number wins first. Equal entry sequence numbers use the sequence
// number of the snapshot that wrote each entry. Snapshot IDs are not compared.
// When the snapshot sequence numbers are equal, or either snapshot is unknown,
// a deletion wins.
func changelogEntrySupersedes(next, current iceberg.ManifestEntry, sequences snapshotSequenceLookup) bool {
	if next.DataSequenceNumber() != current.DataSequenceNumber() {
		return next.DataSequenceNumber() > current.DataSequenceNumber()
	}
	if sequences != nil {
		nextSequence, nextOK := sequences(next.SnapshotID())
		currentSequence, currentOK := sequences(current.SnapshotID())
		if nextOK && currentOK && nextSequence != currentSequence {
			return nextSequence > currentSequence
		}
	}

	return next.Status() == iceberg.EntryStatusDELETED && current.Status() != iceberg.EntryStatusDELETED
}

// entriesLiveBefore selects delete entries that were live before snapshotID.
// A non-deleted entry written by another snapshot was carried forward. A
// deleted entry written by snapshotID was live until this snapshot removed it.
// One entry is kept per delete identity, preferring a non-deleted entry.
func entriesLiveBefore(entries []iceberg.ManifestEntry, snapshotID int64) []iceberg.ManifestEntry {
	latest := make(map[string]iceberg.ManifestEntry)
	order := make([]string, 0)
	for _, entry := range entries {
		carried := entry.Status() != iceberg.EntryStatusDELETED && entry.SnapshotID() != snapshotID
		removedHere := entry.Status() == iceberg.EntryStatusDELETED && entry.SnapshotID() == snapshotID
		if !carried && !removedHere {
			continue
		}
		key := deleteEntryKey(entry.DataFile())
		current, seen := latest[key]
		if !seen {
			order = append(order, key)
			latest[key] = entry

			continue
		}
		if current.Status() == iceberg.EntryStatusDELETED && entry.Status() != iceberg.EntryStatusDELETED {
			latest[key] = entry
		}
	}
	out := make([]iceberg.ManifestEntry, 0, len(order))
	for _, key := range order {
		out = append(out, latest[key])
	}

	return out
}

// deleteEntryKey identifies a delete file. Position and equality deletes use
// their path. Deletion vectors in one Puffin file share that path, so they
// also use the referenced data file and content offset.
func deleteEntryKey(file iceberg.DataFile) string {
	if !IsDeletionVector(file) {
		return file.FilePath()
	}
	offset := int64(-1)
	if fileOffset := file.ContentOffset(); fileOffset != nil {
		offset = *fileOffset
	}

	return file.FilePath() + "\x00" + referencedDataFilePath(file) + "\x00" + strconv.FormatInt(offset, 10)
}

// readCachedManifestEntries returns manifest entries, reusing entries already
// decoded for the same manifest path. Manifest files are immutable.
func readCachedManifestEntries(
	ctx context.Context,
	fs io.IO,
	manifests []iceberg.ManifestFile,
	decoded map[string][]iceberg.ManifestEntry,
) ([]iceberg.ManifestEntry, error) {
	if len(manifests) == 0 {
		return nil, nil
	}
	out := make([]iceberg.ManifestEntry, 0)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		path := manifest.FilePath()
		entries, ok := decoded[path]
		if !ok {
			var err error
			entries, err = readManifestEntries(ctx, fs, []iceberg.ManifestFile{manifest}, false, false)
			if err != nil {
				return nil, err
			}
			if entries == nil {
				entries = []iceberg.ManifestEntry{}
			}
			if decoded != nil {
				decoded[path] = entries
			}
		}
		out = append(out, entries...)
	}

	return out, nil
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

func dropManifestEntries(entries []iceberg.ManifestEntry, gone map[string]struct{}) []iceberg.ManifestEntry {
	if len(gone) == 0 {
		return entries
	}
	out := make([]iceberg.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		if _, drop := gone[deleteEntryKey(entry.DataFile())]; drop {
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
