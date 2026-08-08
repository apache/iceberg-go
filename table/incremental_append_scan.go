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
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/apache/iceberg-go"
)

// IncrementalAppendScan plans data files added by append snapshots between a
// starting snapshot and an ending snapshot. It follows one snapshot ancestry
// chain and never returns files inherited from an earlier snapshot.
type IncrementalAppendScan struct {
	scan           *Scan
	fromSnapshotID *int64
	fromInclusive  bool
	toSnapshotID   *int64
}

// NewIncrementalAppendScan creates an incremental append scan. Scan options
// configure the underlying table scan and are retained for callers that pass
// snapshot, projection, filter, or concurrency options before planning.
// Only local planning is supported; remote and auto planning return
// ErrInvalidOperation until incremental remote planning is implemented.
func (t Table) NewIncrementalAppendScan(opts ...ScanOption) *IncrementalAppendScan {
	return &IncrementalAppendScan{scan: t.Scan(opts...)}
}

// FromSnapshotInclusive includes files added by the starting snapshot.
func (s *IncrementalAppendScan) FromSnapshotInclusive(snapshotID int64) (*IncrementalAppendScan, error) {
	if s.scan.metadata.SnapshotByID(snapshotID) == nil {
		return nil, fmt.Errorf("%w: starting snapshot not found: %d", iceberg.ErrInvalidArgument, snapshotID)
	}
	out := *s
	out.fromSnapshotID = &snapshotID
	out.fromInclusive = true

	return &out, nil
}

// FromSnapshotExclusive starts after the given snapshot. The starting
// snapshot must be an ancestor of the ending snapshot when planning.
func (s *IncrementalAppendScan) FromSnapshotExclusive(snapshotID int64) *IncrementalAppendScan {
	out := *s
	out.fromSnapshotID = &snapshotID
	out.fromInclusive = false

	return &out
}

// ToSnapshot sets the inclusive ending snapshot.
func (s *IncrementalAppendScan) ToSnapshot(snapshotID int64) (*IncrementalAppendScan, error) {
	if s.scan.metadata.SnapshotByID(snapshotID) == nil {
		return nil, fmt.Errorf("%w: ending snapshot not found: %d", iceberg.ErrInvalidArgument, snapshotID)
	}
	out := *s
	out.toSnapshotID = &snapshotID

	return &out, nil
}

// PlanFiles returns one task per newly added data file. Delete files are not
// applied because appended files are not present before the append snapshot.
func (s *IncrementalAppendScan) PlanFiles(ctx context.Context) ([]FileScanTask, error) {
	switch s.scan.planningMode {
	case ScanPlanningLocal:
	case ScanPlanningRemote, ScanPlanningAuto:
		return nil, fmt.Errorf("%w: incremental append scans support local planning only", ErrInvalidOperation)
	default:
		return nil, fmt.Errorf("%w: unknown scan planning mode %q", iceberg.ErrInvalidArgument, s.scan.planningMode)
	}

	toSnapshot, err := s.toSnapshot()
	if err != nil {
		return nil, err
	}
	if toSnapshot == nil {
		return nil, nil
	}

	snapshots, err := s.snapshotsBetween(toSnapshot.SnapshotID)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	appendSnapshots := make(map[int64]struct{}, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.Summary != nil && snapshot.Summary.Operation == OpAppend {
			appendSnapshots[snapshot.SnapshotID] = struct{}{}
		}
	}
	if len(appendSnapshots) == 0 {
		return nil, nil
	}

	if s.scan.ioF == nil {
		return nil, fmt.Errorf("%w: table file IO is not configured", ErrInvalidOperation)
	}
	fs, err := s.scan.ioF(ctx)
	if err != nil {
		return nil, err
	}

	// An inherited manifest can occur in every later snapshot's manifest list.
	// Read each manifest path once, just as the Java incremental append scan
	// collects the selected manifests into a set before opening them.
	manifestsByPath := make(map[string]iceberg.ManifestFile)
	for _, snapshot := range snapshots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		manifests, err := snapshot.Manifests(fs)
		if err != nil {
			return nil, err
		}
		for _, manifest := range manifests {
			if manifest.ManifestContent() != iceberg.ManifestContentData {
				continue
			}
			if _, ok := appendSnapshots[manifest.SnapshotID()]; !ok {
				continue
			}
			manifestsByPath[manifest.FilePath()] = manifest
		}
	}

	paths := make([]string, 0, len(manifestsByPath))
	for path := range manifestsByPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	manifestList := make([]iceberg.ManifestFile, 0, len(paths))
	for _, path := range paths {
		manifestList = append(manifestList, manifestsByPath[path])
	}

	planningScan := *s.scan
	if s.toSnapshotID != nil {
		// An explicit end snapshot is a historical scan and must use that
		// snapshot's schema. An implicit current end remains a live scan so a
		// schema-only metadata update is visible during pruning.
		planningScan.snapshotID = &toSnapshot.SnapshotID
		planningScan.asOfTimestamp = nil
	}
	schema, err := planningScan.effectiveSchema()
	if err != nil {
		return nil, err
	}
	entries, err := planningScan.collectManifestEntriesWithSchema(ctx, manifestList, schema)
	if err != nil {
		return nil, err
	}

	tasks := make([]FileScanTask, 0, len(entries.dataEntries))
	for _, entry := range entries.dataEntries {
		if entry.Status() != iceberg.EntryStatusADDED {
			continue
		}
		if _, ok := appendSnapshots[entry.SnapshotID()]; !ok {
			continue
		}
		file := entry.DataFile()
		task := FileScanTask{File: file, Start: 0, Length: file.FileSizeBytes()}
		task.FirstRowID = file.FirstRowID()
		if sequenceNumber := entry.SequenceNum(); sequenceNumber >= 0 {
			task.DataSequenceNumber = &sequenceNumber
		}
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(left, right int) bool {
		return tasks[left].File.FilePath() < tasks[right].File.FilePath()
	})

	return tasks, nil
}

func (s *IncrementalAppendScan) toSnapshot() (*Snapshot, error) {
	if s.toSnapshotID != nil {
		return s.scan.metadata.SnapshotByID(*s.toSnapshotID), nil
	}

	return s.scan.ResolveSnapshot()
}

func (s *IncrementalAppendScan) snapshotsBetween(toSnapshotID int64) ([]Snapshot, error) {
	ancestors := AncestorsOf(toSnapshotID, s.scan.metadata.SnapshotByID)
	if len(ancestors) == 0 {
		return nil, fmt.Errorf("%w: ending snapshot not found: %d", iceberg.ErrInvalidArgument, toSnapshotID)
	}

	if s.fromSnapshotID == nil {
		slices.Reverse(ancestors)

		return appendOnlySnapshots(ancestors), nil
	}

	fromID := *s.fromSnapshotID
	if !s.fromInclusive {
		between, found := AncestorsBetween(toSnapshotID, fromID, s.scan.metadata.SnapshotByID)
		if !found {
			return nil, fmt.Errorf("%w: starting snapshot %d is not an ancestor of ending snapshot %d", iceberg.ErrInvalidArgument, fromID, toSnapshotID)
		}
		slices.Reverse(between)

		return appendOnlySnapshots(between), nil
	}

	if s.scan.metadata.SnapshotByID(fromID) == nil || !IsAncestorOf(toSnapshotID, fromID, s.scan.metadata.SnapshotByID) {
		return nil, fmt.Errorf("%w: starting snapshot %d is not an ancestor of ending snapshot %d", iceberg.ErrInvalidArgument, fromID, toSnapshotID)
	}
	selected := make([]Snapshot, 0, len(ancestors))
	for _, snapshot := range ancestors {
		selected = append(selected, snapshot)
		if snapshot.SnapshotID == fromID {
			break
		}
	}
	slices.Reverse(selected)

	return appendOnlySnapshots(selected), nil
}

func appendOnlySnapshots(snapshots []Snapshot) []Snapshot {
	result := make([]Snapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.Summary != nil && snapshot.Summary.Operation == OpAppend {
			result = append(result, snapshot)
		}
	}

	return result
}
