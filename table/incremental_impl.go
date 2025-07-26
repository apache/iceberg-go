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
	"iter"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// baseIncrementalScan provides the common implementation for all incremental scan types.
type baseIncrementalScan struct {
	metadata              Metadata
	ioF                   FSysF
	startSnapshotID       *int64
	endSnapshotID         *int64
	startSnapshotExclusive bool
	rowFilter             iceberg.BooleanExpression
	selectedFields        []string
	projectedSchema       *iceberg.Schema
	caseSensitive         bool
	includeColumnStats    bool
	options               iceberg.Properties
}

// newBaseIncrementalScan creates a new base incremental scan.
func newBaseIncrementalScan(metadata Metadata, ioF FSysF) *baseIncrementalScan {
	return &baseIncrementalScan{
		metadata:               metadata,
		ioF:                    ioF,
		startSnapshotExclusive: false,
		rowFilter:              iceberg.AlwaysTrue{},
		selectedFields:         []string{"*"},
		caseSensitive:          true,
		includeColumnStats:     false,
		options:                make(iceberg.Properties),
	}
}

// UseStartSnapshotID sets the starting snapshot ID for the incremental scan.
func (s *baseIncrementalScan) UseStartSnapshotID(snapshotID int64) IncrementalScan {
	s.startSnapshotID = &snapshotID
	return s
}

// UseEndSnapshotID sets the ending snapshot ID for the incremental scan.
func (s *baseIncrementalScan) UseEndSnapshotID(snapshotID int64) IncrementalScan {
	s.endSnapshotID = &snapshotID
	return s
}

// UseStartSnapshotExclusive sets whether the start snapshot should be exclusive.
func (s *baseIncrementalScan) UseStartSnapshotExclusive(exclusive bool) IncrementalScan {
	s.startSnapshotExclusive = exclusive
	return s
}

// Filter applies a row filter to the incremental scan.
func (s *baseIncrementalScan) Filter(expr iceberg.BooleanExpression) IncrementalScan {
	s.rowFilter = expr
	return s
}

// Select specifies which columns to include in the scan.
func (s *baseIncrementalScan) Select(columns ...string) IncrementalScan {
	s.selectedFields = columns
	return s
}

// Project selects columns using a schema projection.
func (s *baseIncrementalScan) Project(schema *iceberg.Schema) IncrementalScan {
	s.projectedSchema = schema
	return s
}

// UseRef specifies a named reference to use as the end snapshot.
func (s *baseIncrementalScan) UseRef(ref string) (IncrementalScan, error) {
	if snap := s.metadata.SnapshotByName(ref); snap != nil {
		s.endSnapshotID = &snap.SnapshotID
		return s, nil
	}
	return nil, fmt.Errorf("%w: cannot find ref=%s", iceberg.ErrInvalidArgument, ref)
}

// Option sets scan options.
func (s *baseIncrementalScan) Option(key, value string) IncrementalScan {
	if s.options == nil {
		s.options = make(iceberg.Properties)
	}
	s.options[key] = value
	return s
}

// CaseSensitive sets whether column name matching should be case sensitive.
func (s *baseIncrementalScan) CaseSensitive(caseSensitive bool) IncrementalScan {
	s.caseSensitive = caseSensitive
	return s
}

// IncludeColumnStats sets whether to include column statistics in the scan.
func (s *baseIncrementalScan) IncludeColumnStats() IncrementalScan {
	s.includeColumnStats = true
	return s
}

// StartSnapshotID returns the starting snapshot ID, if set.
func (s *baseIncrementalScan) StartSnapshotID() *int64 {
	return s.startSnapshotID
}

// EndSnapshotID returns the ending snapshot ID, if set.
func (s *baseIncrementalScan) EndSnapshotID() *int64 {
	return s.endSnapshotID
}

// IsStartSnapshotExclusive returns whether the start snapshot is exclusive.
func (s *baseIncrementalScan) IsStartSnapshotExclusive() bool {
	return s.startSnapshotExclusive
}

// Schema returns the schema that will be used for this scan.
func (s *baseIncrementalScan) Schema() (*iceberg.Schema, error) {
	if s.projectedSchema != nil {
		return s.projectedSchema, nil
	}

	curSchema := s.metadata.CurrentSchema()
	if slices.Contains(s.selectedFields, "*") {
		return curSchema, nil
	}

	return curSchema.Select(s.caseSensitive, s.selectedFields...)
}

// getSnapshotsInRange returns all snapshots between start and end snapshots (inclusive/exclusive based on settings).
func (s *baseIncrementalScan) getSnapshotsInRange() ([]*Snapshot, error) {
	allSnapshots := s.metadata.Snapshots()
	if len(allSnapshots) == 0 {
		return nil, nil
	}

	var startIdx, endIdx int = -1, -1

	// Find the start snapshot index
	if s.startSnapshotID != nil {
		for i, snap := range allSnapshots {
			if snap.SnapshotID == *s.startSnapshotID {
				startIdx = i
				if s.startSnapshotExclusive {
					startIdx++ // exclude the start snapshot
				}
				break
			}
		}
		if startIdx == -1 {
			return nil, fmt.Errorf("start snapshot %d not found", *s.startSnapshotID)
		}
	} else {
		startIdx = 0
	}

	// Find the end snapshot index
	if s.endSnapshotID != nil {
		for i, snap := range allSnapshots {
			if snap.SnapshotID == *s.endSnapshotID {
				endIdx = i
				break
			}
		}
		if endIdx == -1 {
			return nil, fmt.Errorf("end snapshot %d not found", *s.endSnapshotID)
		}
	} else {
		endIdx = len(allSnapshots) - 1
	}

	if startIdx > endIdx {
		return nil, nil // No snapshots in range
	}

	result := make([]*Snapshot, 0, endIdx-startIdx+1)
	for i := startIdx; i <= endIdx; i++ {
		snap := allSnapshots[i]
		result = append(result, &snap)
	}

	return result, nil
}

// PlanFiles returns the files that need to be scanned for this incremental scan.
func (s *baseIncrementalScan) PlanFiles(ctx context.Context) ([]FileScanTask, error) {
	snapshots, err := s.getSnapshotsInRange()
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return []FileScanTask{}, nil
	}

	// Create a regular scan for the end snapshot and filter files by snapshot range
	scan := &Scan{
		metadata:       s.metadata,
		ioF:            s.ioF,
		rowFilter:      s.rowFilter,
		selectedFields: s.selectedFields,
		caseSensitive:  s.caseSensitive,
		snapshotID:     s.endSnapshotID,
		options:        s.options,
		limit:          ScanNoLimit,
	}

	scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)

	// Get all files from the end snapshot scan
	allTasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}

	// Filter tasks to only include files added in our snapshot range
	return s.filterTasksBySnapshotRange(allTasks, snapshots)
}

// filterTasksBySnapshotRange filters tasks to only include files added in the specified snapshot range.
func (s *baseIncrementalScan) filterTasksBySnapshotRange(tasks []FileScanTask, snapshots []*Snapshot) ([]FileScanTask, error) {
	// For the base implementation, we'll include all files.
	// Subclasses can override this to implement more specific filtering.
	return tasks, nil
}

// ToArrowRecords returns the arrow schema and an iterator over arrow records.
func (s *baseIncrementalScan) ToArrowRecords(ctx context.Context) (*arrow.Schema, iter.Seq2[arrow.Record, error], error) {
	tasks, err := s.PlanFiles(ctx)
	if err != nil {
		return nil, nil, err
	}

	var boundFilter iceberg.BooleanExpression
	if s.rowFilter != nil {
		schema := s.metadata.CurrentSchema()
		boundFilter, err = iceberg.BindExpr(schema, s.rowFilter, s.caseSensitive)
		if err != nil {
			return nil, nil, err
		}
	}

	schema, err := s.Schema()
	if err != nil {
		return nil, nil, err
	}

	fs, err := s.ioF(ctx)
	if err != nil {
		return nil, nil, err
	}

	return (&arrowScan{
		metadata:        s.metadata,
		fs:              fs,
		projectedSchema: schema,
		boundRowFilter:  boundFilter,
		caseSensitive:   s.caseSensitive,
		options:         s.options,
	}).GetRecords(ctx, tasks)
}

// ToArrowTable returns all records as an arrow table.
func (s *baseIncrementalScan) ToArrowTable(ctx context.Context) (arrow.Table, error) {
	schema, itr, err := s.ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}

	records := make([]arrow.Record, 0)
	for rec, err := range itr {
		if err != nil {
			return nil, err
		}

		defer rec.Release()
		records = append(records, rec)
	}

	return array.NewTableFromRecords(schema, records), nil
}

// incrementalAppendScan implements IncrementalAppendScan for append-only incremental scans.
type incrementalAppendScan struct {
	*baseIncrementalScan
}

// newIncrementalAppendScan creates a new incremental append scan.
func newIncrementalAppendScan(metadata Metadata, ioF FSysF) *incrementalAppendScan {
	return &incrementalAppendScan{
		baseIncrementalScan: newBaseIncrementalScan(metadata, ioF),
	}
}

// FromSnapshotInclusive creates an incremental scan from the given snapshot (inclusive) to the current snapshot.
func (s *incrementalAppendScan) FromSnapshotInclusive(snapshotID int64) IncrementalAppendScan {
	s.startSnapshotID = &snapshotID
	s.startSnapshotExclusive = false
	if currentSnapshot := s.metadata.CurrentSnapshot(); currentSnapshot != nil {
		s.endSnapshotID = &currentSnapshot.SnapshotID
	}
	return s
}

// FromSnapshotExclusive creates an incremental scan from after the given snapshot to the current snapshot.
func (s *incrementalAppendScan) FromSnapshotExclusive(snapshotID int64) IncrementalAppendScan {
	s.startSnapshotID = &snapshotID
	s.startSnapshotExclusive = true
	if currentSnapshot := s.metadata.CurrentSnapshot(); currentSnapshot != nil {
		s.endSnapshotID = &currentSnapshot.SnapshotID
	}
	return s
}

// ToSnapshot creates an incremental scan up to the given snapshot.
func (s *incrementalAppendScan) ToSnapshot(snapshotID int64) IncrementalAppendScan {
	s.endSnapshotID = &snapshotID
	return s
}

// AsOfTime creates an incremental scan up to the snapshot at the given time.
func (s *incrementalAppendScan) AsOfTime(timestampMs int64) (IncrementalAppendScan, error) {
	snapshots := s.metadata.Snapshots()
	var targetSnapshot *Snapshot
	
	// Find the latest snapshot at or before the given time
	for i := len(snapshots) - 1; i >= 0; i-- {
		if snapshots[i].TimestampMs <= timestampMs {
			targetSnapshot = &snapshots[i]
			break
		}
	}
	
	if targetSnapshot == nil {
		return nil, fmt.Errorf("no snapshot found at or before timestamp %d", timestampMs)
	}
	
	s.endSnapshotID = &targetSnapshot.SnapshotID
	return s, nil
}

// Override interface methods to return IncrementalAppendScan type
func (s *incrementalAppendScan) UseStartSnapshotID(snapshotID int64) IncrementalScan {
	s.baseIncrementalScan.UseStartSnapshotID(snapshotID)
	return s
}

func (s *incrementalAppendScan) UseEndSnapshotID(snapshotID int64) IncrementalScan {
	s.baseIncrementalScan.UseEndSnapshotID(snapshotID)
	return s
}

func (s *incrementalAppendScan) UseStartSnapshotExclusive(exclusive bool) IncrementalScan {
	s.baseIncrementalScan.UseStartSnapshotExclusive(exclusive)
	return s
}

func (s *incrementalAppendScan) Filter(expr iceberg.BooleanExpression) IncrementalScan {
	s.baseIncrementalScan.Filter(expr)
	return s
}

func (s *incrementalAppendScan) Select(columns ...string) IncrementalScan {
	s.baseIncrementalScan.Select(columns...)
	return s
}

func (s *incrementalAppendScan) Project(schema *iceberg.Schema) IncrementalScan {
	s.baseIncrementalScan.Project(schema)
	return s
}

func (s *incrementalAppendScan) UseRef(ref string) (IncrementalScan, error) {
	return s.baseIncrementalScan.UseRef(ref)
}

func (s *incrementalAppendScan) Option(key, value string) IncrementalScan {
	s.baseIncrementalScan.Option(key, value)
	return s
}

func (s *incrementalAppendScan) CaseSensitive(caseSensitive bool) IncrementalScan {
	s.baseIncrementalScan.CaseSensitive(caseSensitive)
	return s
}

func (s *incrementalAppendScan) IncludeColumnStats() IncrementalScan {
	s.baseIncrementalScan.IncludeColumnStats()
	return s
}

// filterTasksBySnapshotRange filters tasks for append-only scans to only include files added in snapshot range.
func (s *incrementalAppendScan) filterTasksBySnapshotRange(tasks []FileScanTask, snapshots []*Snapshot) ([]FileScanTask, error) {
	if len(snapshots) == 0 {
		return []FileScanTask{}, nil
	}

	// For append-only scans, we need to filter files that were added in the specified snapshot range.
	// This is a simplified implementation - in a real implementation, you would need to track
	// which files were added in which snapshots through the manifest files.
	
	fs, err := s.ioF(context.Background())
	if err != nil {
		return nil, err
	}

	var filteredTasks []FileScanTask
	snapshotIDs := make(map[int64]bool)
	for _, snap := range snapshots {
		snapshotIDs[snap.SnapshotID] = true
	}

	// For each snapshot in range, get its manifests and find files added in that snapshot
	for _, snapshot := range snapshots {
		manifests, err := snapshot.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("failed to get manifests for snapshot %d: %w", snapshot.SnapshotID, err)
		}

		for _, manifest := range manifests {
			// Only process data manifests for append scans
			if manifest.ManifestContent() != iceberg.ManifestContentData {
				continue
			}

			entries, err := manifest.FetchEntries(fs, false)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch entries from manifest: %w", err)
			}

			for _, entry := range entries {
				// Include files that were added (not deleted) in this snapshot
				if entry.Status() == iceberg.EntryStatusADDED {
					// Find corresponding task
					for _, task := range tasks {
						if task.File.FilePath() == entry.DataFile().FilePath() {
							filteredTasks = append(filteredTasks, task)
							break
						}
					}
				}
			}
		}
	}

	return filteredTasks, nil
}

// incrementalChangelogScan implements IncrementalChangelogScan for incremental scans that include change information.
type incrementalChangelogScan struct {
	*baseIncrementalScan
}

// newIncrementalChangelogScan creates a new incremental changelog scan.
func newIncrementalChangelogScan(metadata Metadata, ioF FSysF) *incrementalChangelogScan {
	return &incrementalChangelogScan{
		baseIncrementalScan: newBaseIncrementalScan(metadata, ioF),
	}
}

// FromSnapshotInclusive creates an incremental changelog scan from the given snapshot (inclusive).
func (s *incrementalChangelogScan) FromSnapshotInclusive(snapshotID int64) IncrementalChangelogScan {
	s.startSnapshotID = &snapshotID
	s.startSnapshotExclusive = false
	if currentSnapshot := s.metadata.CurrentSnapshot(); currentSnapshot != nil {
		s.endSnapshotID = &currentSnapshot.SnapshotID
	}
	return s
}

// FromSnapshotExclusive creates an incremental changelog scan from after the given snapshot.
func (s *incrementalChangelogScan) FromSnapshotExclusive(snapshotID int64) IncrementalChangelogScan {
	s.startSnapshotID = &snapshotID
	s.startSnapshotExclusive = true
	if currentSnapshot := s.metadata.CurrentSnapshot(); currentSnapshot != nil {
		s.endSnapshotID = &currentSnapshot.SnapshotID
	}
	return s
}

// ToSnapshot creates an incremental changelog scan up to the given snapshot.
func (s *incrementalChangelogScan) ToSnapshot(snapshotID int64) IncrementalChangelogScan {
	s.endSnapshotID = &snapshotID
	return s
}

// Override interface methods to return IncrementalChangelogScan type
func (s *incrementalChangelogScan) UseStartSnapshotID(snapshotID int64) IncrementalScan {
	s.baseIncrementalScan.UseStartSnapshotID(snapshotID)
	return s
}

func (s *incrementalChangelogScan) UseEndSnapshotID(snapshotID int64) IncrementalScan {
	s.baseIncrementalScan.UseEndSnapshotID(snapshotID)
	return s
}

func (s *incrementalChangelogScan) UseStartSnapshotExclusive(exclusive bool) IncrementalScan {
	s.baseIncrementalScan.UseStartSnapshotExclusive(exclusive)
	return s
}

func (s *incrementalChangelogScan) Filter(expr iceberg.BooleanExpression) IncrementalScan {
	s.baseIncrementalScan.Filter(expr)
	return s
}

func (s *incrementalChangelogScan) Select(columns ...string) IncrementalScan {
	s.baseIncrementalScan.Select(columns...)
	return s
}

func (s *incrementalChangelogScan) Project(schema *iceberg.Schema) IncrementalScan {
	s.baseIncrementalScan.Project(schema)
	return s
}

func (s *incrementalChangelogScan) UseRef(ref string) (IncrementalScan, error) {
	return s.baseIncrementalScan.UseRef(ref)
}

func (s *incrementalChangelogScan) Option(key, value string) IncrementalScan {
	s.baseIncrementalScan.Option(key, value)
	return s
}

func (s *incrementalChangelogScan) CaseSensitive(caseSensitive bool) IncrementalScan {
	s.baseIncrementalScan.CaseSensitive(caseSensitive)
	return s
}

func (s *incrementalChangelogScan) IncludeColumnStats() IncrementalScan {
	s.baseIncrementalScan.IncludeColumnStats()
	return s
}

// filterTasksBySnapshotRange filters tasks for changelog scans to include all changes (adds, deletes, updates) in snapshot range.
func (s *incrementalChangelogScan) filterTasksBySnapshotRange(tasks []FileScanTask, snapshots []*Snapshot) ([]FileScanTask, error) {
	if len(snapshots) == 0 {
		return []FileScanTask{}, nil
	}

	// For changelog scans, we need to include all files that were added, deleted, or modified
	// in the specified snapshot range. This includes both data files and delete files.
	
	fs, err := s.ioF(context.Background())
	if err != nil {
		return nil, err
	}

	var filteredTasks []FileScanTask
	addedFiles := make(map[string]bool) // Track files to avoid duplicates
	snapshotIDs := make(map[int64]bool)
	for _, snap := range snapshots {
		snapshotIDs[snap.SnapshotID] = true
	}

	// For each snapshot in range, get its manifests and find all changed files
	for _, snapshot := range snapshots {
		manifests, err := snapshot.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("failed to get manifests for snapshot %d: %w", snapshot.SnapshotID, err)
		}

		for _, manifest := range manifests {
			entries, err := manifest.FetchEntries(fs, false)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch entries from manifest: %w", err)
			}

			for _, entry := range entries {
				filePath := entry.DataFile().FilePath()
				
				// Include files that were added, deleted, or existing (for updates)
				// In a changelog scan, we want to capture all changes
				if entry.Status() == iceberg.EntryStatusADDED ||
				   entry.Status() == iceberg.EntryStatusDELETED ||
				   entry.Status() == iceberg.EntryStatusEXISTING {
					
					if !addedFiles[filePath] {
						// Find corresponding task
						for _, task := range tasks {
							if task.File.FilePath() == filePath {
								filteredTasks = append(filteredTasks, task)
								addedFiles[filePath] = true
								break
							}
						}
					}
				}
			}
		}
	}

	return filteredTasks, nil
} 