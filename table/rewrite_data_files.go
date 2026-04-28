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

	"github.com/apache/iceberg-go"
)

// RewriteResult summarizes a completed compaction.
type RewriteResult struct {
	// RewrittenGroups is the number of compaction groups committed.
	RewrittenGroups int

	// AddedDataFiles is the total number of new data files written.
	AddedDataFiles int

	// RemovedDataFiles is the total number of old data files replaced.
	RemovedDataFiles int

	// RemovedPositionDeleteFiles is the count of position delete files
	// removed because their referenced data file was rewritten.
	RemovedPositionDeleteFiles int

	// RemovedEqualityDeleteFiles is the count of equality delete files
	// removed via RewriteDataFilesOptions.ExtraDeleteFilesToRemove. The
	// caller computes which eq-deletes are dead — typically via
	// table/compaction.CollectDeadEqualityDeletes — and passes the list
	// in.
	RemovedEqualityDeleteFiles int

	// BytesBefore is the total size of input data files (from the compaction plan).
	BytesBefore int64

	// BytesAfter is the total size of output data files (measured from written files).
	BytesAfter int64
}

// CompactionTaskGroup is a set of scan tasks in the same partition that
// should be compacted together. This bridges the compaction planner
// (table/compaction package) and the executor, avoiding a circular
// import between table and table/compaction.
//
// Use compaction.Config.PlanCompaction() to produce groups, then convert
// compaction.Group → CompactionTaskGroup to call RewriteDataFiles.
type CompactionTaskGroup struct {
	// PartitionKey is an opaque grouping key for display/logging.
	PartitionKey string

	// Tasks are the FileScanTasks to compact.
	Tasks []FileScanTask

	// TotalSizeBytes is the sum of data file sizes in this group.
	TotalSizeBytes int64
}

// RewriteDataFilesOptions bundles the per-rewrite knobs for
// Transaction.RewriteDataFiles.
type RewriteDataFilesOptions struct {
	// PartialProgress, when true, stages each group via ReplaceFiles
	// inside the loop so work survives a mid-loop write failure. When
	// false (the default), all groups are committed in a single atomic
	// snapshot.
	//
	// In both modes the final catalog commit happens once at
	// Transaction.Commit() time. True per-group durability (matching
	// Java's behavior) requires committing separate transactions per
	// group, which is left to the caller.
	PartialProgress bool

	// SnapshotProps are added to the rewrite snapshot's summary.
	SnapshotProps iceberg.Properties

	// ExtraDeleteFilesToRemove are delete files (typically equality
	// deletes that are dead after the rewrite) that the caller wants
	// expunged in the same snapshot as the rewrite. The executor
	// passes them through to ReplaceFiles unchanged. Honored only
	// when PartialProgress is false.
	//
	// Use table/compaction.CollectDeadEqualityDeletes to compute this
	// list from the current snapshot. Position delete files that are
	// fully applied are removed automatically and do NOT need to be
	// passed in here.
	ExtraDeleteFilesToRemove []iceberg.DataFile
}

// RewriteDataFiles compacts the given groups by reading data with
// deletes applied, writing new consolidated files, and atomically
// replacing the old files. Position delete files that are fully
// applied (every referenced data file is in the rewrite set) are
// removed automatically.
//
// Equality-delete cleanup is the caller's responsibility: compute the
// dead set with table/compaction.CollectDeadEqualityDeletes (against
// the same snapshot the rewrite is staged on) and pass it via
// opts.ExtraDeleteFilesToRemove. The executor only orchestrates the
// commit; it does not impose a cleanup policy. This split keeps the
// pure spec predicate in table/compaction and the unexported snapshot
// machinery in table.
//
// Use table/compaction.Config.PlanCompaction() to produce the groups,
// then convert compaction.Group → CompactionTaskGroup and pass them
// here.
func (t *Transaction) RewriteDataFiles(ctx context.Context, groups []CompactionTaskGroup, opts RewriteDataFilesOptions) (*RewriteResult, error) {
	if len(groups) == 0 {
		return &RewriteResult{}, nil
	}

	// Use an unfiltered scan to read all surviving rows. Compaction must
	// preserve every non-deleted row in the data files being rewritten.
	scan := t.tbl.Scan()
	result := &RewriteResult{}

	var (
		allOldData    []iceberg.DataFile
		allNewData    []iceberg.DataFile
		allOldDeletes []iceberg.DataFile
	)

	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		if len(group.Tasks) == 0 {
			continue
		}

		// Read with deletes applied.
		arrowSchema, records, err := scan.ReadTasks(ctx, group.Tasks)
		if err != nil {
			return result, fmt.Errorf("read tasks for compaction group %q: %w", group.PartitionKey, err)
		}

		// Write new data files.
		var newFiles []iceberg.DataFile
		for df, err := range WriteRecords(ctx, t.tbl, arrowSchema, records) {
			if err != nil {
				return result, fmt.Errorf("write compacted files for group %q: %w", group.PartitionKey, err)
			}
			newFiles = append(newFiles, df)
		}

		// Collect old data files.
		oldDataFiles := make([]iceberg.DataFile, 0, len(group.Tasks))
		for _, task := range group.Tasks {
			oldDataFiles = append(oldDataFiles, task.File)
		}

		// Collect position delete files safe to remove.
		safeDeletes := collectSafePositionDeletes(group.Tasks)

		// Update result metrics.
		var bytesAfter int64
		for _, df := range newFiles {
			bytesAfter += df.FileSizeBytes()
		}

		result.RewrittenGroups++
		result.AddedDataFiles += len(newFiles)
		result.RemovedDataFiles += len(oldDataFiles)
		result.RemovedPositionDeleteFiles += len(safeDeletes)
		result.BytesBefore += group.TotalSizeBytes
		result.BytesAfter += bytesAfter

		// Always accumulate across groups; partial-progress mode also
		// stages each group via ReplaceFiles so work survives a
		// mid-loop write failure, but the final catalog commit is
		// always one atomic doCommit at Transaction.Commit() time.
		allOldData = append(allOldData, oldDataFiles...)
		allNewData = append(allNewData, newFiles...)
		allOldDeletes = append(allOldDeletes, safeDeletes...)

		if opts.PartialProgress {
			if err := t.ReplaceFiles(ctx, oldDataFiles, newFiles, safeDeletes, opts.SnapshotProps, withRewriteSemantics()); err != nil {
				return result, fmt.Errorf("commit compaction group %q: %w", group.PartitionKey, err)
			}
		}
	}

	// Register the rewrite-specific conflict validator covering every
	// rewritten data file across every group. The validator list is
	// drained at Transaction.Commit() → doCommit. Runs alongside the
	// overwrite producer's suppressed validator (via
	// withRewriteSemantics) so concurrent pos/eq-deletes targeting a
	// rewritten file are caught pre-flight.
	if len(allOldData) > 0 {
		rewritten := make([]string, 0, len(allOldData))
		for _, f := range allOldData {
			rewritten = append(rewritten, f.FilePath())
		}
		t.validators = append(t.validators, rewriteValidator(rewritten))
	}

	if !opts.PartialProgress {
		// Caller-supplied dead eq-deletes (typically from
		// compaction.CollectDeadEqualityDeletes). The caller is
		// responsible for computing these against the same snapshot
		// this transaction is staged on.
		if len(opts.ExtraDeleteFilesToRemove) > 0 {
			allOldDeletes = append(allOldDeletes, opts.ExtraDeleteFilesToRemove...)
			result.RemovedEqualityDeleteFiles += len(opts.ExtraDeleteFilesToRemove)
		}

		if err := t.ReplaceFiles(ctx, allOldData, allNewData, allOldDeletes, opts.SnapshotProps, withRewriteSemantics()); err != nil {
			return result, fmt.Errorf("commit compaction: %w", err)
		}
	}

	return result, nil
}

// rewriteValidator builds a conflictValidatorFunc that rejects the
// commit if a concurrent snapshot added delete files pointing at any
// of the rewritten data-file paths (or eq-deletes during the rewrite,
// conservatively). Always runs — no isolation gating, because rewrite
// is a structural operation, not a user-facing isolation choice.
func rewriteValidator(rewrittenPaths []string) conflictValidatorFunc {
	return func(cc *conflictContext) error {
		if cc == nil {
			return nil
		}

		return validateNoNewDeletesForRewrittenFiles(cc, rewrittenPaths)
	}
}

// collectSafePositionDeletes returns position delete files from the given
// tasks that are safe to remove during compaction.
//
// A position delete file is safe to remove when it was matched to a data
// file (via scan planning) and that data file is being rewritten in this
// compaction group. Since ReadTasks applies the deletes during reading,
// the new output files will not contain the deleted rows.
//
// Only position deletes (EntryContentPosDeletes) are considered.
// Equality deletes are decided by table/compaction.DecideDeadEqualityDeletes
// (which needs partition-wide visibility, not just the task scope).
// Deletion vectors will be handled when DV read support lands.
func collectSafePositionDeletes(tasks []FileScanTask) []iceberg.DataFile {
	seen := make(map[string]bool)
	var safe []iceberg.DataFile

	for _, task := range tasks {
		for _, df := range task.DeleteFiles {
			if df.ContentType() != iceberg.EntryContentPosDeletes {
				continue
			}

			path := df.FilePath()
			if seen[path] {
				continue
			}
			seen[path] = true
			safe = append(safe, df)
		}
	}

	return safe
}
