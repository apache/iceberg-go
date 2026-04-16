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

	// RemovedDeleteFiles is the total number of delete files cleaned up.
	RemovedDeleteFiles int

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

// RewriteDataFiles compacts the given groups by reading data with deletes
// applied, writing new consolidated files, and atomically replacing the
// old files. Position delete files that are fully applied are removed.
//
// Use table/compaction.Config.PlanCompaction() to produce the groups,
// then convert compaction.Group → CompactionTaskGroup and pass them here.
//
// Note: partialProgress accumulates all group commits within this
// transaction. True per-group durability (matching Java's behavior)
// requires committing separate transactions per group, which is left
// to the caller. When partialProgress is false, all groups are committed
// in a single atomic snapshot.
//
// Equality delete files are intentionally preserved — they may apply to
// data files outside the compaction scope. Removal of equality deletes
// requires verifying that ALL data files in the partition are being
// rewritten, which is tracked as a follow-up.
func (t *Transaction) RewriteDataFiles(ctx context.Context, groups []CompactionTaskGroup, partialProgress bool, snapshotProps iceberg.Properties) (*RewriteResult, error) {
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

		// Each compaction group is single-partition by construction, so the
		// read stream is trivially clustered and we can use the clustered writer.
		var newFiles []iceberg.DataFile
		for df, err := range WriteRecords(ctx, t.tbl, arrowSchema, records, WithClusteredWrite()) {
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
		result.RemovedDeleteFiles += len(safeDeletes)
		result.BytesBefore += group.TotalSizeBytes
		result.BytesAfter += bytesAfter

		// Always accumulate across groups; partial-progress mode also
		// stages each group via ReplaceFiles so work survives a
		// mid-loop write failure, but the final catalog commit is
		// always one atomic doCommit at Transaction.Commit() time.
		allOldData = append(allOldData, oldDataFiles...)
		allNewData = append(allNewData, newFiles...)
		allOldDeletes = append(allOldDeletes, safeDeletes...)

		if partialProgress {
			if err := t.ReplaceFiles(ctx, oldDataFiles, newFiles, safeDeletes, snapshotProps, withRewriteSemantics()); err != nil {
				return result, fmt.Errorf("commit compaction group %q: %w", group.PartitionKey, err)
			}
		}
	}

	if !partialProgress {
		if err := t.ReplaceFiles(ctx, allOldData, allNewData, allOldDeletes, snapshotProps, withRewriteSemantics()); err != nil {
			return result, fmt.Errorf("commit compaction: %w", err)
		}
	}

	// Register a single rewrite-specific conflict validator covering
	// every rewritten data file across every group. t.ReplaceFiles
	// only stages updates on the transaction (no per-group catalog
	// commit); the validator list is drained once at
	// Transaction.Commit() → doCommit, so one registration with the
	// union of rewritten paths is what actually fires. This runs
	// alongside the overwrite producer's suppressed validator (via
	// withRewriteSemantics) so concurrent pos/eq-deletes targeting a
	// rewritten file are caught pre-flight.
	if len(allOldData) > 0 {
		rewritten := make([]string, 0, len(allOldData))
		for _, f := range allOldData {
			rewritten = append(rewritten, f.FilePath())
		}
		t.validators = append(t.validators, rewriteValidator(rewritten))
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
// Equality deletes and deletion vectors are intentionally excluded:
//   - Equality deletes may apply to data files outside the compaction
//     scope. Removing them requires verifying the entire partition is
//     being rewritten.
//   - Deletion vectors will be handled when DV read support lands.
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
