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
	"maps"

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
	// removed via [RewriteDataFilesOptions.ExtraDeleteFilesToRemove].
	// The caller computes which eq-deletes are dead — typically via
	// [compaction.CollectDeadEqualityDeletes] — and passes the list in.
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
// Use [compaction.Config.PlanCompaction] to produce groups, then convert
// [compaction.Group] → [CompactionTaskGroup] to call
// [Transaction.RewriteDataFiles] or [ExecuteCompactionGroup].
type CompactionTaskGroup struct {
	// PartitionKey is an opaque grouping key for display/logging.
	PartitionKey string

	// Tasks are the FileScanTasks to compact.
	Tasks []FileScanTask

	// TotalSizeBytes is the sum of data file sizes in this group.
	TotalSizeBytes int64
}

// CompactionGroupResult is the per-group output of a compaction
// worker: the new files written, the old files being replaced, and
// the position delete files safe to expunge in the rewrite snapshot.
//
// A distributed coordinator aggregates results from N workers and
// applies them to a [RewriteFiles] builder via [RewriteFiles.Apply]
// to commit a single atomic snapshot. Each field is plain data
// ([]iceberg.DataFile values plus scalars) — callers serialize the
// contained DataFiles across process boundaries themselves; the
// typical pattern is to have the worker write a manifest containing
// the new files and ship the manifest path to the coordinator, which
// re-reads it.
type CompactionGroupResult struct {
	// PartitionKey mirrors [CompactionTaskGroup.PartitionKey] for
	// display/logging on the coordinator.
	PartitionKey string

	// OldDataFiles are the data files this group replaces.
	OldDataFiles []iceberg.DataFile

	// NewDataFiles are the consolidated outputs the worker wrote.
	NewDataFiles []iceberg.DataFile

	// SafePosDeletes are position-delete files referenced by tasks in
	// this group whose target data file is being rewritten, computed
	// via [CollectSafePositionDeletes]. They are safe to expunge in
	// the rewrite snapshot.
	SafePosDeletes []iceberg.DataFile

	// BytesBefore is [CompactionTaskGroup.TotalSizeBytes] passed
	// through, recorded so the coordinator can roll up metrics
	// without re-reading the plan.
	BytesBefore int64

	// BytesAfter is the sum of [iceberg.DataFile.FileSizeBytes] across
	// NewDataFiles.
	BytesAfter int64
}

// RewriteDataFilesOptions bundles the per-rewrite knobs for
// [Transaction.RewriteDataFiles].
type RewriteDataFilesOptions struct {
	// PartialProgress, when true, stages each group as its own
	// rewrite snapshot inside the loop so a mid-loop write failure
	// leaves the already-completed groups staged on this transaction
	// (the in-memory transaction can be discarded by group rather
	// than wholesale). When false (the default), every group lands in
	// a single atomic rewrite snapshot.
	//
	// In both modes the catalog commit happens once at
	// [Transaction.Commit] time, so a process crash mid-loop loses
	// every staged group regardless of this flag. Callers who need
	// true per-group catalog durability (matching Java's behavior)
	// should drive [Transaction.NewRewrite] themselves and commit a
	// fresh transaction per group.
	PartialProgress bool

	// SnapshotProps are added to the rewrite snapshot's summary.
	// In partial-progress mode the same properties land on every
	// per-group snapshot rather than being summed or split.
	SnapshotProps iceberg.Properties

	// ExtraDeleteFilesToRemove are delete files (typically equality
	// deletes that are dead after the rewrite) that the caller wants
	// expunged in the same snapshot as the rewrite. Honored only when
	// PartialProgress is false.
	//
	// Use [compaction.CollectDeadEqualityDeletes] to compute this list
	// from the current snapshot. Position delete files that are fully
	// applied are removed automatically and do NOT need to be passed
	// in here.
	ExtraDeleteFilesToRemove []iceberg.DataFile

	// GroupOptions are forwarded to every [ExecuteCompactionGroup]
	// call to tune the per-group read+write pipeline (target file
	// size, scan concurrency). See the With* helpers returning
	// [CompactionGroupOption].
	GroupOptions []CompactionGroupOption
}

// CompactionGroupOption configures a single [ExecuteCompactionGroup]
// call. Use the With* helpers to construct values.
type CompactionGroupOption func(*compactionGroupConfig)

type compactionGroupConfig struct {
	targetFileSize  int64
	scanConcurrency int
}

// WithCompactionTargetFileSize sets the size target for output files
// written by [ExecuteCompactionGroup]. Forwarded to [WriteRecords] as
// [WithTargetFileSize]. A non-positive value (including the zero
// default) means inherit the table's `write.target-file-size-bytes`
// property.
func WithCompactionTargetFileSize(size int64) CompactionGroupOption {
	if size <= 0 {
		return func(*compactionGroupConfig) {}
	}

	return func(c *compactionGroupConfig) {
		c.targetFileSize = size
	}
}

// WithCompactionScanConcurrency sets the scan concurrency used when
// reading the group's tasks. Forwarded to [Table.Scan] as
// [WitMaxConcurrency]. Zero (the default) means runtime.GOMAXPROCS.
//
// TODO: the [WitMaxConcurrency] link enshrines a pre-existing typo
// (missing `h`). Update this reference when that symbol is renamed.
func WithCompactionScanConcurrency(n int) CompactionGroupOption {
	return func(c *compactionGroupConfig) {
		c.scanConcurrency = n
	}
}

// RewriteDataFiles compacts the given groups by reading data with
// deletes applied, writing new consolidated files, and atomically
// replacing the old files. Position delete files that are fully
// applied (every referenced data file is in the rewrite set) are
// removed automatically.
//
// Equality-delete cleanup is the caller's responsibility: compute the
// dead set with [compaction.CollectDeadEqualityDeletes] (against the
// same snapshot the rewrite is staged on) and pass it via
// [RewriteDataFilesOptions.ExtraDeleteFilesToRemove]. The executor
// only orchestrates the commit; it does not impose a cleanup policy.
// This split keeps the pure spec predicate in table/compaction and
// the unexported snapshot machinery in table.
//
// Use [compaction.Config.PlanCompaction] to produce the groups, then
// convert [compaction.Group] → [CompactionTaskGroup] and pass them
// here. Distributed coordinators stage worker results via
// [ExecuteCompactionGroup] and commit them via [Transaction.NewRewrite]
// + [RewriteFiles.Apply] + [RewriteFiles.Commit] instead.
func (t *Transaction) RewriteDataFiles(ctx context.Context, groups []CompactionTaskGroup, opts RewriteDataFilesOptions) (*RewriteResult, error) {
	if len(groups) == 0 {
		return &RewriteResult{}, nil
	}

	if opts.PartialProgress {
		return t.rewriteDataFilesPartial(ctx, groups, opts)
	}

	result := &RewriteResult{}
	rewrite := t.NewRewrite(opts.SnapshotProps)

	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		if len(group.Tasks) == 0 {
			continue
		}

		gr, err := ExecuteCompactionGroup(ctx, t.tbl, group, opts.GroupOptions...)
		if err != nil {
			return result, err
		}

		if len(gr.OldDataFiles) == 0 && len(gr.NewDataFiles) == 0 {
			continue
		}

		rewrite.Apply(gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes)
		accumulateGroupMetrics(result, gr)
	}

	if result.RewrittenGroups == 0 {
		return result, nil
	}

	for _, df := range opts.ExtraDeleteFilesToRemove {
		rewrite.DeleteFile(df)
		result.RemovedEqualityDeleteFiles++
	}

	if err := rewrite.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit compaction: %w", err)
	}

	return result, nil
}

// ExecuteCompactionGroup reads a compaction group's tasks (with
// deletes applied), writes consolidated output files via
// [WriteRecords], and computes the position-delete files safe to
// expunge in the rewrite snapshot. It does not commit — the caller
// hands the result to a coordinator that uses [Transaction.NewRewrite]
// + [RewriteFiles.Apply] + [RewriteFiles.Commit] to stage the
// atomic commit.
//
// Empty groups return a zero [CompactionGroupResult] without doing
// any I/O.
//
// In-process callers should prefer [Transaction.RewriteDataFiles],
// which drives this and the commit step in one call.
//
// Tunables are exposed via [CompactionGroupOption]. The clustered
// write path is always used (a compaction group is single-partition
// by construction so its read stream is trivially clustered).
func ExecuteCompactionGroup(ctx context.Context, tbl *Table, group CompactionTaskGroup, opts ...CompactionGroupOption) (CompactionGroupResult, error) {
	if len(group.Tasks) == 0 {
		return CompactionGroupResult{PartitionKey: group.PartitionKey}, nil
	}

	cfg := compactionGroupConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	var scanOpts []ScanOption
	if cfg.scanConcurrency > 0 {
		scanOpts = append(scanOpts, WitMaxConcurrency(cfg.scanConcurrency))
	}

	arrowSchema, records, err := tbl.Scan(scanOpts...).ReadTasks(ctx, group.Tasks)
	if err != nil {
		return CompactionGroupResult{}, fmt.Errorf("read tasks for compaction group %q: %w", group.PartitionKey, err)
	}

	// Each compaction group is single-partition by construction, so the
	// read stream is trivially clustered and we can use the clustered writer.
	writeOpts := []WriteRecordOption{WithClusteredWrite()}
	if cfg.targetFileSize > 0 {
		writeOpts = append(writeOpts, WithTargetFileSize(cfg.targetFileSize))
	}

	var (
		newFiles   []iceberg.DataFile
		bytesAfter int64
	)
	for df, err := range WriteRecords(ctx, tbl, arrowSchema, records, writeOpts...) {
		if err != nil {
			return CompactionGroupResult{}, fmt.Errorf("write compacted files for group %q: %w", group.PartitionKey, err)
		}
		newFiles = append(newFiles, df)
		bytesAfter += df.FileSizeBytes()
	}

	oldFiles := make([]iceberg.DataFile, 0, len(group.Tasks))
	for _, task := range group.Tasks {
		oldFiles = append(oldFiles, task.File)
	}

	return CompactionGroupResult{
		PartitionKey:   group.PartitionKey,
		OldDataFiles:   oldFiles,
		NewDataFiles:   newFiles,
		SafePosDeletes: CollectSafePositionDeletes(group.Tasks),
		BytesBefore:    group.TotalSizeBytes,
		BytesAfter:     bytesAfter,
	}, nil
}

// rewriteDataFilesPartial stages each group as its own rewrite
// snapshot via [Transaction.ReplaceFiles] directly. Per-group staging
// lets a mid-loop write failure leave already-staged groups on the
// transaction; the catalog still receives them at
// [Transaction.Commit] time.
//
// Validator registration is coalesced: a single [rewriteValidator]
// covering every rewritten path across all groups is registered once,
// after the loop, instead of one per group. The transaction's
// validator list otherwise grows linearly with the group count, and
// each entry independently walks the concurrent-snapshot set on
// refresh-replay — the union walk subsumes them.
func (t *Transaction) rewriteDataFilesPartial(ctx context.Context, groups []CompactionTaskGroup, opts RewriteDataFilesOptions) (*RewriteResult, error) {
	result := &RewriteResult{}
	props := maps.Clone(opts.SnapshotProps)
	var allRewritten []string

	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		if len(group.Tasks) == 0 {
			continue
		}

		gr, err := ExecuteCompactionGroup(ctx, t.tbl, group, opts.GroupOptions...)
		if err != nil {
			return result, err
		}

		if len(gr.OldDataFiles) == 0 && len(gr.NewDataFiles) == 0 {
			continue
		}

		if err := t.ReplaceFiles(ctx, gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes,
			props, withRewriteSemantics()); err != nil {
			return result, fmt.Errorf("commit compaction group %q: %w", group.PartitionKey, err)
		}

		for _, f := range gr.OldDataFiles {
			allRewritten = append(allRewritten, f.FilePath())
		}
		accumulateGroupMetrics(result, gr)
	}

	if len(allRewritten) > 0 {
		t.addValidator(rewriteValidator(allRewritten))
	}

	return result, nil
}

func accumulateGroupMetrics(r *RewriteResult, gr CompactionGroupResult) {
	r.RewrittenGroups++
	r.AddedDataFiles += len(gr.NewDataFiles)
	r.RemovedDataFiles += len(gr.OldDataFiles)
	r.RemovedPositionDeleteFiles += len(gr.SafePosDeletes)
	r.BytesBefore += gr.BytesBefore
	r.BytesAfter += gr.BytesAfter
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

// CollectSafePositionDeletes returns position delete files from the
// given tasks that are safe to remove during compaction.
//
// A position delete file is safe to remove when it was matched to a
// data file (via scan planning) and that data file is being rewritten
// in this compaction group. Since ReadTasks applies the deletes during
// reading, the new output files will not contain the deleted rows.
//
// Only position deletes (EntryContentPosDeletes) are considered.
// Equality deletes are decided by [compaction.DecideDeadEqualityDeletes]
// (which needs partition-wide visibility, not just the task scope).
// Deletion vectors will be handled when DV read support lands.
//
// Caller contract: every data file referenced by a returned pos-delete
// must be in the caller's rewrite set across the entire commit.
// This function only sees one group's tasks, but a pos-delete file
// can reference data files across multiple groups (the planner
// bin-packs within a partition via [compaction.Config.PlanCompaction]
// and skips files via MinInputFiles). If a pos-delete is reported safe
// by one group but references a still-live data file in another group
// — or a file the planner skipped — committing only this group's
// rewrite would orphan the still-live data file's deletes. Coordinators
// that aggregate multiple groups into one rewrite snapshot are
// responsible for re-checking against the full set of rewritten paths,
// or for moving this computation leader-side once worker outputs have
// aggregated.
//
// [ExecuteCompactionGroup] calls this internally to populate
// [CompactionGroupResult.SafePosDeletes]. It is kept exported for
// custom workers that want the spec-shaped predicate without taking
// the rest of [ExecuteCompactionGroup]'s read+write pipeline.
func CollectSafePositionDeletes(tasks []FileScanTask) []iceberg.DataFile {
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
