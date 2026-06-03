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

// RewriteFiles is the snapshot-operation builder for rewrite
// (compaction) commits. It is the snapshot-level sibling of [RowDelta]
// and mirrors Java's org.apache.iceberg.RewriteFiles interface
// (returned by Table.newRewrite() in Java).
//
// Compared to a raw [Transaction.ReplaceFiles] call, the builder
// owns the rewrite-specific isolation contract internally:
//
//   - The overwrite producer's default isolation validator is suppressed
//     (concurrent appends into rewritten partitions are allowed; this
//     is the defining behavior of a rewrite).
//   - A rewrite-specific conflict validator is registered so concurrent
//     pos/eq-delete files targeting any rewritten data file are
//     rejected pre-flight at [Transaction.Commit] time. The pos-delete
//     branch only fires when the concurrent writer populated the
//     manifest's referenced_data_file column (field id 143). That
//     column is V2-optional and V3-required for deletion-vector
//     deletes; V2 pos-delete writers commonly leave it empty, in
//     which case only the conservative eq-delete-during-rewrite rule
//     fires.
//
// Distributed compaction coordinators construct one [RewriteFiles] on
// the leader transaction, feed worker outputs in via [RewriteFiles.Apply],
// and commit one snapshot. In-process callers can use
// [Transaction.RewriteDataFiles] which drives this builder internally.
//
// The builder follows the same fail-fast pattern as
// [view.MetadataBuilder]: a method that hits an invalid input stages
// the error and short-circuits all subsequent calls until
// [RewriteFiles.Commit] drains it. The builder is single-use; once
// Commit has been called, a second call returns an error regardless
// of whether the first call succeeded.
//
// Adding new delete files (e.g., rewriting position deletes into
// deletion vectors) is not yet supported; [RewriteFiles.AddDataFile]
// rejects pos/eq-delete inputs at insertion time. Add the support to
// the underlying [Transaction.ReplaceFiles] before lifting that
// restriction.
type RewriteFiles struct {
	txn                 *Transaction
	dataFilesToDelete   []iceberg.DataFile
	dataFilesToAdd      []iceberg.DataFile
	deleteFilesToRemove []iceberg.DataFile
	snapshotProps       iceberg.Properties
	err                 error
	committed           bool
}

// NewRewrite returns a [RewriteFiles] builder bound to this transaction.
// Mirrors Java's org.apache.iceberg.Table#newRewrite. snapshotProps is
// cloned and the clone is added to the rewrite snapshot's summary;
// pass nil for none.
//
// Usage:
//
//	rewrite := tx.NewRewrite(nil)
//	rewrite.DeleteFile(oldDataFile)
//	rewrite.AddDataFile(newDataFile)
//	if err := rewrite.Commit(ctx); err != nil { ... }
//	committed, err := tx.Commit(ctx)
func (t *Transaction) NewRewrite(snapshotProps iceberg.Properties) *RewriteFiles {
	return &RewriteFiles{txn: t, snapshotProps: maps.Clone(snapshotProps)}
}

// DeleteFile marks a file for removal in this rewrite. Routes by
// content type: data files are queued as data-file replacements;
// pos/eq-delete files are queued for delete-file removal alongside
// the data rewrite (typical when a delete is fully applied to data
// files being rewritten and is therefore safe to expunge).
//
// Any other content type stages an error that is returned from the
// next [RewriteFiles.Commit] call.
func (r *RewriteFiles) DeleteFile(df iceberg.DataFile) *RewriteFiles {
	if r.err != nil {
		return r
	}
	if df == nil {
		r.err = fmt.Errorf("%w: DeleteFile got nil data file", ErrInvalidOperation)

		return r
	}

	switch df.ContentType() {
	case iceberg.EntryContentData:
		r.dataFilesToDelete = append(r.dataFilesToDelete, df)
	case iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
		r.deleteFilesToRemove = append(r.deleteFilesToRemove, df)
	default:
		r.err = fmt.Errorf("%w: DeleteFile got unsupported content type %s (%s)",
			ErrInvalidOperation, df.ContentType(), df.FilePath())
	}

	return r
}

// AddDataFile queues a new data file. Adding delete files is not yet
// supported by the underlying snapshot machinery; a pos/eq-delete here
// stages an error that is returned from the next [RewriteFiles.Commit]
// call. The error names the offending file path so callers driving the
// builder via [RewriteFiles.Apply] can identify it without tracking
// queue order.
func (r *RewriteFiles) AddDataFile(df iceberg.DataFile) *RewriteFiles {
	if r.err != nil {
		return r
	}
	if df == nil {
		r.err = fmt.Errorf("%w: AddDataFile got nil data file", ErrInvalidOperation)

		return r
	}

	if df.ContentType() != iceberg.EntryContentData {
		r.err = fmt.Errorf("%w: AddDataFile only supports data files; got content type %s (%s)",
			ErrInvalidOperation, df.ContentType(), df.FilePath())

		return r
	}
	r.dataFilesToAdd = append(r.dataFilesToAdd, df)

	return r
}

// Apply is a bulk shortcut that routes three slices onto this builder:
// every entry in deletes and safeDeletes is queued via
// [RewriteFiles.DeleteFile] (which routes data vs. delete files by
// content type), and every entry in adds via [RewriteFiles.AddDataFile].
//
// Distributed coordinators should prefer [RewriteFiles.ApplyResult],
// which takes a [CompactionGroupResult] directly: the three positional
// same-typed slices here transpose silently under refactor.
func (r *RewriteFiles) Apply(deletes, adds, safeDeletes []iceberg.DataFile) *RewriteFiles {
	if r.err != nil {
		return r
	}

	for _, df := range deletes {
		r.DeleteFile(df)
	}
	for _, df := range adds {
		r.AddDataFile(df)
	}
	for _, df := range safeDeletes {
		r.DeleteFile(df)
	}

	return r
}

// ApplyResult is the typed coordinator entry point: it queues a worker's
// [CompactionGroupResult] onto this builder by routing OldDataFiles
// (via DeleteFile), NewDataFiles (via AddDataFile), and SafePosDeletes
// (via DeleteFile) in one call. Prefer this over [RewriteFiles.Apply]
// when feeding worker outputs — the field names line up with the
// builder semantics, so a refactor of CompactionGroupResult cannot
// silently transpose roles.
//
// Typical distributed-coordinator pattern:
//
//	rewrite := leaderTxn.NewRewrite(snapshotProps)
//	for _, gr := range workerResults {
//	    rewrite.ApplyResult(gr)
//	}
//	if err := rewrite.Commit(ctx); err != nil { ... }
func (r *RewriteFiles) ApplyResult(gr CompactionGroupResult) *RewriteFiles {
	return r.Apply(gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes)
}

// Commit stages the rewrite snapshot on the underlying transaction.
// The catalog commit happens once, later, at [Transaction.Commit] time.
//
// Commit is single-shot: any second call returns an error regardless
// of whether the first call succeeded, and neither re-stages the
// rewrite nor re-registers the conflict validator. Returns an error
// if any file passed to [RewriteFiles.AddDataFile] or
// [RewriteFiles.DeleteFile] had an unsupported content type, if the
// builder has no file changes, or if the underlying
// [Transaction.ReplaceFiles] call fails.
func (r *RewriteFiles) Commit(ctx context.Context) error {
	if r.committed {
		return fmt.Errorf("%w: RewriteFiles.Commit already called on this builder", ErrInvalidOperation)
	}
	r.committed = true

	if r.err != nil {
		return r.err
	}
	if len(r.dataFilesToDelete) == 0 && len(r.dataFilesToAdd) == 0 && len(r.deleteFilesToRemove) == 0 {
		return fmt.Errorf("%w: rewrite must have at least one file change", ErrInvalidOperation)
	}
	// Adds-without-deletes would route through ReplaceFiles →
	// ReplaceDataFilesWithDataFiles → AddDataFiles, an OpAppend
	// producer that never reads cfg.rewriteSemantics. The snapshot
	// would be tagged append with no rewrite validator — silently
	// wrong for a rewrite. A pure delete-file expunge (only
	// deleteFilesToRemove non-empty) is still legitimate.
	if len(r.dataFilesToDelete) == 0 && len(r.dataFilesToAdd) > 0 {
		return fmt.Errorf("%w: rewrite must delete at least one data file when adding data files", ErrInvalidOperation)
	}

	if err := r.txn.ReplaceFiles(ctx, r.dataFilesToDelete, r.dataFilesToAdd, r.deleteFilesToRemove, r.snapshotProps, withRewriteSemantics()); err != nil {
		return err
	}

	if len(r.dataFilesToDelete) > 0 {
		rewritten := make([]string, 0, len(r.dataFilesToDelete))
		for _, df := range r.dataFilesToDelete {
			rewritten = append(rewritten, df.FilePath())
		}
		r.txn.addValidator(rewriteValidator(rewritten))
	}

	return nil
}
