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
//     rejected pre-flight at [Transaction.Commit] time. A pos-delete
//     is matched to a rewritten file via its referenced_data_file
//     column, via equal file_path lower/upper bounds when that column
//     is unset, or — when neither resolves a single path — via
//     partition overlap with a rewritten file. Eq-deletes are matched
//     conservatively: any concurrent eq-delete during the rewrite
//     conflicts.
//
// Distributed compaction coordinators construct one [RewriteFiles] on
// the leader transaction, feed worker outputs in via [RewriteFiles.ApplyResult],
// and commit one snapshot. In-process callers can use
// [Transaction.RewriteDataFiles] which drives this builder internally.
//
// The builder follows the same fail-fast pattern as
// [view.MetadataBuilder]: a method that hits an invalid input stages
// the error and short-circuits all subsequent calls until
// [RewriteFiles.Commit] drains it. The builder is single-use; once
// Commit has been called, a second call returns an error regardless
// of whether the first call succeeded.
type RewriteFiles struct {
	txn                 *Transaction
	dataFilesToDelete   []iceberg.DataFile
	dataFilesToAdd      []iceberg.DataFile
	deleteFilesToAdd    []iceberg.DataFile
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

// AddDataFile queues a new data file. Use [RewriteFiles.AddDeleteFile] for
// positional deletes, equality deletes, or deletion vectors.
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

// AddDeleteFile queues a new positional delete, equality delete, or deletion
// vector for this rewrite. The file is added in the same snapshot as any data
// replacements and delete-file removals staged on the builder.
func (r *RewriteFiles) AddDeleteFile(df iceberg.DataFile) *RewriteFiles {
	if r.err != nil {
		return r
	}
	if df == nil {
		r.err = fmt.Errorf("%w: AddDeleteFile got nil data file", ErrInvalidOperation)

		return r
	}

	switch df.ContentType() {
	case iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
		r.deleteFilesToAdd = append(r.deleteFilesToAdd, df)
	default:
		r.err = fmt.Errorf("%w: AddDeleteFile only supports delete files; got content type %s (%s)",
			ErrInvalidOperation, df.ContentType(), df.FilePath())
	}

	return r
}

// AddFile queues a file according to its content type. It is useful for
// coordinators that carry data and delete files in one result slice.
func (r *RewriteFiles) AddFile(df iceberg.DataFile) *RewriteFiles {
	if df == nil {
		r.err = fmt.Errorf("%w: AddFile got nil data file", ErrInvalidOperation)

		return r
	}

	switch df.ContentType() {
	case iceberg.EntryContentData:
		return r.AddDataFile(df)
	case iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
		return r.AddDeleteFile(df)
	default:
		r.err = fmt.Errorf("%w: AddFile got unsupported content type %s (%s)",
			ErrInvalidOperation, df.ContentType(), df.FilePath())

		return r
	}
}

// Apply is a bulk shortcut that routes three slices onto this builder:
// every entry in deletes and safeDeletes is queued via
// [RewriteFiles.DeleteFile] (which routes data vs. delete files by
// content type), and every entry in adds via [RewriteFiles.AddFile].
//
// Deprecated: use [RewriteFiles.ApplyResult], which also carries
// SafeDeletionVectors. Apply has no slot for them, so a coordinator wiring
// worker output through Apply leaves deletion vectors for the rewritten files
// orphaned.
func (r *RewriteFiles) Apply(deletes, adds, safeDeletes []iceberg.DataFile) *RewriteFiles {
	if r.err != nil {
		return r
	}

	for _, df := range deletes {
		r.DeleteFile(df)
	}
	for _, df := range adds {
		r.AddFile(df)
	}
	for _, df := range safeDeletes {
		r.DeleteFile(df)
	}

	return r
}

// ApplyResult queues a worker's [CompactionGroupResult] onto this builder,
// routing OldDataFiles (DeleteFile), NewDataFiles (AddFile), SafePosDeletes
// and SafeDeletionVectors (DeleteFile). Prefer it over [RewriteFiles.Apply],
// which cannot carry SafeDeletionVectors.
//
// Typical distributed-coordinator pattern:
//
//	rewrite := leaderTxn.NewRewrite(snapshotProps)
//	for _, gr := range workerResults {
//	    rewrite.ApplyResult(gr)
//	}
//	if err := rewrite.Commit(ctx); err != nil { ... }
func (r *RewriteFiles) ApplyResult(gr CompactionGroupResult) *RewriteFiles {
	for _, df := range gr.OldDataFiles {
		r.DeleteFile(df)
	}
	for _, df := range gr.NewDataFiles {
		r.AddFile(df)
	}
	// DVs route through DeleteFile like any pos-delete; ReplaceFiles then
	// re-identifies them by referenced data file.
	for _, df := range gr.SafePosDeletes {
		r.DeleteFile(df)
	}
	for _, df := range gr.SafeDeletionVectors {
		r.DeleteFile(df)
	}

	return r
}

// Commit stages the rewrite snapshot on the underlying transaction.
// The catalog commit happens once, later, at [Transaction.Commit] time.
//
// Commit is single-shot: any second call returns an error regardless
// of whether the first call succeeded, and neither re-stages the
// rewrite nor re-registers the conflict validator. Returns an error
// if any file passed to [RewriteFiles.AddFile], [RewriteFiles.AddDataFile], or
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
	if len(r.dataFilesToDelete) == 0 && len(r.dataFilesToAdd) == 0 &&
		len(r.deleteFilesToAdd) == 0 && len(r.deleteFilesToRemove) == 0 {
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

	if err := r.txn.ReplaceFilesWithDeleteFiles(ctx, r.dataFilesToDelete, r.dataFilesToAdd,
		r.deleteFilesToRemove, r.deleteFilesToAdd, r.snapshotProps, withRewriteSemantics()); err != nil {
		return err
	}

	if len(r.dataFilesToDelete) > 0 {
		r.txn.addValidator(rewriteValidator(r.dataFilesToDelete))
	}

	return nil
}
