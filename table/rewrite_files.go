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
	"errors"
	"fmt"

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
//     rejected pre-flight at [Transaction.Commit] time. Pos-delete
//     conflict detection requires v3 manifests (which carry the
//     referenced-data-file column); on v2, only the conservative
//     eq-delete-during-rewrite rule fires.
//
// Distributed compaction coordinators construct one [RewriteFiles] on
// the leader transaction, feed worker outputs in via [RewriteFiles.Apply],
// and commit one snapshot. In-process callers can use
// [Transaction.RewriteDataFiles] which drives this builder internally.
//
// Adding new delete files (e.g., rewriting position deletes into
// deletion vectors) is not yet supported; [RewriteFiles.AddDataFile]
// rejects pos/eq-delete inputs at commit time. Add the support to
// the underlying [Transaction.ReplaceFiles] before lifting that
// restriction.
type RewriteFiles struct {
	txn                 *Transaction
	dataFilesToDelete   []iceberg.DataFile
	dataFilesToAdd      []iceberg.DataFile
	deleteFilesToRemove []iceberg.DataFile
	snapshotProps       iceberg.Properties
}

// NewRewrite returns a [RewriteFiles] builder bound to this transaction.
// Mirrors Java's org.apache.iceberg.Table#newRewrite. snapshotProps is
// added to the rewrite snapshot's summary; pass nil for none.
//
// Usage:
//
//	rewrite := tx.NewRewrite(nil)
//	rewrite.DeleteFile(oldDataFile)
//	rewrite.AddDataFile(newDataFile)
//	if err := rewrite.Commit(ctx); err != nil { ... }
//	committed, err := tx.Commit(ctx)
func (t *Transaction) NewRewrite(snapshotProps iceberg.Properties) *RewriteFiles {
	return &RewriteFiles{txn: t, snapshotProps: snapshotProps}
}

// DeleteFile marks a file for removal in this rewrite. Routes by
// content type: data files are queued as data-file replacements;
// pos/eq-delete files are queued for delete-file removal alongside
// the data rewrite (typical when a delete is fully applied to data
// files being rewritten and is therefore safe to expunge).
func (r *RewriteFiles) DeleteFile(df iceberg.DataFile) *RewriteFiles {
	if df.ContentType() == iceberg.EntryContentData {
		r.dataFilesToDelete = append(r.dataFilesToDelete, df)
	} else {
		r.deleteFilesToRemove = append(r.deleteFilesToRemove, df)
	}

	return r
}

// AddDataFile queues a new data file. Adding delete files is not yet
// supported by the underlying snapshot machinery; passing a
// pos/eq-delete here is reported at [RewriteFiles.Commit].
func (r *RewriteFiles) AddDataFile(df iceberg.DataFile) *RewriteFiles {
	r.dataFilesToAdd = append(r.dataFilesToAdd, df)

	return r
}

// Apply is a bulk shortcut that routes a worker's outputs onto this
// builder: every entry in deletes and safeDeletes is queued via
// [RewriteFiles.DeleteFile] (which routes data vs. delete files by
// content type), and every entry in adds via [RewriteFiles.AddDataFile].
//
// safeDeletes is the position-delete files referenced by tasks in
// the rewrite group whose target data file is being rewritten — they
// are safe to expunge in the rewrite snapshot. [CollectSafePositionDeletes]
// computes this set; [ExecuteCompactionGroup] populates
// [CompactionGroupResult.SafePosDeletes] from it.
//
// The typical distributed-coordinator pattern is one [RewriteFiles]
// builder + one Apply call per worker result + one Commit:
//
//	rewrite := leaderTxn.NewRewrite(snapshotProps)
//	for _, gr := range workerResults {
//	    rewrite.Apply(gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes)
//	}
//	if err := rewrite.Commit(ctx); err != nil { ... }
func (r *RewriteFiles) Apply(deletes, adds, safeDeletes []iceberg.DataFile) *RewriteFiles {
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

// Commit stages the rewrite snapshot on the underlying transaction.
// The catalog commit happens once, later, at [Transaction.Commit] time.
//
// Returns an error if the builder has no file changes, if any
// [RewriteFiles.AddDataFile] input is not a data file, or if the underlying
// [Transaction.ReplaceFiles] call fails.
func (r *RewriteFiles) Commit(ctx context.Context) error {
	if len(r.dataFilesToDelete) == 0 && len(r.dataFilesToAdd) == 0 && len(r.deleteFilesToRemove) == 0 {
		return errors.New("rewrite must have at least one file change")
	}

	for i, df := range r.dataFilesToAdd {
		if df.ContentType() != iceberg.EntryContentData {
			return fmt.Errorf("AddDataFile only supports data files; got content type %s at index %d (%s)",
				df.ContentType(), i, df.FilePath())
		}
	}

	// Register the rewrite-specific conflict validator covering every
	// rewritten data file before staging the ReplaceFiles. The validator
	// pairs with withRewriteSemantics (which suppresses the overwrite
	// producer's default isolation validator) so concurrent pos/eq
	// deletes targeting a rewritten file are caught pre-flight.
	if len(r.dataFilesToDelete) > 0 {
		rewritten := make([]string, 0, len(r.dataFilesToDelete))
		for _, df := range r.dataFilesToDelete {
			rewritten = append(rewritten, df.FilePath())
		}
		r.txn.validators = append(r.txn.validators, rewriteValidator(rewritten))
	}

	return r.txn.ReplaceFiles(ctx, r.dataFilesToDelete, r.dataFilesToAdd, r.deleteFilesToRemove, r.snapshotProps, withRewriteSemantics())
}
