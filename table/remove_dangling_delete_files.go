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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"

	"github.com/apache/iceberg-go"
)

// danglingDeleteKey follows the scan-planning identity of a delete file.
// Regular position/equality deletes are identified by path. Multiple deletion
// vectors can share a Puffin file, so each vector needs its own blob identity.
type danglingDeleteKey struct {
	path       string
	referenced string
	offset     int64
	length     int64
	vector     bool
}

func keyForDanglingDelete(df iceberg.DataFile) danglingDeleteKey {
	key := danglingDeleteKey{path: df.FilePath()}
	if !IsDeletionVector(df) {
		return key
	}

	key.vector = true
	key.referenced = explicitReferencedDataFile(df)
	key.offset, key.length = -1, -1
	if offset := df.ContentOffset(); offset != nil {
		key.offset = *offset
	}
	if length := df.ContentSizeInBytes(); length != nil {
		key.length = *length
	}

	return key
}

// decideDanglingDeleteFiles mirrors Java's RemoveDanglingDeleteFilesAction:
// delete files that are referenced by at least one scan task are still live.
// This includes metrics pruning and DV supersession, not only partition and
// sequence checks. Do not inspect physical delete-file contents.
func decideDanglingDeleteFiles(candidates []iceberg.DataFile, tasks []FileScanTask) []iceberg.DataFile {
	referenced := make(map[danglingDeleteKey]struct{})
	for _, task := range tasks {
		for _, df := range task.DeleteFiles {
			referenced[keyForDanglingDelete(df)] = struct{}{}
		}
		for _, df := range task.EqualityDeleteFiles {
			referenced[keyForDanglingDelete(df)] = struct{}{}
		}
		for _, df := range task.DeletionVectorFiles {
			referenced[keyForDanglingDelete(df)] = struct{}{}
		}
	}

	var dangling []iceberg.DataFile
	for _, df := range candidates {
		if _, ok := referenced[keyForDanglingDelete(df)]; !ok {
			dangling = append(dangling, df)
		}
	}

	return dangling
}

// RemoveDanglingDeleteFilesResult reports delete-file references removed by cleanup.
type RemoveDanglingDeleteFilesResult struct {
	// Table is set by the table-level action after a successful commit.
	// It is nil when the cleanup is only staged on a transaction.
	Table *Table

	// RemovedDeleteFiles counts removed position/equality delete files and
	// deletion vectors. Physical files are never deleted by this action.
	RemovedDeleteFiles int
}

// PlanDanglingDeleteFiles identifies delete files that are not attached to any
// data file by an unfiltered scan of the current snapshot. It reads metadata
// only; no physical data, delete files, or Puffin blobs are opened.
//
// Position and equality deletes use the scan planner's partition, sequence,
// and metrics checks. DVs are identified by their blob, not just Puffin path.
// Unknown delete sequence numbers and incomplete DV metadata are retained.
func (t Table) PlanDanglingDeleteFiles(ctx context.Context) ([]iceberg.DataFile, error) {
	snap := t.CurrentSnapshot()
	if snap == nil || t.metadata.FormatVersion() < 2 {
		return nil, nil
	}

	return t.planDanglingDeleteFiles(ctx, snap, t.Scan())
}

// PlanDanglingDeleteFiles plans cleanup against the transaction's target branch.
// It also sees snapshots already staged in the transaction, not just the
// original table head. The scan is local so an external planner cannot read
// committed catalog state instead of the staged metadata.
func (tx *Transaction) PlanDanglingDeleteFiles(ctx context.Context) ([]iceberg.DataFile, error) {
	meta, err := tx.txnMeta()
	if err != nil {
		return nil, err
	}
	snap := tx.planningSnapshot(meta)
	if snap == nil || meta.formatVersion < 2 {
		return nil, nil
	}

	built, err := meta.Build()
	if err != nil {
		return nil, err
	}
	view := *tx.tbl
	view.metadata = built
	scan := view.Scan(WithScanPlanningMode(ScanPlanningLocal))
	// Missing branches fork from main, matching Transaction.planningSnapshot.
	// Existing branches must scan their own head using the current schema.
	if tx.branch != "" && tx.branch != MainBranch && built.SnapshotByName(tx.branch) != nil {
		branchScan, err := scan.UseRef(tx.branch)
		_ = scan.Close()
		if err != nil {
			return nil, err
		}
		scan = branchScan
	}

	return view.planDanglingDeleteFiles(ctx, snap, scan)
}

func (t Table) planDanglingDeleteFiles(ctx context.Context, snap *Snapshot, scan *Scan) ([]iceberg.DataFile, error) {
	defer scan.Close()

	fs, err := t.FS(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	seen := make(map[danglingDeleteKey]struct{})
	var candidates []iceberg.DataFile
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentPosDeletes &&
				df.ContentType() != iceberg.EntryContentEqDeletes {
				continue
			}
			if entry.DataSequenceNumber() < 0 {
				continue
			}
			if IsDeletionVector(df) &&
				(df.ReferencedDataFile() == nil || df.ContentOffset() == nil || df.ContentSizeInBytes() == nil) {
				continue
			}

			key := keyForDanglingDelete(df)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			candidates = append(candidates, df)
		}
	}

	// No delete files means no scan planning, which matters for append-only tables.
	if len(candidates) == 0 {
		return nil, nil
	}

	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}

	return decideDanglingDeleteFiles(candidates, tasks), nil
}

// RemoveDanglingDeleteFiles removes metadata references to whole dangling
// delete files in one replace snapshot. It never deletes physical files,
// which may still be reachable through older snapshots.
//
// The returned result contains the committed table. A no-op returns the input
// table without writing a new snapshot. Use Transaction.RemoveDanglingDeleteFiles
// to stage cleanup alongside other updates or target a non-main branch.
func (t Table) RemoveDanglingDeleteFiles(ctx context.Context) (*RemoveDanglingDeleteFilesResult, error) {
	tx, err := t.NewTransactionOnBranch(MainBranch)
	if err != nil {
		return nil, err
	}
	result, err := tx.RemoveDanglingDeleteFiles(ctx)
	if err != nil {
		return nil, err
	}
	if result.RemovedDeleteFiles == 0 {
		result.Table = &t

		return result, nil
	}

	result.Table, err = tx.Commit(ctx)

	return result, err
}

// RemoveDanglingDeleteFiles stages the removal of entire dangling position and
// equality delete files on the transaction's branch. It does not commit the
// transaction or delete physical files. Unpartitioned tables are eligible;
// partially dangling position-delete files require a separate rewrite.
//
// The branch head used for planning is pinned for the eventual commit. If it
// changes concurrently, the commit fails rather than replaying stale removals.
func (tx *Transaction) RemoveDanglingDeleteFiles(ctx context.Context) (*RemoveDanglingDeleteFilesResult, error) {
	candidates, err := tx.PlanDanglingDeleteFiles(ctx)
	if err != nil {
		return nil, err
	}
	result := &RemoveDanglingDeleteFilesResult{RemovedDeleteFiles: len(candidates)}
	if len(candidates) == 0 {
		return result, nil
	}

	// A transaction commit already asserts its base branch head. Pin that
	// assertion against retries without revalidating it against snapshots
	// staged earlier in this same transaction.
	branch := tx.branch
	if branch == "" {
		branch = MainBranch
	}
	if err := tx.applyPinned(nil, nil, []string{branch}); err != nil {
		return nil, err
	}

	rewrite := tx.NewRewrite(nil)
	for _, df := range candidates {
		rewrite.DeleteFile(df)
	}
	if err := rewrite.Commit(ctx); err != nil {
		return nil, err
	}

	return result, nil
}
