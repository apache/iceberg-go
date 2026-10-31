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
	"fmt"
	"maps"
	"strconv"

	"github.com/apache/iceberg-go"
)

// CherryPickSnapshot applies the additions from an existing append snapshot to
// the transaction's target branch. The original snapshot and its branch are
// not modified. If the source directly follows the target head, the target
// fast-forwards to the existing source snapshot (including for non-append
// operations and v3). Otherwise, a supported append is replayed into a new
// snapshot and sequence number without copying the underlying data files.
//
// Source snapshots may come from a different branch or an unreferenced
// snapshot still present in table metadata. A source that is already an
// ancestor of the target branch is rejected to avoid replaying old changes.
// Only append snapshots can be replayed onto a divergent target. Other
// operations require the direct-parent fast-forward described above.
//
// Replaying file additions is currently limited to v1 and v2. In v3,
// copying files into another snapshot may reuse row IDs; fast-forwarding an
// existing snapshot is safe because the lineage is not duplicated.
//
// The target branch is pinned to its base head to prevent a concurrent append
// from introducing duplicate file references between validation and commit.
// A conflict must be retried with a new transaction and refreshed table.
func (t *Transaction) CherryPickSnapshot(ctx context.Context, snapshotID int64, snapshotProps iceberg.Properties) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	source, err := meta.SnapshotByID(snapshotID)
	if err != nil {
		return err
	}
	// Check both direct ancestry and earlier cherry-picks. A later snapshot
	// may have removed the source files, so checking live paths is not enough.
	wapID := ""
	if source.Summary != nil {
		wapID = source.Summary.Properties["wap.id"]
	}
	head := t.planningSnapshot(meta)
	if head != nil {
		lookup := func(id int64) *Snapshot {
			snapshot, _ := meta.SnapshotByID(id)

			return snapshot
		}
		for _, ancestor := range AncestorsOf(head.SnapshotID, lookup) {
			if ancestor.SnapshotID == snapshotID ||
				(ancestor.Summary != nil && ancestor.Summary.Properties["source-snapshot-id"] == strconv.FormatInt(snapshotID, 10)) {
				return fmt.Errorf("%w: snapshot %d has already been applied to target branch %q", ErrInvalidOperation, snapshotID, t.branch)
			}
			if wapID != "" && ancestor.Summary != nil &&
				(ancestor.Summary.Properties["wap.id"] == wapID || ancestor.Summary.Properties["published-wap-id"] == wapID) {
				return fmt.Errorf("%w: WAP ID %q has already been published", ErrInvalidOperation, wapID)
			}
		}
	}

	// Java's CherryPickOperation returns the source snapshot itself when its
	// parent matches the target head. This is valid for all operations and
	// all table versions: no file is copied, so row lineage stays intact.
	// A fast-forward intentionally has no new snapshot summary properties.
	fastForward := head == nil && source.ParentSnapshotID == nil
	if head != nil && source.ParentSnapshotID != nil {
		fastForward = *source.ParentSnapshotID == head.SnapshotID
	}
	if fastForward {
		target := t.branch
		if target == "" {
			target = MainBranch
		}
		if ref, ok := meta.refs[target]; ok && ref.Type != BranchRef {
			return fmt.Errorf("%w: target ref %q is not a branch", ErrInvalidOperation, target)
		}
		update := meta.NewRetainingSnapshotRefUpdate(target, source.SnapshotID, BranchRef)

		return t.applyPinned([]Update{update},
			[]Requirement{assertBranchRefSnapshotID(target, t.baseRefSnapshotID(target))},
			[]string{target})
	}

	if source.Summary == nil || source.Summary.Operation != OpAppend {
		return fmt.Errorf("%w: snapshot %d is not append, dynamic overwrite, or a fast-forward",
			ErrInvalidOperation, snapshotID)
	}
	if meta.formatVersion >= 3 {
		return fmt.Errorf("%w: replaying v3 append snapshots requires row ID reassignment",
			iceberg.ErrNotImplemented)
	}

	props := make(iceberg.Properties, len(snapshotProps)+2)
	maps.Copy(props, snapshotProps)
	props["source-snapshot-id"] = strconv.FormatInt(snapshotID, 10)
	if wapID != "" {
		props["published-wap-id"] = wapID
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	// A source snapshot references its ancestors' files too. Select only
	// entries added by this snapshot, not every live file in its manifests.
	// A valid append snapshot never adds delete files or removes files.
	added := make([]iceberg.DataFile, 0)
	seen := make(map[string]struct{})
	// Manifests are immutable. An entry added by the source snapshot can
	// only appear in a manifest created by that snapshot. Skip inherited
	// manifests rather than scanning the entire table history.
	manifests, err := source.Manifests(fs)
	if err != nil {
		return fmt.Errorf("read cherry-pick source snapshot %d: %w", snapshotID, err)
	}
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return err
		}
		if manifest.SnapshotID() != snapshotID {
			continue
		}
		for entry, err := range manifest.Entries(fs, false) {
			if err != nil {
				return fmt.Errorf("read cherry-pick source snapshot %d: %w", snapshotID, err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.SnapshotID() != snapshotID {
				continue
			}
			switch entry.Status() {
			case iceberg.EntryStatusDELETED:
				return fmt.Errorf("%w: append snapshot %d contains a removed file %q", ErrInvalidOperation, snapshotID, entry.DataFile().FilePath())
			case iceberg.EntryStatusADDED:
				file := entry.DataFile()
				if file.ContentType() != iceberg.EntryContentData {
					return fmt.Errorf("%w: append snapshot %d contains an added delete file %q", ErrInvalidOperation, snapshotID, file.FilePath())
				}
				if _, exists := seen[file.FilePath()]; exists {
					return fmt.Errorf("%w: source snapshot %d adds file %q more than once", iceberg.ErrInvalidArgument, snapshotID, file.FilePath())
				}
				seen[file.FilePath()] = struct{}{}
				added = append(added, file)
			}
		}
	}

	// Unlike a normal fast append, replaying old files is not safe to rebase
	// after the target branch moves. Recheck its original head on commit.
	if err := t.AssertRefSnapshotID(""); err != nil {
		return err
	}
	if len(added) == 0 {
		// Empty append snapshots are still valid commits and must move the
		// target branch head, including when recording ingestion progress.
		return t.AddDataFilesByPath(ctx, nil, props, false)
	}

	return t.AddDataFiles(ctx, added, props)
}
