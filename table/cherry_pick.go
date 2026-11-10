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
// Divergent replace-partitions overwrites also require ancestry, removed-file,
// and changed-partition validation. Both follow-ups are tracked in
// https://github.com/apache/iceberg-go/issues/2415.
//
// snapshotProps applies only when a divergent append is replayed. A fast-forward
// reuses the source snapshot unchanged and ignores these properties. On replay,
// source-snapshot-id and published-wap-id are derived from the source, while a
// caller-supplied wap.id is discarded so the result is not staged.
//
// The target branch is pinned to its base head. Divergent replay onto a new
// target also pins main because its append inherits main's files. A fast-forward
// onto a new target points directly to the source, so does not pin main.
// A commit fails if a pinned ref moves; other table-level conflicts can retry
// by replaying staged updates in order.
//
// Duplicate-publication checks need complete ancestry when REST loaded only
// referenced snapshots (snapshot-loading-mode=refs). Reload in mode=all before
// publishing from an incomplete refs-only view. Normal snapshot expiration may
// remove old ancestors and does not by itself prevent cherry-picking.
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
		ancestors, complete := AncestorsOfChecked(head.SnapshotID, lookup)
		for _, ancestor := range ancestors {
			if ancestor.SnapshotID == snapshotID ||
				(ancestor.Summary != nil && ancestor.Summary.Properties["source-snapshot-id"] == strconv.FormatInt(snapshotID, 10)) {
				return fmt.Errorf("%w: snapshot %d has already been applied to target branch %q", ErrInvalidOperation, snapshotID, t.branch)
			}
			if wapID != "" && ancestor.Summary != nil &&
				(ancestor.Summary.Properties["wap.id"] == wapID || ancestor.Summary.Properties["published-wap-id"] == wapID) {
				return fmt.Errorf("%w: WAP ID %q has already been published", ErrInvalidOperation, wapID)
			}
		}
		if !complete && t.tbl.snapshotHistoryRefsOnly {
			return fmt.Errorf("%w: cannot cherry-pick snapshot %d onto branch %q with incomplete target ancestry under snapshot-loading-mode=refs; reload with snapshot-loading-mode=all",
				ErrInvalidOperation, snapshotID, t.branch)
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
		baseHead := t.baseRefSnapshotID(target)
		req := AssertRefSnapshotID(target, baseHead)
		if baseHead != nil {
			req = assertBranchRefSnapshotID(target, baseHead)
		}
		// A fast-forward to a missing branch references source directly;
		// unlike a replay, it does not fork from main.
		return t.applyPinned([]Update{update}, []Requirement{req}, []string{target})
	}

	if source.Summary == nil || source.Summary.Operation != OpAppend {
		// TODO(#2415): replay eligible replace-partitions overwrites only
		// after validating ancestry, removed files, and partition conflicts.
		return fmt.Errorf("%w: snapshot %d is not an append and cannot be replayed onto a divergent branch",
			ErrInvalidOperation, snapshotID)
	}
	if meta.formatVersion >= 3 {
		// TODO(#2415): reassign row IDs when replaying v3 append files.
		return fmt.Errorf("%w: replaying v3 append snapshots requires row ID reassignment",
			iceberg.ErrNotImplemented)
	}

	props := make(iceberg.Properties, len(snapshotProps)+2)
	maps.Copy(props, snapshotProps)
	props["source-snapshot-id"] = strconv.FormatInt(snapshotID, 10)
	// The cherry-picked snapshot is published, not staged. Never carry a
	// caller-supplied WAP ID into its summary: a later publish could
	// mistake it for an unpublished staged snapshot.
	delete(props, "wap.id")
	// Publication provenance must describe the source snapshot, not a
	// caller-supplied marker that could falsely block a future WAP publish.
	delete(props, "published-wap-id")
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
	manifests, err := source.Manifests(ctx, fs)
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
		for entry, err := range manifest.Entries(ctx, fs, false) {
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
	// after the target branch moves. Register pins only after the append is
	// successfully staged, so a rejected cherry-pick leaves no stale fence.
	if len(added) == 0 {
		// Empty append snapshots are still valid commits and must move the
		// target branch head, including when recording ingestion progress.
		// Commit an empty append directly instead of setting name mapping as
		// a side effect of AddDataFilesByPath.
		wfs, err := requireWriteFileIO(fs)
		if err != nil {
			return err
		}
		meta, err := t.txnMeta()
		if err != nil {
			return err
		}
		updates, reqs, err := t.appendSnapshotProducer(meta, wfs, props).commit(ctx)
		if err != nil {
			return err
		}

		if err := t.apply(updates, reqs); err != nil {
			return err
		}
	} else {
		// The files already belong to this table. Cherry-picking does not
		// need to modify its schema name mapping.
		if err := t.AddDataFiles(ctx, added, props, WithoutAutoNameMapping()); err != nil {
			return err
		}
	}

	target := t.branch
	if target == "" {
		target = MainBranch
	}
	pins := []string{target}
	if target != MainBranch && t.baseRefSnapshotID(target) == nil {
		// Snapshot producers fork a newly created branch from main. Pin
		// main too, or a retry could graft the source's files twice after
		// another writer publishes them on main.
		if err := t.apply(nil, []Requirement{
			AssertRefSnapshotID(MainBranch, t.baseRefSnapshotID(MainBranch)),
		}); err != nil {
			return err
		}
		pins = append(pins, MainBranch)
	}

	// AddDataFiles and appendSnapshotProducer have already registered the
	// base-head requirement on target. Mark it pinned without validating an
	// assertion against the staged (and now advanced) target head.
	return t.applyPinned(nil, nil, pins)
}
