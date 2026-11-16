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
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
)

// FastForwardBranch advances target to the snapshot referenced by source.
// Source can be a branch or tag and is never changed. If target does not
// exist, it is created with default retention settings. If target exists,
// it must be a branch and its snapshot must be an ancestor of source's
// snapshot. The target keeps its retention settings.
//
// Both refs are pinned to their snapshot IDs in the transaction's base
// metadata. A concurrent change to either ID fails Commit. This also stops
// the normal retry of a co-staged append if a pinned ref moves. When the
// source names a snapshot staged in this transaction, the commit branch is
// pinned too, so retry cannot change the source snapshot's ancestry. No catalog's
// snapshot-ID requirement detects retention-only changes, and REST catalogs
// also cannot detect a ref type change with the same snapshot ID.
// Fast-forward needs the full ancestor chain. With a REST catalog configured
// with snapshot-loading-mode=refs, a valid fast-forward can be rejected;
// reload with snapshot-loading-mode=all before calling this method.
// For non-main targets, use Table.NewTransactionOnBranch(target) so an
// unrelated main head move does not conflict with the transaction's implicit
// commit-branch requirement.
// Calls after other ref changes in this transaction use the staged ref heads.
// A transaction without updates does not contact the catalog.
func (t *Transaction) FastForwardBranch(target, source string) error {
	return t.moveBranchToRef(target, source, true)
}

// ReplaceBranchFromRef moves target to the snapshot named by source without
// checking ancestry, matching Java's ManageSnapshots.replaceBranch(from, to).
// Source may be a branch or tag and is left unchanged. If target is absent,
// it is created as a branch with default retention; otherwise the target
// branch retains its existing retention settings.
// For non-main targets, use Table.NewTransactionOnBranch(target) to avoid
// requiring main to remain unchanged during the commit.
//
// Both ref heads are pinned to their base snapshot IDs, so concurrent head
// moves fail the commit and prevent the normal retry of a co-staged append.
// A source snapshot staged in this transaction also pins the commit branch,
// preventing retry from rebuilding that snapshot on a different lineage.
// No catalog's snapshot-ID requirement detects retention-only changes, and
// REST catalogs also cannot detect ref type changes with the same snapshot ID.
func (t *Transaction) ReplaceBranchFromRef(target, source string) error {
	return t.moveBranchToRef(target, source, false)
}

func (t *Transaction) moveBranchToRef(target, source string, fastForward bool) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if target == "" || source == "" {
		return fmt.Errorf("%w: target and source ref names must not be empty", iceberg.ErrInvalidArgument)
	}

	sourceRef, ok := meta.refs[source]
	if !ok {
		return fmt.Errorf("%w: source ref %q does not exist", iceberg.ErrInvalidArgument, source)
	}

	targetRef, exists := meta.refs[target]
	if exists && targetRef.Type != BranchRef {
		return fmt.Errorf("%w: target ref %q is a %s, not a branch", iceberg.ErrInvalidArgument, target, targetRef.Type)
	}
	requirements := []Requirement{assertBranchRefSnapshotID(target, t.baseRefSnapshotID(target))}
	if source != target {
		requirements = append(requirements, AssertRefSnapshotID(source, t.baseRefSnapshotID(source)))
	}
	refsToPin := t.refsToPinForSnapshot(sourceRef.SnapshotID, target, source)
	if exists {
		if targetRef.SnapshotID == sourceRef.SnapshotID {
			// No metadata update is needed, but pin both refs so that a
			// staged metadata-only commit cannot miss a concurrent move.
			return t.applyPinned(nil, requirements, refsToPin)
		}
		lookup := func(id int64) *Snapshot {
			snap, _ := meta.SnapshotByID(id)

			return snap
		}
		if fastForward && !IsAncestorOf(sourceRef.SnapshotID, targetRef.SnapshotID, lookup) {
			return fmt.Errorf("%w: branch %q snapshot %d is not an ancestor of %q snapshot %d (REST snapshot-loading-mode=refs may omit ancestors; reload with snapshot-loading-mode=all)",
				iceberg.ErrInvalidArgument, target, targetRef.SnapshotID, source, sourceRef.SnapshotID)
		}
	}

	update := meta.NewRetainingSnapshotRefUpdate(target, sourceRef.SnapshotID, BranchRef)

	return t.applyPinned([]Update{update}, requirements, refsToPin)
}

// refsToPinForSnapshot also pins the commit branch when a promotion uses a
// snapshot staged in this transaction. Retrying on a different commit-branch
// head could rebuild the snapshot with different ancestry. For an existing
// snapshot, keep normal retries of unrelated branch moves available.
func (t *Transaction) refsToPinForSnapshot(snapshotID int64, refs ...string) []string {
	for _, staged := range t.meta.updates {
		if added, ok := staged.(*addSnapshotUpdate); ok && added.Snapshot != nil &&
			added.Snapshot.SnapshotID == snapshotID {
			return append(refs, t.branch)
		}
	}

	return refs
}

// ReplaceBranch sets the named branch to an existing snapshot without an
// ancestry restriction. Unlike FastForwardBranch, this can move a branch
// backwards or onto a different lineage. The branch must already exist; its
// retention settings are preserved from the transaction's metadata.
// For non-main targets, use Table.NewTransactionOnBranch(name) to avoid
// requiring main to remain unchanged during the commit.
//
// Commit pins the target's snapshot ID, preventing the normal retry of a
// co-staged append when the branch moves. A snapshot staged in this transaction
// also pins the commit branch, preventing a retry from changing its ancestry.
// No catalog's snapshot-ID requirement detects retention-only changes, and REST
// catalogs also miss ref type changes with the same ID. A REST catalog using
// snapshot-loading-mode=refs may omit a requested historical snapshot; reload
// with snapshot-loading-mode=all.
func (t *Transaction) ReplaceBranch(name string, snapshotID int64) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("%w: branch name must not be empty", iceberg.ErrInvalidArgument)
	}
	existing, ok := meta.refs[name]
	if !ok {
		return fmt.Errorf("%w: branch %q does not exist", iceberg.ErrInvalidArgument, name)
	}
	if existing.Type != BranchRef {
		return fmt.Errorf("%w: ref %q is a %s, not a branch", iceberg.ErrInvalidArgument, name, existing.Type)
	}
	requirements := []Requirement{assertBranchRefSnapshotID(name, t.baseRefSnapshotID(name))}
	refsToPin := t.refsToPinForSnapshot(snapshotID, name)
	if existing.SnapshotID == snapshotID {
		// A no-op still fences a co-staged metadata update.
		return t.applyPinned(nil, requirements, refsToPin)
	}

	update := meta.NewRetainingSnapshotRefUpdate(name, snapshotID, BranchRef)

	err = t.applyPinned([]Update{update}, requirements, refsToPin)
	if errors.Is(err, ErrSnapshotNotFound) {
		return fmt.Errorf("cannot replace branch %q with snapshot %d (REST snapshot-loading-mode=refs may omit historical snapshots; reload with snapshot-loading-mode=all): %w",
			name, snapshotID, err)
	}

	return err
}
