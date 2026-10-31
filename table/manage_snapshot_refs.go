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
// metadata. A concurrent change to either snapshot ID fails Commit. REST
// catalogs cannot check ref type or retention changes that leave the ID
// unchanged. Avoid changing the target ref concurrently with this operation.
// Calls after other ref changes in this transaction use the staged ref heads.
// A transaction without updates does not contact the catalog.
func (t *Transaction) FastForwardBranch(target, source string) error {
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
	if sourceRef.Type != BranchRef && sourceRef.Type != TagRef {
		return fmt.Errorf("%w: source ref %q has invalid type %q", iceberg.ErrInvalidArgument, source, sourceRef.Type)
	}
	if _, err := meta.SnapshotByID(sourceRef.SnapshotID); err != nil {
		return fmt.Errorf("source ref %q points to an unknown snapshot: %w", source, err)
	}

	targetRef, exists := meta.refs[target]
	if exists && targetRef.Type != BranchRef {
		return fmt.Errorf("%w: target ref %q is a %s, not a branch", iceberg.ErrInvalidArgument, target, targetRef.Type)
	}
	requirements := []Requirement{assertBranchRefSnapshotID(target, t.baseRefSnapshotID(target))}
	if source != target {
		requirements = append(requirements, AssertRefSnapshotID(source, t.baseRefSnapshotID(source)))
	}
	if exists {
		if targetRef.SnapshotID == sourceRef.SnapshotID {
			// No metadata update is needed, but pin both refs so that a
			// staged metadata-only commit cannot miss a concurrent move.
			return t.applyPinned(nil, requirements, []string{target, source})
		}
		lookup := func(id int64) *Snapshot {
			snap, _ := meta.SnapshotByID(id)

			return snap
		}
		if !IsAncestorOf(sourceRef.SnapshotID, targetRef.SnapshotID, lookup) {
			return fmt.Errorf("%w: branch %q snapshot %d is not an ancestor of %q snapshot %d",
				iceberg.ErrInvalidArgument, target, targetRef.SnapshotID, source, sourceRef.SnapshotID)
		}
	}

	update := meta.NewRetainingSnapshotRefUpdate(target, sourceRef.SnapshotID, BranchRef)

	return t.applyPinned([]Update{update}, requirements, []string{target, source})
}

// ReplaceBranch sets the named branch to an existing snapshot without an
// ancestry restriction. Unlike FastForwardBranch, this can move a branch
// backwards or onto a different lineage. The branch must already exist; its
// retention settings are preserved from the transaction's metadata.
//
// Commit checks the target's snapshot ID. REST catalogs cannot check ref type
// or retention changes that leave the ID unchanged. Avoid changing the target
// ref concurrently with this operation.
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
	if existing.SnapshotID == snapshotID {
		// A no-op still fences a co-staged metadata update.
		return t.applyPinned(nil,
			[]Requirement{assertBranchRefSnapshotID(name, t.baseRefSnapshotID(name))},
			[]string{name})
	}
	if _, err := meta.SnapshotByID(snapshotID); err != nil {
		return fmt.Errorf("%w: cannot replace branch %q with unknown snapshot %d: %v",
			iceberg.ErrInvalidArgument, name, snapshotID, err)
	}

	update := meta.NewRetainingSnapshotRefUpdate(name, snapshotID, BranchRef)

	return t.applyPinned([]Update{update},
		[]Requirement{assertBranchRefSnapshotID(name, t.baseRefSnapshotID(name))},
		[]string{name})
}
