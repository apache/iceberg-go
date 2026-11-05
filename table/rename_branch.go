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

// RenameBranch atomically renames a branch using the table's normal transaction
// commit path. The snapshot and retention policy are preserved.
func (t Table) RenameBranch(ctx context.Context, from, to string) (*Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.metadata == nil || t.cat == nil {
		return nil, fmt.Errorf("%w: rename branch requires a loaded table and catalog", iceberg.ErrInvalidArgument)
	}

	tx := t.NewTransaction()
	if err := tx.RenameBranch(from, to); err != nil {
		return nil, err
	}

	updated, err := tx.Commit(ctx)
	if err != nil {
		return updated, fmt.Errorf("rename branch %q to %q: %w", from, to, err)
	}

	return updated, nil
}

// RenameBranch stages a branch rename in this transaction, so it can be
// committed atomically with other table updates. The source must be a branch,
// the destination must not exist, and main cannot be renamed or replaced.
// The source keeps its snapshot and retention settings.
//
// Both refs are pinned to their values in the transaction's base metadata.
// Concurrent source snapshot moves and destination creation fail the commit.
// Catalogs that validate requirements locally also reject source type changes.
// REST requirements only compare snapshot IDs, so retention-only changes or
// type changes with an unchanged ID cannot be detected by the REST catalog.
func (t *Transaction) RenameBranch(from, to string) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if from == "" || to == "" {
		return fmt.Errorf("%w: branch names must not be empty", iceberg.ErrInvalidArgument)
	}
	if from == to {
		return fmt.Errorf("%w: source and destination branch names must differ", iceberg.ErrInvalidArgument)
	}
	if from == MainBranch || to == MainBranch {
		return fmt.Errorf("%w: cannot rename or replace the main branch", iceberg.ErrInvalidArgument)
	}

	source, ok := meta.refs[from]
	if !ok {
		return fmt.Errorf("%w: branch %q does not exist", iceberg.ErrInvalidArgument, from)
	}
	if source.Type != BranchRef {
		return fmt.Errorf("%w: ref %q is a %s, not a branch", iceberg.ErrInvalidArgument, from, source.Type)
	}
	if _, exists := meta.refs[to]; exists {
		return fmt.Errorf("%w: ref %q already exists", iceberg.ErrInvalidArgument, to)
	}

	var maxRefAgeMs, maxSnapshotAgeMs int64
	var minSnapshotsToKeep int
	if source.MaxRefAgeMs != nil {
		maxRefAgeMs = *source.MaxRefAgeMs
	}
	if source.MaxSnapshotAgeMs != nil {
		maxSnapshotAgeMs = *source.MaxSnapshotAgeMs
	}
	if source.MinSnapshotsToKeep != nil {
		minSnapshotsToKeep = *source.MinSnapshotsToKeep
	}

	updates := []Update{
		NewSetSnapshotRefUpdate(to, source.SnapshotID, BranchRef,
			maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep),
		NewRemoveSnapshotRefUpdate(from),
	}
	requirements := []Requirement{
		assertBranchRefSnapshotID(from, t.baseRefSnapshotID(from)),
		AssertRefSnapshotID(to, t.baseRefSnapshotID(to)),
	}

	return t.applyPinned(updates, requirements, []string{from, to})
}
