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
	"slices"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

type compactionPlanningSnapshots struct {
	id      *int64
	missing bool
}

func (s *compactionPlanningSnapshots) add(id *int64) error {
	if id == nil {
		s.missing = true

		return nil
	}
	if s.id == nil {
		snapshotID := *id
		s.id = &snapshotID
	} else if *s.id != *id {
		return fmt.Errorf("%w: compaction mixes planning snapshots %d and %d", ErrInvalidOperation, *s.id, *id)
	}

	return nil
}

func (s compactionPlanningSnapshots) resolve(explicitID *int64) (*int64, error) {
	if explicitID != nil {
		if s.id != nil && *s.id != *explicitID {
			return nil, fmt.Errorf("%w: worker planning snapshot %d differs from validation snapshot %d",
				ErrInvalidOperation, *s.id, *explicitID)
		}

		return explicitID, nil
	}
	if s.id != nil && s.missing {
		return nil, fmt.Errorf("%w: compaction mixes inputs with and without a planning snapshot ID", ErrInvalidOperation)
	}

	return s.id, nil
}

func compactionPlanningSnapshotID(groups []CompactionTaskGroup) (*int64, error) {
	var snapshots compactionPlanningSnapshots
	for _, group := range groups {
		for _, task := range group.Tasks {
			if err := snapshots.add(task.PlanningSnapshotID); err != nil {
				return nil, err
			}
		}
	}

	return snapshots.resolve(nil)
}

func compactionSourceFiles(groups []CompactionTaskGroup) ([]iceberg.DataFile, error) {
	var files []iceberg.DataFile
	for _, group := range groups {
		for _, task := range group.Tasks {
			if task.File == nil {
				return nil, fmt.Errorf("%w: compaction task has no data file", ErrInvalidOperation)
			}
			files = append(files, task.File)
		}
	}

	return files, nil
}

var errRewriteSourceRemoved = fmt.Errorf("%w: rewrite source file removed", ErrCommitFailed)

func rewriteSourceRemoved(snapshotID int64, path string) error {
	return fmt.Errorf("%w: snapshot %d removed %q after planning", errRewriteSourceRemoved, snapshotID, path)
}

func rewriteSourcePaths(files []iceberg.DataFile) map[string]struct{} {
	paths := make(map[string]struct{}, len(files))
	for _, df := range files {
		paths[df.FilePath()] = struct{}{}
	}

	return paths
}

func validateRewriteConflicts(cc *conflictContext, files []iceberg.DataFile) error {
	if len(files) == 0 || len(cc.concurrent) == 0 {
		return nil
	}
	paths := rewriteSourcePaths(files)
	if err := cc.forEachRemovedDataFile(func(snapshotID int64, path string) error {
		if _, ok := paths[path]; ok {
			return rewriteSourceRemoved(snapshotID, path)
		}

		return nil
	}); err != nil {
		return err
	}

	return validateNoNewDeletesForRewrittenFiles(cc, files)
}

var errSnapshotHistoryIncompleteForRewrite = errors.New(
	"cannot validate compaction: history may be incomplete under REST snapshot-loading-mode=refs; reload with snapshot-loading-mode=all",
)

// rewriteStartSnapshotID uses the transaction's base for plans without provenance.
func (t *Transaction) rewriteStartSnapshotID(planned *int64) *int64 {
	if planned != nil {
		return planned
	}
	if base := latestSnapshotForBranch(t.tbl.metadata, t.branch); base != nil {
		return &base.SnapshotID
	}

	return nil
}

// validateRewriteFromSnapshot covers changes already visible when the rewrite
// is staged. The transaction's existing validators cover subsequent changes
// discovered during commit retries.
func (t *Transaction) validateRewriteFromSnapshot(ctx context.Context, meta *MetadataBuilder, head *Snapshot, snapshotID *int64, rewrittenFiles []iceberg.DataFile) error {
	if snapshotID == nil || (head != nil && head.SnapshotID == *snapshotID) {
		return nil
	}
	current, err := meta.Build()
	if err != nil {
		return err
	}
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	cc, err := newConflictContextAt(ctx, current, t.branch, head, snapshotID, fs, true)
	if errors.Is(err, ErrCommitDiverged) && t.tbl.snapshotHistoryRefsOnly && head != nil {
		_, full, loadErr := loadStagedSnapshotHistory(ctx, t.tbl, meta, errSnapshotHistoryIncompleteForRewrite, ErrCommitFailed)
		if loadErr != nil {
			return loadErr
		}
		current, err = full.Build()
		if err != nil {
			return err
		}
		cc, err = newConflictContextAt(ctx, current, t.branch, current.SnapshotByID(head.SnapshotID), snapshotID, fs, true)
	}
	if err != nil {
		return err
	}

	return validateRewriteConflicts(cc, rewrittenFiles)
}

// rewriteConflictsSincePlanning records added deletes and removed data paths
// since planning. Each validation checks only the snapshots committed since
// the previous check, then applies accumulated changes to the current batch.
type rewriteConflictsSincePlanning struct {
	checkedSnapshotID *int64
	branch            string
	deletes           []addedDelete
	sourcePaths       map[string]struct{}
	removedPaths      map[string]int64
}

func (d *rewriteConflictsSincePlanning) validate(ctx context.Context, current *Table, fs iceio.IO, groups []CompactionTaskGroup) error {
	if err := d.advance(ctx, current, fs); err != nil {
		return err
	}
	if len(d.deletes) == 0 && len(d.removedPaths) == 0 {
		return nil
	}

	files, err := compactionSourceFiles(groups)
	if err != nil {
		return err
	}
	for _, file := range files {
		if snapshotID, ok := d.removedPaths[file.FilePath()]; ok {
			return rewriteSourceRemoved(snapshotID, file.FilePath())
		}
	}
	if len(d.deletes) == 0 {
		return nil
	}
	rewritten, err := newRewrittenFileSet(files)
	if err != nil {
		return err
	}
	for _, added := range d.deletes {
		if err := rewritten.conflict(added); err != nil {
			return err
		}
	}

	return nil
}

// advance records changes up to the branch head before checking a batch.
func (d *rewriteConflictsSincePlanning) advance(ctx context.Context, current *Table, fs iceio.IO) error {
	// The rewrite validator binds no filters, so case sensitivity is moot.
	head := latestSnapshotForBranch(current.metadata, d.branch)
	cc, err := newConflictContextAt(ctx, current.metadata, d.branch, head, d.checkedSnapshotID, fs, true)
	if errors.Is(err, ErrCommitDiverged) && current.snapshotHistoryRefsOnly && head != nil {
		full, loadErr := current.loadFullSnapshotHistory(ctx, errSnapshotHistoryIncompleteForRewrite, ErrCommitFailed)
		if loadErr != nil {
			return loadErr
		}
		cc, err = newConflictContextAt(ctx, full.metadata, d.branch, head, d.checkedSnapshotID, fs, true)
	}
	if err != nil {
		return err
	}
	deletes := d.deletes
	err = cc.forEachAddedEntry(iceberg.ManifestContentDeletes, func(snap Snapshot, e iceberg.ManifestEntry) error {
		added, err := newAddedDelete(snap, e.DataFile())
		if err != nil {
			return err
		}
		deletes = append(deletes, added)

		return nil
	})
	if err != nil {
		return err
	}
	if err := cc.forEachRemovedDataFile(func(snapshotID int64, path string) error {
		if _, ok := d.sourcePaths[path]; ok {
			if d.removedPaths == nil {
				d.removedPaths = make(map[string]int64)
			}
			d.removedPaths[path] = snapshotID
		}

		return nil
	}); err != nil {
		return err
	}
	d.checkedSnapshotID, d.deletes = &head.SnapshotID, deletes

	return nil
}

// commitRewrite writes manifests against the private planning transaction and
// publishes its updates and conflict checks together if staging is still valid.
func (t *Transaction) commitRewrite(ctx context.Context, base *MetadataBuilder, parent *Snapshot, updater *snapshotProducer, rewritesData bool, validator conflictValidatorFunc) (err error) {
	planning := updater.txn
	updates, reqs, pins, err := planning.prepareRewriteCommit(ctx, updater, rewritesData)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			cleanupCtx := context.WithoutCancel(ctx)
			for path := range updater.written.staged {
				if removeErr := updater.io.Remove(cleanupCtx, path); removeErr != nil {
					err = errors.Join(err, fmt.Errorf("remove unstaged rewrite manifest %s: %w", path, removeErr))
				}
			}
			if removeErr := updater.io.Remove(cleanupCtx, updater.written.stagedList); removeErr != nil {
				err = errors.Join(err, fmt.Errorf("remove unstaged rewrite manifest list: %w", removeErr))
			}
		}
	}()

	t.mx.Lock()
	defer t.mx.Unlock()
	if err := t.checkRewriteParentLocked(parent); err != nil {
		return err
	}
	if t.meta != base {
		return fmt.Errorf("%w: transaction metadata changed while staging the rewrite; replan the replacement", ErrCommitFailed)
	}

	updates = slices.Concat(planning.meta.updates[len(base.updates):], updates)
	if err := t.applyLocked(updates, reqs); err != nil {
		return err
	}
	t.recordPinnedRefsLocked(pins)
	t.validators = append(t.validators, planning.validators...)
	if validator != nil {
		t.validators = append(t.validators, validator)
		t.rewriteNeedsFullHistory = true
	}
	t.noReplay = t.noReplay || planning.noReplay

	return nil
}

// prepareRewriteCommit adds the parent requirement when a new branch inherits main.
func (t *Transaction) prepareRewriteCommit(ctx context.Context, updater *snapshotProducer, rewritesData bool) ([]Update, []Requirement, []string, error) {
	updates, reqs, err := updater.commit(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if !rewritesData || t.branch == "" || t.branch == MainBranch || t.baseRefSnapshotID(t.branch) != nil ||
		!t.rewriteNeedsMainRequirement() {
		return updates, reqs, nil, nil
	}

	reqs = append(reqs, AssertRefSnapshotID(MainBranch, t.baseRefSnapshotID(MainBranch)))

	return updates, reqs, []string{t.branch, MainBranch}, nil
}

// A new branch inherits main when a snapshot is staged before its ref is set.
// An explicit ref assignment tells replay where to start the branch.
func (t *Transaction) rewriteNeedsMainRequirement() bool {
	var branchExists bool
	for _, update := range t.meta.updates {
		switch u := update.(type) {
		case *addSnapshotUpdate:
			if !branchExists {
				return true
			}
		case *setSnapshotRefUpdate:
			if u.RefName == t.branch {
				branchExists = true
			}
		case *removeSnapshotRefUpdate:
			if u.RefName == t.branch {
				branchExists = false
			}
		}
	}

	return !branchExists
}
