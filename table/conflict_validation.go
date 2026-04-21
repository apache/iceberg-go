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

// This file is the client-side conflict-validation framework for
// concurrent commits. It lives alongside the catalog-side Requirement
// machinery in requirements.go, and the two serve different roles:
//
//   - A Requirement (requirements.go) is asserted by the catalog on
//     commit. It can only check invariants visible to the catalog
//     (branch heads, schema ids, spec ids, table UUID) and has no
//     access to manifest or data-file content.
//
//   - A validator in this file runs client-side against a freshly
//     loaded Metadata, walks the concurrent snapshots added on the
//     committer's branch since the committer's base, and rejects
//     commits whose semantics would be violated by those concurrent
//     snapshots (e.g. a concurrent append into a partition the
//     committer is overwriting, or a concurrent pos-delete against a
//     file the committer is rewriting).
//
// Both systems cooperate: Requirement ensures the committer's base
// assumption about the branch head still holds, and if it does not
// the retry loop re-fetches metadata; these validators then check
// whether the semantic invariants still hold against the new head.

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// IsolationLevel controls how strictly a commit rejects concurrent
// writes on the same branch. It mirrors
// org.apache.iceberg.IsolationLevel from the Java reference.
type IsolationLevel string

const (
	// IsolationSerializable rejects any concurrent commit that added
	// data files matching the committer's filter since the base
	// snapshot. This is the strongest guarantee: a serializable commit
	// behaves as if it ran in isolation against the base snapshot.
	IsolationSerializable IsolationLevel = "serializable"

	// IsolationSnapshot only rejects concurrent commits that touched
	// files the committer explicitly references (e.g. a position
	// delete whose referenced data file was removed). Concurrent
	// appends into the committer's filter region are allowed.
	IsolationSnapshot IsolationLevel = "snapshot"
)

// Property keys for configuring isolation per write operation. Names
// match the Java TableProperties constants so configs can be ported
// directly between engines.
const (
	// WriteDeleteIsolationLevelKey controls isolation for DeleteFiles
	// and RowDelta eq-delete commits. Default: serializable (matches
	// Java trunk). Older Java releases and some pyiceberg codepaths
	// used "snapshot"; set this property explicitly to snapshot if
	// you are migrating from a pipeline that relied on that behavior.
	WriteDeleteIsolationLevelKey     = "write.delete.isolation-level"
	WriteDeleteIsolationLevelDefault = IsolationSerializable

	// WriteUpdateIsolationLevelKey controls isolation for overwrite /
	// merge commits. Default: serializable (matches Java trunk).
	// Older Java releases and some pyiceberg codepaths used
	// "snapshot"; set this property explicitly to snapshot if you are
	// migrating from a pipeline that relied on that behavior.
	WriteUpdateIsolationLevelKey     = "write.update.isolation-level"
	WriteUpdateIsolationLevelDefault = IsolationSerializable
)

// ReadIsolationLevel returns the isolation level for the given
// property key, falling back to defVal for a missing or unrecognized
// value.
func ReadIsolationLevel(props iceberg.Properties, key string, defVal IsolationLevel) IsolationLevel {
	v, ok := props[key]
	if !ok {
		return defVal
	}
	switch IsolationLevel(v) {
	case IsolationSerializable, IsolationSnapshot:
		return IsolationLevel(v)
	default:
		return defVal
	}
}

// ErrCommitDiverged is returned when the committer's base snapshot is
// no longer on the branch, so conflict validation cannot enumerate
// concurrent snapshots. The committer must refresh and rebuild their
// commit from the new base — naive retry would fail identically.
//
// Unlike the other conflict sentinels in this file, ErrCommitDiverged
// does NOT wrap ErrCommitFailed: it is terminal for the current
// attempt. This mirrors Java's ValidationException, which the
// SnapshotProducer retry machinery does not catch.
var ErrCommitDiverged = errors.New("commit diverged: base snapshot is no longer on the branch")

// Retryable conflict sentinels. All wrap ErrCommitFailed so the retry
// loop in doCommit treats them as retryable. Callers that need to
// distinguish which kind of conflict occurred can match on the
// specific sentinel via errors.Is.
var (
	// ErrConflictingDataFiles is returned when a concurrent commit
	// added data files that satisfy the committer's filter.
	ErrConflictingDataFiles = fmt.Errorf("%w: concurrent data files added", ErrCommitFailed)

	// ErrConflictingDeleteFiles is returned when a concurrent commit
	// added delete files that could mask rows the committer is
	// writing or replacing.
	ErrConflictingDeleteFiles = fmt.Errorf("%w: concurrent delete files added", ErrCommitFailed)

	// ErrDataFilesMissing is returned when a concurrent commit deleted
	// data files the committer explicitly references (e.g. a position
	// delete pointing at a file that is no longer reachable).
	ErrDataFilesMissing = fmt.Errorf("%w: referenced data files missing", ErrCommitFailed)
)

// ConflictContext carries the inputs every validator needs: the
// latest catalog state (current) and the filesystem used to read
// manifest content on demand. Build it with NewConflictContext.
//
// A validator inspects snapshots committed on the committer's branch
// between the writer's base and the current head. When both metadata
// values refer to the same snapshot on the branch, there are no
// concurrent commits and validators return nil without reading
// manifests.
//
// ConflictContext is immutable once constructed — do NOT reuse the
// same context across retry attempts that re-fetch catalog state,
// because the cached concurrent-snapshot walk becomes stale. There
// is deliberately no Refresh or mutator method; a refresh must flow
// as a fresh call to NewConflictContext(newBase, newCurrent, ...).
type ConflictContext struct {
	current       Metadata
	branch        string
	fs            iceio.IO
	caseSensitive bool

	// Resolved once; subsequent validator calls reuse the walk.
	concurrent []Snapshot
}

// NewConflictContext builds a validation context for the given
// branch. It walks the ancestry of the branch's current head back to
// the writer's base snapshot to enumerate concurrent commits.
//
// caseSensitive must match the value the committer used when it bound
// its filter (typically the scan's case-sensitivity) so
// partition-spec projection resolves the same column identifiers.
//
// When base and current point at the same snapshot on the branch, the
// returned context has no concurrent snapshots and validators
// short-circuit to nil. When the base snapshot is no longer on the
// branch (divergent commit, expired base), NewConflictContext returns
// ErrCommitDiverged; the commit cannot be safely revalidated without
// a refresh-and-rebuild.
func NewConflictContext(base, current Metadata, branch string, fs iceio.IO, caseSensitive bool) (*ConflictContext, error) {
	currentHead := current.SnapshotByName(branch)
	if currentHead == nil {
		// Branch does not exist on the current side — either it was
		// deleted concurrently or the writer is creating it. Treat as
		// divergent; refresh-and-replay will decide.
		return nil, fmt.Errorf("%w: branch %q missing on current metadata", ErrCommitDiverged, branch)
	}

	baseHead := base.SnapshotByName(branch)
	if baseHead == nil {
		// Writer has no view of this branch yet (e.g. creating it) —
		// by definition there are no concurrent commits to validate
		// against.
		return &ConflictContext{current: current, branch: branch, fs: fs, caseSensitive: caseSensitive}, nil
	}

	concurrent, baseFound := AncestorsBetween(currentHead.SnapshotID, baseHead.SnapshotID, current.SnapshotByID)
	if !baseFound {
		return nil, fmt.Errorf("%w: base snapshot %d not found in %s ancestry from %d",
			ErrCommitDiverged, baseHead.SnapshotID, branch, currentHead.SnapshotID)
	}

	return &ConflictContext{
		current:       current,
		branch:        branch,
		fs:            fs,
		caseSensitive: caseSensitive,
		concurrent:    concurrent,
	}, nil
}

// forEachAddedEntry visits every ManifestEntry with status ADDED
// whose snapshot id equals one of the concurrent snapshots. This
// matches Java's per-entry filter: an ADDED entry is attributed to
// the snapshot that produced it, independent of which manifest
// happens to carry it today — a manifest-rewrite (e.g. the merging
// append producer) can carry ADDED entries from a prior snapshot
// into a newer manifest whose added-snapshot-id is the committing
// one, so per-manifest filtering alone would miss or
// mis-attribute entries. Filtering on entry.SnapshotID() is the
// only attribution that survives manifest rewrites.
//
// The visitor returns early if the callback returns a non-nil error.
func (c *ConflictContext) forEachAddedEntry(content iceberg.ManifestContent, visit func(Snapshot, iceberg.ManifestEntry) error) error {
	for _, snap := range c.concurrent {
		manifests, err := snap.Manifests(c.fs)
		if err != nil {
			return fmt.Errorf("loading manifests for concurrent snapshot %d: %w", snap.SnapshotID, err)
		}
		for _, mf := range manifests {
			if mf.ManifestContent() != content {
				continue
			}
			entries, err := mf.FetchEntries(c.fs, false)
			if err != nil {
				return fmt.Errorf("reading entries from manifest %s: %w", mf.FilePath(), err)
			}
			for _, e := range entries {
				if e.Status() != iceberg.EntryStatusADDED {
					continue
				}
				if e.SnapshotID() != snap.SnapshotID {
					// Entry was inherited from a prior snapshot and
					// is not attributable to this concurrent commit.
					continue
				}
				if err := visit(snap, e); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// ValidateDataFilesExist verifies that every file path in
// referencedPaths is still reachable from the current branch head.
// This is the check a position-delete commit performs to prove the
// files it references have not been removed by a concurrent commit
// (at which point the pos-delete would apply to rewritten data and
// produce incorrect results).
//
// Returns ErrDataFilesMissing listing the first few missing paths.
// The committer should treat this as unrecoverable for the current
// commit attempt and rebuild the delete against the current
// snapshot's data files.
//
// Cost is O(all data manifests × all entries) regardless of
// len(referencedPaths); callers MUST batch referenced paths into a
// single call rather than calling once per path.
func ValidateDataFilesExist(ctx *ConflictContext, referencedPaths []string) error {
	if len(referencedPaths) == 0 {
		return nil
	}
	needed := make(map[string]struct{}, len(referencedPaths))
	for _, p := range referencedPaths {
		needed[p] = struct{}{}
	}

	head := ctx.current.SnapshotByName(ctx.branch)
	if head == nil {
		return fmt.Errorf("%w: branch %q missing on current metadata", ErrCommitDiverged, ctx.branch)
	}

	manifests, err := head.Manifests(ctx.fs)
	if err != nil {
		return fmt.Errorf("loading manifests for current head %d: %w", head.SnapshotID, err)
	}
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		entries, err := mf.FetchEntries(ctx.fs, true) // discardDeleted=true: reachable entries only
		if err != nil {
			return fmt.Errorf("reading entries from manifest %s: %w", mf.FilePath(), err)
		}
		for _, e := range entries {
			path := e.DataFile().FilePath()
			if _, ok := needed[path]; ok {
				delete(needed, path)
				if len(needed) == 0 {
					return nil
				}
			}
		}
	}

	if len(needed) == 0 {
		return nil
	}

	missing := make([]string, 0, len(needed))
	for p := range needed {
		missing = append(missing, p)
	}
	sort.Strings(missing)
	sample := missing
	if len(sample) > 5 {
		sample = sample[:5]
	}

	return fmt.Errorf("%w: %d files missing, e.g. %v", ErrDataFilesMissing, len(missing), sample)
}

// ValidateAddedDataFilesMatchingFilter returns an error if any
// concurrent snapshot added a data file whose partition satisfies the
// given filter. Overwrite and delete operations that declare a
// predicate call this so that a concurrent append into the same
// partition is rejected before the commit overwrites it.
//
// The check runs in two layers:
//  1. A manifest-level partition-summary evaluator prunes manifests
//     whose summaries cannot overlap the filter.
//  2. Inside surviving manifests, every ADDED entry attributed to the
//     concurrent snapshot is evaluated against a per-spec partition
//     evaluator on its partition tuple, so a manifest whose summary
//     straddles the filter only triggers a conflict when at least
//     one actual added file's partition value satisfies the filter.
//
// Per-file metric evaluation (a third pass in Java that refines
// beyond partition for columns not in the spec) is TODO and tracked
// under issue #830 follow-ups.
func ValidateAddedDataFilesMatchingFilter(ctx *ConflictContext, filter iceberg.BooleanExpression) error {
	if len(ctx.concurrent) == 0 {
		return nil
	}
	if filter == nil {
		filter = iceberg.AlwaysTrue{}
	}

	schema := ctx.current.CurrentSchema()
	filters := newPerSpecFilterCache(filter)

	manifestEvals := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
		projected, err := filters.projectFor(specID, ctx.current, schema, ctx.caseSensitive)
		if err != nil {
			return nil, err
		}
		spec := ctx.current.PartitionSpecByID(specID)
		if spec == nil {
			return nil, fmt.Errorf("%w: partition spec %d not found on current metadata",
				iceberg.ErrInvalidArgument, specID)
		}

		return newManifestEvaluator(*spec, schema, projected, ctx.caseSensitive)
	})

	partitionEvals := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.DataFile) (bool, error), error) {
		projected, err := filters.projectFor(specID, ctx.current, schema, ctx.caseSensitive)
		if err != nil {
			return nil, err
		}
		spec := ctx.current.PartitionSpecByID(specID)
		if spec == nil {
			return nil, fmt.Errorf("%w: partition spec %d not found on current metadata",
				iceberg.ErrInvalidArgument, specID)
		}
		partType := spec.PartitionType(schema)
		partSchema := iceberg.NewSchema(0, partType.FieldList...)
		eval, err := iceberg.ExpressionEvaluator(partSchema, projected, ctx.caseSensitive)
		if err != nil {
			return nil, err
		}

		return func(d iceberg.DataFile) (bool, error) {
			return eval(GetPartitionRecord(d, partType))
		}, nil
	})

	for _, snap := range ctx.concurrent {
		manifests, err := snap.Manifests(ctx.fs)
		if err != nil {
			return fmt.Errorf("loading manifests for concurrent snapshot %d: %w", snap.SnapshotID, err)
		}
		for _, mf := range manifests {
			if mf.ManifestContent() != iceberg.ManifestContentData {
				continue
			}

			mEval := manifestEvals.Get(int(mf.PartitionSpecID()))
			keep, err := mEval(mf)
			if err != nil {
				return err
			}
			if !keep {
				continue
			}

			pEval := partitionEvals.Get(int(mf.PartitionSpecID()))
			entries, err := mf.FetchEntries(ctx.fs, false)
			if err != nil {
				return fmt.Errorf("reading entries from manifest %s: %w", mf.FilePath(), err)
			}
			for _, e := range entries {
				if e.Status() != iceberg.EntryStatusADDED || e.SnapshotID() != snap.SnapshotID {
					continue
				}
				matches, err := pEval(e.DataFile())
				if err != nil {
					return err
				}
				if !matches {
					continue
				}

				return fmt.Errorf("%w: snapshot %d added data file %s matching filter %s",
					ErrConflictingDataFiles, snap.SnapshotID, e.DataFile().FilePath(), filter)
			}
		}
	}

	return nil
}

// ValidateNoConflictingDataFiles is the serializable-isolation check
// used by RowDelta and other delete operations. It rejects the commit
// if any concurrent snapshot added data files into the partition(s)
// the committer is modifying.
//
// Under IsolationSnapshot this validator is a no-op: concurrent
// appends are allowed and will simply land in the same partition
// alongside the new deletes.
func ValidateNoConflictingDataFiles(ctx *ConflictContext, filter iceberg.BooleanExpression, level IsolationLevel) error {
	if level != IsolationSerializable {
		return nil
	}

	return ValidateAddedDataFilesMatchingFilter(ctx, filter)
}

// ValidateNoNewDeletesForRewrittenFiles rejects the commit if any
// concurrent snapshot added delete files that would be lost when the
// committer's rewrite replaces a data file.
//
// Two cases are flagged:
//
//   - A position-delete whose referenced-data-file path is one of
//     rewrittenPaths — the delete applies to a file the rewrite is
//     removing, so the delete would be orphaned and its semantics
//     lost.
//
//   - An equality-delete added by a concurrent snapshot — these apply
//     by predicate, not by path, so we cannot precisely check
//     overlap without the rewritten files' partition tuples. The
//     conservative behavior is to reject any concurrent eq-delete,
//     which matches Java's approach for RewriteFiles and avoids
//     silently losing deletes. A follow-up PR will accept optional
//     partition-overlap hints and narrow this check.
func ValidateNoNewDeletesForRewrittenFiles(ctx *ConflictContext, rewrittenPaths []string) error {
	if len(rewrittenPaths) == 0 || len(ctx.concurrent) == 0 {
		return nil
	}
	rewritten := make(map[string]struct{}, len(rewrittenPaths))
	for _, p := range rewrittenPaths {
		rewritten[p] = struct{}{}
	}

	return ctx.forEachAddedEntry(iceberg.ManifestContentDeletes, func(snap Snapshot, e iceberg.ManifestEntry) error {
		df := e.DataFile()
		switch df.ContentType() {
		case iceberg.EntryContentPosDeletes:
			if ref := df.ReferencedDataFile(); ref != nil {
				if _, overlap := rewritten[*ref]; overlap {
					return fmt.Errorf("%w: snapshot %d added pos-delete %s referencing rewritten file %s",
						ErrConflictingDeleteFiles, snap.SnapshotID, df.FilePath(), *ref)
				}
			}

			return nil

		case iceberg.EntryContentEqDeletes:
			// Conservative: any concurrent eq-delete is a conflict
			// for a rewrite, because the predicate could cover a
			// rewritten file. Follow-up: accept partition-overlap
			// hints to narrow this.
			return fmt.Errorf("%w: snapshot %d added equality delete %s during rewrite",
				ErrConflictingDeleteFiles, snap.SnapshotID, df.FilePath())
		}

		return nil
	})
}

// perSpecFilterCache memoizes inclusive partition-level projections
// of the conflict filter onto each partition spec the table has seen.
// Different concurrent snapshots may have been written against
// different spec ids after a spec evolution, so each needs its own
// projection.
//
// Access is guarded by a mutex to keep the cache safe when a future
// caller evaluates validators across multiple goroutines.
type perSpecFilterCache struct {
	orig iceberg.BooleanExpression

	mu   sync.Mutex
	memo map[int]iceberg.BooleanExpression
}

func newPerSpecFilterCache(filter iceberg.BooleanExpression) *perSpecFilterCache {
	return &perSpecFilterCache{orig: filter, memo: map[int]iceberg.BooleanExpression{}}
}

func (p *perSpecFilterCache) projectFor(specID int, meta Metadata, schema *iceberg.Schema, caseSensitive bool) (iceberg.BooleanExpression, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cached, ok := p.memo[specID]; ok {
		return cached, nil
	}
	spec := meta.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: partition spec %d not found on current metadata",
			iceberg.ErrInvalidArgument, specID)
	}
	projected, err := newInclusiveProjection(schema, *spec, caseSensitive)(p.orig)
	if err != nil {
		return nil, err
	}
	p.memo[specID] = projected

	return projected, nil
}
