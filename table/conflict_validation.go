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
//
// The validators themselves are unexported: they are producer-facing
// plumbing, not a library API. The only user-visible surface is
// IsolationLevel, the isolation property keys, and the conflict
// error sentinels — mirroring Java's split where validators are
// `protected` on MergingSnapshotProducer and only the inputs and
// outputs are public.

import (
	"errors"
	"fmt"
	"sort"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
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

// readIsolationLevel returns the isolation level for the given
// property key, falling back to defVal for a missing or unrecognized
// value.
func readIsolationLevel(props iceberg.Properties, key string, defVal IsolationLevel) IsolationLevel {
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

// conflictContext carries the inputs every validator needs: the
// latest catalog state (current) and the filesystem used to read
// manifest content on demand. Build it with newConflictContext.
//
// A validator inspects snapshots committed on the committer's branch
// between the writer's base and the current head. When both metadata
// values refer to the same snapshot on the branch, there are no
// concurrent commits and validators return nil without reading
// manifests.
//
// conflictContext is immutable once constructed — do NOT reuse the
// same context across retry attempts that re-fetch catalog state,
// because the cached concurrent-snapshot walk becomes stale. There
// is deliberately no Refresh or mutator method; a refresh must flow
// as a fresh call to newConflictContext(newBase, newCurrent, ...).
type conflictContext struct {
	current       Metadata
	branch        string
	fs            iceio.IO
	caseSensitive bool

	// Resolved once; subsequent validator calls reuse the walk.
	concurrent []Snapshot
}

// newConflictContext builds a validation context for the given
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
// branch (divergent commit, expired base), newConflictContext returns
// ErrCommitDiverged; the commit cannot be safely revalidated without
// a refresh-and-rebuild.
func newConflictContext(base, current Metadata, branch string, fs iceio.IO, caseSensitive bool) (*conflictContext, error) {
	currentHead := current.SnapshotByName(branch)
	if currentHead == nil {
		// Branch does not exist on the current side — either it was
		// deleted concurrently or the writer is creating it. Treat as
		// divergent; refresh-and-replay will decide.
		return nil, fmt.Errorf("%w: branch %q missing on current metadata", ErrCommitDiverged, branch)
	}

	baseHead := base.SnapshotByName(branch)
	if baseHead == nil {
		// Writer's base has no view of this branch (empty table or
		// branch created concurrently). Every snapshot now reachable
		// from the branch head is concurrent relative to base.
		// Truncation (missing intermediate, cycle) is treated as
		// divergent for the same reason AncestorsBetween's
		// baseFound=false is: an under-counted concurrent list silently
		// hides conflicts.
		concurrent, complete := AncestorsOfChecked(currentHead.SnapshotID, current.SnapshotByID)
		if !complete {
			return nil, fmt.Errorf("%w: ancestry walk from %d on %s truncated; no base snapshot to bound against",
				ErrCommitDiverged, currentHead.SnapshotID, branch)
		}

		return &conflictContext{
			current:       current,
			branch:        branch,
			fs:            fs,
			caseSensitive: caseSensitive,
			concurrent:    concurrent,
		}, nil
	}

	concurrent, baseFound := AncestorsBetween(currentHead.SnapshotID, baseHead.SnapshotID, current.SnapshotByID)
	if !baseFound {
		return nil, fmt.Errorf("%w: base snapshot %d not found in %s ancestry from %d",
			ErrCommitDiverged, baseHead.SnapshotID, branch, currentHead.SnapshotID)
	}

	return &conflictContext{
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
// one, so per-manifest filtering alone would miss or mis-attribute
// entries. Filtering on entry.SnapshotID() is the only attribution
// that survives manifest rewrites.
//
// The visitor returns early if the callback returns a non-nil error.
func (c *conflictContext) forEachAddedEntry(content iceberg.ManifestContent, visit func(Snapshot, iceberg.ManifestEntry) error) error {
	for _, snap := range c.concurrent {
		for entry, err := range snap.entries(c.fs, content) {
			if err != nil {
				return fmt.Errorf("loading entries for concurrent snapshot %d: %w", snap.SnapshotID, err)
			}
			if entry.Status() != iceberg.EntryStatusADDED {
				continue
			}
			if entry.SnapshotID() != snap.SnapshotID {
				// Entry was inherited from a prior snapshot and is
				// not attributable to this concurrent commit.
				continue
			}
			if err := visit(snap, entry); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateDataFilesExist verifies that every file path in
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
func validateDataFilesExist(ctx *conflictContext, referencedPaths []string) error {
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

	for df, err := range head.dataFiles(ctx.fs, nil) {
		if err != nil {
			return fmt.Errorf("iterating data files for current head %d: %w", head.SnapshotID, err)
		}
		if _, ok := needed[df.FilePath()]; ok {
			delete(needed, df.FilePath())
			if len(needed) == 0 {
				return nil
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

// validateAddedDataFilesMatchingFilter returns an error if any
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
func validateAddedDataFilesMatchingFilter(ctx *conflictContext, filter iceberg.BooleanExpression) error {
	if len(ctx.concurrent) == 0 {
		return nil
	}
	if filter == nil {
		filter = iceberg.AlwaysTrue{}
	}

	// Per-spec projected filter, memoized per spec id. Concurrent
	// snapshots may have been written against different spec ids
	// after a spec evolution, so each needs its own projection.
	// Reuses the same projection/evaluator helpers the scanner uses
	// (buildPartitionProjection, buildManifestEvaluator,
	// buildPartitionEvaluator) so there is one code path that pruning
	// semantics flow through.
	partitionFilters := newKeyDefaultMapWrapErr(func(specID int) (iceberg.BooleanExpression, error) {
		return buildPartitionProjection(specID, ctx.current, filter, ctx.caseSensitive)
	})
	manifestEvals := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
		return buildManifestEvaluator(specID, ctx.current, partitionFilters, ctx.caseSensitive)
	})
	partitionEvals := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.DataFile) (bool, error), error) {
		return buildPartitionEvaluator(specID, ctx.current, partitionFilters, ctx.caseSensitive)
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
			for e, err := range mf.Entries(ctx.fs, false) {
				if err != nil {
					return fmt.Errorf("reading entries from manifest %s: %w", mf.FilePath(), err)
				}
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

// validateNoConflictingDataFiles is the serializable-isolation check
// used by RowDelta and other delete operations. It rejects the commit
// if any concurrent snapshot added data files into the partition(s)
// the committer is modifying.
//
// Under IsolationSnapshot this validator is a no-op: concurrent
// appends are allowed and will simply land in the same partition
// alongside the new deletes.
func validateNoConflictingDataFiles(ctx *conflictContext, filter iceberg.BooleanExpression, level IsolationLevel) error {
	if level != IsolationSerializable {
		return nil
	}

	return validateAddedDataFilesMatchingFilter(ctx, filter)
}

// validateNoConflictingDataFilesInPartitions is like
// validateNoConflictingDataFiles but scoped to the partitions touched
// by equality-delete files. It builds an OR-of-equalities filter from
// the provided partition tuples and delegates to
// validateAddedDataFilesMatchingFilter, which performs per-spec
// projection, manifest-summary pruning, and type-aware evaluation via
// iceberg.Literal — making it safe for UUID, decimal, binary, fixed,
// and future partition types, and correct across partition-spec
// evolution because each concurrent manifest is projected against its
// own spec ID.
//
// Callers are responsible for ensuring the table is partitioned
// (i.e. at least one partition field exists) before calling this
// function. For unpartitioned tables, call
// validateNoConflictingDataFiles(ctx, iceberg.AlwaysTrue{}, level)
// directly.
//
// Under IsolationSnapshot this validator is a no-op.
func validateNoConflictingDataFilesInPartitions(ctx *conflictContext, eqDeleteFiles []iceberg.DataFile, level IsolationLevel) error {
	if level != IsolationSerializable {
		return nil
	}

	if len(ctx.concurrent) == 0 || len(eqDeleteFiles) == 0 {
		return nil
	}

	filter, err := eqDeletePartitionsToFilter(eqDeleteFiles, ctx.current)
	if err != nil {
		return fmt.Errorf("building partition conflict filter: %w", err)
	}

	return validateNoConflictingDataFiles(ctx, filter, level)
}

// eqDeletePartitionsToFilter converts equality-delete data files into an
// OR-of-ANDs BooleanExpression in row (source) space, suitable for passing
// to validateAddedDataFilesMatchingFilter.
//
// For each eq-delete file it resolves each partition field ID to the source
// schema field name via the file's partition spec, then builds an EqualTo
// predicate using Reference(sourceFieldName). Multiple fields within one
// partition are AND-ed; multiple eq-delete files are OR-ed.
//
// The resulting expression is projected per-concurrent-manifest's spec ID
// inside validateAddedDataFilesMatchingFilter (via buildPartitionProjection),
// ensuring correct conflict detection even after partition-spec evolution.
//
// An empty partition tuple (unpartitioned delete) returns AlwaysTrue so the
// caller falls back to the conservative full-table scan. Callers should
// normally guard against calling this function for unpartitioned tables (see
// RowDelta.validate).
func eqDeletePartitionsToFilter(files []iceberg.DataFile, meta Metadata) (iceberg.BooleanExpression, error) {
	terms := make([]iceberg.BooleanExpression, 0, len(files))
	for _, f := range files {
		p := f.Partition()
		if len(p) == 0 {
			return iceberg.AlwaysTrue{}, nil
		}

		spec := meta.PartitionSpecByID(int(f.SpecID()))
		if spec == nil {
			return nil, fmt.Errorf("partition spec ID %d not found in metadata", f.SpecID())
		}

		// Build partition field ID → PartitionField lookup for this spec.
		partFieldByID := make(map[int]iceberg.PartitionField, spec.NumFields())
		for _, pf := range spec.Fields() {
			partFieldByID[pf.FieldID] = pf
		}

		// Sort partition field IDs for deterministic expression order.
		fieldIDs := make([]int, 0, len(p))
		for id := range p {
			fieldIDs = append(fieldIDs, id)
		}
		sort.Ints(fieldIDs)

		// Non-identity transforms (bucket, day, hour, truncate, year, month) store
		// post-transform values in DataFile.Partition(). Building a row-space predicate
		// with those values would cause validateAddedDataFilesMatchingFilter to
		// re-apply the transform, producing wrong matches (double-transformation).
		// Fall back to AlwaysTrue (conservative: treat the eq-delete as table-wide)
		// until a full PartitionSet-style partition-space approach is added.
		identityOnly := true
		for _, fid := range fieldIDs {
			if pf, ok := partFieldByID[fid]; ok {
				if _, isIdentity := pf.Transform.(iceberg.IdentityTransform); !isIdentity {
					identityOnly = false

					break
				}
			}
		}
		if !identityOnly {
			terms = append(terms, iceberg.AlwaysTrue{})

			continue
		}

		conjuncts := make([]iceberg.BooleanExpression, 0, len(p))
		for _, partFieldID := range fieldIDs {
			pf, ok := partFieldByID[partFieldID]
			if !ok {
				return nil, fmt.Errorf("partition field ID %d not found in spec %d", partFieldID, f.SpecID())
			}

			// Resolve to source schema field to obtain the Reference name.
			sourceField, ok := meta.CurrentSchema().FindFieldByID(pf.SourceID())
			if !ok {
				return nil, fmt.Errorf("source field ID %d (partition field %q) not found in schema", pf.SourceID(), pf.Name)
			}

			lit, err := anyToLiteral(p[partFieldID])
			if err != nil {
				return nil, fmt.Errorf("partition field %q: %w", sourceField.Name, err)
			}

			conjuncts = append(conjuncts, iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference(sourceField.Name), lit))
		}

		if len(conjuncts) == 1 {
			terms = append(terms, conjuncts[0])
		} else {
			terms = append(terms, iceberg.NewAnd(conjuncts[0], conjuncts[1], conjuncts[2:]...))
		}
	}

	if len(terms) == 0 {
		return iceberg.AlwaysTrue{}, nil
	}

	if len(terms) == 1 {
		return terms[0], nil
	}

	return iceberg.NewOr(terms[0], terms[1], terms[2:]...), nil
}

// anyToLiteral converts a dynamically-typed partition value (as
// stored in iceberg.DataFile.Partition()) to an iceberg.Literal.
// The supported types mirror the iceberg.LiteralType constraint.
func anyToLiteral(v any) (iceberg.Literal, error) {
	switch val := v.(type) {
	case bool:
		return iceberg.NewLiteral(val), nil
	case int32:
		return iceberg.NewLiteral(val), nil
	case int64:
		return iceberg.NewLiteral(val), nil
	case float32:
		return iceberg.NewLiteral(val), nil
	case float64:
		return iceberg.NewLiteral(val), nil
	case string:
		return iceberg.NewLiteral(val), nil
	case []byte:
		return iceberg.NewLiteral(val), nil
	case iceberg.Date:
		return iceberg.NewLiteral(val), nil
	case iceberg.Time:
		return iceberg.NewLiteral(val), nil
	case iceberg.Timestamp:
		return iceberg.NewLiteral(val), nil
	case iceberg.TimestampNano:
		return iceberg.NewLiteral(val), nil
	case iceberg.Decimal:
		return iceberg.NewLiteral(val), nil
	case iceberg.DecimalLiteral:
		// convertAvroValueToIcebergType returns the named type DecimalLiteral
		// (type DecimalLiteral Decimal), not Decimal itself. Handle both.
		return iceberg.NewLiteral(iceberg.Decimal(val)), nil
	case uuid.UUID:
		return iceberg.NewLiteral(val), nil
	default:
		return nil, fmt.Errorf("unsupported partition value type %T", v)
	}
}

// validateNoNewDeletesForRewrittenFiles rejects the commit if any
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
func validateNoNewDeletesForRewrittenFiles(ctx *conflictContext, rewrittenPaths []string) error {
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
