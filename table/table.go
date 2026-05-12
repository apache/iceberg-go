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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	icebergio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
)

// ErrCommitFailed is the sentinel error returned by catalogs when a
// commit fails due to a concurrent modification (e.g. HTTP 409 Conflict
// from the REST catalog). Catalog implementations should wrap this
// error so that callers using errors.Is(err, table.ErrCommitFailed)
// can detect retryable commit conflicts.
//
// Currently only catalog/rest wraps this sentinel; Glue, SQL, and Hive
// catalogs return their conflict errors raw and will not trigger
// retries until follow-up work wires them through (tracked under
// issue #830).
var ErrCommitFailed = errors.New("commit failed, refresh and try again")

// ErrWriteIORequired is returned by doCommit when the table's file system
// does not implement io.WriteFileIO. Manifest-list rebuild on retry requires
// write access; failing fast here is preferable to silently skipping the
// rebuild and reintroducing the stale-parent data-loss bug. Callers that
// need to detect this condition should use errors.Is(err, ErrWriteIORequired).
var ErrWriteIORequired = errors.New("commit: file system does not implement WriteFileIO")

// ErrSnapshotNotFound is returned (wrapped) by metadata lookups and by
// computeOwnManifests when a snapshot ID does not exist in the table's
// snapshot list. Tests pin meaning via errors.Is(err, ErrSnapshotNotFound).
var ErrSnapshotNotFound = errors.New("snapshot not found")

type FSysF func(ctx context.Context) (icebergio.IO, error)

type Identifier = []string

type CatalogIO interface {
	LoadTable(context.Context, Identifier) (*Table, error)
	CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error)
}

type Table struct {
	identifier       Identifier
	metadata         Metadata
	metadataLocation string
	cat              CatalogIO
	fsF              FSysF
}

func (t Table) Equals(other Table) bool {
	return slices.Equal(t.identifier, other.identifier) &&
		t.metadataLocation == other.metadataLocation &&
		t.metadata.Equals(other.metadata)
}

func (t Table) Identifier() Identifier                       { return t.identifier }
func (t Table) Metadata() Metadata                           { return t.metadata }
func (t Table) MetadataLocation() string                     { return t.metadataLocation }
func (t Table) FS(ctx context.Context) (icebergio.IO, error) { return t.fsF(ctx) }
func (t Table) Schema() *iceberg.Schema                      { return t.metadata.CurrentSchema() }
func (t Table) Spec() iceberg.PartitionSpec                  { return t.metadata.PartitionSpec() }
func (t Table) SortOrder() SortOrder                         { return t.metadata.SortOrder() }
func (t Table) Properties() iceberg.Properties               { return t.metadata.Properties() }
func (t Table) NameMapping() iceberg.NameMapping             { return t.metadata.NameMapping() }
func (t Table) Location() string                             { return t.metadata.Location() }
func (t Table) CurrentSnapshot() *Snapshot                   { return t.metadata.CurrentSnapshot() }
func (t Table) SnapshotByID(id int64) *Snapshot              { return t.metadata.SnapshotByID(id) }
func (t Table) SnapshotByName(name string) *Snapshot         { return t.metadata.SnapshotByName(name) }
func (t Table) Schemas() map[int]*iceberg.Schema {
	m := make(map[int]*iceberg.Schema)
	for _, s := range t.metadata.Schemas() {
		m[s.ID] = s
	}

	return m
}

func (t Table) LocationProvider() (LocationProvider, error) {
	return LoadLocationProvider(t.metadata.Location(), t.metadata.Properties())
}

func (t Table) NewTransaction() *Transaction {
	return t.NewTransactionOnBranch(MainBranch)
}

// NewTransactionOnBranch creates a new transaction that commits to the named
// branch. Use [NewTransaction] to commit to the default "main" branch.
func (t Table) NewTransactionOnBranch(branch string) *Transaction {
	meta, _ := MetadataBuilderFromBase(t.metadata, t.metadataLocation)

	return &Transaction{
		tbl:    &t,
		meta:   meta,
		branch: branch,
		reqs:   []Requirement{},
	}
}

func (t *Table) Refresh(ctx context.Context) error {
	fresh, err := t.cat.LoadTable(ctx, t.identifier)
	if err != nil {
		return err
	}

	t.metadata = fresh.metadata
	t.fsF = fresh.fsF
	t.metadataLocation = fresh.metadataLocation

	return nil
}

// AppendTable is a shortcut for NewTransaction().AppendTable() and then committing the transaction
func (t Table) AppendTable(ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.AppendTable(ctx, tbl, batchSize, snapshotProps); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// Append is a shortcut for NewTransaction().Append() and then committing the transaction
func (t Table) Append(ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.Append(ctx, rdr, snapshotProps); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// OverwriteTable is a shortcut for NewTransaction().OverwriteTable() and then committing the transaction.
//
// An optional filter (see WithOverwriteFilter) determines which existing data to delete or rewrite:
//   - If filter is nil or AlwaysTrue, all existing data files are deleted and replaced with new data.
//   - If a filter is provided, it acts as a row-level predicate on existing data:
//   - Files where all rows match the filter (strict match) are completely deleted
//   - Files where some rows match and others don't (partial match) are rewritten to keep only non-matching rows
//   - Files where no rows match the filter are kept unchanged
//
// The filter uses both inclusive and strict metrics evaluators on file statistics to classify files:
//   - Inclusive evaluator identifies candidate files that may contain matching rows
//   - Strict evaluator determines if all rows in a file must match the filter
//   - Files that pass inclusive but not strict evaluation are rewritten with filtered data
//
// New data from the provided table is written to the table regardless of the filter.
//
// The batchSize parameter refers to the batch size for reading the input data, not the batch size for writes.
// The concurrency parameter controls the level of parallelism for manifest processing and file rewriting and
// can be overridden using the WithOverwriteConcurrency option. Defaults to runtime.GOMAXPROCS(0).
func (t Table) OverwriteTable(ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties, opts ...OverwriteOption) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.OverwriteTable(ctx, tbl, batchSize, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// Overwrite is a shortcut for NewTransaction().Overwrite() and then committing the transaction.
//
// An optional filter (see WithOverwriteFilter) determines which existing data to delete or rewrite:
//   - If filter is nil or AlwaysTrue, all existing data files are deleted and replaced with new data.
//   - If a filter is provided, it acts as a row-level predicate on existing data:
//   - Files where all rows match the filter (strict match) are completely deleted
//   - Files where some rows match and others don't (partial match) are rewritten to keep only non-matching rows
//   - Files where no rows match the filter are kept unchanged
//
// The filter uses both inclusive and strict metrics evaluators on file statistics to classify files:
//   - Inclusive evaluator identifies candidate files that may contain matching rows
//   - Strict evaluator determines if all rows in a file must match the filter
//   - Files that pass inclusive but not strict evaluation are rewritten with filtered data
//
// New data from the provided RecordReader is written to the table regardless of the filter.
//
// The concurrency parameter controls the level of parallelism for manifest processing and file rewriting and
// can be overridden using the WithOverwriteConcurrency option. Defaults to runtime.GOMAXPROCS(0).
func (t Table) Overwrite(ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties, opts ...OverwriteOption) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.Overwrite(ctx, rdr, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// Delete is a shortcut for NewTransaction().Delete() and then committing the transaction.
//
// The provided filter acts as a row-level predicate on existing data:
//   - Files where all rows match the filter (strict match) are completely deleted
//   - Files where some rows match and others don't (partial match) are rewritten to keep only non-matching rows
//   - Files where no rows match the filter are kept unchanged
//
// The filter uses both inclusive and strict metrics evaluators on file statistics to classify files:
//   - Inclusive evaluator identifies candidate files that may contain matching rows
//   - Strict evaluator determines if all rows in a file must match the filter
//   - Files that pass inclusive but not strict evaluation are rewritten with filtered data
//
// The concurrency parameter controls the level of parallelism for manifest processing and file rewriting and
// can be overridden using the WithOverwriteConcurrency option. Defaults to runtime.GOMAXPROCS(0).
func (t Table) Delete(ctx context.Context, filter iceberg.BooleanExpression, snapshotProps iceberg.Properties, opts ...DeleteOption) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.Delete(ctx, filter, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

func (t Table) AllManifests(ctx context.Context) iter.Seq2[iceberg.ManifestFile, error] {
	fs, err := t.fsF(ctx)
	if err != nil {
		return func(yield func(iceberg.ManifestFile, error) bool) {
			yield(nil, err)
		}
	}

	type list = tblutils.Enumerated[[]iceberg.ManifestFile]
	g := errgroup.Group{}

	n := len(t.metadata.Snapshots())
	ch := make(chan list, n)

	for i, sn := range t.metadata.Snapshots() {
		g.Go(func() error {
			manifests, err := sn.Manifests(fs)
			if err != nil {
				return err
			}

			ch <- list{Index: i, Value: manifests, Last: i == n-1}

			return nil
		})
	}

	errch := make(chan error, 1)
	go func() {
		defer close(errch)
		defer close(ch)
		if err := g.Wait(); err != nil {
			errch <- err
		}
	}()

	results := tblutils.MakeSequencedChan(uint(n), ch,
		func(left, right *list) bool {
			switch {
			case left.Index < 0:
				return true
			case right.Index < 0:
				return false
			default:
				return left.Index < right.Index
			}
		}, func(prev, next *list) bool {
			if prev.Index < 0 {
				return next.Index == 0
			}

			return next.Index == prev.Index+1
		}, list{Index: -1})

	return func(yield func(iceberg.ManifestFile, error) bool) {
		defer func() {
			// drain channels if we exited early
			go func() {
				for range results {
				}
				for range errch {
				}
			}()
		}()

		for {
			select {
			case err := <-errch:
				if err != nil {
					yield(nil, err)

					return
				}
			case next, ok := <-results:
				for _, mf := range next.Value {
					if !yield(mf, nil) {
						return
					}
				}

				if next.Last || !ok {
					return
				}
			}
		}
	}
}

// conflictValidatorFunc runs a single producer's client-side conflict
// check against a pre-built conflictContext. Validators return a wrapped
// ErrCommit* sentinel on retryable conflict, ErrCommitDiverged on
// terminal divergence, or nil on success.
type conflictValidatorFunc func(cc *conflictContext) error

// commitOpts controls optional behavior of doCommit beyond the core
// updates/requirements loop. All fields are zero-valued by default and
// callers opt in via the commitOption functional options passed to
// doCommit.
type commitOpts struct {
	// branch is the ref the commit targets. When empty, pre-flight
	// conflict validation is skipped because no conflictContext can
	// be built. Direct doCommit callers (unit tests, low-level utils)
	// may leave this empty; Transaction.Commit always sets it.
	branch string

	// validators runs once before cat.CommitTable on the first attempt
	// only. Refresh-and-replay across retries is deferred to PR 2.5.
	validators []conflictValidatorFunc
}

type commitOption func(*commitOpts)

// withCommitBranch sets the target branch for the pre-flight
// conflict-validation walk. An empty branch is treated as the main
// branch — Transaction.branch is empty when the caller never picked
// one explicitly, and the implicit default is main.
func withCommitBranch(branch string) commitOption {
	if branch == "" {
		branch = MainBranch
	}

	return func(o *commitOpts) { o.branch = branch }
}

func withCommitValidators(vs ...conflictValidatorFunc) commitOption {
	return func(o *commitOpts) { o.validators = append(o.validators, vs...) }
}

func (t Table) doCommit(ctx context.Context, updates []Update, reqs []Requirement, opts ...commitOption) (*Table, error) {
	var co commitOpts
	for _, apply := range opts {
		apply(&co)
	}

	cfg := readRetryConfig(t.metadata.Properties())

	// Bound total retry time with a derived context so both the wait loop
	// and the CommitTable call itself respect the deadline uniformly.
	retryCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.totalTimeoutMs)*time.Millisecond)
	defer cancel()

	fs, err := t.fsF(ctx)
	if err != nil {
		return nil, err
	}

	// Every real commit-path FS implements WriteFileIO. Failing here is
	// preferable to silently skipping the manifest-list rebuild inside the
	// retry loop — a skip reintroduces the original stale-parent data loss.
	wfs, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("%w: manifest list rebuild requires write access", ErrWriteIORequired)
	}

	var (
		newMeta           Metadata
		newLoc            string
		timer             *time.Timer
		orphanedManifests []string // manifest-list files orphaned by rebuilds
	)

	// cleanupOrphans controls whether the defer below removes orphaned manifest-list
	// files on exit. It defaults to true (clean on all safe exits) and is set to
	// false only for the one unsafe case: a non-ErrCommitFailed error from
	// CommitTable, where the catalog may have silently accepted the commit and one
	// of the "orphaned" files may actually be the live snapshot.
	cleanupOrphans := true
	defer func() {
		if !cleanupOrphans || len(orphanedManifests) == 0 {
			return
		}
		for _, path := range orphanedManifests {
			if removeErr := wfs.Remove(path); removeErr != nil {
				log.Printf("Warning: failed to delete orphaned manifest list %s: %v", path, removeErr)
			}
		}
	}()

	// current tracks the catalog state between retries. On attempt 0 it
	// equals t.metadata (so the conflict context's concurrent-snapshot
	// walk is empty and validators short-circuit). On subsequent
	// attempts it is the freshly-loaded post-conflict state.
	current := t.metadata

	// numRetries counts retries; total attempts = 1 initial + numRetries.
	totalAttempts := cfg.numRetries + 1

	for attempt := range totalAttempts {
		if attempt != 0 {
			wait := backoffDuration(attempt-1, cfg.minWaitMs, cfg.maxWaitMs)
			if timer == nil {
				timer = time.NewTimer(wait)
			} else {
				timer.Reset(wait)
			}
			select {
			case <-retryCtx.Done():
				timer.Stop()

				return nil, context.Cause(retryCtx)
			case <-timer.C:
			}

			// Refresh-and-replay: reload the catalog's current state,
			// run the producers' validators against the fresh
			// (base=t.metadata, current=fresh) conflict context, and
			// rewrite any AssertRefSnapshotID requirements to target
			// the new branch head so re-submission is not rejected
			// just because a peer advanced the head with a
			// non-conflicting commit.
			fresh, refreshErr := t.cat.LoadTable(retryCtx, t.identifier)
			if refreshErr != nil {
				return nil, fmt.Errorf("refresh table for retry: %w", refreshErr)
			}
			current = fresh.metadata
			reqs = rewriteRefSnapshotRequirements(reqs, co.branch, current)

			// Rebuild snapshot manifest lists to inherit all files committed
			// by concurrent writers since the snapshot was originally built.
			// Without this, the new snapshot's manifest list would only
			// contain its own files and callers scanning the current snapshot
			// would miss every concurrent writer's data.
			rebuiltUpdates, orphaned, rebuildErr := rebuildSnapshotUpdates(retryCtx, updates, current, co.branch, wfs, int(attempt))
			if rebuildErr != nil {
				return nil, fmt.Errorf("rebuild manifest list for retry attempt %d: %w", attempt, rebuildErr)
			}
			orphanedManifests = append(orphanedManifests, orphaned...)
			updates = rebuiltUpdates
		}

		// Pre-flight client-side conflict validation. Producers can
		// reject commits whose semantics are violated by concurrent
		// peers (partition-filter overlap, referenced-file removal)
		// even when the catalog-side AssertRefSnapshotID would accept
		// them. On attempt 0 base == current → no concurrent
		// snapshots → validators short-circuit. Real divergence
		// detection fires on attempts > 0 once `current` is the
		// post-conflict state.
		//
		// Skipped when the branch does not exist on `current` — that
		// always means "the committer is creating this branch" (e.g.
		// first commit on a fresh table). There are no concurrent
		// snapshots on a branch that does not yet exist, and
		// newConflictContext would otherwise return ErrCommitDiverged.
		if co.branch != "" && len(co.validators) > 0 && current.SnapshotByName(co.branch) != nil {
			// caseSensitive is hardcoded to true here: transaction-
			// level case-sensitivity is not yet threaded through the
			// Commit path, and true is the scan default throughout the
			// codebase.
			cc, ccErr := newConflictContext(t.metadata, current, co.branch, fs, true)
			if ccErr != nil {
				// ErrCommitDiverged — terminal, do not retry. The
				// sentinel deliberately does not wrap ErrCommitFailed.
				return nil, ccErr
			}
			for _, v := range co.validators {
				if vErr := v(cc); vErr != nil {
					return nil, vErr
				}
			}
		}

		if retryCtx.Err() != nil {
			return nil, context.Cause(retryCtx)
		}

		newMeta, newLoc, err = t.cat.CommitTable(retryCtx, t.identifier, reqs, updates)
		if err == nil {
			break
		}

		// Only retry on retryable commit conflicts. Unknown-state errors
		// (5xx, gateway timeouts) must NOT be retried because the commit
		// may have actually succeeded — retrying could duplicate work.
		// Suppress orphan cleanup for the same reason: one of the orphaned
		// manifest-list files may actually be the snapshot the catalog accepted.
		if !errors.Is(err, ErrCommitFailed) {
			cleanupOrphans = false

			return nil, err
		}
	}

	if err != nil {
		return nil, err
	}

	deleteOldMetadata(fs, t.metadata, newMeta)

	return New(t.identifier, newMeta, newLoc, t.fsF, t.cat), nil
}

// rewriteRefSnapshotRequirements returns a copy of reqs with every
// AssertRefSnapshotID targeting `branch` rewritten to point at the
// branch head on `fresh`. Other requirements pass through untouched.
//
// Producers register AssertRefSnapshotID at commit-build time with the
// committer's base snapshot id. After a peer advances the branch head
// with a non-conflicting commit, that assertion no longer matches the
// catalog. Without rewriting the retry would burn the budget on the
// same stale requirement; with it, validators get to decide if the
// commit is still safe to replay against the new head.
//
// Java's SnapshotProducer rewrites the same way between retries. If
// the branch is empty or the new head cannot be resolved (branch
// deleted underneath us), reqs is returned unchanged — newConflict-
// Context will surface the divergence on the next pre-flight pass.
func rewriteRefSnapshotRequirements(reqs []Requirement, branch string, fresh Metadata) []Requirement {
	if branch == "" || fresh == nil {
		return reqs
	}
	head := fresh.SnapshotByName(branch)
	if head == nil {
		return reqs
	}

	out := make([]Requirement, len(reqs))
	for i, r := range reqs {
		if a, ok := r.(*assertRefSnapshotID); ok && a.Ref == branch {
			newID := head.SnapshotID
			out[i] = AssertRefSnapshotID(branch, &newID)

			continue
		}
		out[i] = r
	}

	return out
}

// rebuildSnapshotUpdates returns a new slice of updates where any
// addSnapshotUpdate that carries a rebuildManifestList closure has its
// snapshot regenerated to inherit all data files committed to the branch
// since the original snapshot was built. Updates without a rebuild closure
// pass through unchanged.
//
// It also returns the manifest-list file paths that were superseded by
// the rebuild (i.e., the paths from the input updates that were replaced).
// These become orphaned objects in object storage and should be removed
// by the caller after a successful commit.
//
// This is the manifest-layer "refresh-and-replay" step: the data files
// (already written to object storage) are reused as-is; only the manifest
// list is rewritten to include the fresh parent's manifests so that the
// rebuilt snapshot contains every committed file.
func rebuildSnapshotUpdates(ctx context.Context, updates []Update, freshMeta Metadata, branch string, fs icebergio.WriteFileIO, attempt int) (rebuilt []Update, orphanedPaths []string, err error) {
	// Determine the fresh branch head to use as the rebuilt snapshot's parent.
	var freshHead *Snapshot
	if branch != "" && freshMeta != nil {
		freshHead = freshMeta.SnapshotByName(branch)
	} else if freshMeta != nil {
		freshHead = freshMeta.CurrentSnapshot()
	}

	result := make([]Update, len(updates))
	copy(result, updates)

	for i, u := range result {
		su, ok := u.(*addSnapshotUpdate)
		if !ok || su.rebuildManifestList == nil {
			continue
		}

		// Skip if the parent has not changed — saves an unnecessary S3 write.
		if freshHead != nil && su.Snapshot.ParentSnapshotID != nil &&
			*su.Snapshot.ParentSnapshotID == freshHead.SnapshotID {
			continue
		}

		oldManifestList := su.Snapshot.ManifestList

		newSnap, rebuildErr := su.rebuildManifestList(ctx, freshMeta, freshHead, fs, attempt)
		if rebuildErr != nil {
			return nil, nil, rebuildErr
		}

		result[i] = &addSnapshotUpdate{
			baseUpdate:          su.baseUpdate,
			Snapshot:            newSnap,
			ownManifests:        su.ownManifests,
			rebuildManifestList: su.rebuildManifestList,
		}

		// The old manifest list is now an orphaned object in object storage.
		orphanedPaths = append(orphanedPaths, oldManifestList)
	}

	return result, orphanedPaths, nil
}

type retryConfig struct {
	numRetries     uint
	minWaitMs      uint
	maxWaitMs      uint
	totalTimeoutMs uint
}

func readRetryConfig(props iceberg.Properties) retryConfig {
	return retryConfig{
		numRetries:     iceberg.PropUInt(props, CommitNumRetriesKey, CommitNumRetriesDefault),
		minWaitMs:      iceberg.PropUInt(props, CommitMinRetryWaitMsKey, CommitMinRetryWaitMsDefault),
		maxWaitMs:      iceberg.PropUInt(props, CommitMaxRetryWaitMsKey, CommitMaxRetryWaitMsDefault),
		totalTimeoutMs: iceberg.PropUInt(props, CommitTotalRetryTimeoutMsKey, CommitTotalRetryTimeoutMsDefault),
	}
}

// backoffDuration computes wait time for the given 0-based retry attempt
// using exponential backoff (minMs << attempt) clamped to maxMs, with
// jitter in [minMs, ceiling] to avoid retry stampedes while keeping a
// non-zero floor between attempts. Java Iceberg uses a deterministic
// exponential backoff here; we add jitter to reduce stampede risk on
// concurrent Go writers. Backoff is client-local, so this does not
// affect cross-client interop.
//
// Inputs are trusted: readRetryConfig is responsible for normalizing
// user-supplied properties (negatives, zero, min > max).
func backoffDuration(attempt, minMs, maxMs uint) time.Duration {
	if minMs == 0 {
		minMs = CommitMinRetryWaitMsDefault
	}
	if maxMs == 0 {
		maxMs = CommitMaxRetryWaitMsDefault
	}
	if minMs > maxMs {
		minMs = maxMs
	}
	// Cap the shift count so the signed int64 below does not overflow
	// past its operand width; overflow would just be clamped to maxMs
	// anyway, so keep the math obvious instead.
	if attempt > 62 {
		attempt = 62
	}

	ceiling := int64(minMs) << attempt
	if ceiling <= 0 || ceiling > int64(maxMs) {
		ceiling = int64(maxMs)
	}

	// Jitter in [minMs, ceiling]: keeps a non-zero floor so concurrent
	// writers don't all sample 0 and retry in lockstep.
	//nolint:gosec // non-security randomness, jitter for retry spread
	wait := int64(minMs) + rand.Int64N(ceiling-int64(minMs)+1)

	return time.Duration(wait) * time.Millisecond
}

// SnapshotAsOf finds the snapshot that was current as of or right before the given timestamp.
func (t Table) SnapshotAsOf(timestampMs int64, inclusive bool) *Snapshot {
	entries := slices.Collect(t.metadata.SnapshotLogs())
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if (inclusive && entry.TimestampMs <= timestampMs) || (!inclusive && entry.TimestampMs < timestampMs) {
			return t.metadata.SnapshotByID(entry.SnapshotID)
		}
	}

	return nil
}

func getFiles(it iter.Seq[MetadataLogEntry]) iter.Seq[string] {
	return func(yield func(string) bool) {
		next, stop := iter.Pull(it)
		defer stop()
		for {
			entry, ok := next()
			if !ok {
				return
			}
			if !yield(entry.MetadataFile) {
				return
			}
		}
	}
}

func deleteOldMetadata(fs icebergio.IO, baseMeta, newMeta Metadata) {
	deleteAfterCommit := newMeta.Properties().GetBool(MetadataDeleteAfterCommitEnabledKey,
		MetadataDeleteAfterCommitEnabledDefault)

	if deleteAfterCommit {
		removedPrevious := slices.Collect(getFiles(baseMeta.PreviousFiles()))
		currentMetadata := slices.Collect(getFiles(newMeta.PreviousFiles()))
		toRemove := internal.Difference(removedPrevious, currentMetadata)

		for _, file := range toRemove {
			if err := fs.Remove(file); err != nil {
				// Log the error instead of raising it when deleting old metadata files, as an external entity like a compactor may have already deleted them
				log.Printf("Warning: Failed to delete old metadata file: %s error: %v", file, err)
			}
		}
	}
}

type ScanOption func(*Scan)

func noopOption(*Scan) {}

func WithSelectedFields(fields ...string) ScanOption {
	if len(fields) == 0 || slices.Contains(fields, "*") {
		return noopOption
	}

	return func(scan *Scan) {
		scan.selectedFields = fields
	}
}

func WithRowFilter(e iceberg.BooleanExpression) ScanOption {
	if e == nil || e.Equals(iceberg.AlwaysTrue{}) {
		return noopOption
	}

	return func(scan *Scan) {
		scan.rowFilter = e
	}
}

func WithSnapshotID(n int64) ScanOption {
	if n == 0 {
		return noopOption
	}

	return func(scan *Scan) {
		scan.snapshotID = &n
		scan.asOfTimestamp = nil
	}
}

func WithSnapshotAsOf(timeStampMs int64) ScanOption {
	return func(scan *Scan) {
		scan.asOfTimestamp = &timeStampMs
		scan.snapshotID = nil
	}
}

func WithCaseSensitive(b bool) ScanOption {
	return func(scan *Scan) {
		scan.caseSensitive = b
	}
}

func WithLimit(n int64) ScanOption {
	if n < 0 {
		return noopOption
	}

	return func(scan *Scan) {
		scan.limit = n
	}
}

// WitMaxConcurrency sets the maximum concurrency for table scan and plan
// operations. When unset it defaults to runtime.GOMAXPROCS.
func WitMaxConcurrency(n int) ScanOption {
	if n <= 0 {
		return noopOption
	}

	return func(scan *Scan) {
		scan.concurrency = n
	}
}

func WithOptions(opts iceberg.Properties) ScanOption {
	if opts == nil {
		return noopOption
	}

	return func(scan *Scan) {
		scan.options = opts
	}
}

func (t Table) Scan(opts ...ScanOption) *Scan {
	s := &Scan{
		metadata:       t.metadata,
		ioF:            t.fsF,
		rowFilter:      iceberg.AlwaysTrue{},
		selectedFields: []string{"*"},
		caseSensitive:  true,
		limit:          ScanNoLimit,
		concurrency:    runtime.GOMAXPROCS(0),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.partitionFilters = newKeyDefaultMapWrapErr(s.buildPartitionProjection)

	return s
}

func New(ident Identifier, meta Metadata, metadataLocation string, fsF FSysF, cat CatalogIO) *Table {
	return &Table{
		identifier:       ident,
		metadata:         meta,
		metadataLocation: metadataLocation,
		fsF:              fsF,
		cat:              cat,
	}
}

func NewFromLocation(
	ctx context.Context,
	ident Identifier,
	metalocation string,
	fsysF FSysF,
	cat CatalogIO,
) (_ *Table, err error) {
	var meta Metadata

	fsys, err := fsysF(ctx)
	if err != nil {
		return nil, err
	}
	if rf, ok := fsys.(icebergio.ReadFileIO); ok {
		data, err := rf.ReadFile(metalocation)
		if err != nil {
			return nil, err
		}

		if codec := metadataCompressionCodec(metalocation); codec != "" {
			rc, err := newDecompressor(bytes.NewReader(data), codec)
			if err != nil {
				return nil, err
			}
			defer rc.Close()

			data, err = io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
		}

		if meta, err = ParseMetadataBytes(data); err != nil {
			return nil, err
		}
	} else {
		f, err := fsys.Open(metalocation)
		if err != nil {
			return nil, err
		}
		defer internal.CheckedClose(f, &err)

		var r io.Reader = f
		if codec := metadataCompressionCodec(metalocation); codec != "" {
			rc, err := newDecompressor(f, codec)
			if err != nil {
				return nil, err
			}
			defer rc.Close()

			r = rc
		}

		if meta, err = ParseMetadata(r); err != nil {
			return nil, err
		}
	}

	return New(ident, meta, metalocation, fsysF, cat), nil
}

func metadataCompressionCodec(location string) string {
	switch {
	case strings.HasSuffix(location, ".gz.metadata.json") || strings.HasSuffix(location, "metadata.json.gz"):
		return MetadataCompressionCodecGzip
	case strings.HasSuffix(location, ".zstd.metadata.json") || strings.HasSuffix(location, "metadata.json.zstd"):
		return MetadataCompressionCodecZstd
	default:
		return ""
	}
}

func newDecompressor(r io.Reader, codec string) (io.ReadCloser, error) {
	switch codec {
	case MetadataCompressionCodecGzip:
		return gzip.NewReader(r)
	case MetadataCompressionCodecZstd:
		dec, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}

		return dec.IOReadCloser(), nil
	default:
		return nil, fmt.Errorf("unsupported metadata decompression codec: %s", codec)
	}
}
