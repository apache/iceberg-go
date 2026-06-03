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
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readOnlyIO implements iceio.IO but NOT iceio.WriteFileIO.
// Used to verify that doCommit fails early when the FS cannot write.
type readOnlyIO struct{}

func (readOnlyIO) Open(_ string) (iceio.File, error) { return nil, errors.New("readOnlyIO: no files") }
func (readOnlyIO) Remove(_ string) error             { return errors.New("readOnlyIO: read-only") }

// sequentialCatalog returns a predetermined error per CommitTable attempt.
// If attempts exceed the len(errs) slice it returns nil (success).
// When loadMeta is set, LoadTable returns that metadata instead of c.metadata.
type sequentialCatalog struct {
	metadata Metadata
	loadMeta Metadata // optional: returned by LoadTable if non-nil
	errs     []error
	attempts atomic.Int32
}

func (c *sequentialCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	m := c.metadata
	if c.loadMeta != nil {
		m = c.loadMeta
	}

	return New(ident, m, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *sequentialCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	n := int(c.attempts.Add(1)) - 1 // 0-indexed
	if n < len(c.errs) && c.errs[n] != nil {
		return nil, "", c.errs[n]
	}
	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

// flakyCatalog commits successfully only on a specified attempt number.
// Earlier attempts return the given error.
type flakyCatalog struct {
	metadata         Metadata
	failUntilAttempt int
	failWith         error
	attempts         atomic.Int32
}

func (c *flakyCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	if c.metadata == nil {
		return nil, nil
	}

	return New(ident, c.metadata, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *flakyCatalog) CommitTable(ctx context.Context, ident Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	n := c.attempts.Add(1)
	if int(n) <= c.failUntilAttempt {
		return nil, "", c.failWith
	}

	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

func newRetryTestTable(t *testing.T, cat CatalogIO, props iceberg.Properties) *Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	if props == nil {
		props = iceberg.Properties{}
	}
	props[PropertyFormatVersion] = "2"

	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec,
		UnsortedSortOrder, location, props)
	require.NoError(t, err)

	return New(
		Identifier{"db", "retry_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		cat,
	)
}

func TestDoCommit_SucceedsFirstTry(t *testing.T) {
	cat := &flakyCatalog{}
	tbl := newRetryTestTable(t, cat, nil)
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestDoCommit_RetriesOnCommitFailed(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 2,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "4",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(3), cat.attempts.Load(), "should retry 2x then succeed on 3rd")
}

func TestDoCommit_GivesUpAfterMaxRetries(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 100,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCommitFailed)
	// 1 initial + 2 retries = 3 attempts
	assert.Equal(t, int32(3), cat.attempts.Load())
}

func TestDoCommit_DoesNotRetryUnknownStateError(t *testing.T) {
	unknownErr := errors.New("500 internal server error")
	cat := &flakyCatalog{
		failUntilAttempt: 5,
		failWith:         unknownErr,
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "10",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, unknownErr)
	// Must not retry — unknown state could mean the commit actually succeeded.
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestDoCommit_DoesNotRetryUnrelatedError(t *testing.T) {
	otherErr := errors.New("network unreachable")
	cat := &flakyCatalog{
		failUntilAttempt: 5,
		failWith:         otherErr,
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "10",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, otherErr)
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestDoCommit_RespectsContextCancellation(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 100,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "10",
		CommitMinRetryWaitMsKey: "50",
		CommitMaxRetryWaitMsKey: "200",
	})
	cat.metadata = tbl.Metadata()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()

	_, err := tbl.doCommit(ctx, nil, nil)
	require.Error(t, err)
	// Either the commit error bubbles up or context cancellation does.
	// Both are acceptable outcomes — the test just verifies we don't hang.
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrCommitFailed))
	assert.Less(t, cat.attempts.Load(), int32(10), "should stop retrying after context cancels")
}

func TestDoCommit_ZeroRetriesOnlyOneAttempt(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 5,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "0",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestBackoffDuration_ExponentialWithJitter(t *testing.T) {
	const minMs, maxMs = 100, 60000
	minWait := time.Duration(minMs) * time.Millisecond

	// Attempt 0: cap == minMs, wait is exactly minMs.
	for range 20 {
		d := backoffDuration(0, minMs, maxMs)
		assert.Equal(t, minWait, d)
	}

	// Attempt 3: cap = 800ms (100 << 3), wait in [minMs, 800ms].
	for range 20 {
		d := backoffDuration(3, minMs, maxMs)
		assert.GreaterOrEqual(t, d, minWait)
		assert.LessOrEqual(t, d, 800*time.Millisecond)
	}

	// Attempt 20: overflow protection, wait in [minMs, maxMs].
	for range 20 {
		d := backoffDuration(20, minMs, maxMs)
		assert.GreaterOrEqual(t, d, minWait)
		assert.LessOrEqual(t, d, time.Duration(maxMs)*time.Millisecond)
	}
}

func TestBackoffDuration_HandlesZeroInputs(t *testing.T) {
	// Zero min/max should fall back to defaults rather than return garbage.
	d := backoffDuration(0, 0, 0)
	assert.Equal(t, time.Duration(CommitMinRetryWaitMsDefault)*time.Millisecond, d)

	// Very large attempt counts must not panic on shift; clamps to maxMs.
	d = backoffDuration(100, 100, 60000)
	assert.GreaterOrEqual(t, d, 100*time.Millisecond)
	assert.LessOrEqual(t, d, 60000*time.Millisecond)
}

func TestReadRetryConfig_ClampsNegativeProperties(t *testing.T) {
	// Negative values in properties should be replaced with defaults.
	cfg := readRetryConfig(iceberg.Properties{
		CommitNumRetriesKey:          "-1",
		CommitMinRetryWaitMsKey:      "-100",
		CommitMaxRetryWaitMsKey:      "-1000",
		CommitTotalRetryTimeoutMsKey: "-5",
	})
	assert.Equal(t, uint(CommitNumRetriesDefault), cfg.numRetries)
	assert.Equal(t, uint(CommitMinRetryWaitMsDefault), cfg.minWaitMs)
	assert.Equal(t, uint(CommitMaxRetryWaitMsDefault), cfg.maxWaitMs)
	assert.Equal(t, uint(CommitTotalRetryTimeoutMsDefault), cfg.totalTimeoutMs)
}

// ---------------------------------------------------------------------------
// Fix 5 — mandatory WriteFileIO check at top of doCommit
// ---------------------------------------------------------------------------

// TestDoCommit_NonWriteFileIOReturnsError verifies that doCommit fails
// immediately when the table's file system does not implement WriteFileIO.
// A silent skip would reuse the stale manifest list — exactly the bug
// this PR was designed to fix.
func TestDoCommit_NonWriteFileIOReturnsError(t *testing.T) {
	cat := &flakyCatalog{}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/rotest",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	cat.metadata = meta

	tbl := New(
		Identifier{"db", "ro-test"},
		meta, "file:///tmp/rotest/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return readOnlyIO{}, nil },
		cat,
	)

	_, doErr := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, doErr, "doCommit must fail when FS does not implement WriteFileIO")
	require.ErrorIs(t, doErr, ErrWriteIORequired,
		"doCommit must wrap ErrWriteIORequired so callers can detect this condition with errors.Is")
	assert.Equal(t, int32(0), cat.attempts.Load(),
		"CommitTable must not be called when FS check fails")
}

// ---------------------------------------------------------------------------
// Fix 6 — orphan cleanup via defer
// ---------------------------------------------------------------------------

// newOCCTable creates a table that uses the given wfs for its FS and the given
// catalog for commits. meta should include retry-config properties so that
// doCommit's retry loop allows at least one retry.
func newOCCTable(t *testing.T, meta Metadata, wfs iceio.WriteFileIO, cat CatalogIO) *Table {
	t.Helper()

	return New(
		Identifier{"db", "occ-cleanup-test"},
		meta,
		"mem://default/table-location/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return wfs, nil },
		cat,
	)
}

// newMemIOWithRetryMeta creates a test memIO and a matching table Metadata that
// includes retry-config properties, so doCommit's retry loop allows retries.
// The location matches createTestTransactionWithMemIO so they share the same
// memIO for writing manifest files.
func newMemIOWithRetryMeta(t *testing.T, spec iceberg.PartitionSpec) (*memIO, Metadata) {
	t.Helper()
	wfs := newMemIO(1<<20, nil)
	schema := simpleSchema()
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://default/table-location",
		iceberg.Properties{
			CommitNumRetriesKey:          "3",
			CommitMinRetryWaitMsKey:      "1",
			CommitMaxRetryWaitMsKey:      "2",
			CommitTotalRetryTimeoutMsKey: "60000",
		})
	require.NoError(t, err, "new metadata")

	return wfs, meta
}

// TestDoCommit_OrphanCleanedOnSuccess verifies that manifest-list files
// orphaned by OCC retries are removed after a successful commit. These files
// are written during rebuild and must not leak on the happy path.
func TestDoCommit_OrphanCleanedOnSuccess(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	wfs, meta := newMemIOWithRetryMeta(t, spec)

	// Build a transaction from the retry-enabled meta and commit via the producer.
	tbl := newOCCTable(t, meta, wfs, nil)
	txn := tbl.NewTransaction()
	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, reqs, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	originalManifestList := addSnap.Snapshot.ManifestList

	// R3: assert the producer actually wrote the manifest list (real flow,
	// not pre-populated placeholder). The orphan/cleanup contract is only
	// meaningful when there is a real file to clean up.
	require.Contains(t, wfs.files, originalManifestList,
		"producer.commit() must persist the manifest list via WriteFileIO before doCommit runs")

	// Catalog: fail once with ErrCommitFailed (triggers rebuild that orphans
	// originalManifestList), then succeed.
	cat := &sequentialCatalog{
		metadata: meta,
		errs:     []error{ErrCommitFailed},
	}
	tbl = newOCCTable(t, meta, wfs, cat)

	_, err = tbl.doCommit(t.Context(), updates, reqs, withCommitBranch(MainBranch))
	require.NoError(t, err, "doCommit must succeed on the second attempt")

	_, stillExists := wfs.files[originalManifestList]
	assert.False(t, stillExists,
		"orphaned manifest list must be removed after successful commit")

	// R3 (latest review): prove the LIVE committed path was preserved, not
	// just that something got deleted. The catalog's CurrentSnapshot is the
	// rebuilt one; its ManifestList must (a) differ from the orphan, and
	// (b) still be present in the filesystem. A regression that flipped the
	// cleanup loop to delete the committed path instead of the orphan would
	// fail (b).
	committedSnap := cat.metadata.CurrentSnapshot()
	require.NotNil(t, committedSnap, "catalog must record the committed snapshot")
	committedManifestList := committedSnap.ManifestList
	require.NotEqual(t, originalManifestList, committedManifestList,
		"committed manifest list must be the rebuilt path, not the original")
	require.Contains(t, wfs.files, committedManifestList,
		"live committed manifest list must be preserved by the cleanup defer")
}

// TestDoCommit_OrphanNotCleanedOnUnknownError verifies that manifest-list
// files are NOT removed when CommitTable returns an unknown non-ErrCommitFailed
// error (5xx / gateway timeout). In that case the catalog may have silently
// accepted the commit, meaning one of the "orphaned" files is actually the
// live snapshot. Deleting it would permanently corrupt the table.
func TestDoCommit_OrphanNotCleanedOnUnknownError(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	wfs, meta := newMemIOWithRetryMeta(t, spec)

	tbl := newOCCTable(t, meta, wfs, nil)
	txn := tbl.NewTransaction()
	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, reqs, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	originalManifestList := addSnap.Snapshot.ManifestList

	require.Contains(t, wfs.files, originalManifestList,
		"producer.commit() must persist the manifest list via WriteFileIO before doCommit runs")

	unknown5xxErr := errors.New("simulated 5xx: internal server error")
	// Catalog: fail once (ErrCommitFailed → rebuild → orphan created),
	// then return a 5xx (non-ErrCommitFailed → cleanupOrphans=false).
	cat := &sequentialCatalog{
		metadata: meta,
		errs:     []error{ErrCommitFailed, unknown5xxErr},
	}
	tbl = newOCCTable(t, meta, wfs, cat)

	_, err = tbl.doCommit(t.Context(), updates, reqs, withCommitBranch(MainBranch))
	require.ErrorIs(t, err, unknown5xxErr, "5xx error must propagate")

	_, stillExists := wfs.files[originalManifestList]
	assert.True(t, stillExists,
		"orphaned manifest list must NOT be removed when commit outcome is unknown (5xx)")

	// R3 (latest review): on unknown-state, EVERY rebuild output must be
	// preserved — any of them might be the snapshot the catalog silently
	// accepted. Walk wfs.files and assert there are >=2 distinct manifest-list
	// entries (the original + at least one rebuild). A regression that
	// flipped cleanupOrphans=true on this path would leave only the original.
	manifestListCount := 0
	for name := range wfs.files {
		if strings.HasSuffix(name, ".avro") && strings.Contains(name, "snap-") {
			manifestListCount++
		}
	}
	require.GreaterOrEqual(t, manifestListCount, 2,
		"on unknown 5xx every rebuild's manifest list must be preserved (one may be the live commit)")
}

// TestDoCommit_OrphanCleanedOnCommitDiverged verifies that manifest-list files
// orphaned by rebuild attempts are removed when ErrCommitDiverged is returned
// by a conflict validator. Diverged commits are terminal (no retry), and since
// neither of the orphaned files was ever accepted by the catalog, they are safe
// to delete. The defer cleanup runs with cleanupOrphans=true on this path.
func TestDoCommit_OrphanCleanedOnCommitDiverged(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	wfs, meta := newMemIOWithRetryMeta(t, spec)

	// freshMeta has a snapshot on MainBranch so validators run on the retry attempt.
	freshID := int64(42)
	freshMeta := newConflictTestMetadataWithProps(t, &freshID, iceberg.Properties{
		CommitNumRetriesKey:          "3",
		CommitMinRetryWaitMsKey:      "1",
		CommitMaxRetryWaitMsKey:      "2",
		CommitTotalRetryTimeoutMsKey: "60000",
	})

	tbl := newOCCTable(t, meta, wfs, nil)
	txn := tbl.NewTransaction()
	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, reqs, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	originalManifestList := addSnap.Snapshot.ManifestList

	require.Contains(t, wfs.files, originalManifestList,
		"producer.commit() must persist the manifest list via WriteFileIO before doCommit runs")

	// Catalog: fail once (ErrCommitFailed → rebuild → orphan created).
	// LoadTable returns freshMeta (has branch snapshot → validators run on retry).
	// Validator returns ErrCommitDiverged immediately.
	cat := &sequentialCatalog{
		metadata: meta,
		loadMeta: freshMeta,
		errs:     []error{ErrCommitFailed},
	}
	tbl = newOCCTable(t, meta, wfs, cat)

	divergedValidator := func(*conflictContext) error { return ErrCommitDiverged }

	_, err = tbl.doCommit(t.Context(), updates, reqs,
		withCommitBranch(MainBranch),
		withCommitValidators(divergedValidator),
	)
	require.ErrorIs(t, err, ErrCommitDiverged)

	// The defer must have fired with cleanupOrphans=true and removed the orphan.
	_, stillExists := wfs.files[originalManifestList]
	assert.False(t, stillExists,
		"orphaned manifest list must be removed even on ErrCommitDiverged: "+
			"the file was never accepted by the catalog so it is safe to delete")
}

// TestDoCommit_OrphanCleanedOnRetriesExhausted verifies that when every retry
// attempt fails with ErrCommitFailed and the loop exits with the budget
// exhausted, the defer still fires with cleanupOrphans=true. None of the
// orphaned manifest-list files were ever accepted by the catalog, so they are
// safe to delete on this terminal exit.
func TestDoCommit_OrphanCleanedOnRetriesExhausted(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	wfs, meta := newMemIOWithRetryMeta(t, spec)

	tbl := newOCCTable(t, meta, wfs, nil)
	txn := tbl.NewTransaction()
	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, reqs, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	originalManifestList := addSnap.Snapshot.ManifestList

	require.Contains(t, wfs.files, originalManifestList,
		"producer.commit() must persist the manifest list via WriteFileIO before doCommit runs")

	// numRetries=3 → 4 attempts; every attempt fails with ErrCommitFailed.
	cat := &sequentialCatalog{
		metadata: meta,
		errs:     []error{ErrCommitFailed, ErrCommitFailed, ErrCommitFailed, ErrCommitFailed},
	}
	tbl = newOCCTable(t, meta, wfs, cat)

	_, err = tbl.doCommit(t.Context(), updates, reqs, withCommitBranch(MainBranch))
	require.ErrorIs(t, err, ErrCommitFailed,
		"retries exhausted: terminal error must be ErrCommitFailed")

	_, stillExists := wfs.files[originalManifestList]
	assert.False(t, stillExists,
		"orphaned manifest list must be removed when retries are exhausted with ErrCommitFailed: "+
			"none of the retry attempts were accepted, so all orphans are safe to delete")
}

// ---------------------------------------------------------------------------
// Fix 7 (R4) — retry-progression: every fix exercised end-to-end
// ---------------------------------------------------------------------------

// progressingRebuildCatalog grafts a real peer commit (with a readable
// manifest-list file in wfs.files) onto its tracked metadata between every
// failed retry, then accepts on the final attempt. Each peer commit:
//
//   - advances LastSequenceNumber and the branch head,
//   - publishes a Summary increment ("total-data-files"+1, etc.) so the
//     next retry's rebuild closure observes an evolving freshParent.Summary,
//   - writes a real (empty) manifest-list file at a unique path so
//     freshParent.Manifests(fio) succeeds inside rebuildFn.
//
// This is the helper reviewer R4 asked for: a single test that drives ≥2
// ErrCommitFailed retries with a freshMeta that progresses between attempts,
// so the rebuild closure is exercised for real (not just the retry-loop
// refresh path with nil updates). The existing headTrackingCatalog cannot
// do this — it only advances metadata on a SUCCESSFUL CommitTable call.
type progressingRebuildCatalog struct {
	metadata  Metadata
	wfs       *memIO
	location  string
	branch    string
	failTimes int

	commitTableCalls atomic.Int32

	// Per-call captures (used by assertions).
	loadedLastSeqNums     []int64  // freshMeta.LastSequenceNumber() at each LoadTable
	observedManifestLists []string // ManifestList path submitted at each CommitTable
	observedSeqNums       []int64  // SequenceNumber submitted at each CommitTable

	committedSnapshot *Snapshot // populated when CommitTable accepts
	peerCount         int       // running peer-commit counter (= total-data-files contributed by peers)
}

func (c *progressingRebuildCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	c.loadedLastSeqNums = append(c.loadedLastSeqNums, c.metadata.LastSequenceNumber())

	return New(ident, c.metadata, "",
		func(context.Context) (iceio.IO, error) { return c.wfs, nil }, c), nil
}

func (c *progressingRebuildCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	n := c.commitTableCalls.Add(1)

	// Capture the manifest-list path and sequence number being submitted
	// this attempt — the test asserts each retry produces a unique path
	// and a strictly advancing seq number.
	for _, u := range updates {
		su, ok := u.(*addSnapshotUpdate)
		if !ok {
			continue
		}
		c.observedManifestLists = append(c.observedManifestLists, su.Snapshot.ManifestList)
		c.observedSeqNums = append(c.observedSeqNums, su.Snapshot.SequenceNumber)

		break
	}

	if int(n) <= c.failTimes {
		c.graftPeer()

		return nil, "", ErrCommitFailed
	}

	// Accept: capture the submitted snapshot for assertions and apply
	// the updates so c.metadata.CurrentSnapshot() reflects the commit.
	for _, u := range updates {
		if su, ok := u.(*addSnapshotUpdate); ok {
			snapCopy := *su.Snapshot
			c.committedSnapshot = &snapCopy

			break
		}
	}
	builder, err := MetadataBuilderFromBase(c.metadata, "")
	if err != nil {
		return nil, "", err
	}
	for _, u := range updates {
		if applyErr := u.Apply(builder); applyErr != nil {
			return nil, "", applyErr
		}
	}
	out, err := builder.Build()
	if err != nil {
		return nil, "", err
	}
	c.metadata = out

	return out, "", nil
}

// graftPeer adds a peer snapshot (with a real, readable empty manifest list)
// to c.metadata and advances LastSequenceNumber. The peer's Summary contains
// "total-data-files" = c.peerCount (cumulative), so the producer's rebuild
// closure observes summary chaining when it rebases against this freshParent.
func (c *progressingRebuildCatalog) graftPeer() {
	c.peerCount++
	peerID := int64(9_000) + int64(c.peerCount)
	peerSeq := c.metadata.LastSequenceNumber() + 1

	manifestListPath := fmt.Sprintf("%s/metadata/peer-%d-manifest-list.avro", c.location, peerID)
	out, err := c.wfs.Create(manifestListPath)
	if err != nil {
		panic(err)
	}
	if writeErr := iceberg.WriteManifestList(2, out, peerID, nil, &peerSeq, 0, nil); writeErr != nil {
		panic(writeErr)
	}
	if closeErr := out.Close(); closeErr != nil {
		panic(closeErr)
	}

	parent := c.metadata.SnapshotByName(c.branch)
	var parentID *int64
	if parent != nil {
		id := parent.SnapshotID
		parentID = &id
	}

	builder, err := MetadataBuilderFromBase(c.metadata, "")
	if err != nil {
		panic(err)
	}
	count := strconv.Itoa(c.peerCount)
	if addErr := builder.AddSnapshot(&Snapshot{
		SnapshotID:       peerID,
		ParentSnapshotID: parentID,
		SequenceNumber:   peerSeq,
		TimestampMs:      c.metadata.LastUpdatedMillis() + 1,
		ManifestList:     manifestListPath,
		Summary: &Summary{
			Operation: OpAppend,
			Properties: iceberg.Properties{
				"total-data-files":       count,
				"total-records":          count,
				"total-files-size":       count,
				"total-delete-files":     "0",
				"total-position-deletes": "0",
				"total-equality-deletes": "0",
			},
		},
	}); addErr != nil {
		panic(addErr)
	}
	if refErr := builder.SetSnapshotRef(c.branch, peerID, BranchRef); refErr != nil {
		panic(refErr)
	}
	out2, err := builder.Build()
	if err != nil {
		panic(err)
	}
	c.metadata = out2
}

// TestDoCommit_RetryProgressesFreshMeta is the end-to-end regression test
// for reviewer R4. It drives doCommit through ≥2 ErrCommitFailed retries
// with a freshMeta that progresses between attempts, and asserts every
// PR fix is exercised at once:
//
//   - Fix 3 (newSeq from freshMeta): the rebuilt SequenceNumber observed
//     on each retry strictly advances and matches freshMeta.LastSequenceNumber+1.
//   - Fix 2 (summary chained against fresh parent): the final committed
//     summary's total-data-files == peerCount + this producer's adds.
//   - Fix 6 (orphan defer cleanup): the original (attempt-0) and intermediate
//     rebuild manifest-list paths are deleted; the live committed path is
//     preserved in wfs.files.
//   - "Orphan list grows by N": each retry submits a DISTINCT ManifestList
//     path (proves the rebuild closure rewrote the list every attempt).
//
// A regression that cached freshMeta across retries, skipped the rebuild,
// or carried over the captured-attempt-0 summary would fail one or more
// of these assertions.
func TestDoCommit_RetryProgressesFreshMeta(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	wfs, meta := newMemIOWithRetryMeta(t, spec)

	// Build a real producer commit (with a real rebuildManifestList closure).
	tbl := newOCCTable(t, meta, wfs, nil)
	txn := tbl.NewTransaction()
	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, reqs, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	originalManifestList := addSnap.Snapshot.ManifestList
	require.Contains(t, wfs.files, originalManifestList,
		"producer must persist the attempt-0 manifest list before doCommit runs")

	cat := &progressingRebuildCatalog{
		metadata:  meta,
		wfs:       wfs,
		location:  "mem://default/table-location",
		branch:    MainBranch,
		failTimes: 2, // 2 fails + 1 success = 3 CommitTable calls
	}
	tbl = newOCCTable(t, meta, wfs, cat)

	_, err = tbl.doCommit(context.Background(), updates, reqs, withCommitBranch(MainBranch))
	require.NoError(t, err, "doCommit must succeed on the third attempt")

	// (a) commitTableCalls: 2 fails + 1 success.
	require.Equal(t, int32(3), cat.commitTableCalls.Load(),
		"failTimes=2 → 2 ErrCommitFailed + 1 success = 3 CommitTable calls")

	// (b) Orphan list grows by N: every retry's submitted ManifestList path
	// must be DISTINCT. The first equals the producer's original; attempts
	// 2 and 3 are rebuilt paths from the rebuild closure.
	require.Len(t, cat.observedManifestLists, 3)
	seen := make(map[string]struct{}, 3)
	for i, p := range cat.observedManifestLists {
		require.NotEmpty(t, p, "attempt %d submitted an empty manifest list path", i)
		_, dup := seen[p]
		require.False(t, dup,
			"every retry must submit a UNIQUE manifest list path (orphan list grows by N): %v",
			cat.observedManifestLists)
		seen[p] = struct{}{}
	}
	require.Equal(t, originalManifestList, cat.observedManifestLists[0],
		"attempt 0 must submit the producer's original manifest list")

	// (c) Each rebuild observed a different LastSequenceNumber. LoadTable
	// is called once per retry (attempts 1 and 2 here — attempt 0 uses
	// the writer's own base), so we expect 2 entries that strictly advance.
	require.Len(t, cat.loadedLastSeqNums, 2,
		"LoadTable must be called once per retry attempt (=2)")
	require.Greater(t, cat.loadedLastSeqNums[1], cat.loadedLastSeqNums[0],
		"freshMeta.LastSequenceNumber() must strictly advance across retries: %v",
		cat.loadedLastSeqNums)

	// (d) Fix 3 — newSeq derives from freshMeta.LastSequenceNumber()+1 on
	// each retry. Submitted seq numbers must strictly increase across the
	// 3 attempts, AND attempts 1 and 2 (the rebuilds) must equal the
	// freshMeta.LastSequenceNumber observed by their respective LoadTable.
	require.Len(t, cat.observedSeqNums, 3)
	require.Greater(t, cat.observedSeqNums[1], cat.observedSeqNums[0],
		"rebuild attempt 1 must submit a strictly higher seq num than attempt 0: %v",
		cat.observedSeqNums)
	require.Greater(t, cat.observedSeqNums[2], cat.observedSeqNums[1],
		"rebuild attempt 2 must submit a strictly higher seq num than attempt 1: %v",
		cat.observedSeqNums)
	require.Equal(t, cat.loadedLastSeqNums[0]+1, cat.observedSeqNums[1],
		"attempt-1 rebuild seq must equal freshMeta.LastSequenceNumber()+1 from its LoadTable")
	require.Equal(t, cat.loadedLastSeqNums[1]+1, cat.observedSeqNums[2],
		"attempt-2 rebuild seq must equal freshMeta.LastSequenceNumber()+1 from its LoadTable")

	// (e) Fix 2 — summary chains on top of an evolving freshParent.
	// 2 peer commits (each contributing total-data-files=1, then =2) +
	// this producer's 1 file → final total-data-files == 3.
	require.NotNil(t, cat.committedSnapshot, "committed snapshot must be captured")
	require.NotNil(t, cat.committedSnapshot.Summary)
	gotDataFiles := cat.committedSnapshot.Summary.Properties.GetInt("total-data-files", -1)
	require.Equal(t, 3, gotDataFiles,
		"committed total-data-files must = peer cumulative (2) + producer (1) = 3; "+
			"a regression that ignored freshParent.Summary would publish 1")

	// (f) Fix 6 — defer cleanup: the live committed manifest list is
	// preserved in wfs.files; the original and intermediate rebuilds
	// were orphans and have been removed.
	committedML := cat.committedSnapshot.ManifestList
	require.Equal(t, cat.observedManifestLists[2], committedML,
		"the third (accepted) submission must be the live committed manifest list")
	require.Contains(t, wfs.files, committedML,
		"live committed manifest list must be preserved by the cleanup defer")
	require.NotContains(t, wfs.files, originalManifestList,
		"attempt-0 manifest list must be cleaned as orphan after success")
	require.NotContains(t, wfs.files, cat.observedManifestLists[1],
		"attempt-1 rebuild manifest list must be cleaned as orphan after success")
}
