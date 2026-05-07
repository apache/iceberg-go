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

	// Pre-populate the original manifest list path in the IO so that the
	// defer's wfs.Remove call has a concrete entry to delete.
	wfs.files[originalManifestList] = []byte("placeholder")

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

	wfs.files[originalManifestList] = []byte("placeholder")

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

	wfs.files[originalManifestList] = []byte("placeholder")

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

	wfs.files[originalManifestList] = []byte("placeholder")

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
