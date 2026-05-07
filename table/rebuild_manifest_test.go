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
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMetadataWithLastSeqNum builds a v2 Metadata whose last-sequence-number
// equals lastSeqNum by grafting a synthetic snapshot at that sequence number.
// Used by tests that need to control freshMeta.LastSequenceNumber().
func newMetadataWithLastSeqNum(t *testing.T, lastSeqNum int64) Metadata {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/seqtest",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	snap := Snapshot{
		SnapshotID:     1000,
		SequenceNumber: lastSeqNum,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&snap))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 1000, BranchRef))
	out, err := builder.Build()
	require.NoError(t, err)

	return out
}

// newV3MetadataWithNextRowID builds a v3 Metadata whose NextRowID() returns
// nextRowID by adding a synthetic snapshot that consumes that many rows.
// Used by tests that need to control freshMeta.NextRowID() for v3 row lineage.
func newV3MetadataWithNextRowID(t *testing.T, nextRowID int64) Metadata {
	t.Helper()
	spec := iceberg.NewPartitionSpec()
	txn, _ := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	firstRowID := int64(0)
	addedRows := nextRowID
	snap := Snapshot{
		SnapshotID:     1000,
		SequenceNumber: 1,
		TimestampMs:    txn.meta.base.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
		FirstRowID:     &firstRowID,
		AddedRows:      &addedRows,
	}
	require.NoError(t, txn.meta.AddSnapshot(&snap))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 1000, BranchRef))

	meta, err := txn.meta.Build()
	require.NoError(t, err)
	require.Equal(t, nextRowID, meta.NextRowID())

	return meta
}

// ptr returns a pointer to v, used in test helper expressions.
// NOTE: ptr is also declared in pos_delete_partitioned_fanout_writer_test.go;
// both files share the same package so only one declaration is needed.
// This comment documents the shared usage — do not re-add a declaration here.

// rebuildUpdate constructs an addSnapshotUpdate whose rebuildManifestList
// closure simply records the freshParent it received and returns a new
// snapshot whose ManifestList is the given newManifestList value.
func rebuildUpdate(snap *Snapshot, newManifestList string, gotParent **Snapshot) *addSnapshotUpdate {
	return &addSnapshotUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSnapshot},
		Snapshot:   snap,
		rebuildManifestList: func(_ context.Context, _ Metadata, freshParent *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
			*gotParent = freshParent
			rebuilt := *snap
			rebuilt.ManifestList = newManifestList

			return &rebuilt, nil
		},
	}
}

// TestRebuildSnapshotUpdates_CallsClosureWithFreshParent verifies that
// rebuildSnapshotUpdates invokes the rebuild closure and passes the fresh
// branch head as freshParent.
func TestRebuildSnapshotUpdates_CallsClosureWithFreshParent(t *testing.T) {
	const oldManifest = "s3://bucket/old-manifest-list.avro"
	const newManifest = "s3://bucket/new-manifest-list.avro"

	// Build a fresh metadata that has a different snapshot than the one
	// embedded in the update, so the parent-hasn't-changed guard is
	// bypassed and the closure must be called.
	freshHead := int64(42)
	freshMeta := newConflictTestMetadata(t, &freshHead)

	parentID := int64(7) // original snap's parent — does NOT match freshHead (42)
	snap := &Snapshot{
		SnapshotID:       99,
		ParentSnapshotID: &parentID,
		ManifestList:     oldManifest,
		Summary:          &Summary{Operation: OpAppend},
	}

	var receivedParent *Snapshot
	upd := rebuildUpdate(snap, newManifest, &receivedParent)

	rebuilt, orphaned, err := rebuildSnapshotUpdates(
		t.Context(),
		[]Update{upd},
		freshMeta,
		MainBranch,
		iceio.LocalFS{},
		1,
	)
	require.NoError(t, err)
	require.Len(t, rebuilt, 1)
	require.Len(t, orphaned, 1, "old manifest list must be recorded as orphaned")

	// The closure should have been called with the branch's current head.
	require.NotNil(t, receivedParent)
	assert.Equal(t, freshHead, receivedParent.SnapshotID)

	// The rebuilt update must carry the new manifest list.
	addUpd, ok := rebuilt[0].(*addSnapshotUpdate)
	require.True(t, ok)
	assert.Equal(t, newManifest, addUpd.Snapshot.ManifestList)

	// The superseded manifest list becomes an orphan.
	assert.Equal(t, oldManifest, orphaned[0])
}

// TestRebuildSnapshotUpdates_SkipsWhenParentUnchanged verifies that
// rebuildSnapshotUpdates skips the rebuild when the update's snapshot
// already has the fresh branch head as its parent (no-op retry).
func TestRebuildSnapshotUpdates_SkipsWhenParentUnchanged(t *testing.T) {
	const manifest = "s3://bucket/manifest-list.avro"

	freshHead := int64(42)
	freshMeta := newConflictTestMetadata(t, &freshHead)

	// Parent already equals the fresh head — rebuild must be skipped.
	snap := &Snapshot{
		SnapshotID:       99,
		ParentSnapshotID: &freshHead, // same as fresh head
		ManifestList:     manifest,
		Summary:          &Summary{Operation: OpAppend},
	}

	called := false
	upd := &addSnapshotUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSnapshot},
		Snapshot:   snap,
		rebuildManifestList: func(_ context.Context, _ Metadata, _ *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
			called = true

			return snap, nil
		},
	}

	rebuilt, orphaned, err := rebuildSnapshotUpdates(
		t.Context(),
		[]Update{upd},
		freshMeta,
		MainBranch,
		iceio.LocalFS{},
		1,
	)
	require.NoError(t, err)
	assert.False(t, called, "rebuild closure must not be called when parent is already up-to-date")
	assert.Empty(t, orphaned, "no orphans when rebuild is skipped")
	assert.Same(t, upd, rebuilt[0].(*addSnapshotUpdate), "original update must pass through unchanged")
}

// TestRebuildSnapshotUpdates_PassesThroughNonRebuildUpdates verifies that
// updates without a rebuildManifestList closure are returned unmodified.
func TestRebuildSnapshotUpdates_PassesThroughNonRebuildUpdates(t *testing.T) {
	plainUpd := NewAddSnapshotUpdate(&Snapshot{
		SnapshotID:   1,
		ManifestList: "s3://bucket/no-rebuild.avro",
		Summary:      &Summary{Operation: OpAppend},
	})

	// freshMeta may be nil — the plain update must not be touched.
	rebuilt, orphaned, err := rebuildSnapshotUpdates(
		t.Context(),
		[]Update{plainUpd},
		nil,
		MainBranch,
		iceio.LocalFS{},
		0,
	)
	require.NoError(t, err)
	assert.Empty(t, orphaned)
	assert.Same(t, plainUpd, rebuilt[0].(*addSnapshotUpdate))
}

// TestRebuildSnapshotUpdates_PropagatesClosureError verifies that an error
// returned by the rebuild closure surfaces as the function's return error.
func TestRebuildSnapshotUpdates_PropagatesClosureError(t *testing.T) {
	freshHead := int64(5)
	freshMeta := newConflictTestMetadata(t, &freshHead)

	parent := int64(1)
	snap := &Snapshot{
		SnapshotID:       10,
		ParentSnapshotID: &parent,
		ManifestList:     "s3://bucket/old.avro",
		Summary:          &Summary{Operation: OpAppend},
	}
	wantErr := errors.New("simulated S3 write failure")
	upd := &addSnapshotUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSnapshot},
		Snapshot:   snap,
		rebuildManifestList: func(_ context.Context, _ Metadata, _ *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
			return nil, wantErr
		},
	}

	_, _, err := rebuildSnapshotUpdates(t.Context(), []Update{upd}, freshMeta, MainBranch, iceio.LocalFS{}, 1)
	assert.ErrorIs(t, err, wantErr)
}

// ---------------------------------------------------------------------------
// Fix 3 — newSeq derived from freshMeta.LastSequenceNumber()
// ---------------------------------------------------------------------------

// TestRebuildFn_SeqNumDerivedFromFreshMeta verifies that the rebuilt snapshot's
// SequenceNumber equals freshMeta.LastSequenceNumber()+1, NOT
// freshParent.SequenceNumber+1. A concurrent writer on a different branch can
// advance the table-wide last-sequence-number without advancing this branch's
// parent, so using freshParent.SequenceNumber+1 would violate the spec invariant.
func TestRebuildFn_SeqNumDerivedFromFreshMeta(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	require.NotNil(t, addSnap.rebuildManifestList, "rebuildManifestList closure must be set")

	// Simulate a concurrent writer on another branch that bumped the global
	// last-sequence-number to 99 without advancing this branch's parent.
	// old code: newSeq = capturedSnapshot.SequenceNumber (stale, ≤ 99 — spec violation)
	// new code: newSeq = freshMeta.LastSequenceNumber() + 1 = 100
	freshMeta := newMetadataWithLastSeqNum(t, 99)
	require.Equal(t, int64(99), freshMeta.LastSequenceNumber())

	rebuilt, err := addSnap.rebuildManifestList(context.Background(), freshMeta, nil, wfs, 1)
	require.NoError(t, err)
	require.Equal(t, int64(100), rebuilt.SequenceNumber,
		"SequenceNumber must be freshMeta.LastSequenceNumber()+1; using freshParent.SequenceNumber+1 violates the spec when another branch advanced the global counter")
}

// TestRebuildFn_SeqNumV1TableIsZero verifies that v1 tables keep SequenceNumber == 0
// regardless of what freshMeta reports (v1 does not use sequence numbers).
func TestRebuildFn_SeqNumV1TableIsZero(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 1 // override to v1

	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	require.NotNil(t, addSnap.rebuildManifestList)

	freshMeta := newMetadataWithLastSeqNum(t, 99)
	rebuilt, err := addSnap.rebuildManifestList(context.Background(), freshMeta, nil, wfs, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), rebuilt.SequenceNumber, "v1 tables must always have SequenceNumber == 0")
}

// ---------------------------------------------------------------------------
// Fix 1 — firstRowID derived from freshMeta.NextRowID()
// ---------------------------------------------------------------------------

// TestRebuildFn_V3FirstRowIDDerivedFromFreshMeta verifies that on a v3 table
// the rebuilt snapshot's FirstRowID equals freshMeta.NextRowID() rather than
// the hardcoded 0. If two writers race and the peer commits first, the
// catalog's nextRowID has already advanced; using 0 would produce a
// first-row-id that disagrees with the catalog's view and fails row-lineage
// validation.
func TestRebuildFn_V3FirstRowIDDerivedFromFreshMeta(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	require.NotNil(t, addSnap.rebuildManifestList)

	// Asymmetry probe (R1): mutate the captured snapshot's AddedRows to a
	// sentinel that cannot match a correct recomputation. If a future
	// regression carries this value across instead of recomputing it from
	// writer.NextRowID()-firstRowID, the assertions below will fail.
	require.NotNil(t, addSnap.Snapshot.AddedRows, "captured snapshot must have AddedRows on v3")
	capturedAddedRows := *addSnap.Snapshot.AddedRows
	sentinel := int64(999_999)
	*addSnap.Snapshot.AddedRows = sentinel

	// Simulate a concurrent writer that added 50 rows, advancing nextRowID to 50.
	// old code: firstRowID = 0 (hardcoded)
	// new code: firstRowID = freshMeta.NextRowID() = 50
	freshMeta := newV3MetadataWithNextRowID(t, 50)
	require.Equal(t, int64(50), freshMeta.NextRowID(), "freshMeta.NextRowID() must be 50")

	rebuilt, err := addSnap.rebuildManifestList(context.Background(), freshMeta, nil, wfs, 1)
	require.NoError(t, err)
	require.NotNil(t, rebuilt.FirstRowID, "v3 rebuilt snapshot must have FirstRowID")
	require.Equal(t, int64(50), *rebuilt.FirstRowID,
		"FirstRowID must equal freshMeta.NextRowID(); hardcoded 0 would conflict with catalog's advanced nextRowID")

	// AddedRows must be RECOMPUTED from writer.NextRowID()-firstRowID, not
	// carried over from the captured snapshot. The producer wrote a single
	// 1-row file, so the correct recomputed value is 1 — which is also what
	// capturedAddedRows happened to be before mutation. The sentinel makes
	// the difference visible: if the rebuild path reused the captured slot,
	// rebuilt.AddedRows would equal sentinel instead.
	require.NotNil(t, rebuilt.AddedRows, "v3 rebuilt snapshot must have AddedRows")
	require.NotEqual(t, sentinel, *rebuilt.AddedRows,
		"AddedRows must be recomputed; carrying the captured value (mutated to sentinel) would be a regression")
	require.Equal(t, int64(1), *rebuilt.AddedRows,
		"AddedRows must equal writer.NextRowID()-firstRowID (this producer's 1 data row)")

	// Restore so any later inspection of capturedAddedRows is not misleading.
	*addSnap.Snapshot.AddedRows = capturedAddedRows
}

// TestRebuildFn_V3FirstRowIDZeroWhenNilNextRowID verifies that on a v3 table
// whose freshMeta has NextRowID()==0 (brand-new table — no rows yet) the
// rebuilt snapshot's FirstRowID is 0 and AddedRows reflects only the rows
// added by this producer. The Metadata API returns int64 (not *int64), so
// "nil NextRowID" is encoded as the zero value.
func TestRebuildFn_V3FirstRowIDZeroWhenNilNextRowID(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	require.NotNil(t, addSnap.rebuildManifestList)

	// Asymmetry probe (R1): mutate the captured snapshot's AddedRows to a
	// sentinel that cannot match a correct recomputation.
	require.NotNil(t, addSnap.Snapshot.AddedRows, "captured snapshot must have AddedRows on v3")
	sentinel := int64(888_888)
	*addSnap.Snapshot.AddedRows = sentinel

	// freshMeta with NextRowID()==0 (the "nil" equivalent for the int64 API).
	freshMeta := newV3MetadataWithNextRowID(t, 0)
	require.Equal(t, int64(0), freshMeta.NextRowID())

	rebuilt, err := addSnap.rebuildManifestList(context.Background(), freshMeta, nil, wfs, 1)
	require.NoError(t, err, "rebuild must not panic when freshMeta.NextRowID() is 0")
	require.NotNil(t, rebuilt.FirstRowID)
	require.Equal(t, int64(0), *rebuilt.FirstRowID,
		"FirstRowID must be 0 when freshMeta.NextRowID() is 0 (brand-new table)")
	require.NotNil(t, rebuilt.AddedRows)
	require.NotEqual(t, sentinel, *rebuilt.AddedRows,
		"AddedRows must be recomputed; carrying the captured (sentinel) value would be a regression")
	require.Equal(t, int64(1), *rebuilt.AddedRows,
		"AddedRows must reflect only this producer's contribution (1 row)")
}

// ---------------------------------------------------------------------------
// Fix 2 — snapshot summary recomputed against freshParent
// ---------------------------------------------------------------------------

// TestRebuildFn_SummaryRebasedAgainstFreshParent verifies that the rebuilt
// snapshot's summary totals are computed against the fresh parent's summary,
// not the stale totals captured at attempt 0. If a concurrent writer added
// files between attempt 0 and the retry, publishing the stale totals would
// regress every consumer that reads the summary.
func TestRebuildFn_SummaryRebasedAgainstFreshParent(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	require.NotNil(t, addSnap.rebuildManifestList)

	// Construct a freshParent with a known summary. The concurrent writer
	// added 5 data files (total-data-files = 5) before our retry.
	// Our producer added 1 file (added-data-files = 1 in capturedSnapshot.Summary).
	// Expected rebuilt total-data-files = 5 + 1 = 6.
	freshParentSummary := iceberg.Properties{
		"total-data-files":       "5",
		"total-records":          "500",
		"total-files-size":       "5000",
		"total-delete-files":     "0",
		"total-position-deletes": "0",
		"total-equality-deletes": "0",
	}

	// Write an empty manifest list for the freshParent so Manifests() can open it.
	parentManifestListPath := "mem://default/table-location/metadata/fresh-parent-snap.avro"
	out, createErr := wfs.Create(parentManifestListPath)
	require.NoError(t, createErr)
	writeErr := iceberg.WriteManifestList(2, out, 77, nil, ptr(int64(0)), 0, nil)
	require.NoError(t, writeErr)
	require.NoError(t, out.Close())

	freshParentID := int64(77)
	freshParent := &Snapshot{
		SnapshotID: freshParentID,
		// Set a SequenceNumber that DIFFERS from freshMeta.LastSequenceNumber()
		// so the assertion below can distinguish "newSeq derived from
		// freshMeta.LastSequenceNumber()" (correct) from "newSeq derived from
		// freshParent.SequenceNumber" (regression). Using freshParentSeq = 99
		// versus freshMeta.LastSequenceNumber() = 1 makes the two paths
		// disagree by 98.
		SequenceNumber: 99,
		ManifestList:   parentManifestListPath,
		Summary:        &Summary{Operation: OpAppend, Properties: freshParentSummary},
	}
	freshMeta := newMetadataWithLastSeqNum(t, 1)
	require.NotEqual(t, freshParent.SequenceNumber, freshMeta.LastSequenceNumber(),
		"test fixture must keep these distinct so the seq-num source is observable")

	rebuilt, err := addSnap.rebuildManifestList(context.Background(), freshMeta, freshParent, wfs, 1)
	require.NoError(t, err)
	require.NotNil(t, rebuilt.Summary)

	// R2: pin the seq-num source. Must equal freshMeta.LastSequenceNumber()+1
	// (=2) and must NOT equal freshParent.SequenceNumber+1 (=100). A regression
	// that derives newSeq from freshParent would fail this assertion.
	require.Equal(t, freshMeta.LastSequenceNumber()+1, rebuilt.SequenceNumber,
		"newSeq must derive from freshMeta.LastSequenceNumber(), not freshParent.SequenceNumber")

	// All three totals named in the reviewer comment must be rebased against
	// freshParent: total-data-files, total-records, total-files-size.
	// newTestDataFile contributes record count 1 and file size 1 (both fields
	// of the data file are set to 1 by newTestDataFileWithCount default).
	gotDataFiles := rebuilt.Summary.Properties.GetInt("total-data-files", -1)
	require.Equal(t, 6, gotDataFiles,
		"total-data-files must be freshParent total (5) + this producer's added count (1); stale attempt-0 total would be wrong")
	gotRecords := rebuilt.Summary.Properties.GetInt("total-records", -1)
	require.Equal(t, 501, gotRecords,
		"total-records must be freshParent total (500) + this producer's added records (1)")
	gotFileSize := rebuilt.Summary.Properties.GetInt("total-files-size", -1)
	require.Equal(t, 5001, gotFileSize,
		"total-files-size must be freshParent total (5000) + this producer's added size (1)")
}

// TestRebuildFn_SummaryFreshParentNilKeepsCapturedSummary verifies that when
// freshParent is nil (first snapshot on a brand-new table), the rebuild keeps
// capturedSnapshot.Summary as-is. There is no prior parent to rebase against,
// so the attempt-0 summary is the correct base.
func TestRebuildFn_SummaryFreshParentNilKeepsCapturedSummary(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/f.parquet", nil))

	updates, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	addSnap := updates[0].(*addSnapshotUpdate)
	require.NotNil(t, addSnap.rebuildManifestList)

	capturedSummary := addSnap.Snapshot.Summary
	require.NotNil(t, capturedSummary)

	freshMeta := newMetadataWithLastSeqNum(t, 1)
	rebuilt, err := addSnap.rebuildManifestList(context.Background(), freshMeta, nil, wfs, 1)
	require.NoError(t, err)
	require.NotNil(t, rebuilt.Summary)
	require.Equal(t, capturedSummary.Operation, rebuilt.Summary.Operation,
		"with freshParent=nil, rebuilt.Summary must equal capturedSnapshot.Summary")
	require.Equal(t, capturedSummary.Properties, rebuilt.Summary.Properties,
		"with freshParent=nil, rebuilt.Summary properties must be unchanged")
}
