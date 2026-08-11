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
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestTransactionApplyKeepsDistinctRequirementsOfSameType(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)

	mainSnapshotID := int64(10)
	featureSnapshotID := int64(20)

	err := txn.apply(nil, []Requirement{
		AssertRefSnapshotID(MainBranch, &mainSnapshotID),
		AssertRefSnapshotID("feature", &featureSnapshotID),
	})
	require.NoError(t, err)

	require.Len(t, txn.reqs, 2)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &mainSnapshotID)
	requireContainsRefSnapshotRequirement(t, txn.reqs, "feature", &featureSnapshotID)
}

func TestCurrentSnapshotForRefResolvesBranchHead(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)

	main := txn.meta.currentSnapshotForRef(MainBranch)
	require.NotNil(t, main)
	require.Equal(t, int64(10), main.SnapshotID)

	empty := txn.meta.currentSnapshotForRef("")
	require.NotNil(t, empty)
	require.Equal(t, int64(10), empty.SnapshotID, "empty ref must resolve like main")

	feature := txn.meta.currentSnapshotForRef("feature")
	require.NotNil(t, feature)
	require.Equal(t, int64(20), feature.SnapshotID, "feature branch must resolve to its own head (20), not main (10)")

	missing := txn.meta.currentSnapshotForRef("does-not-exist")
	require.NotNil(t, missing)
	require.Equal(t, int64(10), missing.SnapshotID, "a not-yet-created branch falls back to main's head")
}

func TestCurrentSnapshotIDForRefResolvesBranchHead(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)

	require.NotNil(t, txn.meta.currentSnapshotIDForRef(MainBranch))
	require.Equal(t, int64(10), *txn.meta.currentSnapshotIDForRef(MainBranch))

	require.NotNil(t, txn.meta.currentSnapshotIDForRef("feature"))
	require.Equal(t, int64(20), *txn.meta.currentSnapshotIDForRef("feature"),
		"feature branch assertion id must be the branch head (20), not main (10)")

	require.Nil(t, txn.meta.currentSnapshotIDForRef("does-not-exist"),
		"a not-yet-created branch must assert non-existence (nil), not main's head")
}

func TestCreateSnapshotProducerParentsOnBranchHead(t *testing.T) {
	t.Run("feature branch parents on feature head", func(t *testing.T) {
		txn := newTransactionWithSnapshotRefs(t)
		txn.branch = "feature"
		sp := createSnapshotProducer(OpAppend, txn, nil, nil, nil)
		require.Equal(t, int64(20), sp.parentSnapshotID,
			"append on feature must layer on the feature head (20), not main head (10)")
	})

	t.Run("main branch still parents on main head", func(t *testing.T) {
		txn := newTransactionWithSnapshotRefs(t)
		txn.branch = ""
		sp := createSnapshotProducer(OpAppend, txn, nil, nil, nil)
		require.Equal(t, int64(10), sp.parentSnapshotID)
	})
}

func TestBranchWriteCommitsThroughCatalogPath(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}

	producers := []struct {
		name    string
		op      Operation
		newProd func(Operation, *Transaction, iceio.WriteFileIO, *uuid.UUID, iceberg.Properties) *snapshotProducer
	}{
		{"fast append", OpAppend, newFastAppendFilesProducer},
		{"merge append", OpAppend, newMergeAppendFilesProducer},
		{"overwrite", OpOverwrite, newOverwriteFilesProducer},
	}

	for _, tc := range producers {
		t.Run(tc.name, func(t *testing.T) {
			txn, memIO := createTestTransactionWithMemIO(t, spec)

			// 1. Create the "feature" branch on a fresh table. The branch does
			// not exist yet, so the snapshot has no parent and the requirement
			// asserts the branch is absent (nil).
			txn.branch = "feature"
			sp1 := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
			sp1.appendDataFile(newTestDataFile(t, spec, "file://feature-1.parquet", nil))
			up1, rq1, err := sp1.commit(ctx)
			require.NoError(t, err)
			addSnap1, ok := up1[0].(*addSnapshotUpdate)
			require.True(t, ok)
			require.Nil(t, addSnap1.Snapshot.ParentSnapshotID, "first feature snapshot has no parent")
			requireContainsRefSnapshotRequirement(t, rq1, "feature", nil)
			featureHead := addSnap1.Snapshot.SnapshotID
			require.NoError(t, txn.apply(up1, rq1))
			meta1, err := txn.meta.Build()
			require.NoError(t, err)

			// 2. Advance main independently so feature and main diverge.
			tblMain := New(ident, meta1, "metadata.json", func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
			txnMain := tblMain.NewTransaction()
			spMain := newFastAppendFilesProducer(OpAppend, txnMain, memIO, nil, nil)
			spMain.appendDataFile(newTestDataFile(t, spec, "file://main-1.parquet", nil))
			upM, rqM, err := spMain.commit(ctx)
			require.NoError(t, err)
			addSnapM, ok := upM[0].(*addSnapshotUpdate)
			require.True(t, ok)
			mainHead := addSnapM.Snapshot.SnapshotID
			require.NoError(t, txnMain.apply(upM, rqM))
			divergedMeta, err := txnMain.meta.Build()
			require.NoError(t, err)
			require.NotEqual(t, featureHead, mainHead)

			// 3. Write to feature again with the producer under test and commit
			// through the public path. Staging mirrors what the high-level op
			// (Append/Overwrite/Delete) does internally; Commit runs doCommit ->
			// CommitTable where the branch requirement is validated.
			cat := &headTrackingCatalog{metadata: divergedMeta}
			tbl := New(ident, divergedMeta, "metadata.json", func(context.Context) (iceio.IO, error) { return memIO, nil }, cat)
			txnFeat, err := tbl.NewTransactionOnBranchWithError("feature")
			require.NoError(t, err)

			spFeat := tc.newProd(tc.op, txnFeat, memIO, nil, nil)
			spFeat.appendDataFile(newTestDataFile(t, spec, "file://feature-2.parquet", nil))
			upF, rqF, err := spFeat.commit(ctx)
			require.NoError(t, err)
			require.NoError(t, txnFeat.apply(upF, rqF))

			committed, err := txnFeat.Commit(ctx)
			require.NoError(t, err, "%s: catalog must accept the branch commit; a main-head AssertRefSnapshotID would be rejected", tc.name)
			require.Equal(t, int32(1), cat.attempts.Load(), "no retry expected: the branch assertion matches on the first attempt")

			newFeatureHead := committed.Metadata().SnapshotByName("feature")
			require.NotNil(t, newFeatureHead)
			require.NotNil(t, newFeatureHead.ParentSnapshotID)
			require.Equal(t, featureHead, *newFeatureHead.ParentSnapshotID,
				"%s on feature must be parented on the feature head, not main", tc.name)
			require.NotEqual(t, mainHead, *newFeatureHead.ParentSnapshotID)

			// The feature commit must leave main untouched.
			mainRef := committed.Metadata().SnapshotByName(MainBranch)
			require.NotNil(t, mainRef)
			require.Equal(t, mainHead, mainRef.SnapshotID, "committing to feature must not move main")
		})
	}
}

func liveDataFilePathsForSnapshot(t *testing.T, snap *Snapshot, fs iceio.IO) []string {
	t.Helper()
	require.NotNil(t, snap)
	var paths []string
	for e, err := range snap.entries(fs, iceberg.ManifestContentData) {
		require.NoError(t, err)
		if e.Status() == iceberg.EntryStatusDELETED {
			continue
		}
		if e.DataFile().ContentType() == iceberg.EntryContentData {
			paths = append(paths, e.DataFile().FilePath())
		}
	}

	return paths
}

func TestBranchCreateForksFromMainHead(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	// main -> [main.parquet]
	spMain := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	spMain.appendDataFile(newTestDataFile(t, spec, "file://main.parquet", nil))
	upM, rqM, err := spMain.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txn.apply(upM, rqM))
	mainMeta, err := txn.meta.Build()
	require.NoError(t, err)
	mainHead := mainMeta.CurrentSnapshot().SnapshotID

	// First write to the new "feature" branch.
	cat := &headTrackingCatalog{metadata: mainMeta}
	tbl := New(ident, mainMeta, "metadata.json", func(context.Context) (iceio.IO, error) { return memIO, nil }, cat)
	txnFeat, err := tbl.NewTransactionOnBranchWithError("feature")
	require.NoError(t, err)
	spFeat := newFastAppendFilesProducer(OpAppend, txnFeat, memIO, nil, nil)
	spFeat.appendDataFile(newTestDataFile(t, spec, "file://feature.parquet", nil))
	upF, rqF, err := spFeat.commit(ctx)
	require.NoError(t, err)

	// The new-branch requirement must assert absence (nil), while the parent must
	// be main's head — the two halves of the split that the fallback drives.
	requireContainsRefSnapshotRequirement(t, rqF, "feature", nil)
	addSnapF, ok := upF[0].(*addSnapshotUpdate)
	require.True(t, ok)
	require.NotNil(t, addSnapF.Snapshot.ParentSnapshotID)
	require.Equal(t, mainHead, *addSnapF.Snapshot.ParentSnapshotID,
		"first write to a new branch must fork from main's head")

	require.NoError(t, txnFeat.apply(upF, rqF))
	committed, err := txnFeat.Commit(ctx)
	require.NoError(t, err)

	require.ElementsMatch(t,
		[]string{"file://main.parquet", "file://feature.parquet"},
		liveDataFilePathsForSnapshot(t, committed.Metadata().SnapshotByName("feature"), memIO),
		"a new branch must inherit main's data files, plus its own")
}

func TestBranchCreateForksFromMainHeadAcrossRetry(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	spMain := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	spMain.appendDataFile(newTestDataFile(t, spec, "file://main.parquet", nil))
	upM, rqM, err := spMain.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txn.apply(upM, rqM))
	require.NoError(t, txn.SetProperties(iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}))
	mainMeta, err := txn.meta.Build()
	require.NoError(t, err)
	mainHead := mainMeta.CurrentSnapshot().SnapshotID

	// Fail attempt 0 (forcing a retry through rebuildSnapshotUpdates), apply on 1.
	cat := &flakyCatalog{metadata: mainMeta, failUntilAttempt: 1, failWith: fmt.Errorf("REST: %w", ErrCommitFailed)}
	tbl := New(ident, mainMeta, "metadata.json", func(context.Context) (iceio.IO, error) { return memIO, nil }, cat)

	txnFeat, err := tbl.NewTransactionOnBranchWithError("feature")
	require.NoError(t, err)
	spFeat := newFastAppendFilesProducer(OpAppend, txnFeat, memIO, nil, nil)
	spFeat.appendDataFile(newTestDataFile(t, spec, "file://feature.parquet", nil))
	upF, rqF, err := spFeat.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txnFeat.apply(upF, rqF))

	committed, err := txnFeat.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(2), cat.attempts.Load(), "commit must fail once then succeed on retry (exercising the rebuild path)")

	feat := committed.Metadata().SnapshotByName("feature")
	require.NotNil(t, feat)
	require.NotNil(t, feat.ParentSnapshotID)
	require.Equal(t, mainHead, *feat.ParentSnapshotID, "retry must keep main's head as the new branch's parent")
	require.ElementsMatch(t,
		[]string{"file://main.parquet", "file://feature.parquet"},
		liveDataFilePathsForSnapshot(t, feat, memIO),
		"a new branch must inherit main's data even when the commit retries and rebuilds")
}

// TestBranchRetryParentsOnPeerAdvancedBranchHead covers the retry path that
// TestBranchCreateForksFromMainHeadAcrossRetry does not: the target branch
// already exists and a concurrent peer advances THAT branch (not main)
// between attempts. On retry latestSnapshotForBranch must resolve the branch's
// own fresh head via SnapshotByName and reparent the staged snapshot onto it,
// rather than falling back to main's head.
func TestBranchRetryParentsOnPeerAdvancedBranchHead(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	wfs, meta := newMemIOWithRetryMeta(t, spec)

	// Seed main -> S0.
	seedTbl := New(ident, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return wfs, nil }, nil)
	seedTxn := seedTbl.NewTransaction()
	seedSp := newFastAppendFilesProducer(OpAppend, seedTxn, wfs, nil, nil)
	seedSp.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/seed.parquet", nil))
	upS, rqS, err := seedSp.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, seedTxn.apply(upS, rqS))
	baseMeta, err := seedTxn.meta.Build()
	require.NoError(t, err)
	s0 := baseMeta.CurrentSnapshot().SnapshotID

	// Create a "feature" branch pointing at S0 (same head as main for now).
	fb, err := MetadataBuilderFromBase(baseMeta, "")
	require.NoError(t, err)
	require.NoError(t, fb.SetSnapshotRef("feature", s0, BranchRef))
	baseMeta, err = fb.Build()
	require.NoError(t, err)

	// A concurrent peer advances the FEATURE branch (not main) between attempts.
	cat := &progressingRebuildCatalog{
		metadata:  baseMeta,
		wfs:       wfs,
		location:  "mem://default/table-location",
		branch:    "feature",
		failTimes: 1, // 1 conflict (peer graft) + 1 success
	}
	tbl := New(ident, baseMeta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return wfs, nil }, cat)

	txnFeat, err := tbl.NewTransactionOnBranchWithError("feature")
	require.NoError(t, err)
	spFeat := newFastAppendFilesProducer(OpAppend, txnFeat, wfs, nil, nil)
	spFeat.appendDataFile(newTestDataFile(t, spec, "mem://default/table-location/data/feature.parquet", nil))
	upF, rqF, err := spFeat.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txnFeat.apply(upF, rqF))

	_, err = txnFeat.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(2), cat.commitTableCalls.Load(),
		"commit must fail once (peer advances feature) then succeed on retry")

	// graftPeer assigns the peer feature head id 9000 + peerCount(1) = 9001.
	const peerFeatureHead = int64(9_001)
	committed := cat.committedSnapshot
	require.NotNil(t, committed)
	require.NotNil(t, committed.ParentSnapshotID)
	require.Equal(t, peerFeatureHead, *committed.ParentSnapshotID,
		"retry must reparent onto the peer-advanced feature head, not main")
	require.NotEqual(t, s0, *committed.ParentSnapshotID,
		"a fallback to main's head would (wrongly) parent on S0")
}

// TestBranchCommitRejectsNameThatBecomesATagOnRetry covers the absent-branch to
// tag race. "feature" does not exist when the transaction is constructed, so
// the ref-type guard in NewTransactionOnBranchWithError has nothing to reject
// and the commit carries an "assert this ref is absent" requirement
// (currentSnapshotIDForRef returns nil for an unknown branch). If a peer
// creates that name as a TAG before the retry, the replay must fail: without
// the check, commitManifests emits a BranchRef update that would advance the
// tag onto a new snapshot and strip its immutability.
func TestBranchCommitRejectsNameThatBecomesATagOnRetry(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	spMain := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	spMain.appendDataFile(newTestDataFile(t, spec, "file://main.parquet", nil))
	upM, rqM, err := spMain.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txn.apply(upM, rqM))
	require.NoError(t, txn.SetProperties(iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}))
	mainMeta, err := txn.meta.Build()
	require.NoError(t, err)
	mainHead := mainMeta.CurrentSnapshot().SnapshotID

	// The peer's view: same table, "feature" created as a TAG.
	tagBuilder, err := MetadataBuilderFromBase(mainMeta, "")
	require.NoError(t, err)
	require.NoError(t, tagBuilder.SetSnapshotRef("feature", mainHead, TagRef))
	tagMeta, err := tagBuilder.Build()
	require.NoError(t, err)

	// The table's base is mainMeta (feature absent), while the catalog serves
	// the post-race tagMeta on refresh: attempt 0 fails with a retryable
	// conflict, and the retry reloads the tag before the replay runs.
	cat := &flakyCatalog{metadata: tagMeta, failUntilAttempt: 1, failWith: fmt.Errorf("REST: %w", ErrCommitFailed)}
	tbl := New(ident, mainMeta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, cat)

	txnFeat, err := tbl.NewTransactionOnBranchWithError("feature")
	require.NoError(t, err, "the name is still absent here, so construction cannot reject it")
	spFeat := newFastAppendFilesProducer(OpAppend, txnFeat, memIO, nil, nil)
	spFeat.appendDataFile(newTestDataFile(t, spec, "file://feature.parquet", nil))
	upF, rqF, err := spFeat.commit(ctx)
	require.NoError(t, err)
	requireContainsRefSnapshotRequirement(t, rqF, "feature", nil)
	require.NoError(t, txnFeat.apply(upF, rqF))

	_, err = txnFeat.Commit(ctx)
	require.ErrorContains(t, err, "tags cannot be transaction targets")
	require.Equal(t, int32(1), cat.attempts.Load(),
		"the replay must be rejected before a second CommitTable call")

	var featureRef SnapshotRef
	for name, ref := range tagMeta.Refs() {
		if name == "feature" {
			featureRef = ref
		}
	}
	require.Equal(t, TagRef, featureRef.SnapshotRefType, "the tag must not be converted into a branch")
	require.Equal(t, mainHead, featureRef.SnapshotID, "the tag must not advance")
}

// TestOverwriteOnBranchOnlyTableKeepsOverwriteSemantics pins the branch-aware
// lookup in mergeOverwrite. A table whose only writes went to a branch has a
// branch head but no main head, so the old currentSnapshot() lookup returned
// nil and silently downgraded an overwrite to an append: the branch's existing
// files were never marked deleted.
func TestOverwriteOnBranchOnlyTableKeepsOverwriteSemantics(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	// The first write on a fresh table goes to "feature": the branch gets a
	// head while main has none — the state that separates the two lookups.
	txn.branch = "feature"
	sp := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "file://feature-1.parquet", nil))
	up, rq, err := sp.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txn.apply(up, rq))
	branchOnlyMeta, err := txn.meta.Build()
	require.NoError(t, err)
	require.Nil(t, branchOnlyMeta.CurrentSnapshot(), "fixture must leave main empty")
	require.NotNil(t, branchOnlyMeta.SnapshotByName("feature"))

	t.Run("branch with a head overwrites", func(t *testing.T) {
		tbl := New(ident, branchOnlyMeta, "metadata.json",
			func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
		txnFeat, err := tbl.NewTransactionOnBranchWithError("feature")
		require.NoError(t, err)

		prod := txnFeat.updateSnapshot(memIO, nil, OpOverwrite).mergeOverwrite(nil, nil)
		prod.appendDataFile(newTestDataFile(t, spec, "file://feature-2.parquet", nil))
		ups, _, err := prod.commit(ctx)
		require.NoError(t, err)
		add, ok := ups[0].(*addSnapshotUpdate)
		require.True(t, ok)
		require.NotNil(t, add.Snapshot.Summary)
		require.Equal(t, OpOverwrite, add.Snapshot.Summary.Operation,
			"the branch has files to overwrite, so the operation must stay an overwrite")
	})

	t.Run("branch with no head still degrades to append", func(t *testing.T) {
		emptyTxn, emptyIO := createTestTransactionWithMemIO(t, spec)
		emptyTxn.branch = "feature"

		prod := emptyTxn.updateSnapshot(emptyIO, nil, OpOverwrite).mergeOverwrite(nil, nil)
		prod.appendDataFile(newTestDataFile(t, spec, "file://feature-1.parquet", nil))
		ups, _, err := prod.commit(ctx)
		require.NoError(t, err)
		add, ok := ups[0].(*addSnapshotUpdate)
		require.True(t, ok)
		require.NotNil(t, add.Snapshot.Summary)
		require.Equal(t, OpAppend, add.Snapshot.Summary.Operation,
			"an overwrite with nothing to overwrite must still be recorded as an append")
	})
}

// TestBranchMultipleStagedSnapshotsChainOnRetry is the branch counterpart of
// OCCScenarioTestSuite.TestMultipleStagedSnapshotsChainOnRetry: it combines the
// two fixes. Two snapshots are staged on a not-yet-created branch (both fork
// from main's head) and a forced retry replays them. The replay must keep them
// chained A1 <- A2; rebuilding both against the same fresh head makes them
// siblings, and the branch ref then advances only to A2, dropping A1's file.
func TestBranchMultipleStagedSnapshotsChainOnRetry(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	spMain := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	spMain.appendDataFile(newTestDataFile(t, spec, "file://main.parquet", nil))
	upM, rqM, err := spMain.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txn.apply(upM, rqM))
	require.NoError(t, txn.SetProperties(iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}))
	mainMeta, err := txn.meta.Build()
	require.NoError(t, err)
	mainHead := mainMeta.CurrentSnapshot().SnapshotID

	cat := &flakyCatalog{metadata: mainMeta, failUntilAttempt: 1, failWith: fmt.Errorf("REST: %w", ErrCommitFailed)}
	tbl := New(ident, mainMeta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, cat)

	txnFeat, err := tbl.NewTransactionOnBranchWithError("feature")
	require.NoError(t, err)

	// A1 forks from main's head; A2 is staged on top of A1.
	sp1 := newFastAppendFilesProducer(OpAppend, txnFeat, memIO, nil, nil)
	sp1.appendDataFile(newTestDataFile(t, spec, "file://feature-1.parquet", nil))
	up1, rq1, err := sp1.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txnFeat.apply(up1, rq1))

	sp2 := newFastAppendFilesProducer(OpAppend, txnFeat, memIO, nil, nil)
	sp2.appendDataFile(newTestDataFile(t, spec, "file://feature-2.parquet", nil))
	up2, rq2, err := sp2.commit(ctx)
	require.NoError(t, err)
	require.NoError(t, txnFeat.apply(up2, rq2))

	committed, err := txnFeat.Commit(ctx)
	require.NoError(t, err,
		"both staged snapshots must survive the retry; siblings share a sequence number and AddSnapshot rejects the second")
	require.Equal(t, int32(2), cat.attempts.Load(), "commit must fail once then succeed on retry")

	head := committed.Metadata().SnapshotByName("feature")
	require.NotNil(t, head)
	require.NotNil(t, head.ParentSnapshotID, "the branch head (A2) must have a parent")
	a1 := committed.Metadata().SnapshotByID(*head.ParentSnapshotID)
	require.NotNil(t, a1, "A2's parent (A1) must stay reachable, not be orphaned by a sibling rebuild")
	require.NotNil(t, a1.ParentSnapshotID)
	require.Equal(t, mainHead, *a1.ParentSnapshotID, "A1 must fork from main's head")
	require.Greater(t, head.SequenceNumber, a1.SequenceNumber,
		"the chained snapshot must have a strictly greater sequence number")

	require.ElementsMatch(t,
		[]string{"file://main.parquet", "file://feature-1.parquet", "file://feature-2.parquet"},
		liveDataFilePathsForSnapshot(t, head, memIO),
		"the branch head must inherit main's file plus both staged files")
}

func TestExpireSnapshotsWithOlderThanDoesNotExpireSnapshotRefs(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	now := time.Now().UnixMilli()
	oldTimestamp := time.Now().Add(-8 * 24 * time.Hour).UnixMilli()

	txn.meta.snapshotList[0].TimestampMs = oldTimestamp
	txn.meta.snapshotList[1].TimestampMs = now
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 20, BranchRef))
	require.NoError(t, txn.meta.SetSnapshotRef("old-branch", 10, BranchRef))
	require.NoError(t, txn.meta.SetSnapshotRef("old-tag", 10, TagRef))
	txn.meta.lastUpdatedMS = now

	require.NoError(t, txn.ExpireSnapshots(WithOlderThan(7*24*time.Hour)))

	_, branchExists := txn.meta.refs["old-branch"]
	_, tagExists := txn.meta.refs["old-tag"]
	require.True(t, branchExists, "WithOlderThan must not expire a branch without max-ref-age-ms")
	require.True(t, tagExists, "WithOlderThan must not expire a tag without max-ref-age-ms")
}

func TestTransactionApplyDedupesEquivalentRequirementsWithinAndAcrossCalls(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)

	mainSnapshotID := int64(10)
	first := AssertRefSnapshotID(MainBranch, &mainSnapshotID)
	second := AssertRefSnapshotID(MainBranch, &mainSnapshotID)

	err := txn.apply(nil, []Requirement{first, second})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &mainSnapshotID)

	err = txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &mainSnapshotID)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &mainSnapshotID)
}

func TestNewTransactionOnBranchWithErrorReturnsTransactionInitError(t *testing.T) {
	baseMeta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	txn, err := New(Identifier{"db", "broken"}, brokenMetadata{
		Metadata: baseMeta,
	}, "metadata.json", func(context.Context) (iceio.IO, error) {
		return nil, nil
	}, nil).NewTransactionOnBranchWithError(MainBranch)
	require.Error(t, err, "expected metadata builder initialization to fail")
	require.ErrorContains(t, err, "current schema is missing")
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.Nil(t, txn)
}

func TestNewTransactionOnBranchRejectsTags(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	require.NoError(t, txn.meta.SetSnapshotRef("release", 10, TagRef))

	meta, err := txn.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "table"}, meta, "metadata.json", nil, nil)

	before := tbl.Metadata()
	transaction, err := tbl.NewTransactionOnBranchWithError("release")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.Nil(t, transaction)
	require.True(t, before.Equals(tbl.Metadata()))

	legacyTransaction := tbl.NewTransactionOnBranch("release")
	require.ErrorIs(t, legacyTransaction.initErr, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, legacyTransaction.SetProperties(iceberg.Properties{"k": "v"}), "tags cannot be transaction targets")
}

func TestTransactionCommitRejectsSameIDTagRace(t *testing.T) {
	seed := newTransactionWithSnapshotRefs(t)
	baseMeta, err := seed.meta.Build()
	require.NoError(t, err)

	head := baseMeta.SnapshotByName(MainBranch)
	require.NotNil(t, head)
	tagBuilder, err := MetadataBuilderFromBase(baseMeta, "")
	require.NoError(t, err)
	require.NoError(t, tagBuilder.SetSnapshotRef(MainBranch, head.SnapshotID, TagRef))
	tagMeta, err := tagBuilder.Build()
	require.NoError(t, err)

	cat := &headTrackingCatalog{metadata: tagMeta}
	tbl := New(Identifier{"db", "tag-race"}, baseMeta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"k": "v"}))

	_, err = txn.Commit(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "tags cannot be transaction targets")
	require.Equal(t, int32(1), cat.attempts.Load())
}

func TestNewTransactionOnBranchAllowsBranchesAndNewRefs(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	meta, err := txn.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "table"}, meta, "metadata.json", nil, nil)

	for _, branch := range []string{MainBranch, "feature", "new-branch"} {
		transaction, err := tbl.NewTransactionOnBranchWithError(branch)
		require.NoError(t, err, branch)
		require.NotNil(t, transaction, branch)
	}
}

func TestNewTransactionOnBranchKeepsLegacySignatureAndFailsOnUse(t *testing.T) {
	baseMeta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	txn := New(Identifier{"db", "broken"}, brokenMetadata{
		Metadata: baseMeta,
	}, "metadata.json", func(context.Context) (iceio.IO, error) {
		return nil, nil
	}, nil).NewTransaction()

	t.Run("set properties returns init error", func(t *testing.T) {
		err := txn.SetProperties(iceberg.Properties{"k": "v"})
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("update schema no longer panics", func(t *testing.T) {
		var err error
		require.NotPanics(t, func() {
			err = txn.UpdateSchema(true, false).
				AddColumn([]string{"new_col"}, iceberg.PrimitiveTypes.String, "", false, nil).
				Commit()
		})
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("update spec returns init error", func(t *testing.T) {
		err := txn.UpdateSpec(true).AddIdentity("id").Commit()
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("table commit returns init error", func(t *testing.T) {
		_, err := txn.TableCommit()
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("write equality deletes returns init error", func(t *testing.T) {
		_, err := txn.WriteEqualityDeletes(context.Background(), []int{1}, nil)
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("commit returns init error", func(t *testing.T) {
		_, err := txn.Commit(context.Background())
		require.ErrorIs(t, err, ErrInvalidMetadata)
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("row delta commit returns init error", func(t *testing.T) {
		rowFile, dataErr := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec,
			iceberg.EntryContentData,
			"file://data.parquet",
			iceberg.ParquetFile,
			nil,
			nil,
			nil,
			10,
			10,
		)
		require.NoError(t, dataErr, "new data file builder")

		rd := txn.NewRowDelta(nil).AddRows(rowFile.Build())
		err := rd.Commit(context.Background())
		require.ErrorContains(t, err, "current schema is missing")
	})
}

func TestTransactionEnsureInitializedNilReceiver(t *testing.T) {
	var txn *Transaction

	err := txn.ensureInitialized()
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "transaction is nil")
}

func TestTransactionTxnMetaNilReceiver(t *testing.T) {
	var txn *Transaction

	meta, err := txn.txnMeta()
	require.Nil(t, meta)
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "transaction is nil")
}

// newBrokenTransaction builds a transaction whose deferred initialization fails
// ("current schema is missing") because the base metadata reports a nil current
// schema. It is non-nil and its mutex is usable, so it exercises the
// post-lock/deferred-error path distinctly from a nil receiver.
func newBrokenTransaction(t *testing.T) *Transaction {
	t.Helper()

	baseMeta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	return New(Identifier{"db", "broken"}, brokenMetadata{Metadata: baseMeta}, "metadata.json",
		func(context.Context) (iceio.IO, error) { return nil, nil }, nil).NewTransaction()
}

// TestTransactionEntryPointsRejectNilAndBrokenTransaction asserts the public
// contract that issue #1431 targets: each exported entry point below must
// return an error (never panic) when the transaction is nil or failed to
// initialize. It covers both terminal commit paths (Commit, TableCommit), the
// metadata builders (UpdateSpec, UpdateSchema, RowDelta), and the compaction
// entry points (RewriteManifests, RewriteDataFiles) — not just the txnMeta
// accessor. OverwriteTable is exercised via Overwrite (it only wraps a reader
// around it); NewRewrite is exercised via ReplaceFiles.
func TestTransactionEntryPointsRejectNilAndBrokenTransaction(t *testing.T) {
	dfBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		"file://data.parquet",
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		10,
		10,
	)
	require.NoError(t, err, "new data file builder")
	dataFile := dfBuilder.Build()

	ctx := context.Background()
	entryPoints := []struct {
		name string
		call func(txn *Transaction) error
	}{
		{"SetProperties", func(txn *Transaction) error { return txn.SetProperties(iceberg.Properties{"k": "v"}) }},
		{"UpgradeFormatVersion", func(txn *Transaction) error { return txn.UpgradeFormatVersion(2) }},
		{"RollbackToSnapshot", func(txn *Transaction) error { return txn.RollbackToSnapshot(1) }},
		{"ExpireSnapshots", func(txn *Transaction) error { return txn.ExpireSnapshots() }},
		{"AppendTable", func(txn *Transaction) error { return txn.AppendTable(ctx, nil, 0, nil) }},
		{"Append", func(txn *Transaction) error { return txn.Append(ctx, nil, nil) }},
		{"AddDataFiles", func(txn *Transaction) error { return txn.AddDataFiles(ctx, []iceberg.DataFile{dataFile}, nil) }},
		{"AddFiles", func(txn *Transaction) error { return txn.AddFiles(ctx, []string{"file://a.parquet"}, nil, false) }},
		{"ReplaceDataFiles", func(txn *Transaction) error { return txn.ReplaceDataFiles(ctx, []string{"a"}, []string{"b"}, nil) }},
		{"ReplaceDataFilesWithDataFiles", func(txn *Transaction) error {
			return txn.ReplaceDataFilesWithDataFiles(ctx, []iceberg.DataFile{dataFile}, nil, nil)
		}},
		{"ReplaceFiles", func(txn *Transaction) error {
			return txn.ReplaceFiles(ctx, []iceberg.DataFile{dataFile}, nil, []iceberg.DataFile{dataFile}, nil)
		}},
		{"Delete", func(txn *Transaction) error { return txn.Delete(ctx, iceberg.AlwaysTrue{}, nil) }},
		{"Overwrite", func(txn *Transaction) error { return txn.Overwrite(ctx, nil, nil) }},
		{"Scan", func(txn *Transaction) error {
			_, err := txn.Scan()

			return err
		}},
		{"StagedTable", func(txn *Transaction) error {
			_, err := txn.StagedTable()

			return err
		}},
		{"Commit", func(txn *Transaction) error {
			_, err := txn.Commit(ctx)

			return err
		}},
		{"TableCommit", func(txn *Transaction) error {
			_, err := txn.TableCommit()

			return err
		}},
		{"RewriteManifests", func(txn *Transaction) error {
			_, err := txn.RewriteManifests(ctx)

			return err
		}},
		{"RewriteDataFiles", func(txn *Transaction) error {
			// A non-empty task group forces the path that would otherwise
			// dereference t.tbl (panicking on a nil transaction).
			groups := []CompactionTaskGroup{{Tasks: []FileScanTask{{}}}}
			_, err := txn.RewriteDataFiles(ctx, groups, RewriteDataFilesOptions{})

			return err
		}},
		{"WriteEqualityDeletes", func(txn *Transaction) error {
			_, err := txn.WriteEqualityDeletes(ctx, []int{1}, nil)

			return err
		}},
		{"UpdateSpec", func(txn *Transaction) error { return txn.UpdateSpec(true).AddIdentity("id").Commit() }},
		{"UpdateSchema", func(txn *Transaction) error {
			return txn.UpdateSchema(true, false).
				AddColumn([]string{"new_col"}, iceberg.PrimitiveTypes.String, "", false, nil).
				Commit()
		}},
		{"RowDelta", func(txn *Transaction) error { return txn.NewRowDelta(nil).AddRows(dataFile).Commit(ctx) }},
	}

	t.Run("nil transaction", func(t *testing.T) {
		for _, ep := range entryPoints {
			t.Run(ep.name, func(t *testing.T) {
				var txn *Transaction
				var err error
				require.NotPanics(t, func() { err = ep.call(txn) })
				require.ErrorIs(t, err, ErrInvalidMetadata)
				require.ErrorContains(t, err, "transaction is nil")
			})
		}
	})

	t.Run("broken transaction", func(t *testing.T) {
		for _, ep := range entryPoints {
			t.Run(ep.name, func(t *testing.T) {
				txn := newBrokenTransaction(t)
				var err error
				require.NotPanics(t, func() { err = ep.call(txn) })
				require.ErrorContains(t, err, "current schema is missing")
			})
		}
	})
}

type brokenMetadata struct {
	Metadata
}

func (m brokenMetadata) CurrentSchema() *iceberg.Schema {
	return nil
}

func TestTransactionApplyKeepsMetadataUnchangedOnUpdateFailure(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	baseMeta, err := txn.meta.Build()
	require.NoError(t, err)

	updates := []Update{
		NewUpgradeFormatVersionUpdate(baseMeta.Version() + 1),
		NewSetCurrentSchemaUpdate(9999),
	}

	err = txn.apply(updates, nil)
	require.Error(t, err)

	postMeta, err := txn.meta.Build()
	require.NoError(t, err)
	require.True(t, baseMeta.Equals(postMeta))
	require.Len(t, txn.reqs, 0)
}

func TestTransactionApplyKeepsRequirementsUnchangedOnUpdateFailure(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	baseMeta, err := txn.meta.Build()
	require.NoError(t, err)

	// Stage a requirement with a successful apply so txn.reqs is non-empty
	// going into the failing call; this distinguishes a genuine rollback from
	// an implementation that simply never accumulates requirements.
	err = txn.apply(nil, []Requirement{AssertTableUUID(baseMeta.TableUUID())})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)

	// The requirement passed to the failing call must itself validate, so the
	// rollback is driven by the update failure (not by requirement validation
	// bailing out early). Assert that here so a future fixture change surfaces
	// as a loud failure in the right place.
	require.NoError(t, AssertCurrentSchemaID(0).Validate(baseMeta))

	// A batch whose second update fails must leave both the staged metadata and
	// the requirement list at their pre-call state — not the one extra req this
	// call would add, and not an empty list.
	updates := []Update{
		NewUpgradeFormatVersionUpdate(baseMeta.Version() + 1),
		NewSetCurrentSchemaUpdate(9999),
	}
	err = txn.apply(updates, []Requirement{AssertCurrentSchemaID(0)})
	require.Error(t, err)

	require.Len(t, txn.reqs, 1)
	require.Equal(t, AssertTableUUID(baseMeta.TableUUID()), txn.reqs[0])
}

// TestTransactionApplyDedupesSameRefAssertionsNewTable covers two appends in a
// single new-table transaction: the first asserts main must not exist, and the
// second (after the builder has created main) asserts main == the new snapshot.
// Only the first main == nil assertion, which reflects the pre-transaction base
// state, must be retained.
func TestTransactionApplyDedupesSameRefAssertionsNewTable(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)

	// First append: main does not exist yet in the pre-transaction metadata.
	err := txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, nil)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, nil)

	// Simulate the first append creating main -> 10 in the transaction builder.
	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:     10,
		SequenceNumber: 1,
		ManifestList:   "mem://default/table-location/metadata/manifest-10.avro",
		Summary:        &Summary{Operation: OpAppend},
		TimestampMs:    time.Now().UnixMilli(),
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 10, BranchRef))

	// Second append asserts main == 10; it must dedupe against the first
	// assertion for main rather than adding a contradictory base-state check.
	newHead := int64(10)
	err = txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &newHead)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, nil)
}

// TestTransactionApplyDedupesSameRefAssertionsExistingTable covers two appends
// on an existing table: the first asserts the original base head, and the second
// (after the builder has advanced main) asserts the new head. Only the original
// base-head assertion must be retained.
func TestTransactionApplyDedupesSameRefAssertionsExistingTable(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t) // main -> 10, feature -> 20

	base := int64(10)
	err := txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &base)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &base)

	// Simulate the first append advancing main -> 30.
	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       30,
		ParentSnapshotID: transactionTestPtr(base),
		SequenceNumber:   3,
		ManifestList:     "mem://default/table-location/metadata/manifest-30.avro",
		Summary:          &Summary{Operation: OpAppend},
		TimestampMs:      time.Now().UnixMilli(),
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 30, BranchRef))

	// Second append asserts main == 30; it must dedupe against the original
	// base-head assertion, which is the only one kept.
	newHead := int64(30)
	err = txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &newHead)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &base)
}

// TestTransactionApplyKeepsRefAssertionsForDistinctRefs confirms that dedupe by
// ref name still lets assertions for different refs both survive, even when they
// assert the same snapshot id.
func TestTransactionApplyKeepsRefAssertionsForDistinctRefs(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t) // main -> 10, feature -> 20

	base := int64(10)
	err := txn.apply(nil, []Requirement{
		AssertRefSnapshotID(MainBranch, &base),
		AssertRefSnapshotID("feature", transactionTestPtr(int64(20))),
	})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 2)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &base)
	requireContainsRefSnapshotRequirement(t, txn.reqs, "feature", transactionTestPtr(int64(20)))
}

// TestTransactionApplyDedupesIdenticalNonRefRequirements confirms that non-ref
// requirements keep the canonical JSON dedupe key: identical requirements
// collapse while distinct ones survive.
func TestTransactionApplyDedupesIdenticalNonRefRequirements(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)

	err := txn.apply(nil, []Requirement{
		AssertCurrentSchemaID(0),
		AssertCurrentSchemaID(0),
		AssertDefaultSpecID(0),
	})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 2)

	var schemaAsserts, specAsserts int
	for _, r := range txn.reqs {
		switch r.GetType() {
		case reqAssertCurrentSchemaID:
			schemaAsserts++
		case reqAssertDefaultSpecID:
			specAsserts++
		}
	}
	require.Equal(t, 1, schemaAsserts)
	require.Equal(t, 1, specAsserts)
}

func TestRollbackToSnapshotPreservesRetention(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	now := time.Now().UnixMilli()

	const (
		minKeep      = 5
		maxSnapAgeMs = int64(172800000) // 2 days
		maxRefAgeMs  = int64(604800000) // 7 days
	)

	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:     10,
		SequenceNumber: 1,
		ManifestList:   "mem://default/table-location/metadata/manifest-10.avro",
		Summary:        &Summary{Operation: OpAppend},
		TimestampMs:    now,
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(
		MainBranch, 10, BranchRef,
		WithMinSnapshotsToKeep(minKeep),
		WithMaxSnapshotAgeMs(maxSnapAgeMs),
		WithMaxRefAgeMs(maxRefAgeMs),
	))

	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       20,
		ParentSnapshotID: transactionTestPtr(int64(10)),
		SequenceNumber:   2,
		ManifestList:     "mem://default/table-location/metadata/manifest-20.avro",
		Summary:          &Summary{Operation: OpAppend},
		TimestampMs:      now + 1,
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(
		MainBranch, 20, BranchRef,
		WithMinSnapshotsToKeep(minKeep),
		WithMaxSnapshotAgeMs(maxSnapAgeMs),
		WithMaxRefAgeMs(maxRefAgeMs),
	))

	require.NoError(t, txn.RollbackToSnapshot(10))

	ref := txn.meta.refs[MainBranch]
	require.Equal(t, int64(10), ref.SnapshotID, "rollback should move main to the ancestor")
	require.NotNil(t, ref.MinSnapshotsToKeep, "min-snapshots-to-keep must survive rollback")
	require.Equal(t, minKeep, *ref.MinSnapshotsToKeep)
	require.NotNil(t, ref.MaxSnapshotAgeMs, "max-snapshot-age-ms must survive rollback")
	require.Equal(t, maxSnapAgeMs, *ref.MaxSnapshotAgeMs)
	require.NotNil(t, ref.MaxRefAgeMs, "max-ref-age-ms must survive rollback")
	require.Equal(t, maxRefAgeMs, *ref.MaxRefAgeMs)
}

func newTransactionWithSnapshotRefs(t *testing.T) *Transaction {
	t.Helper()

	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	now := time.Now().UnixMilli()

	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:     10,
		SequenceNumber: 1,
		ManifestList:   "mem://default/table-location/metadata/manifest-10.avro",
		Summary:        &Summary{Operation: OpAppend},
		TimestampMs:    now,
	}))

	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       20,
		ParentSnapshotID: transactionTestPtr(int64(10)),
		SequenceNumber:   2,
		ManifestList:     "mem://default/table-location/metadata/manifest-20.avro",
		Summary:          &Summary{Operation: OpAppend},
		TimestampMs:      now + 1,
	}))

	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 10, BranchRef))
	require.NoError(t, txn.meta.SetSnapshotRef("feature", 20, BranchRef))

	return txn
}

func requireContainsRefSnapshotRequirement(t *testing.T, requirements []Requirement, ref string, snapshotID *int64) {
	t.Helper()

	for _, requirement := range requirements {
		actual, ok := requirement.(*assertRefSnapshotID)
		if ok && actual.Ref == ref && transactionTestInt64PtrEqual(actual.SnapshotID, snapshotID) {
			return
		}
	}

	t.Fatalf("expected assertRefSnapshotID requirement for ref %q and snapshot id %v not found", ref, snapshotID)
}

func transactionTestPtr[T any](v T) *T {
	return &v
}

func transactionTestInt64PtrEqual(left, right *int64) bool {
	if left == nil || right == nil {
		return left == right
	}

	return *left == *right
}
