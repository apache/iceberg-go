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

	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A retry caused by a write to an unrelated branch must preserve the staged
// promotion before rebuilding the append. Otherwise the append inherits main's
// old files instead of the audited branch's files.
func TestBranchPromotionAppendRetryKeepsAuditAncestry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func(*Transaction) error
	}{
		{"fast-forward from branch", func(tx *Transaction) error {
			return tx.FastForwardBranch(MainBranch, "audit")
		}},
		{"replace from branch", func(tx *Transaction) error {
			return tx.ReplaceBranchFromRef(MainBranch, "audit")
		}},
		{"replace from tag", func(tx *Transaction) error {
			return tx.ReplaceBranchFromRef(MainBranch, "release")
		}},
		{"replace by ID", func(tx *Transaction) error {
			return tx.ReplaceBranch(MainBranch, 300)
		}},
		{"two staged moves", func(tx *Transaction) error {
			if err := tx.ReplaceBranch(MainBranch, 200); err != nil {
				return err
			}

			return tx.FastForwardBranch(MainBranch, "audit")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testBranchPromotionAppendRetryKeepsAuditAncestry(t, tc.stage)
		})
	}
}

func testBranchPromotionAppendRetryKeepsAuditAncestry(t *testing.T, stage func(*Transaction) error) {
	t.Helper()
	base := manageSnapshotRefsFixture(t)
	baseBuilder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, baseBuilder.SetProperties(assertRefRetryProps))
	base, err = baseBuilder.Build()
	require.NoError(t, err)

	// Advance only the unrelated diverged branch in the catalog, forcing
	// a table-wide sequence-number change without moving main or audit.
	peer, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	diverged := int64(900)
	require.NoError(t, peer.AddSnapshot(&Snapshot{
		SnapshotID:       901,
		ParentSnapshotID: &diverged,
		SequenceNumber:   base.LastSequenceNumber() + 1,
		TimestampMs:      base.LastUpdatedMillis() + 1000,
		Summary:          &Summary{Operation: OpAppend},
	}))
	require.NoError(t, peer.SetSnapshotRef("diverged", 901, BranchRef))
	catalogMeta, err := peer.Build()
	require.NoError(t, err)

	cat := &flakyCatalog{metadata: catalogMeta, failUntilAttempt: 1, failWith: ErrCommitFailed}
	tbl := New(Identifier{"db", "branch-promotion-retry"}, base, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	tx := tbl.NewTransaction()
	require.NoError(t, stage(tx))

	auditID := int64(300)
	snap := &Snapshot{
		SnapshotID:       1001,
		ParentSnapshotID: &auditID,
		SequenceNumber:   base.LastSequenceNumber() + 1,
		TimestampMs:      base.LastUpdatedMillis() + 2000,
		ManifestList:     "original-manifest-list.avro",
		Summary:          &Summary{Operation: OpAppend},
	}
	var replayParent int64
	add := NewAddSnapshotUpdate(snap)
	add.rebuildManifestList = func(_ context.Context, fresh Metadata, parent *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
		require.NotNil(t, parent)
		replayParent = parent.SnapshotID
		updated := *snap
		updated.ParentSnapshotID = &replayParent
		updated.SequenceNumber = fresh.LastSequenceNumber() + 1
		updated.ManifestList = "rebuilt-manifest-list.avro"

		return &updated, nil
	}
	require.NoError(t, tx.apply([]Update{
		add,
		tx.meta.NewRetainingSnapshotRefUpdate(MainBranch, snap.SnapshotID, BranchRef),
	}, nil))

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), cat.attempts.Load())
	assert.Equal(t, auditID, replayParent, "retry must inherit from the promoted audit head")
	require.NotNil(t, committed.CurrentSnapshot())
	assert.Equal(t, int64(1001), committed.CurrentSnapshot().SnapshotID)
	require.NotNil(t, committed.CurrentSnapshot().ParentSnapshotID)
	assert.Equal(t, auditID, *committed.CurrentSnapshot().ParentSnapshotID)
	assert.Equal(t, auditID, committed.SnapshotByName("audit").SnapshotID)
}

// TestBranchPromotionRetryPreservesTableSequenceAcrossRefReset exercises a
// second append after replacing the branch head with an older lineage. The
// second append must not reuse the first one's table-wide sequence number.
func TestBranchPromotionRetryPreservesTableSequenceAcrossRefReset(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			testBranchPromotionRetryPreservesTableSequenceAcrossRefReset(t, version)
		})
	}
}

func testBranchPromotionRetryPreservesTableSequenceAcrossRefReset(t *testing.T, version int) {
	t.Helper()
	base := manageSnapshotRefsRetryFixture(t)
	if version == 3 {
		upgrade, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, upgrade.SetFormatVersion(3))
		base, err = upgrade.Build()
		require.NoError(t, err)
	}
	peer, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	diverged := int64(900)
	peerSnapshot := &Snapshot{
		SnapshotID:       901,
		ParentSnapshotID: &diverged,
		SequenceNumber:   base.LastSequenceNumber() + 1,
		TimestampMs:      base.LastUpdatedMillis() + 1000,
		Summary:          &Summary{Operation: OpAppend},
	}
	if version == 3 {
		first, rows := int64(0), int64(5)
		peerSnapshot.FirstRowID = &first
		peerSnapshot.AddedRows = &rows
	}
	require.NoError(t, peer.AddSnapshot(peerSnapshot))
	require.NoError(t, peer.SetSnapshotRef("diverged", 901, BranchRef))
	catalogMeta, err := peer.Build()
	require.NoError(t, err)

	cat := &flakyCatalog{metadata: catalogMeta, failUntilAttempt: 1, failWith: ErrCommitFailed}
	tbl := New(Identifier{"db", "branch-promotion-sequence-retry"}, base, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AssertRefSnapshotID(MainBranch))

	stageAppend := func(snapshotID, parentID, sequenceNumber int64) {
		snap := &Snapshot{
			SnapshotID:       snapshotID,
			ParentSnapshotID: &parentID,
			SequenceNumber:   sequenceNumber,
			TimestampMs:      base.LastUpdatedMillis() + snapshotID,
			ManifestList:     fmt.Sprintf("original-%d.avro", snapshotID),
			Summary:          &Summary{Operation: OpAppend},
		}
		if version == 3 {
			first := int64(0)
			if snapshotID == 1002 {
				first = 10
			}
			rows := int64(10)
			snap.FirstRowID = &first
			snap.AddedRows = &rows
		}
		add := NewAddSnapshotUpdate(snap)
		add.rebuildManifestList = func(_ context.Context, fresh Metadata, parent *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
			require.NotNil(t, parent)
			rebuilt := *snap
			id := parent.SnapshotID
			rebuilt.ParentSnapshotID = &id
			rebuilt.SequenceNumber = max(fresh.LastSequenceNumber(), parent.SequenceNumber) + 1
			if version == 3 {
				first := fresh.NextRowID()
				if parent.FirstRowID != nil && parent.AddedRows != nil {
					first = max(first, *parent.FirstRowID+*parent.AddedRows)
				}
				rebuilt.FirstRowID = &first
			}
			rebuilt.ManifestList = fmt.Sprintf("rebuilt-%d.avro", snapshotID)

			return &rebuilt, nil
		}
		require.NoError(t, tx.apply([]Update{
			add,
			tx.meta.NewRetainingSnapshotRefUpdate(MainBranch, snapshotID, BranchRef),
		}, nil))
	}

	stageAppend(1001, 100, base.LastSequenceNumber()+1)
	require.NoError(t, tx.ReplaceBranch(MainBranch, 300))
	stageAppend(1002, 300, base.LastSequenceNumber()+2)

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), cat.attempts.Load())
	first := committed.Metadata().SnapshotByID(1001)
	second := committed.Metadata().SnapshotByID(1002)
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.NotNil(t, first.ParentSnapshotID)
	require.NotNil(t, second.ParentSnapshotID)
	assert.Equal(t, int64(100), *first.ParentSnapshotID)
	assert.Equal(t, int64(300), *second.ParentSnapshotID)
	assert.Greater(t, second.SequenceNumber, first.SequenceNumber)
	assert.Equal(t, int64(1002), committed.CurrentSnapshot().SnapshotID)
	if version == 3 {
		require.NotNil(t, first.FirstRowID)
		require.NotNil(t, second.FirstRowID)
		assert.Equal(t, int64(5), *first.FirstRowID)
		assert.Equal(t, int64(15), *second.FirstRowID)
		assert.Equal(t, int64(25), committed.Metadata().NextRowID())
	}
}

// A source ref can point at a snapshot created earlier in the same
// transaction. Retry must pick the rebuilt version of that snapshot as the
// parent of the next append, while preserving v3 row-ID allocation.
func TestBranchPromotionRetryPromotesStagedSource(t *testing.T) {
	base := manageSnapshotRefsRetryFixture(t)
	upgrade, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, upgrade.SetFormatVersion(3))
	base, err = upgrade.Build()
	require.NoError(t, err)

	peer, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	divergedID := int64(900)
	peerFirst, peerRows := int64(0), int64(5)
	require.NoError(t, peer.AddSnapshot(&Snapshot{
		SnapshotID:       901,
		ParentSnapshotID: &divergedID,
		SequenceNumber:   base.LastSequenceNumber() + 1,
		TimestampMs:      base.LastUpdatedMillis() + 1000,
		FirstRowID:       &peerFirst,
		AddedRows:        &peerRows,
		Summary:          &Summary{Operation: OpAppend},
	}))
	require.NoError(t, peer.SetSnapshotRef("diverged", 901, BranchRef))
	catalogMeta, err := peer.Build()
	require.NoError(t, err)

	cat := &flakyCatalog{metadata: catalogMeta, failUntilAttempt: 1, failWith: ErrCommitFailed}
	tbl := New(Identifier{"db", "staged-source-promotion-retry"}, base, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	tx := tbl.NewTransaction()

	stageSnapshot := func(id, parentID, sequence, firstRowID int64, advanceMain bool) {
		rows := int64(10)
		snap := &Snapshot{
			SnapshotID:       id,
			ParentSnapshotID: &parentID,
			SequenceNumber:   sequence,
			TimestampMs:      base.LastUpdatedMillis() + id,
			ManifestList:     fmt.Sprintf("original-%d.avro", id),
			FirstRowID:       &firstRowID,
			AddedRows:        &rows,
			Summary:          &Summary{Operation: OpAppend},
		}
		add := NewAddSnapshotUpdate(snap)
		add.rebuildManifestList = func(_ context.Context, fresh Metadata, parent *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
			if parent == nil {
				return nil, fmt.Errorf("missing replay parent for snapshot %d", id)
			}
			rebuilt := *snap
			replayedParentID := parent.SnapshotID
			rebuilt.ParentSnapshotID = &replayedParentID
			rebuilt.SequenceNumber = fresh.LastSequenceNumber() + 1
			nextRowID := fresh.NextRowID()
			rebuilt.FirstRowID = &nextRowID
			rebuilt.ManifestList = fmt.Sprintf("rebuilt-%d.avro", id)

			return &rebuilt, nil
		}

		updates := []Update{add}
		if advanceMain {
			updates = append(updates, tx.meta.NewRetainingSnapshotRefUpdate(MainBranch, id, BranchRef))
		}
		require.NoError(t, tx.apply(updates, nil))
	}

	// Create a snapshot without updating main, move audit to it, promote that
	// staged audit head to main, then append on top of the promoted snapshot.
	stageSnapshot(1001, 100, base.LastSequenceNumber()+1, 0, false)
	require.NoError(t, tx.ReplaceBranch("audit", 1001))
	require.NoError(t, tx.FastForwardBranch(MainBranch, "audit"))
	stageSnapshot(1002, 1001, base.LastSequenceNumber()+2, 10, true)

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), cat.attempts.Load())

	first := committed.Metadata().SnapshotByID(1001)
	second := committed.Metadata().SnapshotByID(1002)
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.NotNil(t, first.ParentSnapshotID)
	require.NotNil(t, second.ParentSnapshotID)
	assert.Equal(t, int64(100), *first.ParentSnapshotID)
	assert.Equal(t, int64(1001), *second.ParentSnapshotID)
	assert.Greater(t, second.SequenceNumber, first.SequenceNumber)
	require.NotNil(t, first.FirstRowID)
	require.NotNil(t, second.FirstRowID)
	assert.Equal(t, int64(5), *first.FirstRowID)
	assert.Equal(t, int64(15), *second.FirstRowID)
	assert.Equal(t, int64(25), committed.Metadata().NextRowID())
	assert.Equal(t, int64(1001), committed.SnapshotByName("audit").SnapshotID)
	assert.Equal(t, int64(1002), committed.CurrentSnapshot().SnapshotID)
}
