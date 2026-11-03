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
