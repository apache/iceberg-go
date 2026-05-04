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

	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rebuildUpdate constructs an addSnapshotUpdate whose rebuildManifestList
// closure simply records the freshParent it received and returns a new
// snapshot whose ManifestList is the given newManifestList value.
func rebuildUpdate(snap *Snapshot, newManifestList string, gotParent **Snapshot) *addSnapshotUpdate {
	return &addSnapshotUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSnapshot},
		Snapshot:   snap,
		rebuildManifestList: func(_ context.Context, freshParent *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
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
		rebuildManifestList: func(_ context.Context, _ *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
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
		rebuildManifestList: func(_ context.Context, _ *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
			return nil, wantErr
		},
	}

	_, _, err := rebuildSnapshotUpdates(t.Context(), []Update{upd}, freshMeta, MainBranch, iceio.LocalFS{}, 1)
	assert.ErrorIs(t, err, wantErr)
}
