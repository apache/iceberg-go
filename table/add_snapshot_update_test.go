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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// addSnapshotUpdate.Apply / MetadataBuilder.AddSnapshotUpdate
//
// These tests pin the contract that runtime-only fields on *addSnapshotUpdate
// (ownManifests, rebuildManifestList) survive Apply. The previous
// implementation called builder.AddSnapshot(u.Snapshot), which constructed a
// fresh *addSnapshotUpdate, then copied the runtime fields onto
// builder.updates[n-1] via a back-door. That back-door silently broke if
// MetadataBuilder.AddSnapshot ever appended a helper update after the
// snapshot one, dropping the rebuild closure with no error or log — the
// "silent fix can be bypassed" shape reviewer laskoviymishka flagged.
//
// The current implementation routes through MetadataBuilder.AddSnapshotUpdate,
// which appends the supplied update verbatim. The tests below assert IDENTITY
// (assert.Same), not just field equality, so a future regression that
// reintroduces NewAddSnapshotUpdate(snapshot) inside AddSnapshotUpdate would
// fail here.
// ---------------------------------------------------------------------------

// snapshotForApplyTest returns a v2-compatible Snapshot whose SequenceNumber
// is greater than baseMetaJSON's last-sequence-number (1) so AddSnapshot's
// monotonicity check passes.
func snapshotForApplyTest(id int64) *Snapshot {
	return &Snapshot{
		SnapshotID:     id,
		SequenceNumber: 2,
		TimestampMs:    1_700_000_000_000,
		ManifestList:   "s3://bucket/snap.avro",
		Summary:        &Summary{Operation: OpAppend},
	}
}

// findStoredSnapshotUpdate returns the last *addSnapshotUpdate appended to
// builder.updates, or nil if none exists. Tests use this to inspect what was
// stored after AddSnapshot / AddSnapshotUpdate / Apply.
func findStoredSnapshotUpdate(b *MetadataBuilder) *addSnapshotUpdate {
	for i := len(b.updates) - 1; i >= 0; i-- {
		if su, ok := b.updates[i].(*addSnapshotUpdate); ok {
			return su
		}
	}

	return nil
}

// TestAddSnapshotUpdate_PreservesIdentity verifies that AddSnapshotUpdate
// stores the supplied *addSnapshotUpdate verbatim in builder.updates rather
// than constructing a fresh one. Identity preservation is what makes the
// back-door unnecessary; assert.Same is intentional — equality of fields
// would mask a regression that constructs a fresh update with the same
// values but different identity.
func TestAddSnapshotUpdate_PreservesIdentity(t *testing.T) {
	b := buildFromBase(t)
	u := NewAddSnapshotUpdate(snapshotForApplyTest(100))

	require.NoError(t, b.AddSnapshotUpdate(u))

	stored := findStoredSnapshotUpdate(b)
	require.NotNil(t, stored, "builder.updates must contain an *addSnapshotUpdate")
	assert.Same(t, u, stored,
		"AddSnapshotUpdate must store the supplied *addSnapshotUpdate verbatim; "+
			"constructing a fresh object reintroduces the back-door failure mode")
}

// TestAddSnapshotUpdate_PreservesRebuildClosure verifies that the rebuild
// closure attached to the supplied update survives Apply. Without identity
// preservation, the closure would be silently dropped — exactly the failure
// mode reviewer laskoviymishka flagged on PR #982.
func TestAddSnapshotUpdate_PreservesRebuildClosure(t *testing.T) {
	b := buildFromBase(t)

	called := false
	u := NewAddSnapshotUpdate(snapshotForApplyTest(101))
	u.rebuildManifestList = func(_ context.Context, _ Metadata, _ *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
		called = true

		return u.Snapshot, nil
	}
	u.ownManifests = []iceberg.ManifestFile{}

	require.NoError(t, b.AddSnapshotUpdate(u))

	stored := findStoredSnapshotUpdate(b)
	require.NotNil(t, stored)
	require.Same(t, u, stored, "stored update must be the supplied instance, not a copy")
	require.NotNil(t, stored.rebuildManifestList,
		"rebuildManifestList closure must survive Apply; dropping it silently disables the OCC rebuild path")
	assert.NotNil(t, stored.ownManifests, "ownManifests slice must survive Apply")

	// Invoke the closure to prove it is the original function value, not
	// a re-zeroed nil-equivalent that happens to compile.
	_, err := stored.rebuildManifestList(t.Context(), nil, nil, nil, 0)
	require.NoError(t, err)
	assert.True(t, called, "stored closure must be the original; a fresh-object regression would lose it")
}

// TestAddSnapshotUpdate_ApplyRoutesThroughAddSnapshotUpdate verifies that the
// Update interface entry point (Apply) preserves identity. This is the path
// exercised by Transaction.Commit -> u.Apply(t.meta), so a regression here
// would break the production OCC retry path.
func TestAddSnapshotUpdate_ApplyRoutesThroughAddSnapshotUpdate(t *testing.T) {
	b := buildFromBase(t)

	u := NewAddSnapshotUpdate(snapshotForApplyTest(102))
	u.rebuildManifestList = func(_ context.Context, _ Metadata, _ *Snapshot, _ iceio.WriteFileIO, _ int) (*Snapshot, error) {
		return u.Snapshot, nil
	}

	require.NoError(t, u.Apply(b))

	stored := findStoredSnapshotUpdate(b)
	require.NotNil(t, stored)
	assert.Same(t, u, stored,
		"Apply must route through AddSnapshotUpdate so identity (and runtime fields) is preserved")
	require.NotNil(t, stored.rebuildManifestList,
		"rebuildManifestList must survive Apply via the Update interface")
}

// TestAddSnapshotUpdate_NilNoOp verifies defensive handling: a nil update or
// a nil-Snapshot update is a no-op rather than a panic. This mirrors the
// behavior of AddSnapshot(nil) which existed before this refactor.
func TestAddSnapshotUpdate_NilNoOp(t *testing.T) {
	b := buildFromBase(t)
	updatesBefore := len(b.updates)

	require.NoError(t, b.AddSnapshotUpdate(nil),
		"nil *addSnapshotUpdate must be a no-op")
	require.NoError(t, b.AddSnapshotUpdate(&addSnapshotUpdate{}),
		"update with nil Snapshot must be a no-op (matches AddSnapshot(nil))")

	assert.Equal(t, updatesBefore, len(b.updates),
		"no-op calls must not append to builder.updates")
}

// TestAddSnapshot_PreservedForNonRebuildCallers verifies that the existing
// AddSnapshot(*Snapshot) entry point — used by ~30 call sites across the
// codebase — continues to work unchanged after the refactor. Any breakage
// here would be a wide regression.
func TestAddSnapshot_PreservedForNonRebuildCallers(t *testing.T) {
	b := buildFromBase(t)
	snap := snapshotForApplyTest(103)

	require.NoError(t, b.AddSnapshot(snap))

	// The fresh path constructs an *addSnapshotUpdate internally, so the
	// stored update must NOT have rebuildManifestList set.
	stored := findStoredSnapshotUpdate(b)
	require.NotNil(t, stored)
	assert.Equal(t, snap.SnapshotID, stored.Snapshot.SnapshotID)
	assert.Nil(t, stored.rebuildManifestList,
		"AddSnapshot path must produce updates with no rebuild closure")
}
