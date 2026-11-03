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
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func manageSnapshotRefsFixture(t *testing.T) Metadata {
	t.Helper()
	base := newConflictTestMetadataWithChain(t, []int64{100, 200, 300})
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 100, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("qa", 100, BranchRef,
		WithMaxRefAgeMs(1000), WithMaxSnapshotAgeMs(2000), WithMinSnapshotsToKeep(3)))
	require.NoError(t, builder.SetSnapshotRef("audit", 300, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("release", 300, TagRef, WithMaxRefAgeMs(5000)))
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: 900, SequenceNumber: 4,
		TimestampMs: base.LastUpdatedMillis() + 1000,
		Summary:     &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef("diverged", 900, BranchRef))
	result, err := builder.Build()
	require.NoError(t, err)

	return result
}

func manageSnapshotRefsRetryFixture(t *testing.T) Metadata {
	t.Helper()
	base := manageSnapshotRefsFixture(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetProperties(assertRefRetryProps))
	result, err := builder.Build()
	require.NoError(t, err)

	return result
}

func TestFastForwardBranch(t *testing.T) {
	t.Run("preserves retention and leaves source unchanged", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.FastForwardBranch("qa", "audit"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(300), cat.metadata.SnapshotByName("qa").SnapshotID)
		assert.Equal(t, int64(300), cat.metadata.SnapshotByName("audit").SnapshotID)
		assert.Equal(t, int64(100), cat.metadata.CurrentSnapshot().SnapshotID)
		for name, ref := range cat.metadata.Refs() {
			if name == "qa" {
				require.NotNil(t, ref.MaxRefAgeMs)
				require.NotNil(t, ref.MaxSnapshotAgeMs)
				require.NotNil(t, ref.MinSnapshotsToKeep)
				assert.Equal(t, int64(1000), *ref.MaxRefAgeMs)
				assert.Equal(t, int64(2000), *ref.MaxSnapshotAgeMs)
				assert.Equal(t, 3, *ref.MinSnapshotsToKeep)
			}
		}
	})
	t.Run("tag source and main target", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.FastForwardBranch(MainBranch, "release"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(300), cat.metadata.CurrentSnapshot().SnapshotID)
		assert.Equal(t, int64(300), cat.metadata.SnapshotByName("release").SnapshotID)
	})
	t.Run("creates absent target", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.FastForwardBranch("fresh", "audit"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		var found bool
		for name, ref := range cat.metadata.Refs() {
			if name == "fresh" {
				found = true
				assert.Equal(t, int64(300), ref.SnapshotID)
				assert.Equal(t, BranchRef, ref.Type)
				assert.Nil(t, ref.MinSnapshotsToKeep)
			}
		}
		assert.True(t, found)
	})
	t.Run("uses transaction staged source", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceBranch("audit", 200))
		require.NoError(t, tx.FastForwardBranch("qa", "audit"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(200), cat.metadata.SnapshotByName("qa").SnapshotID)
		assert.Equal(t, int64(200), cat.metadata.SnapshotByName("audit").SnapshotID)
	})
	t.Run("same heads are no-op", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.FastForwardBranch("audit", "release"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Zero(t, cat.attempts.Load())
		assert.ErrorContains(t, tx.FastForwardBranch("audit", "release"), "already been committed")
	})
	t.Run("reject invalid refs and divergent heads", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, _ := newAssertRefTestTable(t, base, base)
		for _, tc := range []struct{ target, source string }{
			{"qa", "missing"},
			{"release", "audit"},
			{"audit", "qa"},
			{"qa", "diverged"},
			{"", "audit"},
			{"qa", ""},
		} {
			tx := tbl.NewTransaction()
			require.ErrorIs(t, tx.FastForwardBranch(tc.target, tc.source), iceberg.ErrInvalidArgument)
			assert.Empty(t, tx.meta.updates)
		}
	})
}

func TestFastForwardBranchPinsBothRefs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func(*Transaction) error
		move  func(*MetadataBuilder) error
	}{
		{
			name:  "target moved",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch("qa", "audit") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef("qa", 200, BranchRef) },
		},
		{
			name:  "source moved",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch("qa", "audit") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef("audit", 200, BranchRef) },
		},
		{
			name:  "target became tag with same id",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch("qa", "audit") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef("qa", 100, TagRef) },
		},
		{
			name:  "main target moved",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch(MainBranch, "audit") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef(MainBranch, 200, BranchRef) },
		},
		{
			name:  "main source moved",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch("qa", MainBranch) },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef(MainBranch, 200, BranchRef) },
		},
		{
			name:  "no-op main target moved",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch(MainBranch, "qa") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef(MainBranch, 200, BranchRef) },
		},
		{
			name:  "replace main moved",
			stage: func(tx *Transaction) error { return tx.ReplaceBranch(MainBranch, 900) },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef(MainBranch, 200, BranchRef) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := manageSnapshotRefsRetryFixture(t)
			builder, err := MetadataBuilderFromBase(base, "")
			require.NoError(t, err)
			require.NoError(t, tc.move(builder))
			current, err := builder.Build()
			require.NoError(t, err)

			tbl, cat := newAssertRefTestTable(t, base, current)
			tx := tbl.NewTransaction()
			// Force a catalog commit even when the promotion is a no-op.
			require.NoError(t, tx.SetProperties(iceberg.Properties{"promotion-marker": "staged"}))
			require.NoError(t, tc.stage(tx))
			_, err = tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrCommitFailed)
			assert.Equal(t, int32(1), cat.attempts.Load(),
				"a pinned ref must abort on refresh without a second commit attempt")
			assert.NotContains(t, cat.metadata.Properties(), "promotion-marker")
		})
	}
}

func TestFastForwardBranchRejectsConcurrentTargetCreation(t *testing.T) {
	base := manageSnapshotRefsFixture(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	// Another writer creates the target after our transaction loaded metadata.
	require.NoError(t, builder.SetSnapshotRef("fresh", 200, BranchRef))
	current, err := builder.Build()
	require.NoError(t, err)

	tbl, cat := newAssertRefTestTable(t, base, current)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.FastForwardBranch("fresh", "audit"))
	_, err = tx.Commit(t.Context())
	require.ErrorIs(t, err, ErrCommitFailed)
	assert.Equal(t, int64(200), cat.metadata.SnapshotByName("fresh").SnapshotID,
		"promotion must not overwrite a concurrently created branch")
}

func TestReplaceBranchFromRef(t *testing.T) {
	t.Run("moves branch to source tag preserving retention", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceBranchFromRef("qa", "release"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(300), cat.metadata.SnapshotByName("qa").SnapshotID)
		assert.Equal(t, int64(300), cat.metadata.SnapshotByName("release").SnapshotID)
		for name, ref := range cat.metadata.Refs() {
			if name == "qa" {
				require.NotNil(t, ref.MaxRefAgeMs)
				require.NotNil(t, ref.MaxSnapshotAgeMs)
				require.NotNil(t, ref.MinSnapshotsToKeep)
				assert.Equal(t, int64(1000), *ref.MaxRefAgeMs)
				assert.Equal(t, int64(2000), *ref.MaxSnapshotAgeMs)
				assert.Equal(t, 3, *ref.MinSnapshotsToKeep)
			}
		}
	})

	t.Run("moves a divergent branch", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceBranchFromRef("diverged", "release"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(300), cat.metadata.SnapshotByName("diverged").SnapshotID)
		assert.Equal(t, int64(300), cat.metadata.SnapshotByName("release").SnapshotID)
	})

	t.Run("creates absent target", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceBranchFromRef("published", "release"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(300), cat.metadata.SnapshotByName("published").SnapshotID)
		for name, ref := range cat.metadata.Refs() {
			if name == "published" {
				assert.Equal(t, BranchRef, ref.Type)
				assert.Nil(t, ref.MaxRefAgeMs)
			}
		}
	})

	t.Run("rejects source absent or tag target", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, _ := newAssertRefTestTable(t, base, base)
		for _, tc := range []struct{ target, source string }{
			{"qa", "missing"},
			{"release", "qa"},
			{"qa", ""},
			{"", "release"},
		} {
			tx := tbl.NewTransaction()
			require.ErrorIs(t, tx.ReplaceBranchFromRef(tc.target, tc.source), iceberg.ErrInvalidArgument)
			assert.Empty(t, tx.meta.updates)
		}
	})
}

func TestReplaceBranch(t *testing.T) {
	t.Run("divergent snapshot and retention", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceBranch("qa", 900))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(900), cat.metadata.SnapshotByName("qa").SnapshotID)
		for name, ref := range cat.metadata.Refs() {
			if name == "qa" {
				assert.Equal(t, int64(1000), *ref.MaxRefAgeMs)
				assert.Equal(t, int64(2000), *ref.MaxSnapshotAgeMs)
				assert.Equal(t, 3, *ref.MinSnapshotsToKeep)
			}
		}
	})
	t.Run("reject invalid inputs", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		tbl, _ := newAssertRefTestTable(t, base, base)
		for _, tc := range []struct {
			name string
			id   int64
		}{{"release", 100}, {"qa", 99999}, {"", 200}, {"fresh", 200}} {
			tx := tbl.NewTransaction()
			require.ErrorIs(t, tx.ReplaceBranch(tc.name, tc.id), iceberg.ErrInvalidArgument)
			assert.Empty(t, tx.meta.updates)
		}
	})
	t.Run("concurrent target move", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		peer, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, peer.SetSnapshotRef("qa", 200, BranchRef))
		current, err := peer.Build()
		require.NoError(t, err)
		tbl, cat := newAssertRefTestTable(t, base, current)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceBranch("qa", 900))
		_, err = tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrCommitFailed)
		assert.Equal(t, int64(200), cat.metadata.SnapshotByName("qa").SnapshotID)
	})
	t.Run("concurrent tag conversion", func(t *testing.T) {
		base := manageSnapshotRefsFixture(t)
		peer, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, peer.SetSnapshotRef("qa", 100, TagRef))
		current, err := peer.Build()
		require.NoError(t, err)
		tbl, _ := newAssertRefTestTable(t, base, current)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceBranch("qa", 200))
		_, err = tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrCommitFailed)
	})
}

func TestNoOpBranchRefOperationsFenceMixedTransactions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage func(*Transaction) error
		move  func(*MetadataBuilder) error
	}{
		{
			name:  "fast-forward target changes",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch("audit", "release") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef("audit", 200, BranchRef) },
		},
		{
			name:  "fast-forward source changes",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch("audit", "release") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef("release", 200, TagRef) },
		},
		{
			name:  "replacement target changes",
			stage: func(tx *Transaction) error { return tx.ReplaceBranch("audit", 300) },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef("audit", 200, BranchRef) },
		},
		{
			name:  "no-op fast-forward target main changes",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch(MainBranch, "qa") },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef(MainBranch, 200, BranchRef) },
		},
		{
			name:  "no-op fast-forward source main changes",
			stage: func(tx *Transaction) error { return tx.FastForwardBranch("qa", MainBranch) },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef(MainBranch, 200, BranchRef) },
		},
		{
			name:  "main replacement changes",
			stage: func(tx *Transaction) error { return tx.ReplaceBranch(MainBranch, 300) },
			move:  func(b *MetadataBuilder) error { return b.SetSnapshotRef(MainBranch, 200, BranchRef) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := manageSnapshotRefsRetryFixture(t)
			peer, err := MetadataBuilderFromBase(base, "")
			require.NoError(t, err)
			require.NoError(t, tc.move(peer))
			current, err := peer.Build()
			require.NoError(t, err)

			tbl, cat := newAssertRefTestTable(t, base, current)
			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetProperties(iceberg.Properties{"promotion-marker": "staged"}))
			require.NoError(t, tc.stage(tx))
			_, err = tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrCommitFailed)
			assert.NotContains(t, cat.metadata.Properties(), "promotion-marker")
			assert.Equal(t, int32(1), cat.attempts.Load())
		})
	}
}

// Check the serialized requirements a REST catalog receives, not just the
// client-side requirement implementation.
func TestFastForwardBranchWireRequirements(t *testing.T) {
	base := manageSnapshotRefsFixture(t)
	tbl, _ := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.FastForwardBranch("qa", "audit"))
	commit, err := tx.TableCommit()
	require.NoError(t, err)
	data, err := json.Marshal(commit.Requirements)
	require.NoError(t, err)
	var decoded Requirements
	require.NoError(t, json.Unmarshal(data, &decoded))
	for _, tc := range []struct {
		name       string
		ref        string
		snapshotID int64
	}{
		{"target moves", "qa", 200},
		{"source moves", "audit", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, err := MetadataBuilderFromBase(base, "")
			require.NoError(t, err)
			require.NoError(t, peer.SetSnapshotRef(tc.ref, tc.snapshotID, BranchRef))
			current, err := peer.Build()
			require.NoError(t, err)
			var rejected bool
			for _, requirement := range decoded {
				if requirement.Validate(current) != nil {
					rejected = true
				}
			}
			require.True(t, rejected, "serialized requirements must reject a moved ref")
		})
	}
}
