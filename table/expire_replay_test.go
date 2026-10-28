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
	"maps"
	"path"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A millisecond sleep in synctest makes earlier snapshots older than the expiry cutoff.

func newExpireReplayTable(t *testing.T, props iceberg.Properties) (*memIO, *reqValidatingCatalog, *Table) {
	t.Helper()
	spec := iceberg.NewPartitionSpec()
	all := iceberg.Properties{
		PropertyFormatVersion:        "2",
		CommitNumRetriesKey:          "3",
		CommitMinRetryWaitMsKey:      "1",
		CommitMaxRetryWaitMsKey:      "2",
		CommitTotalRetryTimeoutMsKey: "60000",
	}
	maps.Copy(all, props)
	meta, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder, retryCleanupLocation, all)
	require.NoError(t, err)
	fs := newMemIO(1<<20, nil)
	cat := &reqValidatingCatalog{metadata: meta}

	return fs, cat, newOCCTable(t, meta, fs, cat)
}

func stageFiles(t *testing.T, tx *Transaction, names ...string) {
	t.Helper()
	files := make([]iceberg.DataFile, 0, len(names))
	for _, name := range names {
		files = append(files, retryCleanupFile(t, name))
	}
	require.NoError(t, tx.AddDataFiles(t.Context(), files, nil))
}

func appendOnBranch(t *testing.T, tbl *Table, branch string, names ...string) (*Table, int64) {
	t.Helper()
	tx, err := tbl.NewTransactionOnBranch(branch)
	require.NoError(t, err)
	stageFiles(t, tx, names...)
	out, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return out, out.Metadata().SnapshotByName(branch).SnapshotID
}

func peerAppend(t *testing.T, fs *memIO, cat *reqValidatingCatalog, branch string, names ...string) int64 {
	t.Helper()
	_, id := appendOnBranch(t, newOCCTable(t, cat.metadata, fs, cat), branch, names...)

	return id
}

// peerUpdate mutates cat.metadata without counting a commit attempt.
func peerUpdate(t *testing.T, cat *reqValidatingCatalog, updates ...Update) {
	t.Helper()
	meta, err := UpdateTableMetadata(cat.metadata, updates, "")
	require.NoError(t, err)
	cat.metadata = meta
}

func stagedHead(t *testing.T, tx *Transaction, branch string) int64 {
	t.Helper()
	staged, err := tx.StagedTable()
	require.NoError(t, err)

	return staged.Metadata().SnapshotByName(branch).SnapshotID
}

func survivors(meta Metadata) []int64 {
	return slices.Sorted(slices.Values(snapshotIDs(meta.Snapshots())))
}

func sortedIDs(ids ...int64) []int64 {
	return slices.Sorted(slices.Values(ids))
}

func refHead(t *testing.T, meta Metadata, ref string) int64 {
	t.Helper()
	snap := meta.SnapshotByName(ref)
	require.NotNil(t, snap, "ref %s", ref)

	return snap.SnapshotID
}

func parentOf(t *testing.T, meta Metadata, id int64) int64 {
	t.Helper()
	snap := meta.SnapshotByID(id)
	require.NotNil(t, snap)
	require.NotNil(t, snap.ParentSnapshotID)

	return *snap.ParentSnapshotID
}

func branchFiles(t *testing.T, fs *memIO, meta Metadata, branch string) []string {
	t.Helper()
	head := meta.SnapshotByName(branch)
	require.NotNil(t, head)
	manifests, err := head.Manifests(fs)
	require.NoError(t, err)

	var names []string
	for _, m := range manifests {
		for entry, err := range m.Entries(fs, true) {
			require.NoError(t, err)
			names = append(names, strings.TrimSuffix(path.Base(entry.DataFile().FilePath()), ".parquet"))
		}
	}
	slices.Sort(names)

	return names
}

func TestExpireReplayPlansWhereExpiryWasStaged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "b1")
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "b2")
		tbl, b3 := appendOnBranch(t, tbl, MainBranch, "b3")
		time.Sleep(time.Millisecond)

		tx := tbl.NewTransaction()
		stageFiles(t, tx, "a1")
		a1 := stagedHead(t, tx, MainBranch)
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(3)))
		stageFiles(t, tx, "a2")
		a2 := stagedHead(t, tx, MainBranch)

		p := peerAppend(t, fs, cat, MainBranch, "p")

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		meta := committed.Metadata()
		assert.Equal(t, sortedIDs(b3, p, a1, a2), survivors(meta))
		assert.Equal(t, a2, refHead(t, meta, MainBranch))
		assert.Equal(t, a1, parentOf(t, meta, a2))
		assert.Equal(t, p, parentOf(t, meta, a1))
		assert.Equal(t, []string{"a1", "a2", "b1", "b2", "b3", "p"}, branchFiles(t, fs, meta, MainBranch))
	})
}

func TestExpireReplayAfterRollback(t *testing.T) {
	stage := func(t *testing.T) (*memIO, *reqValidatingCatalog, *Transaction, [3]int64, int64) {
		t.Helper()
		fs, cat, tbl := newExpireReplayTable(t, nil)
		var base [3]int64
		for i, name := range []string{"b1", "b2", "b3"} {
			tbl, base[i] = appendOnBranch(t, tbl, MainBranch, name)
		}
		time.Sleep(time.Millisecond)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.RollbackToSnapshot(base[1]))
		stageFiles(t, tx, "a")
		a := stagedHead(t, tx, MainBranch)
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(2)))

		return fs, cat, tx, base, a
	}

	t.Run("peer commits to another branch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs, cat, tx, base, a := stage(t)
			// The peer's sequence number forces a rebuild of a.
			p := peerAppend(t, fs, cat, "other", "p")

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)

			meta := committed.Metadata()
			assert.Equal(t, sortedIDs(base[1], base[2], p, a), survivors(meta))
			assert.Equal(t, a, refHead(t, meta, MainBranch))
			assert.Equal(t, base[1], parentOf(t, meta, a))
			assert.Equal(t, []string{"a", "b1", "b2"}, branchFiles(t, fs, meta, MainBranch))
			assert.Equal(t, p, refHead(t, meta, "other"))
		})
	})

	t.Run("peer moves the rolled back branch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs, cat, tx, _, _ := stage(t)
			p := peerAppend(t, fs, cat, MainBranch, "p")
			before := cat.metadata

			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrTransactionUnusable)
			require.ErrorIs(t, err, ErrCommitFailed)
			assert.Contains(t, err.Error(), "requirement no longer holds after refresh")
			assert.Same(t, before, cat.metadata)
			assert.Equal(t, p, refHead(t, cat.metadata, MainBranch))
		})
	})
}

func TestExpireReplayPlansEachExpiration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		for _, name := range []string{"b1", "b2", "b3", "b4"} {
			tbl, _ = appendOnBranch(t, tbl, MainBranch, name)
		}
		time.Sleep(time.Millisecond)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(3)))
		stageFiles(t, tx, "a")
		a := stagedHead(t, tx, MainBranch)
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(2)))

		p := peerAppend(t, fs, cat, MainBranch, "p")

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		meta := committed.Metadata()
		assert.Equal(t, sortedIDs(p, a), survivors(meta))
		assert.Equal(t, p, parentOf(t, meta, a))
		assert.Equal(t, []string{"a", "b1", "b2", "b3", "b4", "p"}, branchFiles(t, fs, meta, MainBranch))
	})
}

func TestExpireReplayOnNonMainBranch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "b1")
		tbl, b2 := appendOnBranch(t, tbl, MainBranch, "b2")
		tbl, d1 := appendOnBranch(t, tbl, "dev", "d1")
		time.Sleep(time.Millisecond)

		tx, err := tbl.NewTransactionOnBranch("dev")
		require.NoError(t, err)
		stageFiles(t, tx, "x")
		x := stagedHead(t, tx, "dev")
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))

		p := peerAppend(t, fs, cat, "dev", "p")
		peerUpdate(t, cat, NewSetSnapshotRefUpdate("hold", d1, TagRef, 0, 0, 0))

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		meta := committed.Metadata()
		assert.Equal(t, sortedIDs(b2, d1, p, x), survivors(meta))
		assert.Equal(t, x, refHead(t, meta, "dev"))
		assert.Equal(t, p, parentOf(t, meta, x))
		assert.Equal(t, d1, refHead(t, meta, "hold"))
		assert.Equal(t, []string{"b1", "b2", "d1", "p", "x"}, branchFiles(t, fs, meta, "dev"))
		assert.Equal(t, b2, refHead(t, meta, MainBranch))

		assert.Contains(t, cat.lastReqs, AssertRefSnapshotID("hold", &d1))
		assert.Contains(t, cat.lastReqs, AssertRefSnapshotID(MainBranch, &b2))
		assert.Contains(t, cat.lastReqs, assertBranchRefSnapshotID("dev", &p))
	})
}

func TestExpireReplayKeepsOriginalCutoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "b1")
		time.Sleep(2 * time.Hour)
		tbl, b2 := appendOnBranch(t, tbl, MainBranch, "b2")
		tbl, b3 := appendOnBranch(t, tbl, MainBranch, "b3")

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(time.Hour), WithRetainLast(1)))

		time.Sleep(2 * time.Hour)
		p := peerAppend(t, fs, cat, MainBranch, "p")

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		// From the retry's clock, b2 and b3 would be two hours old.
		assert.Equal(t, sortedIDs(b2, b3, p), survivors(committed.Metadata()))
	})
}

func TestExpireReplayFailsWhenCommitBranchRemovalChanges(t *testing.T) {
	props := iceberg.Properties{MaxRefAgeMsKey: "3600000"}

	t.Run("staged plan removed the branch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs, cat, tbl := newExpireReplayTable(t, props)
			tbl, _ = appendOnBranch(t, tbl, MainBranch, "b1")
			tbl, _ = appendOnBranch(t, tbl, "dev", "d1")
			time.Sleep(2 * time.Hour)

			tx, err := tbl.NewTransactionOnBranch("dev")
			require.NoError(t, err)
			require.NoError(t, tx.ExpireSnapshots())

			p := peerAppend(t, fs, cat, "dev", "p")
			before := cat.metadata

			_, err = tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrTransactionUnusable)
			require.ErrorIs(t, err, ErrCommitFailed)
			assert.EqualError(t, err, `transaction cannot be committed again: commit failed, refresh and try again: `+
				`replanned snapshot expiration keeps commit branch "dev", which the staged plan removed`)
			assert.Same(t, before, cat.metadata)
			assert.Equal(t, p, refHead(t, cat.metadata, "dev"))
		})
	})

	t.Run("staged plan kept the branch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, cat, tbl := newExpireReplayTable(t, props)
			tbl, _ = appendOnBranch(t, tbl, MainBranch, "b1")
			tbl, d1 := appendOnBranch(t, tbl, "dev", "d1")
			time.Sleep(2 * time.Hour)
			tbl, _ = appendOnBranch(t, tbl, "dev", "d2")

			tx, err := tbl.NewTransactionOnBranch("dev")
			require.NoError(t, err)
			require.NoError(t, tx.ExpireSnapshots())
			require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))

			peerUpdate(t, cat, NewSetSnapshotRefUpdate("dev", d1, BranchRef, 0, 0, 0))
			before := cat.metadata

			_, err = tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrTransactionUnusable)
			require.ErrorIs(t, err, ErrCommitFailed)
			assert.EqualError(t, err, `transaction cannot be committed again: commit failed, refresh and try again: `+
				`replanned snapshot expiration removes commit branch "dev", which the staged plan kept`)
			assert.Same(t, before, cat.metadata)
		})
	})
}

func TestExpireReplayPlansNoOpExpiryAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, s1 := appendOnBranch(t, tbl, MainBranch, "s1")
		appendOnBranch(t, tbl, MainBranch, "s2")
		peerUpdate(t, cat, NewSetSnapshotRefUpdate("keep", s1, BranchRef, 0, 0, 0))
		tbl = newOCCTable(t, cat.metadata, fs, cat)
		time.Sleep(time.Millisecond)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))
		staged, err := tx.StagedTable()
		require.NoError(t, err)
		require.Len(t, staged.Metadata().Snapshots(), 2, "keep protects s1, so the expiry stages nothing")
		stageFiles(t, tx, "a")
		a := stagedHead(t, tx, MainBranch)

		peerUpdate(t, cat, NewRemoveSnapshotRefUpdate("keep"))
		p := peerAppend(t, fs, cat, MainBranch, "p")

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		meta := committed.Metadata()
		assert.Equal(t, sortedIDs(p, a), survivors(meta))
		assert.Equal(t, []string{"a", "p", "s1", "s2"}, branchFiles(t, fs, meta, MainBranch))
	})
}

type recordingCatalog struct {
	*reqValidatingCatalog
	updates [][]Update
	loads   int

	before func(attempt int)
}

func (c *recordingCatalog) CommitTable(ctx context.Context, ident Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	if c.before != nil {
		c.before(len(c.updates))
	}
	c.updates = append(c.updates, updates)

	return c.reqValidatingCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *recordingCatalog) LoadTable(ctx context.Context, ident Identifier) (*Table, error) {
	c.loads++

	return c.reqValidatingCatalog.LoadTable(ctx, ident)
}

func removedSnapshots(updates []Update) []int64 {
	var ids []int64
	for _, u := range updates {
		if r, ok := u.(*removeSnapshotsUpdate); ok {
			ids = append(ids, r.SnapshotIDs...)
		}
	}

	return ids
}

func TestExpireReplayKeepsExpiryThatReplannedToNothing(t *testing.T) {
	stage := func(t *testing.T, numRetries string) (*memIO, *recordingCatalog, *Transaction, int64, int64) {
		t.Helper()
		fs, inner, tbl := newExpireReplayTable(t, iceberg.Properties{CommitNumRetriesKey: numRetries})
		tbl, s1 := appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, s2 := appendOnBranch(t, tbl, MainBranch, "s2")
		time.Sleep(time.Millisecond)

		cat := &recordingCatalog{reqValidatingCatalog: inner}
		tx := newOCCTable(t, tbl.Metadata(), fs, cat).NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))
		require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))
		inner.failFirst = inner.attempts.Load() + 2

		return fs, cat, tx, s1, s2
	}

	t.Run("retry", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			_, cat, tx, s1, s2 := stage(t, "2")
			cat.before = func(attempt int) {
				switch attempt {
				case 0:
					peerUpdate(t, cat.reqValidatingCatalog, NewSetSnapshotRefUpdate("hold", s1, TagRef, 0, 0, 0))
				case 1:
					peerUpdate(t, cat.reqValidatingCatalog, NewRemoveSnapshotRefUpdate("hold"))
				}
			}

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)

			require.Len(t, cat.updates, 3)
			assert.Equal(t, []int64{s1}, removedSnapshots(cat.updates[0]))
			assert.Empty(t, removedSnapshots(cat.updates[1]))
			assert.Equal(t, []int64{s1}, removedSnapshots(cat.updates[2]))
			assert.Equal(t, []int64{s2}, survivors(committed.Metadata()))
			assert.Equal(t, "v", committed.Properties()["k"])
		})
	})

	t.Run("resume", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs, cat, tx, s1, s2 := stage(t, "0")

			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrCommitFailed)

			peerUpdate(t, cat.reqValidatingCatalog, NewSetSnapshotRefUpdate("hold", s1, TagRef, 0, 0, 0))
			_, err = tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrCommitFailed)
			require.NotErrorIs(t, err, ErrTransactionUnusable)

			peerUpdate(t, cat.reqValidatingCatalog, NewRemoveSnapshotRefUpdate("hold"))
			p := peerAppend(t, fs, cat.reqValidatingCatalog, MainBranch, "p")
			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)

			require.Len(t, cat.updates, 3)
			assert.Equal(t, []int64{s1}, removedSnapshots(cat.updates[0]))
			assert.Empty(t, removedSnapshots(cat.updates[1]))
			assert.ElementsMatch(t, []int64{s1, s2}, removedSnapshots(cat.updates[2]))
			assert.Equal(t, []int64{p}, survivors(committed.Metadata()))
			assert.Equal(t, "v", committed.Properties()["k"])
		})
	})
}

func TestExpireReplayToNothingCompletesWithoutCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, s1 := appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s2")
		time.Sleep(time.Millisecond)
		list := tbl.Metadata().SnapshotByID(s1).ManifestList

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))

		// Inject a catalog conflict because no ref moved.
		peerUpdate(t, cat, NewRemoveSnapshotsUpdate([]int64{s1}, false))
		attempts := cat.attempts.Load()
		cat.failFirst = attempts + 1
		peerMeta := cat.metadata

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		assert.Equal(t, attempts+1, cat.attempts.Load(), "the replay must not reach the catalog")
		assert.Same(t, peerMeta, cat.metadata)
		assert.Same(t, peerMeta, committed.Metadata())
		assert.Contains(t, fs.files, list, "the peer's expiry owns the cleanup")

		_, err = tx.Commit(t.Context())
		assert.EqualError(t, err, "transaction has already been committed")
	})
}

func TestExpirationAssertionsCoverPlansWithUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s0")
		tbl, s1 := appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, s2 := appendOnBranch(t, tbl, MainBranch, "s2")
		peerUpdate(t, cat,
			NewSetSnapshotRefUpdate("t", s2, TagRef, 0, 0, 0),
			NewSetSnapshotRefUpdate("b", s1, BranchRef, 0, 0, 0))
		base := cat.metadata
		time.Sleep(time.Millisecond)

		noOp := New(tbl.Identifier(), base, "", nil, cat).NewTransaction()
		require.NoError(t, noOp.ExpireSnapshots(WithOlderThan(time.Hour)))
		require.NoError(t, noOp.SetProperties(iceberg.Properties{"k": "v"}))
		commit, err := noOp.TableCommit()
		require.NoError(t, err)
		assert.Equal(t, []Requirement{
			assertBranchRefSnapshotID(MainBranch, &s2),
			AssertTableUUID(base.TableUUID()),
		}, commit.Requirements)

		tx := New(tbl.Identifier(), base, "", nil, cat).NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(time.Hour)))
		require.NoError(t, tx.AssertRefSnapshotID("b"))
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))
		commit, err = tx.TableCommit()
		require.NoError(t, err)
		assert.Equal(t, []Requirement{
			AssertRefSnapshotID("b", &s1),
			assertBranchRefSnapshotID(MainBranch, &s2),
			AssertTableUUID(base.TableUUID()),
			AssertRefSnapshotID("t", &s2),
		}, commit.Requirements)
	})
}

func TestExpireReplayAssertsWalkedRefFromAttemptBase(t *testing.T) {
	// The staged plan keeps s3 for tag t. A peer then moves t to s2, which
	// that plan expires.
	stage := func(t *testing.T) (*recordingCatalog, *Transaction, int64, int64, int64) {
		t.Helper()
		fs, inner, tbl := newExpireReplayTable(t, nil)
		tbl, s1 := appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, s2 := appendOnBranch(t, tbl, MainBranch, "s2")
		tbl, s3 := appendOnBranch(t, tbl, MainBranch, "s3")
		_, s4 := appendOnBranch(t, tbl, MainBranch, "s4")
		peerUpdate(t, inner, NewSetSnapshotRefUpdate("t", s3, TagRef, 0, 0, 0))
		time.Sleep(time.Millisecond)

		cat := &recordingCatalog{reqValidatingCatalog: inner}
		tx := newOCCTable(t, inner.metadata, fs, cat).NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))
		staged, err := tx.StagedTable()
		require.NoError(t, err)
		require.Equal(t, sortedIDs(s3, s4), survivors(staged.Metadata()))

		peerUpdate(t, inner, NewSetSnapshotRefUpdate("t", s2, TagRef, 0, 0, 0))

		return cat, tx, s1, s2, s4
	}

	t.Run("moved before the first attempt", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cat, tx, _, s2, s4 := stage(t)

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)

			meta := committed.Metadata()
			assert.Equal(t, s2, refHead(t, meta, "t"))
			assert.Equal(t, sortedIDs(s2, s4), survivors(meta))
			assert.Len(t, cat.updates, 2)
			assert.Contains(t, cat.lastReqs, AssertRefSnapshotID("t", &s2))
		})
	})

	t.Run("moved again after the first refresh", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cat, tx, s1, _, s4 := stage(t)
			cat.before = func(attempt int) {
				if attempt == 1 {
					peerUpdate(t, cat.reqValidatingCatalog, NewSetSnapshotRefUpdate("t", s1, TagRef, 0, 0, 0))
				}
			}

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)

			meta := committed.Metadata()
			assert.Equal(t, s1, refHead(t, meta, "t"))
			assert.Equal(t, sortedIDs(s1, s4), survivors(meta))
			assert.Len(t, cat.updates, 3)
			assert.Contains(t, cat.lastReqs, AssertRefSnapshotID("t", &s1))
		})
	})
}

func TestExpireReplayFailsWhenPeerDisablesGC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s2")
		time.Sleep(time.Millisecond)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))
		// Inject a conflict because no requirement checks this property.
		peerUpdate(t, cat, NewSetPropertiesUpdate(iceberg.Properties{GCEnabledKey: "false"}))
		cat.failFirst = cat.attempts.Load() + 1
		before := cat.metadata

		_, err := tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrTransactionUnusable)
		require.NotErrorIs(t, err, ErrCommitFailed)
		assert.ErrorContains(t, err, "GC is disabled")
		assert.Same(t, before, cat.metadata)
	})
}

// The expiry enables replay of the staged rollback on retry.
func TestExpireReplayFailsWhenStagedUpdateDoesNotApply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, inner, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, s2 := appendOnBranch(t, tbl, MainBranch, "s2")
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s3")

		cat := &recordingCatalog{reqValidatingCatalog: inner}
		tx := newOCCTable(t, tbl.Metadata(), fs, cat).NewTransaction()
		require.NoError(t, tx.RollbackToSnapshot(s2))
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(time.Hour)))

		// Keep main at s3 so the rollback assertion still holds after refresh.
		peerUpdate(t, inner, NewRemoveSnapshotsUpdate([]int64{s2}, false))
		inner.failFirst = inner.attempts.Load() + 1
		before := inner.metadata

		_, err := tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrTransactionUnusable)
		require.NotErrorIs(t, err, ErrCommitFailed)
		require.ErrorIs(t, err, ErrSnapshotNotFound)
		assert.ErrorContains(t, err, "staged update does not apply after refresh: apply set-snapshot-ref")
		assert.Len(t, cat.updates, 1)
		assert.Equal(t, 1, cat.loads)
		assert.Same(t, before, inner.metadata)

		_, err = tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrTransactionUnusable)
		assert.Len(t, cat.updates, 1, "an unusable transaction must not commit")
		assert.Equal(t, 1, cat.loads, "an unusable transaction must not refresh")
	})
}

func TestExpireReplayKeepsExplicitPin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s2")
		time.Sleep(time.Millisecond)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))
		peerAppend(t, fs, cat, MainBranch, "p")
		before := cat.metadata

		_, err := tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrTransactionUnusable)
		assert.Contains(t, err.Error(), "requirement no longer holds after refresh")
		assert.Same(t, before, cat.metadata)
	})
}

func manifestLists(fs *memIO) []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var lists []string
	for name := range fs.files {
		if strings.HasPrefix(path.Base(name), "snap-") {
			lists = append(lists, name)
		}
	}
	slices.Sort(lists)

	return lists
}

func manifestListsOf(meta Metadata, ids ...int64) []string {
	lists := make([]string, len(ids))
	for i, id := range ids {
		lists[i] = meta.SnapshotByID(id).ManifestList
	}
	slices.Sort(lists)

	return lists
}

func TestExpireReplayCleansUpReplannedRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s1")
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "s2")
		tbl, s3 := appendOnBranch(t, tbl, MainBranch, "s3")
		time.Sleep(time.Millisecond)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(2)))
		p := peerAppend(t, fs, cat, MainBranch, "p")

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		meta := committed.Metadata()
		assert.Equal(t, sortedIDs(s3, p), survivors(meta))
		assert.Equal(t, manifestListsOf(meta, s3, p), manifestLists(fs))
	})
}

func TestExpireReplayCleansUpRebuiltSnapshotItExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "b1")
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "b2")

		tx := tbl.NewTransaction()
		stageFiles(t, tx, "a1")
		a1 := stagedHead(t, tx, MainBranch)
		stageFiles(t, tx, "a2")
		a2 := stagedHead(t, tx, MainBranch)
		time.Sleep(time.Millisecond)
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(0), WithRetainLast(1)))
		staged, err := tx.StagedTable()
		require.NoError(t, err)
		require.Nil(t, staged.Metadata().SnapshotByID(a1), "the staged plan expires a1")

		peerAppend(t, fs, cat, MainBranch, "p")

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)

		meta := committed.Metadata()
		assert.Equal(t, []int64{a2}, survivors(meta))
		assert.Equal(t, a1, parentOf(t, meta, a2))
		assert.Equal(t, []string{"a1", "a2", "b1", "b2", "p"}, branchFiles(t, fs, meta, MainBranch))
		assert.Equal(t, manifestListsOf(meta, a2), manifestLists(fs))
	})
}

// The retry rebuilds a on the peer's head, but a keeps its staging timestamp,
// which is more than a minute before the peer's snapshot.
func TestExpireReplayRemovesRebuiltListWhenApplyFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "b1")

		tx := tbl.NewTransaction()
		stageFiles(t, tx, "a")
		require.NoError(t, tx.ExpireSnapshots(WithOlderThan(time.Hour)))

		time.Sleep(2 * time.Minute)
		peerAppend(t, fs, cat, MainBranch, "p")
		files := slices.Sorted(maps.Keys(fs.files))

		_, err := tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrTransactionUnusable)
		require.NotErrorIs(t, err, ErrCommitFailed)
		assert.ErrorContains(t, err, "apply add-snapshot: invalid snapshot timestamp")
		assert.Equal(t, files, slices.Sorted(maps.Keys(fs.files)), "the staged files are kept and rebuilt files are removed")
	})
}
