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
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const retryCleanupLocation = "mem://default/table-location"

// peerHeadsCatalog serves heads in order, one per LoadTable, and stays on the
// last. CommitTable returns errs[n] for its nth call and otherwise applies the
// updates to the head it last served.
type peerHeadsCatalog struct {
	heads    []Metadata
	loads    int
	current  Metadata
	errs     []error
	attempts int
}

func (c *peerHeadsCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	c.current = c.heads[min(c.loads, len(c.heads)-1)]
	c.loads++

	return New(ident, c.current, "", nil, c), nil
}

func (c *peerHeadsCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	n := c.attempts
	c.attempts++
	if n < len(c.errs) && c.errs[n] != nil {
		return nil, "", c.errs[n]
	}
	meta, err := UpdateTableMetadata(c.current, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.current = meta

	return meta, "", nil
}

// landingCatalog calls landed with the updates of the commit it accepts.
type landingCatalog struct {
	*peerHeadsCatalog
	landed func([]Update)
}

func (c *landingCatalog) CommitTable(ctx context.Context, ident Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	meta, loc, err := c.peerHeadsCatalog.CommitTable(ctx, ident, reqs, updates)
	if err == nil {
		c.landed(updates)
	}

	return meta, loc, err
}

// unreadableListIO fails Open for the path in unreadable and records every
// Remove.
type unreadableListIO struct {
	*memIO
	mu         sync.Mutex
	unreadable string
	removed    []string
}

func (u *unreadableListIO) Open(ctx context.Context, name string) (iceio.File, error) {
	u.mu.Lock()
	unreadable := u.unreadable
	u.mu.Unlock()
	if name == unreadable {
		return nil, errors.New("simulated read failure")
	}

	return u.memIO.Open(ctx, name)
}

func (u *unreadableListIO) Remove(ctx context.Context, name string) error {
	u.mu.Lock()
	u.removed = append(u.removed, name)
	u.mu.Unlock()

	return u.memIO.Remove(ctx, name)
}

// retryCleanupFixture is a table on memIO whose base head h0 holds f1 and f2
// in one manifest. Peers then appended f3 (h1) and f5 (h2).
type retryCleanupFixture struct {
	wfs         *memIO
	h0, h1, h2  Metadata
	preExisting []string
}

func newRetryCleanupFixture(t *testing.T, numRetries string, props iceberg.Properties) retryCleanupFixture {
	t.Helper()
	spec := iceberg.NewPartitionSpec()
	all := iceberg.Properties{
		PropertyFormatVersion:        "2",
		CommitNumRetriesKey:          numRetries,
		CommitMinRetryWaitMsKey:      "1",
		CommitMaxRetryWaitMsKey:      "2",
		CommitTotalRetryTimeoutMsKey: "60000",
	}
	maps.Copy(all, props)
	meta, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder, retryCleanupLocation, all)
	require.NoError(t, err)

	f := retryCleanupFixture{wfs: newMemIO(1<<20, nil)}
	tbl := newOCCTable(t, meta, f.wfs, &sequentialCatalog{metadata: meta})
	tbl = appendRetryCleanupFiles(t, tbl, "f1", "f2")
	f.h0 = tbl.Metadata()
	tbl = appendRetryCleanupFiles(t, tbl, "f3")
	f.h1 = tbl.Metadata()
	f.h2 = appendRetryCleanupFiles(t, tbl, "f5").Metadata()
	f.preExisting = slices.Sorted(maps.Keys(f.wfs.files))

	return f
}

func retryCleanupFile(t *testing.T, name string) iceberg.DataFile {
	t.Helper()

	return newTestDataFile(t, iceberg.NewPartitionSpec(), retryCleanupLocation+"/data/"+name+".parquet", nil)
}

func appendRetryCleanupFiles(t *testing.T, tbl *Table, names ...string) *Table {
	t.Helper()
	files := make([]iceberg.DataFile, 0, len(names))
	for _, name := range names {
		files = append(files, retryCleanupFile(t, name))
	}
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), files, nil))
	out, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return out
}

func (f retryCleanupFixture) newTable(t *testing.T, heads []Metadata, errs ...error) (*Table, *peerHeadsCatalog) {
	t.Helper()
	cat := &peerHeadsCatalog{current: f.h0, heads: heads, errs: errs}

	return newOCCTable(t, f.h0, f.wfs, cat), cat
}

func (f retryCleanupFixture) newFiles() []string {
	files := make([]string, 0, len(f.wfs.files))
	for path := range f.wfs.files {
		if !slices.Contains(f.preExisting, path) {
			files = append(files, path)
		}
	}
	slices.Sort(files)

	return files
}

// assertOnlyCommittedFilesAdded checks that the files written since the
// fixture was built are exactly the manifest lists and manifests of the
// snapshots committed on top of the peers, and that each of them is readable.
func (f retryCleanupFixture) assertOnlyCommittedFilesAdded(t *testing.T, committed *Table) {
	t.Helper()
	prior := make(map[int64]struct{})
	for _, s := range f.h2.Snapshots() {
		prior[s.SnapshotID] = struct{}{}
	}

	want := make(map[string]struct{})
	for _, path := range f.preExisting {
		want[path] = struct{}{}
	}
	var added int
	for _, s := range committed.Metadata().Snapshots() {
		if _, ok := prior[s.SnapshotID]; ok {
			continue
		}
		added++
		want[s.ManifestList] = struct{}{}
		manifests, err := s.Manifests(f.wfs)
		require.NoError(t, err, "committed manifest list %s must be readable", s.ManifestList)
		for _, m := range manifests {
			want[m.FilePath()] = struct{}{}
			for _, err := range m.Entries(f.wfs, false) {
				require.NoError(t, err, "committed manifest %s must be readable", m.FilePath())
			}
		}
	}
	require.NotZero(t, added, "the commit must add a snapshot")
	assert.Equal(t, slices.Sorted(maps.Keys(want)), slices.Sorted(maps.Keys(f.wfs.files)),
		"storage must hold the pre-existing files and the committed snapshots' files, nothing else")
}

func headManifests(t *testing.T, tbl *Table, fs *memIO) (*Snapshot, []iceberg.ManifestFile) {
	t.Helper()
	head := tbl.CurrentSnapshot()
	require.NotNil(t, head)
	manifests, err := head.Manifests(fs)
	require.NoError(t, err)

	return head, manifests
}

var retryCleanupProducers = []struct {
	name      string
	props     iceberg.Properties
	stage     func(*testing.T, *Transaction)
	checkHead func(*testing.T, *Snapshot, []iceberg.ManifestFile)
}{
	{
		name:  "merge append",
		props: iceberg.Properties{ManifestMergeEnabledKey: "true", ManifestMinMergeCountKey: "2"},
		stage: func(t *testing.T, tx *Transaction) {
			require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{retryCleanupFile(t, "f4")}, nil))
		},
		checkHead: func(t *testing.T, head *Snapshot, manifests []iceberg.ManifestFile) {
			require.Len(t, manifests, 1, "every manifest must merge into one")
			assert.Equal(t, head.SnapshotID, manifests[0].SnapshotID())
			assert.EqualValues(t, 1, manifests[0].AddedDataFiles())
		},
	},
	{
		name: "overwrite with deletes",
		props: iceberg.Properties{
			ManifestMergeEnabledKey:      "false",
			WriteUpdateIsolationLevelKey: string(IsolationSnapshot),
		},
		stage: func(t *testing.T, tx *Transaction) {
			require.NoError(t, tx.ReplaceDataFiles(t.Context(),
				[]iceberg.DataFile{retryCleanupFile(t, "f1")}, []iceberg.DataFile{retryCleanupFile(t, "f4")}, nil))
		},
		checkHead: func(t *testing.T, head *Snapshot, manifests []iceberg.ManifestFile) {
			var tombstones, rewritten int
			for _, m := range manifests {
				if m.SnapshotID() != head.SnapshotID {
					continue
				}
				if m.DeletedDataFiles() > 0 {
					tombstones++
				}
				if m.ExistingDataFiles() > 0 {
					rewritten++
				}
			}
			assert.Equal(t, 1, tombstones, "the removal of f1 must be written as a tombstone manifest")
			assert.Equal(t, 1, rewritten, "the manifest holding f1 must be rewritten to keep f2")
		},
	},
}

func TestCommitLeavesOnlyCommittedFiles(t *testing.T) {
	runs := []struct {
		name string
		errs []error
	}{
		{name: "first attempt"},
		{name: "after two conflicts", errs: []error{ErrCommitFailed, ErrCommitFailed}},
	}
	for _, p := range retryCleanupProducers {
		for _, run := range runs {
			t.Run(p.name+"/"+run.name, func(t *testing.T) {
				f := newRetryCleanupFixture(t, "2", p.props)
				tbl, cat := f.newTable(t, []Metadata{f.h1, f.h2}, run.errs...)
				tx := tbl.NewTransaction()
				p.stage(t, tx)

				committed, err := tx.Commit(t.Context())
				require.NoError(t, err)
				require.Equal(t, len(run.errs)+1, cat.attempts)
				head, manifests := headManifests(t, committed, f.wfs)
				p.checkHead(t, head, manifests)
				f.assertOnlyCommittedFilesAdded(t, committed)
			})
		}
	}
}

func TestFailedCommitKeepsOnlyStagedFilesUntilResumeLands(t *testing.T) {
	for _, p := range retryCleanupProducers {
		t.Run(p.name, func(t *testing.T) {
			f := newRetryCleanupFixture(t, "2", p.props)
			tbl, cat := f.newTable(t, []Metadata{f.h1, f.h2}, ErrCommitFailed, ErrCommitFailed, ErrCommitFailed)
			tx := tbl.NewTransaction()
			p.stage(t, tx)
			staged := f.newFiles()

			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrCommitFailed)
			require.NotErrorIs(t, err, ErrTransactionUnusable)
			require.Equal(t, 3, cat.attempts)
			assert.Equal(t, staged, f.newFiles(), "only the staged files may remain, and all of them")

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)
			head, manifests := headManifests(t, committed, f.wfs)
			p.checkHead(t, head, manifests)
			f.assertOnlyCommittedFilesAdded(t, committed)
		})
	}
}

// Cleanup must not guess which files a landed commit references when it
// cannot read a committed manifest list.
func TestCommitCleanupSkippedWhenCommittedListUnreadable(t *testing.T) {
	for _, p := range retryCleanupProducers {
		t.Run(p.name, func(t *testing.T) {
			f := newRetryCleanupFixture(t, "2", p.props)
			fs := &unreadableListIO{memIO: f.wfs}
			cat := &landingCatalog{
				peerHeadsCatalog: &peerHeadsCatalog{
					current: f.h0,
					heads:   []Metadata{f.h1, f.h2},
					errs:    []error{ErrCommitFailed, ErrCommitFailed},
				},
				landed: func(updates []Update) {
					for _, u := range updates {
						if su, ok := u.(*addSnapshotUpdate); ok {
							fs.mu.Lock()
							fs.unreadable = su.Snapshot.ManifestList
							fs.mu.Unlock()
						}
					}
				},
			}
			tx := newOCCTable(t, f.h0, fs, cat).NewTransaction()
			p.stage(t, tx)
			staged := f.newFiles()

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.Equal(t, 3, cat.attempts)
			require.NotNil(t, committed.CurrentSnapshot())
			require.NotEmpty(t, fs.unreadable)
			assert.Empty(t, fs.removed, "nothing may be deleted without reading what the commit references")
			after := f.newFiles()
			for _, path := range staged {
				assert.Contains(t, after, path, "superseded staged files must be kept too")
			}
		})
	}
}

func TestUnknownCommitStateDeletesNoManifests(t *testing.T) {
	for _, p := range retryCleanupProducers {
		t.Run(p.name, func(t *testing.T) {
			f := newRetryCleanupFixture(t, "2", p.props)
			unknown := errors.New("simulated 5xx: internal server error")
			tbl, _ := f.newTable(t, []Metadata{f.h1}, ErrCommitFailed, unknown)
			tx := tbl.NewTransaction()
			p.stage(t, tx)
			before := f.newFiles()

			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, unknown)

			after := f.newFiles()
			for _, path := range before {
				assert.Contains(t, after, path, "nothing may be deleted when the commit state is unknown")
			}
			var rebuiltManifests int
			for _, path := range after {
				if !slices.Contains(before, path) && !strings.HasPrefix(filepath.Base(path), "snap-") {
					rebuiltManifests++
				}
			}
			assert.NotZero(t, rebuiltManifests, "the rebuild must have written manifests, all of them kept")
		})
	}
}

// A transaction staging a fast append and then a merging append commits both
// snapshots. The merge consumes the first snapshot's added manifest, which only
// the first committed snapshot still references.
func TestChainedSnapshotsKeepEveryCommittedManifest(t *testing.T) {
	f := newRetryCleanupFixture(t, "1", iceberg.Properties{
		ManifestMergeEnabledKey:  "true",
		ManifestMinMergeCountKey: "2",
	})
	tbl, cat := f.newTable(t, []Metadata{f.h1}, ErrCommitFailed)
	tx := tbl.NewTransaction()

	fastAppend := newFastAppendFilesProducer(OpAppend, tx, f.wfs, nil, nil)
	fastAppend.appendDataFile(retryCleanupFile(t, "f4"))
	updates, reqs, err := fastAppend.commit(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.apply(updates, reqs))
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{retryCleanupFile(t, "f6")}, nil))

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, cat.attempts)

	head, manifests := headManifests(t, committed, f.wfs)
	require.Len(t, manifests, 1, "the merging append must merge every manifest")
	require.NotNil(t, head.ParentSnapshotID)
	first := committed.Metadata().SnapshotByID(*head.ParentSnapshotID)
	require.NotNil(t, first)
	firstManifests, err := first.Manifests(f.wfs)
	require.NoError(t, err)
	var firstAdded int
	for _, m := range firstManifests {
		if m.SnapshotID() == first.SnapshotID {
			firstAdded++
		}
	}
	require.Equal(t, 1, firstAdded, "the first snapshot must reference its own added manifest")
	f.assertOnlyCommittedFilesAdded(t, committed)
}
