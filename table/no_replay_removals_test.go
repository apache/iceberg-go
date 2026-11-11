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

package table_test

// Delete-file removals are resolved against the snapshot the writer
// built on — removal identity is snapshot-relative. These tests pin
// that commits carrying such removals fail on the first CAS conflict
// (ErrCommitFailed, exactly one CommitTable attempt) instead of
// entering doCommit's refresh-and-replay, and that removal-free
// commits keep replaying. The exception (#2195) is a deletion vector
// removed together with its data file under a validator that rejects
// concurrent deletes on that file: such commits replay.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newNoReplayV3Table builds a v3 merge-on-read table with retries
// enabled against a requirement-enforcing catalog, so a stale
// AssertRefSnapshotID fails the first CommitTable attempt and would
// arm refresh-and-replay for replayable commits.
func newNoReplayV3Table(t *testing.T) (*table.Table, *concurrentTestCatalog) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:        "3",
			table.WriteDeleteModeKey:           table.WriteModeMergeOnRead,
			table.CommitNumRetriesKey:          "2",
			table.CommitMinRetryWaitMsKey:      "1",
			table.CommitMaxRetryWaitMsKey:      "2",
			table.CommitTotalRetryTimeoutMsKey: "1000",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "no_replay_removals"}, meta, metaLoc, fsF, cat), cat
}

// Why: a v3 merge-on-read delete that supersedes an existing deletion
// vector commits a ref-keyed DV removal. If that commit entered
// refresh-and-replay after a CAS conflict, the peer's replacement DV
// would be inherited from the fresh base while the stale removal
// replays as a no-op — two live DVs on one data file, which the v3
// spec forbids ("at most one deletion vector per data file").
// Condition: a data file carries DV1; a peer supersedes DV1 with its
// own merged DV; a stale writer (still seeing DV1 live) then commits
// its own supersession with retries enabled.
// Assertion: the stale commit fails wrapping ErrCommitFailed after
// exactly one CommitTable attempt (no replay), the re-commit finds the
// branch moved and is refused before the catalog, and the table carries
// exactly one live DV — the peer's — so only the peer's deletes apply.
func TestMoRDeleteSupersedingDVFailsInsteadOfReplaying(t *testing.T) {
	ctx := context.Background()
	tbl, cat := newNoReplayV3Table(t)
	tbl = appendTenRows(t, tbl)

	// First delete writes DV1 against the single data file. No prior DV
	// exists, so this commit carries no removals and is replayable.
	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil)
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataFilePath := tasks[0].File.FilePath()
	require.Len(t, deleteEntriesReferencing(t, tbl, map[string]struct{}{dataFilePath: {}}), 1,
		"setup: exactly one live DV must reference the data file")

	// Stage the stale writer's supersession first, from the current
	// view: it folds DV1 into a merged DV and marks DV1 for removal.
	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil))

	// Peer supersedes DV1 from the same view and wins the race.
	staleHead := tbl.CurrentSnapshot().SnapshotID
	peer, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)

	attemptsBefore := cat.attempts.Load()
	_, err = staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
		"a commit carrying DV removals must fail on the first CAS conflict, not refresh-and-replay")

	assert.NotErrorIs(t, err, table.ErrTransactionUnusable)
	assert.Contains(t, err.Error(), "reload the table and rebuild the removals")

	attemptsBefore = cat.attempts.Load()
	_, err = staleTxn.Commit(ctx)
	require.ErrorIs(t, err, table.ErrTransactionUnusable)
	assert.EqualError(t, err, fmt.Sprintf("transaction cannot be committed again: requirement no longer holds after refresh "+
		"(commit carries snapshot-relative delete-file removals and cannot be replayed; reload the table and rebuild the removals): "+
		"requirement failed: branch \"main\" has changed: expected id %d, found %d", staleHead, peer.CurrentSnapshot().SnapshotID))
	assert.Equal(t, attemptsBefore, cat.attempts.Load(), "the re-commit must stop before the catalog")

	// The table is uncorrupted: exactly one live DV (the peer's merged
	// one) references the data file, and only the peer's deletes apply.
	committed, err := cat.LoadTable(ctx, table.Identifier{"db", "no_replay_removals"})
	require.NoError(t, err)
	assert.Len(t, deleteEntriesReferencing(t, committed, map[string]struct{}{dataFilePath: {}}), 1,
		"the data file must carry exactly one live DV: the peer's")
	assert.Equal(t, []int64{1, 3, 4, 5, 6, 8, 9, 10}, idsInTable(t, committed),
		"ids 2 and 7 are deleted by the peer's merged DV; the stale delete of id 3 must not have landed")
}

// Why: ReplaceFiles resolves deleteFilesToRemove against the snapshot
// the writer built on. If the commit entered refresh-and-replay, a
// peer's concurrently added delete file would be inherited against a
// data file the replay simultaneously removes: the peer's delete
// silently no-ops and its deleted row resurrects inside the compacted
// replacement. The pre-flight serializable validator does not catch
// this — it only inspects the peer's added data files.
// Condition: a compaction-style ReplaceFiles (replace the data file,
// remove its fully-applied position delete) races a peer that commits
// a new position delete against the same data file, with retries
// enabled.
// Assertion: exactly one CommitTable attempt, error wraps
// ErrCommitFailed, and the table still reflects only the peer's
// commit (both deletes live, no resurrected row).
func TestReplaceFilesWithRemovalsFailsInsteadOfReplaying(t *testing.T) {
	tbl, cat := newConcurrentRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc,
		`[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}, {"id": 3, "data": "gamma"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Add a position delete for row 1 (id 2) so there is a delete file
	// for the compaction to remove.
	posDelPath := tbl.Location() + "/data/pos-del-001.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		`[{"file_path": "`+dataPath+`", "pos": 1}]`)
	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	tx2 := tbl.NewTransaction()
	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(posDelBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	// Stage the stale compaction from the current view: replace the
	// data file with its compacted form and remove the applied delete.
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeleteFiles, 1)

	compactedPath := tbl.Location() + "/data/data-compacted.parquet"
	writeParquetFile(t, compactedPath, arrowSc,
		`[{"id": 1, "data": "alpha"}, {"id": 3, "data": "gamma"}]`)
	compactedBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		compactedPath, iceberg.ParquetFile, nil, nil, nil, 2, 512)
	require.NoError(t, err)

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{tasks[0].File},
		[]iceberg.DataFile{compactedBuilder.Build()},
		tasks[0].DeleteFiles,
		nil,
	))

	// Peer commits a new position delete for row 0 (id 1) against the
	// same data file from the same view and wins the race. A delete
	// file is invisible to the serializable added-data-files validator,
	// so without the noReplay guard the stale compaction would replay:
	// it removes the data file the peer's delete references, silently
	// no-oping that delete and resurrecting id 1 in the compacted file.
	peerDelPath := tbl.Location() + "/data/pos-del-peer.parquet"
	writeParquetFile(t, peerDelPath, table.PositionalDeleteArrowSchema,
		`[{"file_path": "`+dataPath+`", "pos": 0}]`)
	peerDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		peerDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	peerTxn := tbl.NewTransaction()
	peerRd := peerTxn.NewRowDelta(nil)
	peerRd.AddDeletes(peerDelBuilder.Build())
	require.NoError(t, peerRd.Commit(t.Context()))
	_, err = peerTxn.Commit(t.Context())
	require.NoError(t, err)

	attemptsBefore := cat.attempts.Load()
	_, err = staleTxn.Commit(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
		"a ReplaceFiles carrying delete-file removals must fail on the first CAS conflict, not refresh-and-replay")

	// Only the peer's commit landed: both position deletes still apply
	// to the original data file and the compacted file is absent.
	committed, err := cat.LoadTable(t.Context(), table.Identifier{"db", "concurrent_rewrite"})
	require.NoError(t, err)
	assert.Equal(t, []int64{3}, idsInTable(t, committed),
		"the stale compaction must not have landed; ids 1 and 2 stay deleted by the live position deletes")
}

// A peer snapshot on another branch leaves main and every requirement
// unchanged but takes the staged snapshot's sequence number. The resume of a
// non-replayable compaction rebuilds onto the same pinned parent and lands.
func TestReplaceFilesWithRemovalsResumesAfterPeerOnAnotherBranch(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc,
		`[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}, {"id": 3, "data": "gamma"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	posDelPath := tbl.Location() + "/data/pos-del-001.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		`[{"file_path": "`+dataPath+`", "pos": 1}]`)
	tx2 := tbl.NewTransaction()
	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(newPosDeleteFile(t, posDelPath))
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)
	mainHead := tbl.CurrentSnapshot().SnapshotID

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	compactedPath := tbl.Location() + "/data/data-compacted.parquet"
	writeParquetFile(t, compactedPath, arrowSc, `[{"id": 1, "data": "alpha"}, {"id": 3, "data": "gamma"}]`)
	compacted, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		compactedPath, iceberg.ParquetFile, nil, nil, nil, 2, 512)
	require.NoError(t, err)
	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{tasks[0].File}, []iceberg.DataFile{compacted.Build()}, tasks[0].DeleteFiles, nil))

	peerPath := tbl.Location() + "/data/data-peer.parquet"
	writeParquetFile(t, peerPath, arrowSc, `[{"id": 9, "data": "peer"}]`)
	peerTxn, err := tbl.NewTransactionOnBranch("other")
	require.NoError(t, err)
	require.NoError(t, peerTxn.AddDataFilesByPath(t.Context(), []string{peerPath}, nil, false))
	_, err = peerTxn.Commit(t.Context())
	require.NoError(t, err)

	_, err = staleTxn.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.NotErrorIs(t, err, table.ErrTransactionUnusable)
	assert.Contains(t, err.Error(), "reload the table and rebuild the removals")

	committed, err := staleTxn.Commit(t.Context())
	require.NoError(t, err)
	head := committed.CurrentSnapshot()
	require.NotNil(t, head.ParentSnapshotID)
	assert.Equal(t, mainHead, *head.ParentSnapshotID, "the resume must keep the pinned parent")
	assert.Equal(t, []int64{1, 3}, idsInTable(t, committed))
	assert.Empty(t, deleteEntriesReferencing(t, committed, map[string]struct{}{dataPath: {}, compactedPath: {}}),
		"the compaction removed the only live delete")
	other := committed.Metadata().SnapshotByName("other")
	require.NotNil(t, other.ParentSnapshotID)
	assert.Equal(t, mainHead, *other.ParentSnapshotID, "the peer branch is untouched")
}

// A peer that removed a file this compaction replaces makes the resume's
// rebuild fail the same way on every later attempt, so the transaction
// must become unusable.
func TestReplaceFilesResumeAfterPeerRemovedInputIsUnusable(t *testing.T) {
	tbl, cat := newConcurrentRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{dataPath}, nil, false))
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.CommitNumRetriesKey: "0"}))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	replacement := func(name string) iceberg.DataFile {
		path := tbl.Location() + "/data/" + name + ".parquet"
		writeParquetFile(t, path, arrowSc, `[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}]`)
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
			path, iceberg.ParquetFile, nil, nil, nil, 2, 512)
		require.NoError(t, err)

		return b.Build()
	}

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{tasks[0].File}, []iceberg.DataFile{replacement("stale")}, nil, nil))

	peerTxn := tbl.NewTransaction()
	require.NoError(t, peerTxn.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{tasks[0].File}, []iceberg.DataFile{replacement("peer")}, nil, nil))
	_, err = peerTxn.Commit(t.Context())
	require.NoError(t, err)

	_, err = staleTxn.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.NotErrorIs(t, err, table.ErrTransactionUnusable)

	attempts := cat.attempts.Load()
	_, err = staleTxn.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrTransactionUnusable)
	require.ErrorIs(t, err, table.ErrCommitDiverged)
	require.NotErrorIs(t, err, table.ErrCommitFailed, "a caller retrying on ErrCommitFailed must stop")
	assert.EqualError(t, err, fmt.Sprintf("transaction cannot be committed again: files to rewrite were removed: "+
		"commit diverged: base snapshot is no longer on the branch: 1 data file(s) to rewrite are no longer on the branch head, e.g. [%q]", dataPath))
	assert.NotContains(t, err.Error(), "refresh and try again")
	assert.Equal(t, attempts, cat.attempts.Load(), "the resume must stop before the catalog")

	_, err = staleTxn.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrTransactionUnusable)
	assert.Equal(t, attempts, cat.attempts.Load())
}

// Regression guard: the noReplay flag must not leak to commits that
// remove only data files. Those removals are path-keyed and the retry
// rebuild fails terminally when the path is gone from the fresh base,
// so refresh-and-replay stays sound (and enabled) for them.
// Condition: a data-only ReplaceFiles races a peer append, with
// retries enabled and snapshot isolation (so the peer's append is not
// itself a semantic conflict).
// Assertion: the commit succeeds on the second CommitTable attempt and
// the final table contains both writers' files.
func TestReplaceFilesDataOnlyStillReplays(t *testing.T) {
	tbl, cat := newConcurrentRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc,
		`[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{dataPath}, nil, false))
	// Overwrites validate added data files under the default
	// serializable isolation; relax to snapshot so the peer's append is
	// not rejected as a semantic conflict and the replay itself is
	// exercised.
	require.NoError(t, tx.SetProperties(iceberg.Properties{
		table.WriteUpdateIsolationLevelKey: string(table.IsolationSnapshot),
	}))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	compactedPath := tbl.Location() + "/data/data-compacted.parquet"
	writeParquetFile(t, compactedPath, arrowSc,
		`[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}]`)
	compactedBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		compactedPath, iceberg.ParquetFile, nil, nil, nil, 2, 512)
	require.NoError(t, err)

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{tasks[0].File},
		[]iceberg.DataFile{compactedBuilder.Build()},
		nil,
		nil,
	))

	// Peer appends from the same view and wins the race.
	peerPath := tbl.Location() + "/data/data-peer.parquet"
	writeParquetFile(t, peerPath, arrowSc, `[{"id": 100, "data": "peer"}]`)
	peerTxn := tbl.NewTransaction()
	require.NoError(t, peerTxn.AddDataFilesByPath(t.Context(), []string{peerPath}, nil, false))
	_, err = peerTxn.Commit(t.Context())
	require.NoError(t, err)

	attemptsBefore := cat.attempts.Load()
	committed, err := staleTxn.Commit(t.Context())
	require.NoError(t, err, "a removal-free replace must still refresh-and-replay to success")
	assert.Equal(t, attemptsBefore+2, cat.attempts.Load(),
		"expected exactly one conflict followed by one successful replay")
	assert.Equal(t, []int64{1, 2, 100}, idsInTable(t, committed),
		"the replay must inherit the peer's append alongside the compacted file")
}

// A copy-on-write delete rewrites the data file and removes its deletion vector together, under a
// validator that rejects concurrent deletes on the file, so it retries over a concurrent append
// whether or not the file carried a deletion vector (#2195). Both writes land, and the rows the
// old deletion vector deleted stay deleted.
func TestCoWDeleteRemovingDVReplaysOverConcurrentAppend(t *testing.T) {
	cases := []struct {
		name      string
		seededDVs int
		wantIDs   []int64
	}{
		{name: "file with a deletion vector", seededDVs: 1, wantIDs: []int64{1, 3, 4, 5, 6, 8, 9, 10, 100}},
		{name: "file without a deletion vector", seededDVs: 0, wantIDs: []int64{1, 3, 4, 5, 6, 7, 8, 9, 10, 100}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			tbl, cat := newNoReplayV3Table(t)
			tbl = appendTenRows(t, tbl)
			if tc.seededDVs > 0 {
				tbl = mergeOnReadDelete(t, tbl, 7)
			}
			require.Equal(t, tc.seededDVs, liveDVCount(t, tbl))

			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeCopyOnWrite}))
			tbl, err := tx.Commit(ctx)
			require.NoError(t, err)

			staleTxn := tbl.NewTransaction()
			require.NoError(t, staleTxn.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil))

			appendRowsOnRef(t, tbl, table.MainBranch, 100)

			attemptsBefore := cat.attempts.Load()
			_, err = staleTxn.Commit(ctx)
			require.NoError(t, err)
			assert.Equal(t, attemptsBefore+2, cat.attempts.Load(),
				"expected exactly one conflict followed by one successful replay")

			committed, loadErr := cat.LoadTable(ctx, table.Identifier{"db", "no_replay_removals"})
			require.NoError(t, loadErr)
			assert.Equal(t, tc.wantIDs, idsInTable(t, committed),
				"the delete replays over the peer's append, so both writers land")
			assert.Zero(t, liveDVCount(t, committed),
				"the rewritten file must not keep the removed deletion vector")
		})
	}
}

// seedTwoFileTable returns a v3 merge-on-read table with ids 1-10 in one data
// file and ids 100 and 101 in a second, so a filter on id >= 100 drops the
// second file whole.
func seedTwoFileTable(t *testing.T) (*table.Table, *concurrentTestCatalog) {
	t.Helper()

	tbl, cat := newNoReplayV3Table(t)
	tbl = appendTenRows(t, tbl)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 100, 101)

	return tbl, cat
}

// dataFilePathFor returns the path of the single data file holding id.
func dataFilePathFor(t *testing.T, tbl *table.Table, id int64) string {
	t.Helper()

	tasks, err := tbl.Scan(table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), id))).PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return tasks[0].File.FilePath()
}

func switchToCopyOnWrite(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeCopyOnWrite}))
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

func loadNoReplayTable(t *testing.T, cat *concurrentTestCatalog) *table.Table {
	t.Helper()

	committed, err := cat.LoadTable(t.Context(), table.Identifier{"db", "no_replay_removals"})
	require.NoError(t, err)

	return committed
}

var idAtLeast100 = iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(100))

// Why: #2195. A copy-on-write delete that drops a whole file removes the file's
// deletion vector with it, under rewriteValidator over the removed file, so a
// concurrent unrelated append must not make it fail.
// Condition: the second file carries a DV; a stale CoW delete drops that file
// while a peer appends.
// Assertion: the delete lands on the replay (two CommitTable attempts), the
// peer's row survives, and no live DV is left.
func TestCoWWholeFileDropWithDVReplaysOverConcurrentAppend(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	tbl = mergeOnReadDelete(t, tbl, 101)
	require.Equal(t, 1, liveDVCount(t, tbl))
	tbl = switchToCopyOnWrite(t, tbl)

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.Delete(ctx, idAtLeast100, nil))
	appendRowsOnRef(t, tbl, table.MainBranch, 50)

	attemptsBefore := cat.attempts.Load()
	_, err := staleTxn.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, attemptsBefore+2, cat.attempts.Load(), "expected one conflict and one successful replay")

	committed := loadNoReplayTable(t, cat)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 50}, idsInTable(t, committed))
	assert.Zero(t, liveDVCount(t, committed))
}

// Why: #2195. Overwrite shares the copy-on-write deletion path, so the same
// replay applies when its filter drops a file that carries a DV.
// Condition: the second file carries a DV; a stale overwrite of id >= 100
// races a peer append.
// Assertion: the overwrite lands on the replay with both writers' rows.
func TestCoWOverwriteRemovingDVReplaysOverConcurrentAppend(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	tbl = mergeOnReadDelete(t, tbl, 101)
	tbl = switchToCopyOnWrite(t, tbl)

	staleTxn := tbl.NewTransaction()
	rdr := idRecordReader(t, tbl, 150)
	defer rdr.Release()
	require.NoError(t, staleTxn.Overwrite(ctx, rdr, nil, table.WithOverwriteFilter(idAtLeast100)))
	appendRowsOnRef(t, tbl, table.MainBranch, 50)

	attemptsBefore := cat.attempts.Load()
	_, err := staleTxn.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, attemptsBefore+2, cat.attempts.Load(), "expected one conflict and one successful replay")

	committed := loadNoReplayTable(t, cat)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 50, 150}, idsInTable(t, committed))
	assert.Zero(t, liveDVCount(t, committed))
}

// Why: #2195. A merge-on-read delete that drops a whole file also takes the
// file's DV with it. It now registers rewriteValidator over that file, so it
// can replay over an unrelated append.
// Condition: the second file carries a DV; a stale MoR delete of id >= 100
// races a peer append.
// Assertion: the delete lands on the replay and no DV references the dropped
// file.
func TestMoRWholeFileDropWithDVReplaysOverConcurrentAppend(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	tbl = mergeOnReadDelete(t, tbl, 101)
	dropped := dataFilePathFor(t, tbl, 100)

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.Delete(ctx, idAtLeast100, nil))
	appendRowsOnRef(t, tbl, table.MainBranch, 50)

	attemptsBefore := cat.attempts.Load()
	_, err := staleTxn.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, attemptsBefore+2, cat.attempts.Load(), "expected one conflict and one successful replay")

	committed := loadNoReplayTable(t, cat)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 50}, idsInTable(t, committed))
	assert.Empty(t, deleteEntriesReferencing(t, committed, map[string]struct{}{dropped: {}}),
		"no DV may reference the dropped file")
}

// Why: #2195. Replaying a whole-file drop is only safe because the validator
// rejects a peer's new DV on the dropped file. Without it, the stale DV removal
// would replay as a no-op and leave the peer's DV orphaned on a removed file.
// Condition: the second file carries a DV; a peer supersedes it with its own
// merged DV while a stale MoR delete drops the file.
// Assertion: the stale commit is rejected without committing, and the table
// is the peer's, with exactly one live DV on the second file.
func TestMoRWholeFileDropRejectsConcurrentDVOnDroppedFile(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	tbl = mergeOnReadDelete(t, tbl, 101)
	dropped := dataFilePathFor(t, tbl, 100)

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.Delete(ctx, idAtLeast100, nil))
	mergeOnReadDelete(t, tbl, 100)

	attemptsBefore := cat.attempts.Load()
	_, err := staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
	assert.ErrorIs(t, err, table.ErrTransactionUnusable)
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(), "the replay must stop before the catalog")

	committed := loadNoReplayTable(t, cat)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, idsInTable(t, committed),
		"the peer's merged DV deletes both rows of the second file")
	assert.Len(t, deleteEntriesReferencing(t, committed, map[string]struct{}{dropped: {}}), 1,
		"the second file keeps exactly one live DV: the peer's")
}

// Why: a delete that drops one file whole and merges a partial delete into
// another file's existing DV removes a DV whose data file stays live. That
// removal is still snapshot-relative, so the whole commit must stay
// non-replayable even though the drop alone could replay.
// Condition: both files carry a DV; a stale MoR delete drops the second file
// and deletes id 3 from the first while a peer appends.
// Assertion: exactly one CommitTable attempt, and only the peer's append lands.
func TestMoRDeleteMixingWholeFileDropAndPartialMergeStaysNonReplayable(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	tbl = mergeOnReadDelete(t, tbl, 7)
	tbl = mergeOnReadDelete(t, tbl, 101)
	require.Equal(t, 2, liveDVCount(t, tbl))

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.Delete(ctx,
		iceberg.NewOr(idAtLeast100, iceberg.EqualTo(iceberg.Reference("id"), int64(3))), nil))
	appendRowsOnRef(t, tbl, table.MainBranch, 50)

	attemptsBefore := cat.attempts.Load()
	_, err := staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Contains(t, err.Error(), "reload the table and rebuild the removals")
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(), "the commit must not replay")

	committed := loadNoReplayTable(t, cat)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 8, 9, 10, 50, 100}, idsInTable(t, committed))
}

// Why: a partial delete gives a file that stays live a new DV, and nothing
// checks that file for a concurrent DV. When the same delete also drops a file
// with a DV, that removal keeps the commit non-replayable, which is what stops
// a replay from leaving two live DVs on the partially deleted file.
// Condition: only the second file carries a DV; a stale MoR delete drops it and
// gives the first file its first DV (id 3) while a peer gives the first file a
// DV of its own (id 5).
// Assertion: exactly one CommitTable attempt, and the table stays readable with
// one live DV on the first file: the peer's.
func TestMoRDeleteMixingWholeFileDropAndFirstDVStaysNonReplayable(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	tbl = mergeOnReadDelete(t, tbl, 101)
	require.Equal(t, 1, liveDVCount(t, tbl))
	partial := dataFilePathFor(t, tbl, 1)

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.Delete(ctx,
		iceberg.NewOr(idAtLeast100, iceberg.EqualTo(iceberg.Reference("id"), int64(3))), nil))
	mergeOnReadDelete(t, tbl, 5)

	attemptsBefore := cat.attempts.Load()
	_, err := staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Contains(t, err.Error(), "reload the table and rebuild the removals")
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(), "the commit must not replay")

	committed := loadNoReplayTable(t, cat)
	assert.Len(t, deleteEntriesReferencing(t, committed, map[string]struct{}{partial: {}}), 1,
		"the partially deleted file keeps exactly one live DV: the peer's")
	assert.Equal(t, []int64{1, 2, 3, 4, 6, 7, 8, 9, 10, 100}, idsInTable(t, committed))
}

// Why: ReplaceDataFiles removes a data file and its DV but registers no
// validator against concurrent deletes on that file, so replaying it could
// bring back rows a peer deleted. It must stay non-replayable (#2195 review).
// Condition: the second file carries a DV; a stale ReplaceDataFilesByPath
// replaces it while a peer appends.
// Assertion: exactly one CommitTable attempt, the no-replay hint, and only the
// peer's append lands.
func TestReplaceDataFilesRemovingDVStaysNonReplayable(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	tbl = mergeOnReadDelete(t, tbl, 101)
	replaced := dataFilePathFor(t, tbl, 100)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	replacement := tbl.Location() + "/data/replacement.parquet"
	writeParquetFile(t, replacement, arrowSc, `[{"id": 150}]`)

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.ReplaceDataFilesByPath(ctx, []string{replaced}, []string{replacement}, nil))
	appendRowsOnRef(t, tbl, table.MainBranch, 50)

	attemptsBefore := cat.attempts.Load()
	_, err = staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Contains(t, err.Error(), "reload the table and rebuild the removals")
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(), "the commit must not replay")

	committed := loadNoReplayTable(t, cat)
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 50, 100}, idsInTable(t, committed))
}

// Why: rewriteValidator treats any concurrent equality delete as a conflict, so
// the merge-on-read drop registers it only for files that carry a DV. A drop of
// a file without a DV removes no DV and must keep replaying exactly as before.
// Condition: no DVs; a stale MoR delete drops the second file while a peer
// commits an equality delete for id 5.
// Assertion: the drop lands on the replay and both deletes apply.
func TestMoRWholeFileDropWithoutDVStillReplaysOverEqualityDelete(t *testing.T) {
	ctx := t.Context()
	tbl, cat := seedTwoFileTable(t)
	require.Zero(t, liveDVCount(t, tbl))

	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.Delete(ctx, idAtLeast100, nil))

	records, release := makeEqDeleteRecords(t, idKeyArrowSchema(t), `[{"id": 5}]`)
	defer release()
	peerTxn := tbl.NewTransaction()
	require.NoError(t, peerTxn.CommitEqualityDeletes(ctx, []int{1}, records, nil))
	_, err := peerTxn.Commit(ctx)
	require.NoError(t, err)

	attemptsBefore := cat.attempts.Load()
	_, err = staleTxn.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, attemptsBefore+2, cat.attempts.Load(), "expected one conflict and one successful replay")

	committed := loadNoReplayTable(t, cat)
	assert.Equal(t, []int64{1, 2, 3, 4, 6, 7, 8, 9, 10}, idsInTable(t, committed))
}
