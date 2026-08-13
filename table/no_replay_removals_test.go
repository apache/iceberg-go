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
// commits keep replaying.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table"
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
// exactly one CommitTable attempt (no replay), and the table carries
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
	_, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)

	attemptsBefore := cat.attempts.Load()
	_, err = staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
		"a commit carrying DV removals must fail on the first CAS conflict, not refresh-and-replay")

	// The clean conflict leaves the transaction retriable, not latched
	// as committed: a naive same-transaction retry surfaces a fresh
	// conflict (its requirement still targets the stale base) rather
	// than "transaction has already been committed".
	_, err = staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)

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
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
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
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
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
	require.NoError(t, peerTxn.AddFiles(t.Context(), []string{peerPath}, nil, false))
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
