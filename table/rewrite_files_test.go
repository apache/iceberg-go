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

// Black-box coverage for the [table.RewriteFiles] snapshot-operation
// builder, including the supporting [table.ExecuteCompactionGroup]
// worker function and the [table.CollectSafePositionDeletes]
// predicate. Tests cover the in-process path (one transaction stages
// and commits) and the distributed-coordinator path (workers produce
// [table.CompactionGroupResult]s, a leader builds a single
// [table.RewriteFiles] from them and commits).

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// concurrentTestCatalog enforces Requirement.Validate against its
// current metadata on every CommitTable, so a leader transaction
// whose AssertRefSnapshotID points at a stale snapshot fails its
// first attempt and triggers refresh-and-replay. attempts counts
// CommitTable invocations so tests can prove the retry boundary.
type concurrentTestCatalog struct {
	metadata table.Metadata
	location string
	fsF      table.FSysF
	attempts atomic.Int32
}

func (c *concurrentTestCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	return table.New(ident, c.metadata, c.location, c.fsF, c), nil
}

func (c *concurrentTestCatalog) CommitTable(_ context.Context, _ table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.attempts.Add(1)
	for _, req := range reqs {
		if err := req.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
		}
	}
	meta, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	// Returning the seed location is enough to keep subsequent
	// NewTransaction calls from adding AssertCreate; the value is not
	// re-read by the table machinery between commits in this stub.
	return meta, c.location, nil
}

func newConcurrentRewriteTestTable(t *testing.T) (*table.Table, *concurrentTestCatalog) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:        "2",
			table.CommitNumRetriesKey:          "2",
			table.CommitMinRetryWaitMsKey:      "1",
			table.CommitMaxRetryWaitMsKey:      "2",
			table.CommitTotalRetryTimeoutMsKey: "1000",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "concurrent_rewrite"}, meta, metaLoc, fsF, cat), cat
}

func newPosDeleteFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	return b.Build()
}

func newEqDeleteFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	b = b.EqualityFieldIDs([]int{1})

	return b.Build()
}

func newDataFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	return b.Build()
}

func TestCollectSafePositionDeletes_FiltersAndDedupes(t *testing.T) {
	posA := newPosDeleteFile(t, "pos-a.parquet")
	posB := newPosDeleteFile(t, "pos-b.parquet")
	eq := newEqDeleteFile(t, "eq.parquet")

	tasks := []table.FileScanTask{
		{DeleteFiles: []iceberg.DataFile{posA, posB}, EqualityDeleteFiles: []iceberg.DataFile{eq}},
		{DeleteFiles: []iceberg.DataFile{posA}}, // duplicate of posA
	}

	got := table.CollectSafePositionDeletes(tasks)
	require.Len(t, got, 2, "duplicate pos-deletes must be deduped, eq-deletes must be filtered out")

	paths := []string{got[0].FilePath(), got[1].FilePath()}
	assert.ElementsMatch(t, []string{"pos-a.parquet", "pos-b.parquet"}, paths)
}

func TestCollectSafePositionDeletes_EmptyTasks(t *testing.T) {
	assert.Empty(t, table.CollectSafePositionDeletes(nil))
	assert.Empty(t, table.CollectSafePositionDeletes([]table.FileScanTask{{}}))
}

func TestExecuteCompactionGroup_EmptyGroup(t *testing.T) {
	tbl := newRewriteTestTable(t)

	got, err := table.ExecuteCompactionGroup(t.Context(), tbl,
		table.CompactionTaskGroup{PartitionKey: "p"})
	require.NoError(t, err)
	assert.Equal(t, "p", got.PartitionKey)
	assert.Empty(t, got.OldDataFiles)
	assert.Empty(t, got.NewDataFiles)
	assert.Empty(t, got.SafePosDeletes)
}

func TestRewriteFiles_EmptyCommit_Errors(t *testing.T) {
	tbl := newRewriteTestTable(t)
	tx := tbl.NewTransaction()

	err := tx.NewRewrite(nil).Commit(t.Context())
	require.Error(t, err, "an empty rewrite has nothing to stage and must reject")
	assert.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.Contains(t, err.Error(), "at least one file change")
}

func TestRewriteFiles_AddDataFile_RejectsNonDataFile(t *testing.T) {
	tbl := newRewriteTestTable(t)
	tx := tbl.NewTransaction()

	posDel := newPosDeleteFile(t, "spurious-pos-del.parquet")

	err := tx.NewRewrite(nil).AddDataFile(posDel).Commit(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.Contains(t, err.Error(), "AddDataFile only supports data files",
		"adding a delete file via AddDataFile must be reported at commit time")
	assert.Contains(t, err.Error(), "spurious-pos-del.parquet",
		"error must name the offending file path")
}

func TestRewriteFiles_Commit_SingleShot(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 2 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/seed-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "seed"}]`, i+1))
		seedTx := tbl.NewTransaction()
		require.NoError(t, seedTx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = seedTx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	groups := toTaskGroups(plan.Groups)

	results := make([]table.CompactionGroupResult, 0, len(groups))
	for _, g := range groups {
		gr, err := table.ExecuteCompactionGroup(t.Context(), tbl, g)
		require.NoError(t, err)
		results = append(results, gr)
	}

	tx := tbl.NewTransaction()
	rewrite := tx.NewRewrite(nil)
	for _, gr := range results {
		rewrite.Apply(gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes)
	}
	require.NoError(t, rewrite.Commit(t.Context()))

	err = rewrite.Commit(t.Context())
	require.Error(t, err, "double commit must reject so validators are not re-appended")
	assert.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.Contains(t, err.Error(), "already called")
}

// TestRewriteFiles_Commit_SingleShot_AfterFailure pins down the
// stricter contract: a builder is dead even when the first Commit
// failed before reaching ReplaceFiles. Without this guard, an
// empty-rewrite or stranger-file failure would leave the builder
// reusable, and a retry would slip through and append the conflict
// validator a second time.
func TestRewriteFiles_Commit_SingleShot_AfterFailure(t *testing.T) {
	t.Run("empty rewrite", func(t *testing.T) {
		tbl := newRewriteTestTable(t)
		tx := tbl.NewTransaction()

		rewrite := tx.NewRewrite(nil)

		err := rewrite.Commit(t.Context())
		require.Error(t, err, "first Commit on an empty builder must fail")
		assert.ErrorIs(t, err, table.ErrInvalidOperation)
		assert.Contains(t, err.Error(), "at least one file change")

		err = rewrite.Commit(t.Context())
		require.Error(t, err, "second Commit must reject even though the first never staged anything")
		assert.ErrorIs(t, err, table.ErrInvalidOperation)
		assert.Contains(t, err.Error(), "already called")
	})

	t.Run("ReplaceFiles failure", func(t *testing.T) {
		tbl := newRewriteTestTable(t)
		tx := tbl.NewTransaction()

		stranger := newDataFile(t, tbl.Location()+"/data/stranger.parquet")
		replacement := newDataFile(t, tbl.Location()+"/data/replacement.parquet")

		rewrite := tx.NewRewrite(nil).
			DeleteFile(stranger).
			AddDataFile(replacement)

		err := rewrite.Commit(t.Context())
		require.Error(t, err, "ReplaceFiles must reject a stranger data file")

		err = rewrite.Commit(t.Context())
		require.Error(t, err, "second Commit must reject so a retry can't re-stage ReplaceFiles or re-append the validator")
		assert.ErrorIs(t, err, table.ErrInvalidOperation)
		assert.Contains(t, err.Error(), "already called")
	})
}

func TestRewriteFiles_DeleteFile_RoutesByContentType(t *testing.T) {
	// Validates routing into the right slice via observable behavior:
	// the data-slice and delete-file-slice membership checks in
	// ReplaceFiles produce distinct error messages, so a mis-routed
	// file would surface the wrong error class.
	//
	//   Builder A — DeleteFile(strangerData): if routed correctly, the
	//   transaction has an empty deleteFilesToRemove slice and falls
	//   through to ReplaceDataFilesWithDataFiles, which rejects with
	//   "cannot delete files that do not belong to the table". A
	//   mis-route into deleteFilesToRemove would error with "cannot
	//   remove delete files that do not belong to the table" instead.
	//
	//   Builder B — DeleteFile(strangerPosDel) only: if routed
	//   correctly, ReplaceFiles' main path rejects with "cannot remove
	//   delete files that do not belong to the table". A mis-route
	//   into dataFilesToDelete would surface "cannot delete files
	//   that do not belong to the table" instead. (No AddDataFile
	//   here: adds-without-data-deletes is rejected pre-flight by
	//   Commit, so it would mask the routing error.)
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	dataPath := tbl.Location() + "/data/seed.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "seed"}]`)
	seedTx := tbl.NewTransaction()
	require.NoError(t, seedTx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = seedTx.Commit(t.Context())
	require.NoError(t, err)

	strangerData := newDataFile(t, tbl.Location()+"/data/stranger.parquet")
	strangerPosDel := newPosDeleteFile(t, tbl.Location()+"/data/stranger-pos-del.parquet")
	replacement := newDataFile(t, tbl.Location()+"/data/replacement.parquet")

	dataSliceMiss := "cannot delete files that do not belong to the table"
	deleteFileSliceMiss := "cannot remove delete files that do not belong to the table"

	dataTx := tbl.NewTransaction()
	err = dataTx.NewRewrite(nil).
		DeleteFile(strangerData).
		AddDataFile(replacement).
		Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), dataSliceMiss,
		"DeleteFile(data) must route into dataFilesToDelete; mis-routed it would surface the delete-file-slice error")
	assert.NotContains(t, err.Error(), deleteFileSliceMiss)

	delTx := tbl.NewTransaction()
	err = delTx.NewRewrite(nil).
		DeleteFile(strangerPosDel).
		Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), deleteFileSliceMiss,
		"DeleteFile(pos-delete) must route into deleteFilesToRemove; mis-routed it would surface the data-slice error")
	assert.NotContains(t, err.Error(), dataSliceMiss)
}

// TestRewriteFiles_RejectsAddsWithoutDataDeletes proves that staging
// new data files with no data files to delete is rejected up front —
// otherwise the chain would slip through ReplaceFiles →
// ReplaceDataFilesWithDataFiles → AddDataFiles (an OpAppend producer)
// and silently tag the snapshot append instead of replace, with no
// rewrite validator registered.
func TestRewriteFiles_RejectsAddsWithoutDataDeletes(t *testing.T) {
	tbl := newRewriteTestTable(t)
	tx := tbl.NewTransaction()

	add := newDataFile(t, tbl.Location()+"/data/lonely-add.parquet")

	err := tx.NewRewrite(nil).AddDataFile(add).Commit(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.Contains(t, err.Error(), "must delete at least one data file when adding")
}

func TestRewriteFiles_RejectsNilDataFile(t *testing.T) {
	t.Run("DeleteFile", func(t *testing.T) {
		tbl := newRewriteTestTable(t)
		tx := tbl.NewTransaction()

		err := tx.NewRewrite(nil).DeleteFile(nil).Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, table.ErrInvalidOperation)
		assert.Contains(t, err.Error(), "DeleteFile got nil data file")
	})

	t.Run("AddDataFile", func(t *testing.T) {
		tbl := newRewriteTestTable(t)
		tx := tbl.NewTransaction()

		err := tx.NewRewrite(nil).AddDataFile(nil).Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, table.ErrInvalidOperation)
		assert.Contains(t, err.Error(), "AddDataFile got nil data file")
	})
}

// TestRewriteFiles_DistributedEquivalence proves the worker+coordinator
// pipeline lands at the same end state as the bundled in-process
// [Transaction.RewriteDataFiles]: workers run [table.ExecuteCompactionGroup]
// per group (here inline; in distributed compaction this happens on
// remote peers and the results travel over the wire), then a single
// leader transaction stages [table.RewriteFiles.Apply] +
// [table.RewriteFiles.Commit] for one atomic snapshot.
func TestRewriteFiles_DistributedEquivalence(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 4 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row-%d"}]`, i+1, i+1))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 4)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	groups := toTaskGroups(plan.Groups)

	results := make([]table.CompactionGroupResult, 0, len(groups))
	for _, g := range groups {
		gr, err := table.ExecuteCompactionGroup(t.Context(), tbl, g)
		require.NoError(t, err)
		require.NotEmpty(t, gr.NewDataFiles)
		results = append(results, gr)
	}

	leaderTxn := tbl.NewTransaction()
	rewrite := leaderTxn.NewRewrite(nil)
	for _, gr := range results {
		rewrite.Apply(gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes)
	}
	require.NoError(t, rewrite.Commit(t.Context()))

	committed, err := leaderTxn.Commit(t.Context())
	require.NoError(t, err)

	assertRowCount(t, committed, 4)

	snap := committed.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpReplace, snap.Summary.Operation,
		"rewrite snapshot must be tagged replace, not overwrite")

	// Lock the cross-client summary contract under OpReplace. PyIceberg
	// (and other readers) parse these keys; the OpOverwrite → OpReplace
	// flip must not silently drop any of them.
	props := snap.Summary.Properties
	require.NotNil(t, props)
	assert.Equal(t, "1", props["added-data-files"])
	assert.Equal(t, "4", props["deleted-data-files"])
	assert.Equal(t, "4", props["added-records"])
	assert.Equal(t, "4", props["deleted-records"])
	assert.Equal(t, "1", props["total-data-files"])
	assert.Equal(t, "4", props["total-records"])
	assert.NotEmpty(t, props["added-files-size"], "byte counter must be populated")
	assert.NotEmpty(t, props["removed-files-size"], "byte counter must be populated")
	assert.NotEmpty(t, props["total-files-size"], "byte counter must be populated")

	postTasks, err := committed.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	assert.Len(t, postTasks, 1, "leader-side commit must produce one consolidated file")
}

// TestRewriteFiles_DropsSafePositionDeletes drives the pipeline with a
// position-delete present at plan time and asserts the pos-delete is
// expunged in the rewrite snapshot.
func TestRewriteFiles_DropsSafePositionDeletes(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}, {"id": %d, "data": "b"}]`, i*2+1, i*2+2))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	firstDataPath := tbl.Location() + "/data/file-0.parquet"
	posDelPath := tbl.Location() + "/data/pos-del-001.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 0}]`, firstDataPath))

	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(posDelBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 5)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	groups := toTaskGroups(plan.Groups)

	results := make([]table.CompactionGroupResult, 0, len(groups))
	totalSafe := 0
	for _, g := range groups {
		gr, err := table.ExecuteCompactionGroup(t.Context(), tbl, g)
		require.NoError(t, err)
		results = append(results, gr)
		totalSafe += len(gr.SafePosDeletes)
	}
	require.Equal(t, 1, totalSafe, "the staged pos-delete must be reported safe by exactly one group")

	leaderTxn := tbl.NewTransaction()
	rewrite := leaderTxn.NewRewrite(nil)
	for _, gr := range results {
		rewrite.Apply(gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes)
	}
	require.NoError(t, rewrite.Commit(t.Context()))

	committed, err := leaderTxn.Commit(t.Context())
	require.NoError(t, err)

	assertRowCount(t, committed, 5)

	// Pos-delete removal must show up in the OpReplace summary —
	// `removed-position-delete-files` (count of files) and
	// `removed-position-deletes` (count of rows) are the keys other
	// clients read.
	snap := committed.CurrentSnapshot()
	require.NotNil(t, snap)
	props := snap.Summary.Properties
	require.NotNil(t, props)
	assert.Equal(t, "1", props["removed-position-delete-files"])
	assert.Equal(t, "1", props["removed-position-deletes"])

	postTasks, err := committed.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	for _, task := range postTasks {
		assert.Empty(t, task.DeleteFiles,
			"safe pos-delete must be expunged in the rewrite snapshot")
	}
}

// TestRewriteFiles_RejectsConcurrentEqDelete is the negative path: a
// leader stages a rewrite, a concurrent peer commits an equality-delete
// during the rewrite window, and the leader's [table.Transaction.Commit]
// must fail with [table.ErrConflictingDeleteFiles] — proving the
// rewrite-specific conflict validator that [table.RewriteFiles.Commit]
// registers internally fires under refresh-and-replay.
//
// First Commit attempt fails on the stale AssertRefSnapshotID (the
// catalog has advanced past the leader's base). The retry refreshes,
// builds a fresh conflictContext walking S0 → S1, and the rewrite
// validator's conservative rule rejects any concurrent equality-delete
// during a rewrite — terminal exit before any further CommitTable
// call.
//
// Equality deletes are used here rather than positional deletes because
// the v2 manifest schema does not carry a pos-delete's referenced data
// file path; the validator's pos-delete branch is only effective on v3
// tables. The eq-delete branch is the conservative rule for v2.
func TestRewriteFiles_RejectsConcurrentEqDelete(t *testing.T) {
	tbl, cat := newConcurrentRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row-%d"}]`, i+1, i+1))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	groups := toTaskGroups(plan.Groups)

	results := make([]table.CompactionGroupResult, 0, len(groups))
	for _, g := range groups {
		gr, err := table.ExecuteCompactionGroup(t.Context(), tbl, g)
		require.NoError(t, err)
		require.NotEmpty(t, gr.OldDataFiles)
		results = append(results, gr)
	}

	leaderTxn := tbl.NewTransaction()
	rewrite := leaderTxn.NewRewrite(nil)
	for _, gr := range results {
		rewrite.Apply(gr.OldDataFiles, gr.NewDataFiles, gr.SafePosDeletes)
	}
	require.NoError(t, rewrite.Commit(t.Context()),
		"staging the rewrite must succeed; the conflict surfaces at Commit time")

	eqDelPath := tbl.Location() + "/data/concurrent-eq-del.parquet"
	eqDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	eqDelBuilder = eqDelBuilder.EqualityFieldIDs([]int{1})

	peerTxn := tbl.NewTransaction()
	rd := peerTxn.NewRowDelta(nil)
	rd.AddDeletes(eqDelBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))
	_, err = peerTxn.Commit(t.Context())
	require.NoError(t, err, "peer commit advances the catalog so the leader's first attempt fails")

	beforeLeader := cat.attempts.Load()
	_, err = leaderTxn.Commit(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrConflictingDeleteFiles,
		"refresh-and-replay must detect the concurrent equality-delete during a rewrite")
	// The expected delta is exactly 1 CommitTable invocation, the
	// first attempt that fails on the stale AssertRefSnapshotID. The
	// retry refreshes the conflictContext, the rewrite validator
	// rejects the concurrent eq-delete, and the leader exits before
	// re-issuing CommitTable. A delta of 0 means the validator fired
	// pre-flight; a delta of ≥2 means the retry reached the catalog.
	assert.Equal(t, int32(1), cat.attempts.Load()-beforeLeader,
		"only the stale-assertion attempt landed; the retry never reached CommitTable")
}
