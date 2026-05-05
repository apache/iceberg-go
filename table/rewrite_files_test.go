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
	"strings"
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
	assert.Contains(t, err.Error(), "at least one file change")
}

func TestRewriteFiles_AddDataFile_RejectsNonDataFile(t *testing.T) {
	tbl := newRewriteTestTable(t)
	tx := tbl.NewTransaction()

	posDel := newPosDeleteFile(t, "spurious-pos-del.parquet")

	err := tx.NewRewrite(nil).AddDataFile(posDel).Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AddDataFile only supports data files",
		"adding a delete file via AddDataFile must be reported at commit time")
}

func TestRewriteFiles_DeleteFile_RoutesByContentType(t *testing.T) {
	// Validates the routing via observable behavior: a builder that
	// is given only a pos-delete to delete (no data files) must reach
	// ReplaceFiles, where the underlying check rejects removing a
	// delete file that is not in the table. That distinct error proves
	// DeleteFile routed it to deleteFilesToRemove (not dataFilesToDelete).
	tbl := newRewriteTestTable(t)
	tx := tbl.NewTransaction()

	dummyData := newDataFile(t, tbl.Location()+"/data/dummy.parquet")
	posDel := newPosDeleteFile(t, tbl.Location()+"/data/spurious-pos-del.parquet")

	err := tx.NewRewrite(nil).
		DeleteFile(dummyData).
		AddDataFile(newDataFile(t, tbl.Location()+"/data/replacement.parquet")).
		DeleteFile(posDel).
		Commit(t.Context())
	require.Error(t, err, "ReplaceFiles fails because the dummy files are not in the empty table")
	// Any of these messages confirms we got past the builder's own
	// validation and into ReplaceFiles' membership checks — i.e., the
	// pos-delete was routed to deleteFilesToRemove and the data file
	// to dataFilesToDelete.
	msg := err.Error()
	assert.True(t,
		strings.Contains(msg, "do not belong to the table") ||
			strings.Contains(msg, "without an existing snapshot"),
		"got %q — expected the underlying ReplaceFiles membership/snapshot error", msg)
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
	assert.Equal(t, int32(1), cat.attempts.Load()-beforeLeader,
		"leader's first attempt fails on stale assertion; validator rejects on retry before re-issuing CommitTable")
}
