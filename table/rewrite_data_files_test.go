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

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type partialProgressCatalog struct {
	metadata  table.Metadata
	failOn    int
	unknownOn int
	calls     int
}

func (c *partialProgressCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (c *partialProgressCatalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.calls++
	if c.failOn > 0 && c.calls == c.failOn {
		return nil, "", fmt.Errorf("%w: injected partial progress commit failure", table.ErrCommitFailed)
	}

	meta, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta
	if c.unknownOn > 0 && c.calls == c.unknownOn {
		return meta, "", errors.New("injected unknown commit state")
	}

	return meta, "", nil
}

func newPartialProgressTestTable(t *testing.T) (*table.Table, *partialProgressCatalog) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	cat := &partialProgressCatalog{metadata: meta}
	tbl := table.New(
		table.Identifier{"db", "partial_progress_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)

	return tbl, cat
}

func newRewriteTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "rewrite_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

// toTaskGroups converts compaction.Plan groups to table.CompactionTaskGroup.
// This bridge is needed because table/ cannot import table/compaction/
// (circular dependency).
func toTaskGroups(groups []compaction.Group) []table.CompactionTaskGroup {
	out := make([]table.CompactionTaskGroup, len(groups))
	for i, g := range groups {
		out[i] = table.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		}
	}

	return out
}

// runRewriteWithCleanup is the test-side orchestration: compute dead
// eq-deletes against the current snapshot, then invoke the executor.
// Mirrors what cmd/iceberg/compact.go does.
func runRewriteWithCleanup(t *testing.T, tbl *table.Table, groups []table.CompactionTaskGroup, partialProgress bool) (*table.RewriteResult, *table.Table) {
	t.Helper()

	tx := tbl.NewTransaction()

	rewriteOpts := table.RewriteDataFilesOptions{PartialProgress: partialProgress}
	if !partialProgress {
		snap := tbl.CurrentSnapshot()
		if snap != nil {
			rewrittenSet := make(map[string]struct{})
			for _, g := range groups {
				for _, task := range g.Tasks {
					rewrittenSet[task.File.FilePath()] = struct{}{}
				}
			}
			deadEqDeletes, err := compaction.CollectDeadEqualityDeletesWithSpecs(
				t.Context(), iceio.LocalFS{}, tbl.Metadata(), snap, rewrittenSet)
			require.NoError(t, err)
			rewriteOpts.ExtraDeleteFilesToRemove = deadEqDeletes
		}
	}

	result, err := tx.RewriteDataFiles(t.Context(), groups, rewriteOpts)
	require.NoError(t, err)
	if partialProgress {
		require.NotNil(t, result.Table)
		_, err := tx.Commit(t.Context())
		require.Error(t, err, "partial progress is terminal and must not make the parent Commit idempotent")

		return result, result.Table
	}

	out, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return result, out
}

var defaultTestCompactionCfg = compaction.Config{
	TargetFileSizeBytes: 10 * 1024 * 1024,
	MinFileSizeBytes:    5 * 1024 * 1024,
	MaxFileSizeBytes:    20 * 1024 * 1024,
	MinInputFiles:       2,
	DeleteFileThreshold: 5,
	PackingLookback:     compaction.DefaultPackingLookback,
}

func TestRewriteDataFiles_SmallFiles(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 5 small data files, one per commit.
	for i := range 5 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row-%d"}]`, i+1, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	assertRowCount(t, tbl, 5)

	// Plan compaction.
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 5)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	// Execute compaction.
	result, tbl := runRewriteWithCleanup(t, tbl, toTaskGroups(plan.Groups), false)

	// Verify: same rows, fewer files.
	assertRowCount(t, tbl, 5)
	assert.Equal(t, 5, result.RemovedDataFiles)
	assert.Equal(t, 1, result.AddedDataFiles)
	assert.Equal(t, 1, result.RewrittenGroups)
	assert.Greater(t, result.BytesBefore, int64(0))
	assert.Greater(t, result.BytesAfter, int64(0))

	// Verify new snapshot has fewer files.
	newTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	assert.Len(t, newTasks, 1)
}

func TestRewriteDataFiles_WithPositionDeletes(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 3 data files with 2 rows each.
	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}, {"id": %d, "data": "b"}]`, i*2+1, i*2+2))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	assertRowCount(t, tbl, 6)

	// Add a position delete targeting the first data file.
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

	assertRowCount(t, tbl, 5) // one row deleted

	// Plan and execute compaction with low delete threshold.
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1 // force compaction due to deletes

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	result, tbl := runRewriteWithCleanup(t, tbl, toTaskGroups(plan.Groups), false)

	// After compaction: delete is applied, 5 rows remain, delete file removed.
	assertRowCount(t, tbl, 5)
	assert.Greater(t, result.RemovedPositionDeleteFiles, 0)

	// Verify no delete files remain.
	newTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	for _, task := range newTasks {
		assert.Empty(t, task.DeleteFiles, "no position deletes should remain after compaction")
	}
}

func TestRewriteDataFiles_EmptyPlan(t *testing.T) {
	tbl := newRewriteTestTable(t)

	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), nil, table.RewriteDataFilesOptions{})
	require.NoError(t, err)

	assert.Equal(t, 0, result.RewrittenGroups)
	assert.Equal(t, 0, result.AddedDataFiles)
	assert.Equal(t, 0, result.RemovedDataFiles)
	assert.Equal(t, int64(0), result.BytesBefore)

	partialTx := tbl.NewTransaction()
	partialResult, err := partialTx.RewriteDataFiles(t.Context(), nil,
		table.RewriteDataFilesOptions{PartialProgress: true})
	require.NoError(t, err)
	assert.Equal(t, tbl.Location(), partialResult.Table.Location())
}

// TestExecuteCompactionGroup_TargetFileSizeForwarded verifies that
// WithCompactionTargetFileSize reaches the underlying WriteRecords
// call: a tiny target size on a multi-row group must force the
// writer to emit more than one output file.
func TestExecuteCompactionGroup_TargetFileSizeForwarded(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 5 {
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
	require.Len(t, tasks, 5)

	// Build the group manually so option-forwarding is decoupled from
	// the planner's bin-packing / MinInputFiles knobs.
	scanTasks := make([]table.FileScanTask, len(tasks))
	var totalSize int64
	for i, st := range tasks {
		scanTasks[i] = st
		totalSize += st.File.FileSizeBytes()
	}
	group := table.CompactionTaskGroup{
		PartitionKey:   "single",
		Tasks:          scanTasks,
		TotalSizeBytes: totalSize,
	}

	withTiny, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionTargetFileSize(1))
	require.NoError(t, err)
	assert.Greater(t, len(withTiny.NewDataFiles), 1,
		"WithCompactionTargetFileSize(1) must force the writer to roll over per row")

	withDefault, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
	require.NoError(t, err)
	assert.Len(t, withDefault.NewDataFiles, 1,
		"without the option, the same group consolidates into a single file")
}

// TestExecuteCompactionGroup_ScanConcurrencyForwarded is a smoke test
// confirming WithCompactionScanConcurrency is wired through without
// breaking the read path. We can't easily observe scan parallelism
// from the result, so the assertion is correctness equivalence with a
// no-option baseline: same group → same files in / out.
func TestExecuteCompactionGroup_ScanConcurrencyForwarded(t *testing.T) {
	tbl := newRewriteTestTable(t)

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

	group := toTaskGroups(plan.Groups)[0]

	withOption, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionScanConcurrency(1))
	require.NoError(t, err)

	baseline, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
	require.NoError(t, err)

	assert.Equal(t, len(baseline.OldDataFiles), len(withOption.OldDataFiles),
		"setting scan concurrency must not change the set of old files read")
	assert.Equal(t, len(baseline.NewDataFiles), len(withOption.NewDataFiles),
		"setting scan concurrency must not change the set of consolidated outputs")
}

// TestRewriteDataFiles_GroupOptionsForwarded verifies that
// RewriteDataFilesOptions.GroupOptions are piped through to every
// ExecuteCompactionGroup call.
func TestRewriteDataFiles_GroupOptionsForwarded(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 5 {
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

	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), toTaskGroups(plan.Groups), table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{
			table.WithCompactionTargetFileSize(1),
		},
	})
	require.NoError(t, err)

	assert.Greater(t, result.AddedDataFiles, 1,
		"GroupOptions must reach ExecuteCompactionGroup; tiny target size should split output")

	// Drive the commit through to catch regressions that break
	// ReplaceFiles under tiny-target rewrites — the in-process counter
	// above only proves the option reached the writer.
	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, committed, 5)
}

func TestRewriteDataFiles_EmptyGroupSkipped(t *testing.T) {
	tbl := newRewriteTestTable(t)

	groups := []table.CompactionTaskGroup{
		{PartitionKey: "empty", Tasks: nil, TotalSizeBytes: 0},
	}

	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{})
	require.NoError(t, err)

	assert.Equal(t, 0, result.RewrittenGroups)

	tx = tbl.NewTransaction()
	_, err = tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress: true,
		MaxCommits:      -1,
	})
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	assert.Contains(t, err.Error(), "MaxCommits")
}

func TestRewriteDataFiles_PartialProgress(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 6 small files.
	for i := range 6 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row"}]`, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)

	plannedGroups := toTaskGroups(plan.Groups)
	require.NotEmpty(t, plannedGroups)
	// Split the planned group so the test can distinguish one durable snapshot
	// per group from one snapshot containing all staged rewrites.
	first := plannedGroups[0]
	require.GreaterOrEqual(t, len(first.Tasks), 2)
	mid := len(first.Tasks) / 2
	groups := []table.CompactionTaskGroup{
		{PartitionKey: first.PartitionKey + "/a", Tasks: first.Tasks[:mid], TotalSizeBytes: first.TotalSizeBytes / 2},
		{PartitionKey: first.PartitionKey + "/b", Tasks: first.Tasks[mid:], TotalSizeBytes: first.TotalSizeBytes - first.TotalSizeBytes/2},
	}
	groups = append(groups, plannedGroups[1:]...)
	beforeSnapshots := len(tbl.Metadata().Snapshots())

	result, tbl := runRewriteWithCleanup(t, tbl, groups, true)

	assertRowCount(t, tbl, 6)
	assert.Greater(t, result.RewrittenGroups, 0)
	assert.Len(t, result.CompletedGroups, result.RewrittenGroups)
	assert.Same(t, tbl, result.Table)
	assert.Equal(t, 6, result.RemovedDataFiles)
	assert.Equal(t, beforeSnapshots+result.RewrittenGroups, len(tbl.Metadata().Snapshots()))
}

func TestRewriteDataFiles_PartialProgressMaxCommitsProcessesAllGroups(t *testing.T) {
	tbl := newRewriteTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/max-commit-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row"}]`, i+1))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	groups := make([]table.CompactionTaskGroup, 0, len(tasks))
	for i, task := range tasks {
		groups = append(groups, table.CompactionTaskGroup{
			PartitionKey:   fmt.Sprintf("group-%d", i),
			Tasks:          []table.FileScanTask{task},
			TotalSizeBytes: task.File.FileSizeBytes(),
		})
	}

	beforeSnapshots := len(tbl.Metadata().Snapshots())
	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress: true,
		MaxCommits:      2,
	})
	require.NoError(t, err)

	assert.Equal(t, 3, result.RewrittenGroups,
		"MaxCommits limits durable snapshots, not the number of groups processed")
	assert.Len(t, result.CompletedGroups, 3)
	assert.Equal(t, beforeSnapshots+2, len(result.Table.Metadata().Snapshots()))
	assertRowCount(t, result.Table, 3)
}

func TestRewriteDataFiles_PartialProgressRejectsCommittedParentReuse(t *testing.T) {
	tbl, cat := newPartialProgressTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/reuse.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "row"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	groups := []table.CompactionTaskGroup{{
		PartitionKey:   "reuse",
		Tasks:          []table.FileScanTask{tasks[0]},
		TotalSizeBytes: tasks[0].File.FileSizeBytes(),
	}}

	tx = tbl.NewTransaction()
	_, err = tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress: true,
	})
	require.NoError(t, err)
	calls := cat.calls

	result, err := tx.RewriteDataFiles(t.Context(), nil, table.RewriteDataFilesOptions{
		PartialProgress: true,
	})
	require.ErrorContains(t, err, "transaction has already been committed")
	assert.Nil(t, result)
	assert.Equal(t, calls, cat.calls, "an empty reused parent must be rejected before another catalog commit")

	result, err = tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress: true,
	})
	require.ErrorContains(t, err, "transaction has already been committed")
	assert.Nil(t, result)
	assert.Equal(t, calls, cat.calls, "a reused parent must be rejected before another catalog commit")
}

func TestRewriteDataFiles_PartialProgressStopsOnUnknownCommitState(t *testing.T) {
	tbl, cat := newPartialProgressTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 2 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/unknown-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row"}, {"id": %d, "data": "row"}]`, i*2+1, i*2+2))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	groups := []table.CompactionTaskGroup{
		{PartitionKey: "unknown-0", Tasks: []table.FileScanTask{tasks[0]}, TotalSizeBytes: tasks[0].File.FileSizeBytes()},
		{PartitionKey: "unknown-1", Tasks: []table.FileScanTask{tasks[1]}, TotalSizeBytes: tasks[1].File.FileSizeBytes()},
	}

	cat.unknownOn = cat.calls + 1
	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress: true,
		MaxCommits:      2,
	})
	require.ErrorContains(t, err, "unknown commit state")
	require.NotNil(t, result)
	assert.Zero(t, result.RewrittenGroups)
	assert.Equal(t, 1, len(cat.metadata.Snapshots())-len(tbl.Metadata().Snapshots()))
	assert.Equal(t, cat.unknownOn, cat.calls, "later groups must not be planned after an unknown commit")
	_, commitErr := tx.Commit(t.Context())
	assert.Error(t, commitErr, "the parent transaction must remain terminal after an unknown commit")
}

func TestRewriteDataFiles_PartialProgressRetainsSharedPositionDeleteAfterFailure(t *testing.T) {
	tbl, cat := newPartialProgressTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 2 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/shared-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}, {"id": %d, "data": "b"}]`, i*2+1, i*2+2))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	// This partition-scoped position delete targets both data files. The first
	// partial batch must not remove it while the second data file is still live.
	posDelPath := tbl.Location() + "/data/shared-position-delete.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema, fmt.Sprintf(
		`[{"file_path": "%s", "pos": 0}, {"file_path": "%s", "pos": 0}]`,
		tbl.Location()+"/data/shared-0.parquet", tbl.Location()+"/data/shared-1.parquet"))
	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(posDelBuilder.Build()).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 2)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	groups := make([]table.CompactionTaskGroup, 0, len(tasks))
	for i, task := range tasks {
		groups = append(groups, table.CompactionTaskGroup{
			PartitionKey:   fmt.Sprintf("shared-group-%d", i),
			Tasks:          []table.FileScanTask{task},
			TotalSizeBytes: task.File.FileSizeBytes(),
		})
	}

	// The first batch commits; the second batch fails. One tolerated failure
	// must still return a successful result and leave its old data plus delete
	// file in the table.
	beforeFiles := parquetFiles(t, tbl.Location())
	cat.failOn = cat.calls + 2
	tx = tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress:  true,
		MaxCommits:       2,
		MaxFailedCommits: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, result.RewrittenGroups)
	require.Len(t, result.CompletedGroups, 1)
	require.Len(t, result.FailedGroups, 1)
	assert.Equal(t, 1, len(result.Table.Metadata().Snapshots())-len(tbl.Metadata().Snapshots()))

	// The row deleted from the failed group's data file must remain hidden.
	// If the shared position delete were removed by the first batch, this would
	// be three rows instead of two.
	assertRowCount(t, result.Table, 2)
	newTasks, err := result.Table.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	var remainingPositionDeletes int
	for _, task := range newTasks {
		remainingPositionDeletes += len(task.DeleteFiles)
	}
	assert.Greater(t, remainingPositionDeletes, 0,
		"a shared position delete must remain while a referenced data file survives")

	expectedFiles := append([]string{}, beforeFiles...)
	for _, completed := range result.CompletedGroups {
		for _, file := range completed.NewDataFiles {
			expectedFiles = append(expectedFiles, file.FilePath())
		}
	}
	assert.ElementsMatch(t, expectedFiles, parquetFiles(t, tbl.Location()),
		"generated files from tolerated failed batches must be removed")
}

func parquetFiles(t *testing.T, location string) []string {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(location, "data", "*.parquet"))
	require.NoError(t, err)

	return paths
}

func newPartialProgressPartitionedTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "data",
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	cat := &partialProgressCatalog{metadata: meta}

	return table.New(
		table.Identifier{"db", "partial_progress_branch_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)
}

func addPartitionedRowsOnRef(t *testing.T, tbl *table.Table, ref, name, partition string, ids ...int64) *table.Table {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	rows := make([]string, len(ids))
	for i, id := range ids {
		rows[i] = fmt.Sprintf(`{"id": %d, "data": "%s"}`, id, partition)
	}
	dataPath := tbl.Location() + "/data/" + name + ".parquet"
	writeParquetFile(t, dataPath, arrowSc, "["+strings.Join(rows, ",")+"]")
	info, err := os.Stat(dataPath)
	require.NoError(t, err)

	txn, err := tbl.NewTransactionOnBranchWithError(ref)
	require.NoError(t, err)
	builder, err := iceberg.NewDataFileBuilder(
		tbl.Spec(), iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
		map[int]any{1000: partition}, nil, nil, int64(len(ids)), info.Size())
	require.NoError(t, err)
	require.NoError(t, txn.AddDataFiles(t.Context(), []iceberg.DataFile{builder.Build()}, nil))
	committed, err := txn.Commit(t.Context())
	require.NoError(t, err)

	return committed
}

func TestRewriteDataFiles_PartialProgressUsesTargetBranchSnapshot(t *testing.T) {
	const branch = "feature"
	tbl := newPartialProgressPartitionedTable(t)
	tbl = addPartitionedRowsOnRef(t, tbl, table.MainBranch, "main-0", "main", 1, 2)
	tbl = addPartitionedRowsOnRef(t, tbl, branch, "branch-0", "branch", 3, 4)
	tbl = addPartitionedRowsOnRef(t, tbl, table.MainBranch, "main-1", "main", 5, 6)
	tbl = addPartitionedRowsOnRef(t, tbl, branch, "branch-1", "branch", 7, 8)

	mainPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	branchPaths, _ := liveFilesOnRef(t, tbl, branch)
	mainPathSet := make(map[string]struct{}, len(mainPaths))
	for _, path := range mainPaths {
		mainPathSet[path] = struct{}{}
	}
	branchOnly := make([]string, 0, 2)
	for _, path := range branchPaths {
		if _, ok := mainPathSet[path]; !ok {
			branchOnly = append(branchOnly, path)
		}
	}
	require.Len(t, branchOnly, 2)

	posDelPath := tbl.Location() + "/data/branch-position-delete.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema, fmt.Sprintf(
		`[{"file_path": %q, "pos": 0}, {"file_path": %q, "pos": 0}]`,
		branchOnly[0], branchOnly[1]))
	info, err := os.Stat(posDelPath)
	require.NoError(t, err)
	delBuilder, err := iceberg.NewDataFileBuilder(
		tbl.Spec(), iceberg.EntryContentPosDeletes, posDelPath, iceberg.ParquetFile,
		map[int]any{1000: "branch"}, nil, nil, 2, info.Size())
	require.NoError(t, err)
	txn, err := tbl.NewTransactionOnBranchWithError(branch)
	require.NoError(t, err)
	require.NoError(t, txn.NewRowDelta(nil).AddDeletes(delBuilder.Build()).Commit(t.Context()))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	scan := tbl.Scan()
	scan, err = scan.UseRef(branch)
	require.NoError(t, err)
	tasks, err := scan.PlanFiles(t.Context())
	require.NoError(t, err)
	var rewriteTask table.FileScanTask
	for _, task := range tasks {
		if task.File.FilePath() == branchOnly[0] {
			rewriteTask = task

			break
		}
	}
	require.NotNil(t, rewriteTask.File)

	txn, err = tbl.NewTransactionOnBranchWithError(branch)
	require.NoError(t, err)
	result, err := txn.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{{
		PartitionKey:   "branch",
		Tasks:          []table.FileScanTask{rewriteTask},
		TotalSizeBytes: rewriteTask.File.FileSizeBytes(),
	}}, table.RewriteDataFilesOptions{PartialProgress: true})
	require.NoError(t, err)
	require.NotNil(t, result.Table)

	assert.Equal(t, []int64{1, 2, 4, 8}, idsOnRef(t, result.Table, branch),
		"a branch-only survivor must keep its partition-scoped delete after another branch file is rewritten")
	branchScan := result.Table.Scan()
	branchScan, err = branchScan.UseRef(branch)
	require.NoError(t, err)
	remainingTasks, err := branchScan.PlanFiles(t.Context())
	require.NoError(t, err)
	var remainingDeletes int
	for _, task := range remainingTasks {
		remainingDeletes += len(task.DeleteFiles)
	}
	assert.Greater(t, remainingDeletes, 0, "the branch position delete must remain live")
}

func TestRewriteDataFiles_ContextCancellation(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 3 files.
	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row"}]`, i+1))

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

	// Cancel context before execution.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	tx := tbl.NewTransaction()
	_, err = tx.RewriteDataFiles(ctx, toTaskGroups(plan.Groups), table.RewriteDataFilesOptions{})
	require.Error(t, err)
}

// TestRewriteDataFiles_DeadEqualityDeletesDropped covers the common
// CDC case: every data file in the (sole) partition is being rewritten,
// so every equality delete in that partition is provably dead and must
// be expunged from the new snapshot's manifests.
func TestRewriteDataFiles_DeadEqualityDeletesDropped(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 3 data files with 2 rows each.
	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}, {"id": %d, "data": "b"}]`, i*2+1, i*2+2))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	assertRowCount(t, tbl, 6)

	// Add an equality delete that removes id=2.
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id": 2}]`)

	assertRowCount(t, tbl, 5) // id=2 deleted

	// Plan and execute compaction.
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	result, tbl := runRewriteWithCleanup(t, tbl, toTaskGroups(plan.Groups), false)

	// The eq-delete is dead after compaction — every preexisting data
	// file it could have applied to has been rewritten, so it must be
	// dropped from the new snapshot.
	assertRowCount(t, tbl, 5)
	assert.Equal(t, 1, result.RemovedEqualityDeleteFiles,
		"dead equality delete file should be removed by compaction")
	assert.Greater(t, result.RemovedDataFiles, 0)

	// The deleted row (id=2) must not reappear after compaction.
	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(0).(*array.Int64)
		for i := range col.Len() {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}

	assert.NotContains(t, ids, int64(2), "deleted row id=2 must not reappear after compaction")

	// New scan tasks must carry no eq-delete files.
	newTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	for _, task := range newTasks {
		assert.Empty(t, task.EqualityDeleteFiles, "no equality deletes should remain after compaction")
	}
}

// TestRewriteDataFiles_PartialRewritePreservesEqDelete covers the case
// where compaction rewrites some but not all data files in a partition.
// An eq-delete whose seq is greater than a surviving (non-rewritten)
// data file's seq must be preserved — it still applies to that file.
//
// Layout: commits land in this order to engineer a survivor with
// seq < eq-delete.seq, then a high-seq file we'll rewrite alone.
//
//	commit 1: small-0.parquet      (data, will survive — low seq)
//	commit 2: small-1.parquet      (data, will survive — low seq)
//	commit 3: eq-delete (id=99)    (eq-delete; seq > both small files)
//	commit 4: preserve.parquet     (data, will be rewritten — high seq)
//
// The post-rewrite check: eq-delete must remain because small-0/1
// (which have lower seq) are still alive and applicable.
func TestRewriteDataFiles_PartialRewritePreservesEqDelete(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// commit 1, 2: small data files.
	for i := range 2 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/small-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}]`, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	// commit 3: eq-delete.
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id": 99}]`)

	// commit 4: preserve.parquet — the high-seq file we'll compact alone.
	preserveDataPath := tbl.Location() + "/data/preserve.parquet"
	writeParquetFile(t, preserveDataPath, arrowSc, `[{"id": 50, "data": "preserved"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{preserveDataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(tasks), 3)

	// Defensive: verify the seq-num invariant the test relies on
	// (eq-delete.seq is greater than at least one small-* file's seq).
	// If this ever drifts, fail loudly with a useful message.
	eqDeleteSeq, smallMinSeq, preserveSeq := readKeySeqNums(t, tbl)
	require.Greater(t, eqDeleteSeq, smallMinSeq,
		"test setup expects eq-delete.seq > min(small-*.seq)")
	require.Greater(t, preserveSeq, eqDeleteSeq,
		"test setup expects preserve.seq > eq-delete.seq")
	var preserveTask table.FileScanTask
	for _, task := range tasks {
		if strings.HasSuffix(task.File.FilePath(), "/preserve.parquet") {
			preserveTask = task

			break
		}
	}
	require.NotEmpty(t, preserveTask.File.FilePath())

	// Build a single-task group manually (bypass the planner so we can
	// force a partial-partition rewrite).
	groups := []table.CompactionTaskGroup{
		{
			PartitionKey:   "force-partial",
			Tasks:          []table.FileScanTask{preserveTask},
			TotalSizeBytes: preserveTask.File.FileSizeBytes(),
		},
	}

	result, tbl := runRewriteWithCleanup(t, tbl, groups, false)

	// The eq-delete must still be present — it applies to the unrewritten
	// small-0 / small-1 files (seq 1 and 2), and eq-delete.seq=3 > 2.
	assert.Equal(t, 0, result.RemovedEqualityDeleteFiles,
		"eq-delete must be preserved when an unrewritten low-seq data file remains in its partition")

	// Sanity: the eq-delete is still attached to the surviving data
	// files in scan tasks.
	postTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	hasEqDelete := false
	for _, task := range postTasks {
		if len(task.EqualityDeleteFiles) > 0 {
			hasEqDelete = true

			break
		}
	}
	assert.True(t, hasEqDelete, "at least one surviving data file should still see the eq-delete")
}

// TestRewriteDataFiles_PreserveDeadEqualityDeletesOptOut verifies that
// passing WithPreserveDeadEqualityDeletes(true) leaves dead
// eq-deletes in place (legacy / opt-out behavior).
func TestRewriteDataFiles_PreserveDeadEqualityDeletesOptOut(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}]`, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id": 1}]`)

	// Snapshot the eq-delete file path BEFORE compaction so we can
	// confirm post-compaction that the manifest still references it.
	preTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	preEqDeletePaths := collectEqDeletePaths(preTasks)
	require.Len(t, preEqDeletePaths, 1, "test setup expects a single eq-delete")

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1
	cfg.PreserveDeadEqualityDeletes = true // opt out of cleanup

	plan, err := cfg.PlanCompaction(preTasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	// Opt-out path: do NOT compute / pass dead eq-deletes. Mirrors the
	// CLI behavior when --preserve-dead-equality-deletes is set.
	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), toTaskGroups(plan.Groups),
		table.RewriteDataFilesOptions{PartialProgress: false})
	require.NoError(t, err)

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, 0, result.RemovedEqualityDeleteFiles,
		"opt-out: dead eq-deletes must be preserved")

	// The eq-delete file must still exist in the new snapshot's
	// manifests, even though it no longer matches the new high-seq
	// compacted files (so scan tasks will not list it).
	assert.True(t, snapshotContainsDeleteFile(t, tbl, preEqDeletePaths[0]),
		"opt-out: the eq-delete file %s must remain in the snapshot's manifests",
		preEqDeletePaths[0])
}

// snapshotContainsDeleteFile returns true if the table's current
// snapshot's delete manifests reference the given file path with a
// non-DELETED status.
func snapshotContainsDeleteFile(t *testing.T, tbl *table.Table, path string) bool {
	t.Helper()

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)

	fs := iceio.LocalFS{}
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if e.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			if e.DataFile().FilePath() == path {
				return true
			}
		}
	}

	return false
}

func collectEqDeletePaths(tasks []table.FileScanTask) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, task := range tasks {
		for _, df := range task.EqualityDeleteFiles {
			if _, ok := seen[df.FilePath()]; ok {
				continue
			}
			seen[df.FilePath()] = struct{}{}
			out = append(out, df.FilePath())
		}
	}

	return out
}

// TestRewriteDataFiles_PartialProgressPreservesEqDeletes verifies that
// in partial-progress mode, eq-deletes are NOT dropped (the docstring
// caveat). This protects against a per-group commit dropping an
// eq-delete that is still alive for a later group's partition.
func TestRewriteDataFiles_PartialProgressPreservesEqDeletes(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}]`, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id": 1}]`)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	result, _ := runRewriteWithCleanup(t, tbl, toTaskGroups(plan.Groups), true)
	// runRewriteWithCleanup skips ExtraDeleteFilesToRemove for
	// partial-progress mode, mirroring the documented limitation:
	// per-group eq-delete cleanup is a follow-up.
	assert.Equal(t, 0, result.RemovedEqualityDeleteFiles,
		"partial-progress mode does not drop eq-deletes (yet)")
}

// readKeySeqNums walks the table's current snapshot manifests and
// returns the seq numbers of (a) the equality-delete file, (b) the
// minimum seq among files prefixed `/data/small-`, and (c) the seq
// of `/data/preserve.parquet`. Used to assert seq-num invariants
// without hardcoding values.
func readKeySeqNums(t *testing.T, tbl *table.Table) (eqDelete, smallMin, preserve int64) {
	t.Helper()

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)

	fs := iceio.LocalFS{}
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	smallMin = int64(1<<62 - 1)
	for _, m := range manifests {
		for e, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if e.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			path := e.DataFile().FilePath()
			seq := e.SequenceNum()
			switch {
			case e.DataFile().ContentType() == iceberg.EntryContentEqDeletes:
				eqDelete = seq
			case strings.Contains(path, "/data/small-"):
				if seq < smallMin {
					smallMin = seq
				}
			case strings.HasSuffix(path, "/data/preserve.parquet"):
				preserve = seq
			}
		}
	}

	return eqDelete, smallMin, preserve
}

// appendEqualityDelete is a test helper that writes one equality
// delete file containing the given JSON records and commits it onto
// the table. equalityFieldIDs identifies the schema fields used for
// equality matching.
func appendEqualityDelete(t *testing.T, tbl *table.Table, equalityFieldIDs []int, recordsJSON string) *table.Table {
	t.Helper()

	delFields := make([]iceberg.NestedField, 0, len(equalityFieldIDs))
	for _, id := range equalityFieldIDs {
		f, ok := tbl.Schema().FindFieldByID(id)
		require.True(t, ok)
		delFields = append(delFields, f)
	}
	delSchema := iceberg.NewSchema(0, delFields...)
	delArrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, delArrowSc, strings.NewReader(recordsJSON))
	require.NoError(t, err)
	defer rec.Release()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}

	tx := tbl.NewTransaction()
	deleteFiles, err := tx.WriteEqualityDeletes(t.Context(), equalityFieldIDs, records)
	require.NoError(t, err)

	rd := tx.NewRowDelta(nil)
	for _, df := range deleteFiles {
		rd.AddDeletes(df)
	}
	require.NoError(t, rd.Commit(t.Context()))
	out, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return out
}
