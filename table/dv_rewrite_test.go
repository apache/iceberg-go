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
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRewriteDataFilesRemovesDeletionVectors guarantees that compacting a data
// file carrying a deletion vector expunges that DV in the same rewrite snapshot,
// leaving no manifest entry referencing the removed data file. Both commit
// paths of [Transaction.RewriteDataFiles] are covered: the single atomic
// snapshot and the per-group partial-progress path.
//
// Background: a DV manifest entry is 1:1 with its referenced data file but rides
// in FileScanTask.DeletionVectorFiles, separate from the pos-delete files in
// FileScanTask.DeleteFiles. A rewrite that collected only the latter would leave
// the DV behind, referencing a data file no longer in the table — an orphan.
//
// An orphaned DV is invisible to scan planning — matchDVToData drops a DV whose
// referenced data file is gone — so the assertion walks the raw delete manifests
// of the post-compaction snapshot.
func TestRewriteDataFilesRemovesDeletionVectors(t *testing.T) {
	for _, partialProgress := range []bool{false, true} {
		name := "atomic"
		if partialProgress {
			name = "partial-progress"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tbl, dvTarget := seedV3TableWithDV(t)

			groups, rewritten := planDVCompaction(t, tbl, dvTarget)

			rtx := tbl.NewTransaction()
			_, err := rtx.RewriteDataFiles(ctx, groups,
				table.RewriteDataFilesOptions{PartialProgress: partialProgress})
			require.NoError(t, err)
			tbl, err = rtx.Commit(ctx)
			require.NoError(t, err)

			assertRowCount(t, tbl, 3) // DV applied during the rewrite; id=1 stays gone
			assert.Empty(t, deleteEntriesReferencing(t, tbl, rewritten),
				"compaction must remove deletion vectors for rewritten data files")
		})
	}
}

// TestRewriteDataFilesPreservesRowIDThroughDeletionVector: applying a DV during
// compaction must not renumber survivors' _row_id (= first_row_id + ORIGINAL
// position). Regression guard against synthesizing lineage after the DV filter
// drops rows, which yields a dense 0..N renumbering.
func TestRewriteDataFilesPreservesRowIDThroughDeletionVector(t *testing.T) {
	for _, partialProgress := range []bool{false, true} {
		name := "atomic"
		if partialProgress {
			name = "partial-progress"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tbl, dvTarget := seedV3TableWithDV(t)

			// Survivors after the DV deletes id=1: id=2 (_row_id 1, first file),
			// id=3 (_row_id 2) and id=4 (_row_id 3, second file).
			before := rowIDByID(t, tbl)
			require.Equal(t, map[int64]int64{2: 1, 3: 2, 4: 3}, before)

			groups, _ := planDVCompaction(t, tbl, dvTarget)
			rtx := tbl.NewTransaction()
			_, err := rtx.RewriteDataFiles(ctx, groups,
				table.RewriteDataFilesOptions{PartialProgress: partialProgress})
			require.NoError(t, err)
			tbl, err = rtx.Commit(ctx)
			require.NoError(t, err)

			assert.Equal(t, before, rowIDByID(t, tbl),
				"_row_id must be preserved for survivors when a deletion vector is applied during compaction")
		})
	}
}

// rowIDByID scans the table with row lineage (plus any extra options) and
// returns a map of id -> _row_id.
func rowIDByID(t *testing.T, tbl *table.Table, opts ...table.ScanOption) map[int64]int64 {
	t.Helper()

	scanOpts := append([]table.ScanOption{table.WithRowLineage()}, opts...)
	_, itr, err := tbl.Scan(scanOpts...).ToArrowRecords(context.Background())
	require.NoError(t, err)

	out := map[int64]int64{}
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		rowIDIdx := rec.Schema().FieldIndices(iceberg.RowIDColumnName)
		require.NotEmpty(t, rowIDIdx, "_row_id must be in the row-lineage scan projection")
		idCol := rec.Column(idIdx[0]).(*array.Int64)
		rowIDCol := rec.Column(rowIDIdx[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			require.False(t, rowIDCol.IsNull(i), "_row_id must not be null for id=%d", idCol.Value(i))
			out[idCol.Value(i)] = rowIDCol.Value(i)
		}
		rec.Release()
	}

	return out
}

// TestRewriteFilesApplyResultRemovesDeletionVectors drives the distributed
// coordinator path — a worker runs [ExecuteCompactionGroup], then a leader
// stages the results with [Transaction.NewRewrite] + [RewriteFiles.ApplyResult]
// + Commit. It locks the ApplyResult→ReplaceFiles coupling that carries
// SafeDeletionVectors and expunges them by referenced data file.
func TestRewriteFilesApplyResultRemovesDeletionVectors(t *testing.T) {
	ctx := context.Background()
	tbl, dvTarget := seedV3TableWithDV(t)

	before := rowIDByID(t, tbl)
	require.Equal(t, map[int64]int64{2: 1, 3: 2, 4: 3}, before)

	groups, rewritten := planDVCompaction(t, tbl, dvTarget)

	var results []table.CompactionGroupResult
	dvCount := 0
	for _, g := range groups {
		gr, err := table.ExecuteCompactionGroup(ctx, tbl, g)
		require.NoError(t, err)
		results = append(results, gr)
		dvCount += len(gr.SafeDeletionVectors)
	}
	require.Positive(t, dvCount, "worker must collect the DV for removal")

	tx := tbl.NewTransaction()
	rewrite := tx.NewRewrite(nil)
	for _, gr := range results {
		rewrite.ApplyResult(gr)
	}
	require.NoError(t, rewrite.Commit(ctx))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	assertRowCount(t, tbl, 3)
	assert.Empty(t, deleteEntriesReferencing(t, tbl, rewritten),
		"coordinator commit must remove deletion vectors for rewritten data files")
	assert.Equal(t, before, rowIDByID(t, tbl),
		"the coordinator path must preserve survivors' _row_id")
}

// TestRewriteDataFilesPreservesSiblingDeletionVector pins the granularity of DV
// removal: one Puffin file holds DV blobs for two data files (a shared path),
// but only one of those data files is rewritten. The rewritten file's DV must
// be expunged while the sibling DV — sharing the same Puffin path — must
// survive. A removal keyed by path instead of referenced data file would drop
// both and silently resurrect the sibling's deleted rows.
func TestRewriteDataFilesPreservesSiblingDeletionVector(t *testing.T) {
	ctx := context.Background()
	fs := iceio.LocalFS{}

	tbl := appendTwoDataFiles(t, newV3RowLineageTestTable(t))

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	rewriteTarget := tasks[0].File.FilePath()
	sibling := tasks[1].File.FilePath()

	// One Flush writes a single Puffin holding a DV blob for each data file,
	// so both manifest entries share the Puffin path. Each deletes id=1 / id=3.
	w := dv.NewDVWriter(fs, unpartitionedSpecByID)
	require.NoError(t, w.Add(rewriteTarget, []int64{0}, 0, nil))
	require.NoError(t, w.Add(sibling, []int64{0}, 0, nil))
	dvFiles, err := w.Flush(ctx, filepath.Join(filepath.Dir(rewriteTarget), "dv-shared.puffin"))
	require.NoError(t, err)
	require.Len(t, dvFiles, 2)
	require.Equal(t, dvFiles[0].FilePath(), dvFiles[1].FilePath(),
		"both DV blobs must live in one shared Puffin file for this test to be meaningful")

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dvFiles...).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	assertRowCount(t, tbl, 2) // id=1 and id=3 deleted

	tasks, err = tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)

	var group table.CompactionTaskGroup
	for _, tk := range tasks {
		if tk.File.FilePath() == rewriteTarget {
			group = table.CompactionTaskGroup{
				PartitionKey:   "p",
				Tasks:          []table.FileScanTask{tk},
				TotalSizeBytes: tk.File.FileSizeBytes(),
			}
		}
	}
	require.NotEmpty(t, group.Tasks)

	rtx := tbl.NewTransaction()
	_, err = rtx.RewriteDataFiles(ctx, []table.CompactionTaskGroup{group}, table.RewriteDataFilesOptions{})
	require.NoError(t, err)
	tbl, err = rtx.Commit(ctx)
	require.NoError(t, err)

	assert.Empty(t, deleteEntriesReferencing(t, tbl, map[string]struct{}{rewriteTarget: {}}),
		"the rewritten file's DV must be expunged")
	assert.Len(t, deleteEntriesReferencing(t, tbl, map[string]struct{}{sibling: {}}), 1,
		"the sibling DV in the same Puffin must survive")
	assertRowCount(t, tbl, 2) // sibling DV still applies: id=3 stays deleted
}

// seedV3TableWithDV builds a v3 table with two data files ([1,2] and [3,4]) and
// commits a deletion vector deleting id=1 from the first. Returns the table and
// the DV's target data-file path.
func seedV3TableWithDV(t *testing.T) (*table.Table, string) {
	t.Helper()
	ctx := context.Background()
	fs := iceio.LocalFS{}

	tbl := appendTwoDataFiles(t, newV3RowLineageTestTable(t))

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	// PlanFiles order is not stable, so pick the first-appended file (the one
	// holding ids 1,2) by its first_row_id. Its row at position 0 is id=1, so
	// the DV below deterministically deletes id=1.
	target := tasks[0]
	for _, tk := range tasks[1:] {
		if tk.FirstRowID != nil && (target.FirstRowID == nil || *tk.FirstRowID < *target.FirstRowID) {
			target = tk
		}
	}
	require.NotNil(t, target.FirstRowID)
	require.Zero(t, *target.FirstRowID, "first-appended file must have first_row_id 0")
	dvTarget := target.File.FilePath()

	w := dv.NewDVWriter(fs, unpartitionedSpecByID)
	require.NoError(t, w.Add(dvTarget, []int64{0}, 0, nil))
	dvFiles, err := w.Flush(ctx, filepath.Join(filepath.Dir(dvTarget), "dv-0001.puffin"))
	require.NoError(t, err)
	require.Len(t, dvFiles, 1)
	require.Equal(t, dvTarget, *dvFiles[0].ReferencedDataFile())

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dvFiles...).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	assertRowCount(t, tbl, 3)

	return tbl, dvTarget
}

// appendTwoDataFiles appends [1,2] and [3,4] as two separate data files. The
// returned table reflects both commits.
func appendTwoDataFiles(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()
	ctx := context.Background()
	mem := memory.DefaultAllocator

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	for _, rows := range []string{
		`[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}]`,
		`[{"id": 3, "data": "c"}, {"id": 4, "data": "d"}]`,
	} {
		tab, err := array.TableFromJSON(mem, arrowSchema, []string{rows})
		require.NoError(t, err)
		tbl, err = tbl.Append(ctx, array.NewTableReader(tab, -1), nil)
		tab.Release()
		require.NoError(t, err)
	}

	return tbl
}

// planDVCompaction plans a compaction forced by the delete threshold and
// returns the task groups plus the set of data-file paths being rewritten,
// asserting the DV target is among them.
func planDVCompaction(t *testing.T, tbl *table.Table, dvTarget string) ([]table.CompactionTaskGroup, map[string]struct{}) {
	t.Helper()

	tasks, err := tbl.Scan().PlanFiles(context.Background())
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	groups := toTaskGroups(plan.Groups)
	rewritten := map[string]struct{}{}
	for _, g := range groups {
		for _, tk := range g.Tasks {
			rewritten[tk.File.FilePath()] = struct{}{}
		}
	}
	require.Contains(t, rewritten, dvTarget,
		"the DV's referenced data file must be in the rewrite set for this test to be meaningful")

	return groups, rewritten
}

func unpartitionedSpecByID(id int32) *iceberg.PartitionSpec {
	if id == 0 {
		return iceberg.UnpartitionedSpec
	}

	return nil
}

// deleteEntriesReferencing walks the current snapshot's delete manifests and
// returns live entries whose referenced_data_file is in refs — i.e. deletion
// vectors still bound to those data files.
func deleteEntriesReferencing(t *testing.T, tbl *table.Table, refs map[string]struct{}) []string {
	t.Helper()

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)

	fs := iceio.LocalFS{}
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	var found []string
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if e.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			ref := e.DataFile().ReferencedDataFile()
			if ref == nil {
				continue
			}
			if _, ok := refs[*ref]; ok {
				found = append(found, e.DataFile().FilePath()+" -> "+*ref)
			}
		}
	}

	return found
}
