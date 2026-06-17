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
// leaving no manifest entry referencing the removed data file.
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
	ctx := context.Background()
	mem := memory.DefaultAllocator
	fs := iceio.LocalFS{}

	tbl := newV3RowLineageTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// Two appends produce two data files so the planner forms a compaction group.
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
	assertRowCount(t, tbl, 4)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	dvTarget := tasks[0].File.FilePath()

	// Author a real DV (puffin) deleting position 0 (id=1) of the first data
	// file, then commit it as a delete manifest entry.
	specByID := func(id int32) *iceberg.PartitionSpec {
		if id == 0 {
			return iceberg.UnpartitionedSpec
		}

		return nil
	}
	w := dv.NewDVWriter(fs, specByID)
	w.Add(dvTarget, []int64{0}, 0, nil)
	dvFiles, err := w.Flush(ctx, filepath.Join(filepath.Dir(dvTarget), "dv-0001.puffin"))
	require.NoError(t, err)
	require.Len(t, dvFiles, 1)
	require.Equal(t, dvTarget, *dvFiles[0].ReferencedDataFile())

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dvFiles...).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	assertRowCount(t, tbl, 3) // id=1 deleted by the DV

	tasks, err = tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1 // force compaction of the DV-bearing file
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	rewritten := map[string]struct{}{}
	for _, g := range plan.Groups {
		for _, tk := range g.Tasks {
			rewritten[tk.File.FilePath()] = struct{}{}
		}
	}
	require.Contains(t, rewritten, dvTarget,
		"the DV's referenced data file must be in the rewrite set for this test to exercise the bug")

	rtx := tbl.NewTransaction()
	_, err = rtx.RewriteDataFiles(ctx, toTaskGroups(plan.Groups), table.RewriteDataFilesOptions{})
	require.NoError(t, err)
	tbl, err = rtx.Commit(ctx)
	require.NoError(t, err)

	assertRowCount(t, tbl, 3) // DV applied during the rewrite; id=1 stays gone

	orphans := deleteEntriesReferencing(t, tbl, rewritten)
	assert.Empty(t, orphans,
		"compaction must remove deletion vectors for rewritten data files; "+
			"these DV entries still reference files removed by the rewrite: %v", orphans)
}

// TestRewriteDataFilesPreservesSiblingDeletionVector pins the granularity of DV
// removal: one Puffin file holds DV blobs for two data files (a shared path),
// but only one of those data files is rewritten. The rewritten file's DV must
// be expunged while the sibling DV — sharing the same Puffin path — must
// survive. A removal keyed by path instead of referenced data file would drop
// both and silently resurrect the sibling's deleted rows.
func TestRewriteDataFilesPreservesSiblingDeletionVector(t *testing.T) {
	ctx := context.Background()
	mem := memory.DefaultAllocator
	fs := iceio.LocalFS{}

	tbl := newV3RowLineageTestTable(t)

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

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	rewriteTarget := tasks[0].File.FilePath()
	sibling := tasks[1].File.FilePath()

	specByID := func(id int32) *iceberg.PartitionSpec {
		if id == 0 {
			return iceberg.UnpartitionedSpec
		}

		return nil
	}

	// One Flush writes a single Puffin holding a DV blob for each data file,
	// so both manifest entries share the Puffin path. Each deletes id=1 / id=3.
	w := dv.NewDVWriter(fs, specByID)
	w.Add(rewriteTarget, []int64{0}, 0, nil)
	w.Add(sibling, []int64{0}, 0, nil)
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

// deleteEntriesReferencing walks the current snapshot's delete manifests and
// returns live entries whose referenced_data_file is in rewritten — i.e.
// deletion vectors orphaned by a rewrite that removed their data file.
func deleteEntriesReferencing(t *testing.T, tbl *table.Table, rewritten map[string]struct{}) []string {
	t.Helper()

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)

	fs := iceio.LocalFS{}
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	var orphans []string
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
			if _, ok := rewritten[*ref]; ok {
				orphans = append(orphans, e.DataFile().FilePath()+" -> "+*ref)
			}
		}
	}

	return orphans
}
