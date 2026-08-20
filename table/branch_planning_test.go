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

// Black-box coverage for branch-aware read-side planning. Every fixture gives
// the branch and main a file the other lacks, so a plan resolved against main is
// observably wrong rather than accidentally identical.

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const branchPlanningRef = "feature"

var branchPlanningIdent = table.Identifier{"db", "branch_planning"}

func branchPlanningFS(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

// newBranchPlanningMetadata builds the fixture's single-column metadata in a
// fresh location. props overrides the defaults: format version 2, copy-on-write deletes.
func newBranchPlanningMetadata(t *testing.T, props iceberg.Properties) (location string, meta table.Metadata) {
	t.Helper()

	location = filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	tblProps := iceberg.Properties{table.PropertyFormatVersion: "2"}
	maps.Copy(tblProps, props)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, tblProps)
	require.NoError(t, err)

	return location, meta
}

// newBranchPlanningTable wraps that metadata in a table whose catalog stub
// validates commit requirements.
func newBranchPlanningTable(t *testing.T, props iceberg.Properties) *table.Table {
	t.Helper()

	location, meta := newBranchPlanningMetadata(t, props)
	metaLoc := location + "/metadata/v1.metadata.json"
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: branchPlanningFS}

	return table.New(branchPlanningIdent, meta, metaLoc, branchPlanningFS, cat)
}

// idRecordReader returns a reader the caller must release.
func idRecordReader(t *testing.T, tbl *table.Table, ids ...int64) array.RecordReader {
	t.Helper()

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	rows := make([]string, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, fmt.Sprintf(`{"id":%d}`, id))
	}

	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{"[" + strings.Join(rows, ",") + "]"})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	return array.NewTableReader(data, -1)
}

func appendRowsOnRef(t *testing.T, tbl *table.Table, ref string, ids ...int64) *table.Table {
	t.Helper()

	txn, err := tbl.NewTransactionOnBranchWithError(ref)
	require.NoError(t, err)

	rdr := idRecordReader(t, tbl, ids...)
	defer rdr.Release()

	require.NoError(t, txn.Append(t.Context(), rdr, nil))
	committed, err := txn.Commit(t.Context())
	require.NoError(t, err)

	return committed
}

// seedDivergentBranch leaves main holding {1,2,5,6} and the branch {1,2,3,4},
// so each ref owns one data file the other does not reference.
func seedDivergentBranch(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()

	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 3, 4)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 5, 6)

	require.ElementsMatch(t, []int64{1, 2, 3, 4}, idsOnRef(t, tbl, branchPlanningRef))
	require.ElementsMatch(t, []int64{1, 2, 5, 6}, idsOnRef(t, tbl, table.MainBranch))

	return tbl
}

func idsOnRef(t *testing.T, tbl *table.Table, ref string) []int64 {
	t.Helper()

	scan, err := tbl.Scan().UseRef(ref)
	require.NoError(t, err)

	_, itr, err := scan.ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		col, ok := rec.Column(idIdx[0]).(*array.Int64)
		require.True(t, ok, "id column must be int64")
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	slices.Sort(ids)

	return ids
}

func liveFilesOnRef(t *testing.T, tbl *table.Table, ref string) (dataPaths, deletePaths []string) {
	t.Helper()

	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)

	snapshot := tbl.SnapshotByName(ref)
	require.NotNil(t, snapshot, "ref %q must have a head", ref)

	manifests, err := snapshot.Manifests(fs)
	require.NoError(t, err)

	for _, m := range manifests {
		for entry, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			df := entry.DataFile()
			if df.ContentType() == iceberg.EntryContentData {
				dataPaths = append(dataPaths, df.FilePath())
			} else {
				deletePaths = append(deletePaths, df.FilePath())
			}
		}
	}

	return dataPaths, deletePaths
}

// liveDVCountOnRef counts the live deletion vectors reachable from ref's head.
func liveDVCountOnRef(t *testing.T, tbl *table.Table, ref string) int {
	t.Helper()

	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)

	snapshot := tbl.SnapshotByName(ref)
	require.NotNil(t, snapshot)

	manifests, err := snapshot.Manifests(fs)
	require.NoError(t, err)

	count := 0
	for _, m := range manifests {
		for entry, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			if df := entry.DataFile(); table.IsDeletionVector(df) && df.ReferencedDataFile() != nil {
				count++
			}
		}
	}

	return count
}

// branchOnlyDataPath returns the data file only the branch references.
func branchOnlyDataPath(t *testing.T, tbl *table.Table) string {
	t.Helper()

	branchPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	mainPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)

	var only []string
	for _, p := range branchPaths {
		if !slices.Contains(mainPaths, p) {
			only = append(only, p)
		}
	}
	require.Len(t, only, 1, "fixture must give the branch exactly one file of its own")

	return only[0]
}

// mainOnlyDataPath returns the data file only main references.
func mainOnlyDataPath(t *testing.T, tbl *table.Table) string {
	t.Helper()

	branchPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	mainPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)

	var only []string
	for _, p := range mainPaths {
		if !slices.Contains(branchPaths, p) {
			only = append(only, p)
		}
	}
	require.Len(t, only, 1, "fixture must give main exactly one file of its own")

	return only[0]
}

// Planning against main leaves the branch-only file untouched, so rows the
// filter matched survive the delete.
func TestBranchDeleteClassifiesBranchFiles(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))

	txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.Delete(t.Context(), iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(3)), nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, branchPlanningRef),
		"the branch-only file holding {3,4} matches the filter and must be deleted")
	assert.Equal(t, []int64{1, 2, 5, 6}, idsOnRef(t, tbl, table.MainBranch),
		"a delete on the branch must not move or rewrite main")
}

// A partial match drives the rewrite arm of classification, which reads the
// source file and writes its survivors back.
func TestBranchDeleteRewritesPartiallyMatchedBranchFile(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))

	txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 4}, idsOnRef(t, tbl, branchPlanningRef),
		"the branch-only file must be rewritten without id 3")
	assert.Equal(t, []int64{1, 2, 5, 6}, idsOnRef(t, tbl, table.MainBranch))
}

// A full overwrite must replace everything the branch references; a main-based
// plan never marks the branch-only file deleted.
func TestBranchOverwriteReplacesBranchOnlyFiles(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))

	txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)

	rdr := idRecordReader(t, tbl, 9)
	defer rdr.Release()
	require.NoError(t, txn.Overwrite(t.Context(), rdr, nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []int64{9}, idsOnRef(t, tbl, branchPlanningRef),
		"a full overwrite must retain nothing the branch previously referenced")
	assert.Equal(t, []int64{1, 2, 5, 6}, idsOnRef(t, tbl, table.MainBranch))
}

// The first write to a new branch forks from main's head, so the plan must come
// from that same head or the fork's files survive an overwrite of the branch.
func TestOverwriteCreatingBranchPlansAgainstMainHead(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2)
	require.Nil(t, tbl.SnapshotByName(branchPlanningRef), "the branch must not exist yet")

	txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)

	rdr := idRecordReader(t, tbl, 9)
	defer rdr.Release()
	require.NoError(t, txn.Overwrite(t.Context(), rdr, nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	mainHead := tbl.SnapshotByName(table.MainBranch)
	branchHead := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, mainHead)
	require.NotNil(t, branchHead)
	require.NotNil(t, branchHead.ParentSnapshotID)
	assert.Equal(t, mainHead.SnapshotID, *branchHead.ParentSnapshotID, "the new branch forks from main's head")
	assert.Equal(t, []int64{9}, idsOnRef(t, tbl, branchPlanningRef),
		"the fork's files were the plan's input, so none may survive the overwrite")
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
}

// A losing attempt replays the plan it already built, and must rebase on the
// branch's own fresh head: a peer advancing main between attempts may neither
// reparent the snapshot nor leak its file onto the branch.
func TestBranchOverwriteReplaysOnBranchHeadAfterConflict(t *testing.T) {
	location, meta := newBranchPlanningMetadata(t, iceberg.Properties{
		table.CommitMinRetryWaitMsKey: "0",
		table.CommitMaxRetryWaitMsKey: "0",
		table.CommitNumRetriesKey:     "2",
	})
	metaLoc := location + "/metadata/v1.metadata.json"

	seedCat := &occScenarioCatalog{current: meta, location: location}
	tbl := table.New(branchPlanningIdent, meta, metaLoc, branchPlanningFS, seedCat)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 3, 4)

	branchHeadBefore := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, branchHeadBefore)

	appendOnMain := func(current table.Metadata) table.Metadata {
		peerCat := &occScenarioCatalog{current: current, location: location}
		peerTbl := table.New(branchPlanningIdent, current, metaLoc, branchPlanningFS, peerCat)
		appendRowsOnRef(t, peerTbl, table.MainBranch, 7)

		return peerCat.current
	}

	cat := &occScenarioCatalog{
		current: seedCat.current, conflictsLeft: 1, location: location, onConflict: appendOnMain,
	}
	writer := table.New(branchPlanningIdent, seedCat.current, metaLoc, branchPlanningFS, cat)

	txn, err := writer.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	rdr := idRecordReader(t, writer, 9)
	defer rdr.Release()
	require.NoError(t, txn.Overwrite(t.Context(), rdr, nil))
	committed, err := txn.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, int32(2), cat.commitTableCalls.Load(), "one conflict, then success")

	branchHead := committed.Metadata().SnapshotByName(branchPlanningRef)
	require.NotNil(t, branchHead)
	require.NotNil(t, branchHead.ParentSnapshotID)
	assert.Equal(t, branchHeadBefore.SnapshotID, *branchHead.ParentSnapshotID,
		"the rebuilt snapshot must stay parented on the branch head")
	assert.Equal(t, []int64{9}, idsOnRef(t, committed, branchPlanningRef),
		"the overwrite plan must still hold after the replay")
	assert.Equal(t, []int64{1, 2, 7}, idsOnRef(t, committed, table.MainBranch),
		"the peer's row belongs to main alone")
}

// Filtered overwrite classifies through a different manifest walk than the
// unfiltered one, so it needs its own coverage.
func TestBranchFilteredOverwriteClassifiesBranchFiles(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))

	txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)

	rdr := idRecordReader(t, tbl, 7)
	defer rdr.Release()
	require.NoError(t, txn.Overwrite(t.Context(), rdr, nil,
		table.WithOverwriteFilter(iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(3)))))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 7}, idsOnRef(t, tbl, branchPlanningRef),
		"only the branch file matching the filter must be replaced")
	assert.Equal(t, []int64{1, 2, 5, 6}, idsOnRef(t, tbl, table.MainBranch))
}

// The duplicate check must ask the branch in both directions: main never seeing
// a file does not make it addable, and main holding one does not block it.
func TestBranchAddDataFilesDuplicateCheckUsesBranchHead(t *testing.T) {
	t.Run("rejects a file the branch already references", func(t *testing.T) {
		tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
		branchOnly := branchOnlyDataPath(t, tbl)

		txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)

		err = txn.AddDataFiles(t.Context(), []iceberg.DataFile{buildDataFile(t, branchOnly)}, nil)
		require.ErrorContains(t, err, "cannot add files that are already referenced by table")
		assert.Contains(t, err.Error(), branchOnly)
	})

	t.Run("accepts a file only main references", func(t *testing.T) {
		tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
		mainOnly := mainOnlyDataPath(t, tbl)

		txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)

		require.NoError(t, txn.AddDataFiles(t.Context(), []iceberg.DataFile{buildDataFile(t, mainOnly)}, nil))
		tbl, err = txn.Commit(t.Context())
		require.NoError(t, err)

		branchPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
		assert.Contains(t, branchPaths, mainOnly, "the branch did not reference the file, so the add must land")
	})
}

// AddFiles runs the same check from paths, through its own snapshot lookup.
func TestBranchAddFilesDuplicateCheckUsesBranchHead(t *testing.T) {
	t.Run("rejects a file the branch already references", func(t *testing.T) {
		tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
		branchOnly := branchOnlyDataPath(t, tbl)

		txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)

		err = txn.AddFiles(t.Context(), []string{branchOnly}, nil, false)
		require.ErrorContains(t, err, "cannot add files that are already referenced by table")
		assert.Contains(t, err.Error(), branchOnly)
	})

	t.Run("accepts a file only main references", func(t *testing.T) {
		tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
		mainOnly := mainOnlyDataPath(t, tbl)

		txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)

		require.NoError(t, txn.AddFiles(t.Context(), []string{mainOnly}, nil, false))
		tbl, err = txn.Commit(t.Context())
		require.NoError(t, err)

		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, idsOnRef(t, tbl, branchPlanningRef),
			"the branch must gain the rows of main's file")
	})
}

// A table written only through a branch has no main head, so a main-based plan
// rejects the replace outright instead of performing it.
func TestReplaceOnBranchOnlyTable(t *testing.T) {
	t.Run("ReplaceDataFiles", func(t *testing.T) {
		tbl := newBranchPlanningTable(t, nil)
		tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 1, 2)
		require.Nil(t, tbl.CurrentSnapshot(), "fixture must leave main without a head")

		original, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
		require.Len(t, original, 1)

		replacement := tbl.Location() + "/data/replacement.parquet"
		arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
		require.NoError(t, err)
		writeParquetFile(t, replacement, arrowSchema, `[{"id":8}]`)

		txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)
		require.NoError(t, txn.ReplaceDataFiles(t.Context(), original, []string{replacement}, nil))
		tbl, err = txn.Commit(t.Context())
		require.NoError(t, err)

		assert.Equal(t, []int64{8}, idsOnRef(t, tbl, branchPlanningRef))
		assert.Nil(t, tbl.CurrentSnapshot(), "replacing on the branch must not create a main head")
	})

	t.Run("ReplaceDataFilesWithDataFiles", func(t *testing.T) {
		tbl := newBranchPlanningTable(t, nil)
		staged := buildDataFile(t, tbl.Location()+"/data/branch-1.parquet")

		txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)
		require.NoError(t, txn.AddDataFiles(t.Context(), []iceberg.DataFile{staged}, nil))
		tbl, err = txn.Commit(t.Context())
		require.NoError(t, err)
		require.Nil(t, tbl.CurrentSnapshot(), "fixture must leave main without a head")

		replacement := buildDataFile(t, tbl.Location()+"/data/branch-2.parquet")
		txn, err = tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)
		require.NoError(t, txn.ReplaceDataFilesWithDataFiles(t.Context(),
			[]iceberg.DataFile{staged}, []iceberg.DataFile{replacement}, nil))
		tbl, err = txn.Commit(t.Context())
		require.NoError(t, err)

		dataPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
		assert.Equal(t, []string{replacement.FilePath()}, dataPaths)
	})
}

// ReplaceFiles validates the data files and the delete files to remove in one
// pass; both live only on the branch here.
func TestBranchReplaceFilesResolvesBranchDeleteFiles(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
	mainDataPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)

	staged := buildDataFile(t, tbl.Location()+"/data/branch-staged.parquet")
	posDelete := buildPosDeleteFile(t, tbl.Location()+"/data/branch-pos-delete.parquet")

	txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.NewRowDelta(nil).AddRows(staged).AddDeletes(posDelete).Commit(t.Context()))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	replacement := buildDataFile(t, tbl.Location()+"/data/branch-compacted.parquet")
	txn, err = tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{staged}, []iceberg.DataFile{replacement}, []iceberg.DataFile{posDelete}, nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	branchData, branchDeletes := liveFilesOnRef(t, tbl, branchPlanningRef)
	assert.Contains(t, branchData, replacement.FilePath())
	assert.NotContains(t, branchData, staged.FilePath())
	assert.Empty(t, branchDeletes, "the branch's delete file must be removed by the rewrite")

	mainDataAfter, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	assert.ElementsMatch(t, mainDataPaths, mainDataAfter, "a rewrite on the branch must not touch main")
}

// A second delete folds into the existing vector and supersedes it. Missing the
// branch's vector strands two live vectors on one data file, which violates the
// spec and resurrects the first delete.
func TestBranchMergeOnReadDeleteMergesBranchDeletionVector(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{
		table.PropertyFormatVersion: "3",
		table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
	})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2, 3, 4, 5)

	for _, id := range []int64{2, 3} {
		txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
		require.NoError(t, err)
		require.NoError(t, txn.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), id), nil))
		tbl, err = txn.Commit(t.Context())
		require.NoError(t, err)
	}

	assert.Equal(t, 1, liveDVCountOnRef(t, tbl, branchPlanningRef),
		"the second delete must merge into the branch's existing deletion vector")
	assert.Equal(t, []int64{1, 4, 5}, idsOnRef(t, tbl, branchPlanningRef),
		"both deletes must hold; a fresh vector would resurrect id 2")
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, idsOnRef(t, tbl, table.MainBranch),
		"deletes on the branch must not change main")
}

// Merge-on-read classifies files before writing any vector: classified against
// main, a branch-only file matches nothing and the delete hides no row.
func TestBranchMergeOnReadDeleteTargetsBranchOnlyFile(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{
		table.PropertyFormatVersion: "3",
		table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
	})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 3, 4)

	txn, err := tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 4}, idsOnRef(t, tbl, branchPlanningRef))
	assert.Equal(t, 1, liveDVCountOnRef(t, tbl, branchPlanningRef),
		"the delete must write a vector against the branch-only file")
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
}

// RowDelta resolves the vectors it removes against the planning snapshot's
// delete manifests, where a branch-staged vector is invisible on main.
func TestBranchRowDeltaResolvesRemovedDeletesOnBranchHead(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{table.PropertyFormatVersion: "3"})
	location := tbl.Location()

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	dataPath := location + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSchema, `[{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}]`)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	// The first vector (hiding position 1, id 2) exists only on the branch.
	dv1 := writeDV(t, location, "dv-001.puffin", dataPath, []int64{1})
	txn, err = tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.NewRowDelta(nil).AddDeletes(dv1).Commit(t.Context()))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 4, 5}, idsOnRef(t, tbl, branchPlanningRef))

	dv2 := writeDV(t, location, "dv-002.puffin", dataPath, []int64{1, 3})
	txn, err = tbl.NewTransactionOnBranchWithError(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.NewRowDelta(nil).AddDeletes(dv2).RemoveDeletes(dv1).Commit(t.Context()))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	branchHead := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, branchHead)
	live, removed := snapshotDVEntries(t, branchHead, iceio.LocalFS{})
	assert.Equal(t, []string{dv2.FilePath()}, live, "the replacement must be the branch's only live vector")
	assert.Equal(t, []string{dv1.FilePath()}, removed, "the superseded vector must be recorded as removed")

	assert.Equal(t, []int64{1, 3, 5}, idsOnRef(t, tbl, branchPlanningRef))
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, idsOnRef(t, tbl, table.MainBranch),
		"row-delta supersession on the branch must not change main")
}
