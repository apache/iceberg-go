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
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergeOnReadDeleteAcrossPrunedRowGroups guards the row-group pruning hazard
// in generated position deletes: a merge-on-read DELETE whose filter prunes a
// leading row group must still delete the right physical rows. enrichRecords-
// WithPosDeleteFields stamps each surviving row's position from a rowPositionCursor
// seeded with the surviving row groups, so a pruned group's rows are still counted
// and the survivors keep their original file positions.
//
// The file has two row groups (ids 1..5, then 6..10). Deleting id == 7 matches
// only the second group, so the first is pruned by stats; a dense position
// count would target physical position 1 (id=2) instead of 6 (id=7).
func TestMergeOnReadDeleteAcrossPrunedRowGroups(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},` +
			`{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},` +
			`{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, idsInTable(t, tbl))

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 8, 9, 10}, idsInTable(t, tbl),
		"only id=7 must be deleted; a pruned leading row group must not shift positions")
}

// newMergeOnReadTestTable builds a v2 table that deletes via merge-on-read and
// caps row groups at 5 rows so a 10-row append spans two of them.
func newMergeOnReadTestTable(t *testing.T) *table.Table {
	t.Helper()

	return newMergeOnReadTestTableVersion(t, "2")
}

// newMergeOnReadTestTableVersion builds a merge-on-read table at the given
// format version, capping row groups at 5 rows so a 10-row append spans two.
func newMergeOnReadTestTableVersion(t testing.TB, formatVersion string) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:   formatVersion,
			table.WriteDeleteModeKey:      table.WriteModeMergeOnRead,
			table.ParquetRowGroupLimitKey: "5",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "mor_delete_test"}, meta, metaLoc, fsF, cat)
}

// TestV3MergeOnReadDoubleDeleteMergesDeletionVector guards against a second
// merge-on-read DELETE on a data file that already has a deletion vector
// writing a second live DV. The spec permits at most one DV per data file, so
// the new deletes must merge into the existing DV (which is then superseded),
// not accumulate as a parallel one. A second live DV corrupts the table:
// buildDVIndex rejects it at scan time with "can't index multiple deletion
// vectors". See issue #1372.
func TestV3MergeOnReadDoubleDeleteMergesDeletionVector(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTableVersion(t, "3")

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	// First delete writes DV #1 against the single data file.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 4, 5}, idsInTable(t, tbl))
	require.Equal(t, 1, liveDVCount(t, tbl), "first delete must write exactly one DV")

	// Second delete on the same data file must merge into DV #1, not add a
	// second live DV.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
	require.NoError(t, err)

	require.Equal(t, 1, liveDVCount(t, tbl),
		"the two deletes must collapse into a single merged deletion vector")
	assert.Equal(t, []int64{1, 3, 5}, idsInTable(t, tbl),
		"both id=2 and id=4 must be deleted and no previously deleted row may resurrect")
}

// TestV3MergeOnReadDeleteWithoutNewPositionsKeepsDV checks that a delete
// matching only already-deleted positions does not remove or rewrite its DV.
// Stats may still select the file even when no live row matches.
func TestV3MergeOnReadDeleteWithoutNewPositionsKeepsDV(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`)

	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, idsInTable(t, tbl))
	require.Equal(t, 1, liveDVCount(t, tbl))

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeletionVectorFiles, 1)
	originalDVPath := tasks[0].DeletionVectorFiles[0].FilePath()

	// This filter only matches the row already deleted by the first DV.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, idsInTable(t, tbl),
		"an empty delete must not resurrect the originally deleted row")
	require.Equal(t, 1, liveDVCount(t, tbl), "the existing DV must stay live")

	tasks, err = tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeletionVectorFiles, 1)
	require.Equal(t, originalDVPath, tasks[0].DeletionVectorFiles[0].FilePath(),
		"an empty delete must not replace or drop the existing DV")
}

// TestV3MergeOnReadTripleDeleteMergesDeletionVector extends the double-delete
// guard to a third delete. The second delete supersedes DV #1, leaving it as a
// DELETED-status entry in the deleted-files manifest against the same data file.
// The third delete's readPlanningDeletes must skip that ghost entry; otherwise it
// seeds the merged DV from the stale first bitmap (resurrecting id=4, removed by
// the second delete) and tries to remove a DV that no longer exists. See #1372.
func TestV3MergeOnReadTripleDeleteMergesDeletionVector(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTableVersion(t, "3")

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCount(t, tbl), "first delete must write exactly one DV")

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCount(t, tbl), "second delete must merge into the single DV")

	// Third delete: DV #1 is now a superseded DELETED entry. readPlanningDeletes
	// must ignore it and seed only from the live merged DV.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)

	require.Equal(t, 1, liveDVCount(t, tbl),
		"the three deletes must collapse into a single merged deletion vector")
	assert.Equal(t, []int64{3, 5}, idsInTable(t, tbl),
		"id=1, id=2 and id=4 must all be deleted and none may resurrect")
}

// TestV3MergeOnReadDoubleDeletePartitionedMergesDeletionVectors runs the merge
// on a partitioned table where a single delete touches two data files that each
// already carry a DV. It exercises the per-file partition-context capture in the
// DV seed loop, which the single unpartitioned data file does not.
func TestV3MergeOnReadDoubleDeletePartitionedMergesDeletionVectors(t *testing.T) {
	ctx := context.Background()
	tbl := newPartitionedMergeOnReadTestTable(t)

	// Two categories produce two partition data files.
	tbl = appendRows(t, tbl,
		`[{"id":1,"category":"x"},{"id":2,"category":"x"},{"id":3,"category":"x"},`+
			`{"id":4,"category":"y"},{"id":5,"category":"y"},{"id":6,"category":"y"}]`)
	var err error
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, idsInTable(t, tbl))

	// Seed a DV on each partition's data file.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(5)), nil)
	require.NoError(t, err)
	require.Equal(t, 2, liveDVCount(t, tbl), "one live DV per partition data file")

	// A single delete touching both partitions must merge into each existing DV
	// rather than add parallel ones — seeding both from their own partition ctx.
	tbl, err = tbl.Delete(ctx, iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
		iceberg.EqualTo(iceberg.Reference("id"), int64(4)),
	), nil)
	require.NoError(t, err)

	require.Equal(t, 2, liveDVCount(t, tbl),
		"each partition data file must keep exactly one merged DV")
	assert.Equal(t, []int64{3, 6}, idsInTable(t, tbl),
		"ids 1,2 (x) and 4,5 (y) must be deleted and none may resurrect")
}

// newPartitionedMergeOnReadTestTable builds a v3 merge-on-read table partitioned
// by identity(category) so distinct category values land in distinct data files.
func newPartitionedMergeOnReadTestTable(t *testing.T) *table.Table {
	return newPartitionedMergeOnReadTestTableVersion(t, "3")
}

func newPartitionedMergeOnReadTestTableVersion(t testing.TB, formatVersion string) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Transform: iceberg.IdentityTransform{},
		Name:      "category",
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: formatVersion,
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "mor_delete_partitioned_test"}, meta, metaLoc, fsF, cat)
}

// liveDVCount returns the number of live deletion-vector entries in the table's
// current snapshot, which is main's head.
func liveDVCount(t *testing.T, tbl *table.Table) int {
	t.Helper()

	return liveDVCountOnRef(t, tbl, table.MainBranch)
}

// idsInTable scans the table and returns the surviving id values, sorted.
func idsInTable(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()

	_, itr, err := tbl.Scan().ToArrowRecords(context.Background())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		col := rec.Column(idIdx[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	slices.Sort(ids)

	return ids
}

// TestMergeOnReadDeleteSkipsRowsAlreadyDeleted checks that a v2 merge-on-read
// delete doesn't write position deletes for rows an earlier delete removed.
// Such deletes change nothing, but they grow the delete files on every update
// and make the snapshot's total-position-deletes overcount.
func TestMergeOnReadDeleteSkipsRowsAlreadyDeleted(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)

	tbl, err = tbl.Delete(ctx, iceberg.IsIn(iceberg.Reference("id"), int64(1), int64(2)), nil)
	require.NoError(t, err)

	assert.Equal(t, []int64{3}, idsInTable(t, tbl))
	summary := tbl.CurrentSnapshot().Summary.Properties
	assert.Equal(t, "1", summary["added-position-deletes"], "id=1 was already deleted")
	assert.Equal(t, "2", summary["total-position-deletes"])

	t.Run("branch", func(t *testing.T) {
		const branch = "audit"
		tbl := newMergeOnReadTestTable(t)
		tbl, err := tbl.Append(ctx, array.NewTableReader(data, -1), nil)
		require.NoError(t, err)

		// The branch forks from main here, before main deletes id=1.
		extra, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{`[{"id":4,"data":"d"}]`})
		require.NoError(t, err)
		defer extra.Release()
		txn, err := tbl.NewTransactionOnBranch(branch)
		require.NoError(t, err)
		require.NoError(t, txn.Append(ctx, array.NewTableReader(extra, -1), nil))
		tbl, err = txn.Commit(ctx)
		require.NoError(t, err)

		tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
		require.NoError(t, err)

		txn, err = tbl.NewTransactionOnBranch(branch)
		require.NoError(t, err)
		require.NoError(t, txn.Delete(ctx, iceberg.IsIn(iceberg.Reference("id"), int64(1), int64(2)), nil))
		tbl, err = txn.Commit(ctx)
		require.NoError(t, err)

		assert.Equal(t, []int64{2, 3}, idsInTable(t, tbl), "main only deleted id=1")
		scan, err := tbl.Scan().UseRef(branch)
		require.NoError(t, err)
		_, itr, err := scan.ToArrowRecords(ctx)
		require.NoError(t, err)
		var ids []int64
		for rec, err := range itr {
			require.NoError(t, err)
			col := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
			ids = append(ids, col.Int64Values()...)
			rec.Release()
		}
		slices.Sort(ids)
		assert.Equal(t, []int64{3, 4}, ids, "main's delete of id=1 doesn't count on the branch")
	})
}

// TestV3MergeOnReadUpgradePreservesPositionDeletes covers the first DV written
// after a v2 to v3 upgrade. The DV must include older position deletes, even
// when one position-delete file targets more than one data file.
func TestV3MergeOnReadUpgradePreservesPositionDeletes(t *testing.T) {
	tests := []struct {
		name        string
		batches     []string
		firstFilter iceberg.BooleanExpression
		wantV2      []int64
		wantV3      []int64
		shared      bool
	}{
		{
			name: "single file",
			batches: []string{
				`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`,
			},
			firstFilter: iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
			wantV2:      []int64{2, 3},
			wantV3:      []int64{3},
		},
		{
			name: "older position delete follows the new DV position",
			batches: []string{
				`[{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":1,"data":"a"}]`,
			},
			firstFilter: iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
			wantV2:      []int64{2, 3},
			wantV3:      []int64{3},
		},
		{
			name: "shared position delete file",
			batches: []string{
				`[{"id":1,"data":"a"},{"id":2,"data":"b"}]`,
				`[{"id":3,"data":"c"},{"id":4,"data":"d"}]`,
			},
			firstFilter: iceberg.IsIn(iceberg.Reference("id"), int64(1), int64(3)),
			wantV2:      []int64{2, 4},
			wantV3:      []int64{4},
			shared:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			tbl := newMergeOnReadTestTable(t)
			for _, batch := range tt.batches {
				tbl = appendRows(t, tbl, batch)
			}

			var err error
			tbl, err = tbl.Delete(ctx, tt.firstFilter, nil)
			require.NoError(t, err)
			require.Equal(t, tt.wantV2, idsInTable(t, tbl))

			if tt.shared {
				tasks, err := tbl.Scan().PlanFiles(ctx)
				require.NoError(t, err)
				require.Len(t, tasks, 2)
				require.Len(t, tasks[0].DeleteFiles, 1)
				require.Len(t, tasks[1].DeleteFiles, 1)
				require.Equal(t, tasks[0].DeleteFiles[0].FilePath(), tasks[1].DeleteFiles[0].FilePath())
			}

			tx := tbl.NewTransaction()
			require.NoError(t, tx.UpgradeFormatVersion(3))
			tbl, err = tx.Commit(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.wantV2, idsInTable(t, tbl))

			tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
			require.NoError(t, err)
			require.Equal(t, tt.wantV3, idsInTable(t, tbl),
				"the first v3 DV must preserve previously deleted rows")
			require.Equal(t, 1, liveDVCount(t, tbl))
			if tt.shared {
				require.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes),
					"a shared legacy delete file must remain live")
			} else {
				require.Zero(t, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes),
					"file-scoped legacy deletes must be removed after their DV replacement")
			}

			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			var withDV, withOldPositionDeletes int
			for _, task := range tasks {
				if len(task.DeletionVectorFiles) > 0 {
					withDV++
					require.Empty(t, task.DeleteFiles)
				}
				if len(task.DeleteFiles) > 0 {
					withOldPositionDeletes++
					require.Empty(t, task.DeletionVectorFiles)
				}
			}
			require.Equal(t, 1, withDV)
			if tt.shared {
				require.Equal(t, 1, withOldPositionDeletes,
					"the sibling file must still use the shared old position-delete file")
				tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
				require.NoError(t, err)
				require.Empty(t, idsInTable(t, tbl))
				require.Equal(t, 2, liveDVCount(t, tbl))
				require.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes),
					"the shared legacy file is still live after both data files have DVs")
			} else {
				require.Zero(t, withOldPositionDeletes)
			}
		})
	}
}

func TestV3MergeOnReadUpgradeReplansLegacyDeleteReplacementAfterConflict(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newMergeOnReadTestTable(t), `[{"id":1},{"id":2},{"id":3}]`)
	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes))

	upgrade := tbl.NewTransaction()
	require.NoError(t, upgrade.UpgradeFormatVersion(3))
	tbl, err = upgrade.Commit(ctx)
	require.NoError(t, err)

	filter := iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	staged := tbl.NewTransaction()
	require.NoError(t, staged.Delete(ctx, filter, nil))
	peer := appendRows(t, tbl, `[{"id":4}]`)
	_, err = staged.Commit(ctx)
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.Equal(t, 1, liveDeleteFileCount(t, peer, iceberg.EntryContentPosDeletes))
	require.Zero(t, liveDVCount(t, peer))

	tbl, err = peer.Delete(ctx, filter, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, idsInTable(t, tbl))
	require.Equal(t, 1, liveDVCount(t, tbl))
	require.Zero(t, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes))
}

func TestV3MergeOnReadUpgradeUsesBranchPositionDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := newBranchPlanningTable(t, iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2, 3, 4)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 5)

	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	tx, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4}, idsOnRef(t, tbl, table.MainBranch))
	require.Equal(t, []int64{1, 3, 4, 5}, idsOnRef(t, tbl, branchPlanningRef))

	upgrade := tbl.NewTransaction()
	require.NoError(t, upgrade.UpgradeFormatVersion(3))
	tbl, err = upgrade.Commit(ctx)
	require.NoError(t, err)
	_, mainDeletes := liveFilesOnRef(t, tbl, table.MainBranch)
	require.Len(t, mainDeletes, 1)

	tx, err = tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, tx.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	require.Equal(t, []int64{1, 4, 5}, idsOnRef(t, tbl, branchPlanningRef))
	require.Equal(t, 1, liveDVCountOnRef(t, tbl, branchPlanningRef))
	_, branchDeletes := liveFilesOnRef(t, tbl, branchPlanningRef)
	require.Len(t, branchDeletes, 1, "the branch's legacy delete should be replaced by its DV")
	require.Equal(t, []int64{2, 3, 4}, idsOnRef(t, tbl, table.MainBranch))
	_, remainingMainDeletes := liveFilesOnRef(t, tbl, table.MainBranch)
	require.Equal(t, mainDeletes, remainingMainDeletes)
}

func TestV3MergeOnReadUpgradeKeepsEqualityDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newMergeOnReadTestTable(t),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"}]`)
	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2}]`)
	require.Equal(t, []int64{3, 4}, idsInTable(t, tbl))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{4}, idsInTable(t, tbl))

	scan := tbl.Scan()
	defer scan.Close()
	tasks, err := scan.PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeletionVectorFiles, 1)
	require.EqualValues(t, 2, tasks[0].DeletionVectorFiles[0].Count(),
		"the DV should carry the old position and the newly deleted row")
	require.Empty(t, tasks[0].DeleteFiles)
	require.Len(t, tasks[0].EqualityDeleteFiles, 1, "the equality delete still applies independently")
}

// TestV3MergeOnReadUpgradeKeepsUntouchedFileScopedDeletes ensures a DV
// supersedes only legacy files for its own target. An old position-delete file
// for another data file must stay live until that file is rewritten as a DV.
func TestV3MergeOnReadUpgradeKeepsUntouchedFileScopedDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := newMergeOnReadTestTable(t)
	tbl = appendRows(t, tbl,
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`)
	tbl = appendRows(t, tbl,
		`[{"id":4,"data":"d"},{"id":5,"data":"e"},{"id":6,"data":"f"}]`)

	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 5, 6}, idsInTable(t, tbl))
	require.Equal(t, 2, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// Only the first data file gets a new DV. The sibling's old delete file
	// must still hide id=4, and must not be removed from the manifest.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 5, 6}, idsInTable(t, tbl))
	require.Equal(t, 1, liveDVCount(t, tbl))
	require.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes))

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	var dvTasks, legacyTasks int
	for _, task := range tasks {
		switch {
		case len(task.DeletionVectorFiles) == 1:
			dvTasks++
			require.Empty(t, task.DeleteFiles)
		case len(task.DeleteFiles) == 1:
			legacyTasks++
			require.Empty(t, task.DeletionVectorFiles)
		default:
			t.Fatalf("expected one DV or legacy position delete per data file: %+v", task)
		}
	}
	require.Equal(t, 1, dvTasks)
	require.Equal(t, 1, legacyTasks)

	// Replacing the other file's legacy delete should remove only that file.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(5)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 6}, idsInTable(t, tbl))
	require.Equal(t, 2, liveDVCount(t, tbl))
	require.Zero(t, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes))
}

// TestV3MergeOnReadMixedExistingDVsKeepsUnchangedFile guards the case
// where one candidate has no new matching rows but another gets new deletes.
// The first file's DV must not be removed or unnecessarily rewritten.
func TestV3MergeOnReadMixedExistingDVsKeepsUnchangedFile(t *testing.T) {
	ctx := t.Context()
	tbl := newPartitionedMergeOnReadTestTable(t)
	tbl = appendRows(t, tbl,
		`[{"id":1,"category":"x"},{"id":2,"category":"x"},{"id":3,"category":"x"},`+
			`{"id":4,"category":"y"},{"id":5,"category":"y"},{"id":6,"category":"y"}]`)
	var err error

	tbl, err = tbl.Delete(ctx, iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
		iceberg.EqualTo(iceberg.Reference("id"), int64(4)),
	), nil)
	require.NoError(t, err)
	require.Equal(t, 2, liveDVCount(t, tbl))
	beforeTasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, beforeTasks, 2)
	oldDVPaths := make(map[string]string, 2)
	for _, task := range beforeTasks {
		require.Len(t, task.DeletionVectorFiles, 1)
		oldDVPaths[task.File.FilePath()] = task.DeletionVectorFiles[0].FilePath()
	}

	// id=1 is already deleted in x; id=5 is new in y. Only y needs a new DV.
	tbl, err = tbl.Delete(ctx, iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
		iceberg.EqualTo(iceberg.Reference("id"), int64(5)),
	), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 6}, idsInTable(t, tbl))
	require.Equal(t, 2, liveDVCount(t, tbl))
	afterTasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, afterTasks, 2)
	var unchanged, replaced int
	for _, task := range afterTasks {
		require.Len(t, task.DeletionVectorFiles, 1)
		require.Empty(t, task.DeleteFiles)
		previousPath, ok := oldDVPaths[task.File.FilePath()]
		require.True(t, ok, "data files should not be rewritten by merge-on-read")
		if task.DeletionVectorFiles[0].FilePath() == previousPath {
			unchanged++
		} else {
			replaced++
		}
	}
	require.Equal(t, 1, unchanged, "the untouched file must retain its DV")
	require.Equal(t, 1, replaced, "only the file with new deletes gets a replacement DV")
}

// TestV3MergeOnReadUpgradeMergesMultiplePositionDeleteFiles checks that the
// first DV includes deletes written by separate v2 commits for the same file.
func TestV3MergeOnReadUpgradeMergesMultiplePositionDeleteFiles(t *testing.T) {
	ctx := t.Context()
	tbl := newMergeOnReadTestTable(t)
	tbl = appendRows(t, tbl, `[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`)

	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeleteFiles, 2, "each v2 delete must leave a position-delete file")
	require.NotEqual(t, tasks[0].DeleteFiles[0].FilePath(), tasks[0].DeleteFiles[1].FilePath())

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4, 5}, idsInTable(t, tbl))

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{4, 5}, idsInTable(t, tbl),
		"the first DV must include both older position-delete files")
	require.Equal(t, 1, liveDVCount(t, tbl))
	require.Zero(t, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes),
		"both file-scoped legacy delete files must be removed")

	tasks, err = tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeletionVectorFiles, 1)
	require.Empty(t, tasks[0].DeleteFiles,
		"the DV replaces both position-delete files for this data file")
}

// TestV3MergeOnReadUpgradeSkipsDVWhenDeleteFindsNoRows checks that legacy
// deletes do not become a DV when a later delete matches only already-deleted
// rows. Candidate files may pass stats pruning even when the scan returns no
// new position-delete records.
func TestV3MergeOnReadUpgradeSkipsDVWhenDeleteFindsNoRows(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newMergeOnReadTestTable(t),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`)

	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, idsInTable(t, tbl))

	// id=1 is already covered by the v2 position-delete file, so this delete
	// has no new positions to write as a DV.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, idsInTable(t, tbl))
	require.Zero(t, liveDVCount(t, tbl), "an empty delete must not write a DV")
	require.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes),
		"the original position-delete file must remain live")
}

// TestV3MergeOnReadUpgradePreservesPartitionedPositionDeletes checks that
// legacy delete positions stay attached to their own partitioned data files.
func TestV3MergeOnReadUpgradePreservesPartitionedPositionDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := newPartitionedMergeOnReadTestTableVersion(t, "2")

	tbl = appendRows(t, tbl,
		`[{"id":1,"category":"x"},{"id":2,"category":"x"},{"id":3,"category":"x"},`+
			`{"id":4,"category":"y"},{"id":5,"category":"y"},{"id":6,"category":"y"}]`)
	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 5, 6}, idsInTable(t, tbl))

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	for _, task := range tasks {
		require.Len(t, task.DeleteFiles, 1)
		require.Empty(t, task.DeletionVectorFiles)
	}

	// Evolve the spec while the old files and delete files are still live. The
	// first DVs must keep each data file's historical spec and partition.
	evolve := tbl.NewTransaction()
	require.NoError(t, evolve.UpdateSpec(true).AddIdentity("id").Commit())
	tbl, err = evolve.Commit(ctx)
	require.NoError(t, err)
	currentSpec := tbl.Spec()
	require.Equal(t, 1, currentSpec.ID())

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	tbl, err = tbl.Delete(ctx, iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("id"), int64(2)),
		iceberg.EqualTo(iceberg.Reference("id"), int64(5)),
	), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 6}, idsInTable(t, tbl),
		"each new DV must preserve deletes from its own partitioned data file")
	require.Equal(t, 2, liveDVCount(t, tbl))
	require.Zero(t, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes),
		"partitioned file-scoped deletes must be removed after DV replacement")

	tasks, err = tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	for _, task := range tasks {
		require.Len(t, task.DeletionVectorFiles, 1)
		require.Empty(t, task.DeleteFiles)
		dvFile := task.DeletionVectorFiles[0]
		require.Equal(t, task.File.SpecID(), dvFile.SpecID(),
			"the DV must use the historical spec of its data file")
		assert.Equal(t, task.File.Partition(), dvFile.Partition(),
			"the DV must keep the historical partition of its data file")
		require.NotEqual(t, int32(currentSpec.ID()), task.File.SpecID(),
			"the data file must still use the historical spec after evolution")
	}
}
