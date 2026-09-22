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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cowRewriteOp struct {
	name        string
	run         func(*testing.T, *table.Table) *table.Table
	wantIDs     []int64
	wantRecords int64
}

// TestCoWRewriteKeepsRowsDeleted rewrites a data file that already has a delete on id=1.
// Once the file is replaced that delete no longer reaches the copied rows, so the rewrite itself must drop id=1.
func TestCoWRewriteKeepsRowsDeleted(t *testing.T) {
	deleteID1 := func(t *testing.T, tbl *table.Table) *table.Table {
		return mergeOnReadDelete(t, tbl, 1)
	}
	equalityDeleteID1 := func(t *testing.T, tbl *table.Table) *table.Table {
		t.Helper()

		return appendEqualityDelete(t, tbl, []int{1}, `[{"id": 1}]`)
	}

	deletes := []struct {
		name          string
		formatVersion string
		seed          func(*testing.T, *table.Table) *table.Table
	}{
		{name: "position delete", formatVersion: "2", seed: deleteID1},
		{name: "deletion vector", formatVersion: "3", seed: deleteID1},
		{name: "equality delete on v2", formatVersion: "2", seed: equalityDeleteID1},
		{name: "equality delete on v3", formatVersion: "3", seed: equalityDeleteID1},
	}

	matchID2 := iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	operations := []cowRewriteOp{
		{
			name:        "copy-on-write delete",
			run:         func(t *testing.T, tbl *table.Table) *table.Table { return copyOnWriteDelete(t, tbl, matchID2) },
			wantIDs:     []int64{3, 4, 5},
			wantRecords: 3,
		},
		{
			name: "filtered overwrite",
			run: func(t *testing.T, tbl *table.Table) *table.Table {
				return filteredOverwrite(t, tbl, matchID2, `[{"id": 6}]`)
			},
			wantIDs:     []int64{3, 4, 5, 6},
			wantRecords: 4,
		},
	}

	for _, del := range deletes {
		for _, op := range operations {
			t.Run(del.name+"/"+op.name, func(t *testing.T) {
				tbl := newMergeOnReadTestTableVersion(t, del.formatVersion)
				tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2, 3, 4, 5)
				tbl = del.seed(t, tbl)
				require.Equal(t, []int64{2, 3, 4, 5}, idsInTable(t, tbl))

				var rowIDsBefore map[int64]int64
				if del.formatVersion == "3" {
					rowIDsBefore = readRowIDsByID(t, t.Context(), tbl)
					require.Len(t, rowIDsBefore, 4)
				}

				tbl = op.run(t, tbl)

				assert.Equal(t, op.wantIDs, idsInTable(t, tbl), "id=1 was deleted before the rewrite and must stay deleted")
				assert.Equal(t, op.wantRecords, liveDataRecordCount(t, tbl), "the rewritten file must not carry the already-deleted row")

				if rowIDsBefore != nil {
					rowIDsAfter := readRowIDsByID(t, t.Context(), tbl)
					for _, id := range []int64{3, 4, 5} {
						assert.Equal(t, rowIDsBefore[id], rowIDsAfter[id],
							"id=%d must keep its _row_id through the rewrite", id)
					}
				}
			})
		}
	}
}

// TestCoWRewriteKeepsEachFilesOwnRowsDeleted rewrites two partitioned files in one operation.
// Each carrying a deletion vector on a different row position.
// Every rewritten file must drop the row its own vector removed, and no other.
func TestCoWRewriteKeepsEachFilesOwnRowsDeleted(t *testing.T) {
	matchID2Or5 := iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("id"), int64(2)),
		iceberg.EqualTo(iceberg.Reference("id"), int64(5)),
	)

	operations := []cowRewriteOp{
		{
			name:        "copy-on-write delete",
			run:         func(t *testing.T, tbl *table.Table) *table.Table { return copyOnWriteDelete(t, tbl, matchID2Or5) },
			wantIDs:     []int64{3, 4},
			wantRecords: 2,
		},
		{
			name: "filtered overwrite",
			run: func(t *testing.T, tbl *table.Table) *table.Table {
				return filteredOverwrite(t, tbl, matchID2Or5, `[{"id": 7, "category": "x"}]`)
			},
			wantIDs:     []int64{3, 4, 7},
			wantRecords: 3,
		},
	}

	recordsJSON := `[
		{"id": 1, "category": "x"}, {"id": 2, "category": "x"}, {"id": 3, "category": "x"},
		{"id": 4, "category": "y"}, {"id": 5, "category": "y"}, {"id": 6, "category": "y"}
	]`

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			tbl := newPartitionedMergeOnReadTestTable(t)
			tbl = appendRows(t, tbl, recordsJSON)

			// Distinct positions per file: id=1 is first in its file, id=6 is last in its own.
			// So applying one file's vector to the other changes the result.
			tbl = mergeOnReadDelete(t, tbl, 1)
			tbl = mergeOnReadDelete(t, tbl, 6)
			require.Equal(t, 2, liveDVCount(t, tbl))
			require.Equal(t, []int64{2, 3, 4, 5}, idsInTable(t, tbl))

			tbl = op.run(t, tbl)

			assert.Equal(t, op.wantIDs, idsInTable(t, tbl), "each rewritten file must drop only the row its own deletion vector removed")
			assert.Equal(t, op.wantRecords, liveDataRecordCount(t, tbl))
		})
	}
}

// TestCoWRewriteSkipsDeletesThatDoNotApply re-inserts id=1 after an equality delete on it.
// The delete has a lower sequence number than the new file, so rewriting that file must keep the row,
// while the untouched older file must keep its own id=1 hidden.
func TestCoWRewriteSkipsDeletesThatDoNotApply(t *testing.T) {
	matchID4 := iceberg.EqualTo(iceberg.Reference("id"), int64(4))
	operations := []cowRewriteOp{
		{
			name:        "copy-on-write delete",
			run:         func(t *testing.T, tbl *table.Table) *table.Table { return copyOnWriteDelete(t, tbl, matchID4) },
			wantIDs:     []int64{1, 2, 3, 5},
			wantRecords: 5,
		},
		{
			name: "filtered overwrite",
			run: func(t *testing.T, tbl *table.Table) *table.Table {
				return filteredOverwrite(t, tbl, matchID4, `[{"id": 6}]`)
			},
			wantIDs:     []int64{1, 2, 3, 5, 6},
			wantRecords: 6,
		},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			tbl := newMergeOnReadTestTableVersion(t, "2")
			tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 2, 3)
			tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id": 1}]`)
			tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1, 4, 5)
			require.Equal(t, []int64{1, 2, 3, 4, 5}, idsInTable(t, tbl))

			tbl = op.run(t, tbl)

			assert.Equal(t, op.wantIDs, idsInTable(t, tbl), "the re-inserted id=1 is newer than the equality delete and must survive the rewrite")
			assert.Equal(t, op.wantRecords, liveDataRecordCount(t, tbl), "only the file holding id=4 is rewritten; the other keeps all 3 of its rows")
		})
	}
}

// mergeOnReadDelete deletes id on a table configured for merge-on-read deletes.
func mergeOnReadDelete(t *testing.T, tbl *table.Table, id int64) *table.Table {
	t.Helper()

	tbl, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), id), nil)
	require.NoError(t, err)

	return tbl
}

// copyOnWriteDelete switches the table to copy-on-write in its own commit, then deletes the rows matching filter.
func copyOnWriteDelete(t *testing.T, tbl *table.Table, filter iceberg.BooleanExpression) *table.Table {
	t.Helper()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeCopyOnWrite}))
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	tbl, err = tbl.Delete(t.Context(), filter, nil)
	require.NoError(t, err)

	return tbl
}

// filteredOverwrite replaces the rows matching id with a single row newID.
func filteredOverwrite(t *testing.T, tbl *table.Table, filter iceberg.BooleanExpression, recordsJSON string) *table.Table {
	t.Helper()

	rdr := rowsReader(t, tbl, recordsJSON)
	defer rdr.Release()

	tbl, err := tbl.Overwrite(t.Context(), rdr, nil, table.WithOverwriteFilter(filter))
	require.NoError(t, err)

	return tbl
}

func appendRows(t *testing.T, tbl *table.Table, recordsJSON string) *table.Table {
	t.Helper()

	rdr := rowsReader(t, tbl, recordsJSON)
	defer rdr.Release()

	tbl, err := tbl.Append(t.Context(), rdr, nil)
	require.NoError(t, err)

	return tbl
}

func rowsReader(t *testing.T, tbl *table.Table, recordsJSON string) array.RecordReader {
	t.Helper()

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{recordsJSON})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	return array.NewTableReader(data, -1)
}

// liveDataRecordCount sums record_count over the live data files of the current snapshot,
// including rows that deletes hide from a scan.
func liveDataRecordCount(t *testing.T, tbl *table.Table) int64 {
	t.Helper()

	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)

	var total int64
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range m.Entries(fs, true) {
			require.NoError(t, err)
			total += entry.DataFile().Count()
		}
	}

	return total
}
