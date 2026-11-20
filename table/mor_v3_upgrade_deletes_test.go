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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const abcde = `[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`

func deleteID(t *testing.T, tbl *table.Table, id int64) *table.Table {
	t.Helper()

	tbl, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), id), nil)
	require.NoError(t, err)

	return tbl
}

func upgradeToV3(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

func livePositionDeleteFiles(t *testing.T, tbl *table.Table) int {
	t.Helper()

	return liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes)
}

// The first v3 delete on a file must carry over the v2 position deletes, or readers
// (which ignore position deletes once a file has a DV) bring those rows back. See #2604.
func TestV3MergeOnReadUpgradeKeepsPositionDeletes(t *testing.T) {
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "2"), abcde)
	tbl = deleteID(t, tbl, 1)
	tbl = upgradeToV3(t, tbl)
	require.Equal(t, []int64{2, 3, 4, 5}, idsInTable(t, tbl))

	tbl = deleteID(t, tbl, 2)
	assert.Equal(t, []int64{3, 4, 5}, idsInTable(t, tbl), "the pre-upgrade delete of id=1 must survive")
	assert.Equal(t, 1, liveDVCount(t, tbl))
	assert.Zero(t, livePositionDeleteFiles(t, tbl), "the folded position delete file must be dropped")

	tbl = deleteID(t, tbl, 3)
	assert.Equal(t, []int64{4, 5}, idsInTable(t, tbl))
	assert.Equal(t, 1, liveDVCount(t, tbl))
}

func TestV3MergeOnReadUpgradeKeepsMultiplePositionDeleteFiles(t *testing.T) {
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "2"), abcde)
	tbl = deleteID(t, tbl, 1)
	tbl = deleteID(t, tbl, 2)
	require.Equal(t, 2, livePositionDeleteFiles(t, tbl))
	tbl = upgradeToV3(t, tbl)

	tbl = deleteID(t, tbl, 3)
	assert.Equal(t, []int64{4, 5}, idsInTable(t, tbl))
	assert.Equal(t, 1, liveDVCount(t, tbl))
	assert.Zero(t, livePositionDeleteFiles(t, tbl))
}

// One partition-scoped position delete file covers two data files. Like Java, it stays in the table
// (readers ignore it for a file with a DV and still apply it to the other), but its positions
// are folded into each file's first DV.
func TestV3MergeOnReadUpgradeKeepsSharedPositionDeleteFile(t *testing.T) {
	tbl := newMergeOnReadTestTableVersion(t, "2")
	tbl = appendRows(t, tbl, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tbl = appendRows(t, tbl, `[{"id":3,"data":"c"},{"id":4,"data":"d"}]`)

	tbl, err := tbl.Delete(t.Context(), iceberg.IsIn(iceberg.Reference("id"), int64(1), int64(3)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, livePositionDeleteFiles(t, tbl), "one delete file must cover both data files")
	tbl = upgradeToV3(t, tbl)

	tbl = deleteID(t, tbl, 2)
	assert.Equal(t, []int64{4}, idsInTable(t, tbl))
	assert.Equal(t, 1, livePositionDeleteFiles(t, tbl), "the second file still needs the shared delete file")

	tbl = deleteID(t, tbl, 4)
	assert.Empty(t, idsInTable(t, tbl))
	assert.Equal(t, 2, liveDVCount(t, tbl))
	assert.Equal(t, 1, livePositionDeleteFiles(t, tbl), "a shared delete file is not dropped")
}

func TestV3MergeOnReadUpgradeKeepsPartitionedPositionDeletes(t *testing.T) {
	tbl := newPartitionedMergeOnReadTestTableVersion(t, "2")
	tbl = appendRows(t, tbl, `[{"id":1,"category":"x"},{"id":2,"category":"x"},{"id":3,"category":"x"},`+
		`{"id":4,"category":"y"},{"id":5,"category":"y"},{"id":6,"category":"y"}]`)
	tbl = deleteID(t, tbl, 1)
	tbl = deleteID(t, tbl, 4)
	tbl = upgradeToV3(t, tbl)

	tbl = deleteID(t, tbl, 2)
	tbl = deleteID(t, tbl, 5)
	assert.Equal(t, []int64{3, 6}, idsInTable(t, tbl))
	assert.Equal(t, 2, liveDVCount(t, tbl))
	assert.Zero(t, livePositionDeleteFiles(t, tbl))
}

func TestV3MergeOnReadUpgradeKeepsEqualityDeletes(t *testing.T) {
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "2"), abcde)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":1}]`)
	tbl = deleteID(t, tbl, 2)
	tbl = upgradeToV3(t, tbl)

	tbl = deleteID(t, tbl, 3)
	assert.Equal(t, []int64{4, 5}, idsInTable(t, tbl))
	assert.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentEqDeletes))
	assert.Zero(t, livePositionDeleteFiles(t, tbl))
}

// Dropping the v2 position delete file cannot be replayed on a changed table: a peer commit
// must fail the staged delete instead of being merged over it.
func TestV3MergeOnReadUpgradeFoldFailsOnConcurrentCommit(t *testing.T) {
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "2"), abcde)
	tbl = deleteID(t, tbl, 1)
	tbl = upgradeToV3(t, tbl)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil))
	peer := appendRows(t, tbl, `[{"id":10,"data":"z"}]`)

	_, err := tx.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Equal(t, []int64{2, 3, 4, 5, 10}, idsInTable(t, peer))

	peer = deleteID(t, peer, 2)
	assert.Equal(t, []int64{3, 4, 5, 10}, idsInTable(t, peer), "a manual retry on the new table succeeds")
}

// A partition spec change between the v2 delete and the upgrade must not lose the old delete.
func TestV3MergeOnReadUpgradeAfterSpecEvolutionKeepsPositionDeletes(t *testing.T) {
	tests := map[string]struct {
		tbl  func(*testing.T) *table.Table
		rows string
	}{
		"unpartitioned": {
			func(t *testing.T) *table.Table { return newMergeOnReadTestTableVersion(t, "2") },
			`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`,
		},
		"partitioned": {
			func(t *testing.T) *table.Table { return newPartitionedMergeOnReadTestTableVersion(t, "2") },
			`[{"id":1,"category":"x"},{"id":2,"category":"x"},{"id":3,"category":"x"}]`,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tbl := deleteID(t, appendRows(t, tc.tbl(t), tc.rows), 1)

			evolve := tbl.NewTransaction()
			require.NoError(t, evolve.UpdateSpec(true).AddIdentity("id").Commit())
			tbl, err := evolve.Commit(t.Context())
			require.NoError(t, err)
			tbl = deleteID(t, upgradeToV3(t, tbl), 2)

			assert.Equal(t, []int64{3}, idsInTable(t, tbl))
			assert.Equal(t, 1, liveDVCount(t, tbl))
			assert.Zero(t, livePositionDeleteFiles(t, tbl))
		})
	}
}

// A delete whose filter passes file-level metrics but matches no row still writes the DV seeded
// with the v2 positions. The file-scoped position delete file is dropped in the same commit, so
// without that DV its deletes would be lost.
func TestV3MergeOnReadUpgradeSeedOnlyDeletionVector(t *testing.T) {
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "2"), abcde)
	tbl = deleteID(t, tbl, 1)
	tbl = upgradeToV3(t, tbl)

	// "bb" lies between the data column bounds "a" and "e" but is not a value.
	tbl, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("data"), "bb"), nil)
	require.NoError(t, err)

	assert.Equal(t, []int64{2, 3, 4, 5}, idsInTable(t, tbl))
	assert.Equal(t, 1, liveDVCount(t, tbl), "the seeded DV must be written even with no new positions")
	assert.Zero(t, livePositionDeleteFiles(t, tbl))
}
