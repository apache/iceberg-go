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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_test

import (
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCherryPickSnapshotFromDivergentBranch(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	base := tbl.CurrentSnapshot().SnapshotID
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	require.Equal(t, table.OpAppend, source.Summary.Operation)

	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)
	targetBefore := tbl.CurrentSnapshot().SnapshotID
	require.NotEqual(t, base, targetBefore)
	require.NotContains(t, tbl.Metadata().Properties(), table.DefaultNameMappingKey)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID, iceberg.Properties{
		"cherry-picked": "yes", "published-wap-id": "forged", "source-snapshot-id": "forged", "wap.id": "forged",
	}))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	require.NotEqual(t, targetBefore, result.CurrentSnapshot().SnapshotID)
	require.NotEqual(t, source.SnapshotID, result.CurrentSnapshot().SnapshotID)
	require.Equal(t, table.OpAppend, result.CurrentSnapshot().Summary.Operation)
	assert.Equal(t, "yes", result.CurrentSnapshot().Summary.Properties["cherry-picked"])
	assert.Equal(t, strconv.FormatInt(source.SnapshotID, 10), result.CurrentSnapshot().Summary.Properties["source-snapshot-id"])
	assert.NotContains(t, result.CurrentSnapshot().Summary.Properties, "published-wap-id",
		"a snapshot without WAP must not acquire a caller-supplied publication marker")
	assert.NotContains(t, result.CurrentSnapshot().Summary.Properties, "wap.id",
		"the published cherry-pick snapshot must not carry a staged WAP marker")
	assert.NotContains(t, result.Metadata().Properties(), table.DefaultNameMappingKey,
		"cherry-picking existing files must not add a name mapping")
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, result, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, branchPlanningRef))

	// The source snapshot's inherited file must not be cherry-picked twice.
	// Replaying the same source after a successful commit also fails rather
	// than creating a snapshot with two live references to the same file.
	duplicate := result.NewTransaction()
	err = duplicate.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorContains(t, err, "already been applied")

	// Removing the original file must not make the same source replayable.
	overwrite := result.NewTransaction()
	reader := idRecordReader(t, result, 10)
	require.NoError(t, overwrite.Overwrite(t.Context(), reader, nil))
	reader.Release()
	result, err = overwrite.Commit(t.Context())
	require.NoError(t, err)
	retry := result.NewTransaction()
	err = retry.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "already been applied")
}

func TestCherryPickSnapshotFastForwardsDirectChild(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, *source.ParentSnapshotID)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID,
		iceberg.Properties{"ignored-on-fast-forward": "true"}))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, source.SnapshotID, result.CurrentSnapshot().SnapshotID,
		"fast-forward reuses the source snapshot ID")
	assert.NotContains(t, result.CurrentSnapshot().Summary.Properties, "ignored-on-fast-forward")
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, branchPlanningRef))
}

func TestCherryPickSnapshotFastForwardsToMissingBranch(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	const target = "cherry-fast-forward"
	txn, err := tbl.NewTransactionOnBranch(target)
	require.NoError(t, err)
	require.Nil(t, tbl.SnapshotByName(target))
	require.NoError(t, txn.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := txn.Commit(t.Context())
	require.NoError(t, err)

	picked := result.SnapshotByName(target)
	require.NotNil(t, picked)
	require.Equal(t, source.SnapshotID, picked.SnapshotID)
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, target))
	assert.ElementsMatch(t, []int64{1}, idsOnRef(t, result, table.MainBranch))
}

// A missing branch starts at main. Even a direct-child fast-forward must
// pin that inherited head, because concurrent changes would invalidate it.
func TestCherryPickSnapshotFastForwardToMissingBranchPinsMain(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, cherryPickRetryProps()), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	const target = "cherry-fast-forward"
	stale, err := tbl.NewTransactionOnBranch(target)
	require.NoError(t, err)
	require.NoError(t, stale.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))

	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)
	_, err = stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.Nil(t, tbl.SnapshotByName(target))
	assert.ElementsMatch(t, []int64{1, 3}, idsOnRef(t, tbl, table.MainBranch))
}

func TestCherryPickSnapshotFastForwardsNonAppendSnapshot(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	branchTxn, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	reader := idRecordReader(t, tbl, 2)
	require.NoError(t, branchTxn.Overwrite(t.Context(), reader, nil))
	reader.Release()
	tbl, err = branchTxn.Commit(t.Context())
	require.NoError(t, err)

	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	require.Equal(t, table.OpOverwrite, source.Summary.Operation)
	require.NotNil(t, source.ParentSnapshotID)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, *source.ParentSnapshotID)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, source.SnapshotID, result.CurrentSnapshot().SnapshotID)
	assert.ElementsMatch(t, []int64{2}, idsOnRef(t, result, table.MainBranch))
}

func TestCherryPickSnapshotRejectsAncestor(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	id := tbl.CurrentSnapshot().SnapshotID

	tx := tbl.NewTransaction()
	err := tx.CherryPickSnapshot(t.Context(), id, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "already been applied")
}

func TestCherryPickSnapshotAllowsEmptyAppend(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 11)
	oldBranch := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, oldBranch)

	empty := tbl.NewTransaction()
	require.NoError(t, empty.AddDataFilesByPath(t.Context(), nil, nil, false))
	tbl, err := empty.Commit(t.Context())
	require.NoError(t, err)
	source := tbl.CurrentSnapshot()
	require.NotNil(t, source)
	require.Equal(t, table.OpAppend, source.Summary.Operation)

	// AddDataFilesByPath used to create the empty source snapshot installs
	// name mapping. Remove it to verify cherry-picking does not re-add it.
	removeMapping := tbl.NewTransaction()
	require.NoError(t, removeMapping.RemoveProperties([]string{table.DefaultNameMappingKey}))
	tbl, err = removeMapping.Commit(t.Context())
	require.NoError(t, err)
	require.NotContains(t, tbl.Metadata().Properties(), table.DefaultNameMappingKey)

	tx, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	newBranch := result.SnapshotByName(branchPlanningRef)
	require.NotNil(t, newBranch)
	assert.NotEqual(t, oldBranch.SnapshotID, newBranch.SnapshotID)
	assert.Equal(t, oldBranch.SnapshotID, *newBranch.ParentSnapshotID)
	assert.ElementsMatch(t, []int64{11}, idsOnRef(t, result, branchPlanningRef))
	assert.NotContains(t, result.Metadata().Properties(), table.DefaultNameMappingKey,
		"empty cherry-pick must not change the name mapping")

	retry, err := result.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	err = retry.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "already been applied")
}

// A file already live on the target must not be added again even when the
// source snapshot itself has not been cherry-picked before.
func TestCherryPickSnapshotRejectsSourceFileAlreadyOnTarget(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	mainPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	require.Len(t, mainPaths, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	branchPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	require.Len(t, branchPaths, 2)
	var sourcePath string
	for _, path := range branchPaths {
		if path != mainPaths[0] {
			sourcePath = path

			break
		}
	}
	require.NotEmpty(t, sourcePath)

	// Import the same physical file directly, but not the source snapshot.
	duplicate := tbl.NewTransaction()
	require.NoError(t, duplicate.AddDataFilesByPath(t.Context(), []string{sourcePath}, nil, false))
	var err error
	tbl, err = duplicate.Commit(t.Context())
	require.NoError(t, err)

	replay := tbl.NewTransaction()
	err = replay.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorContains(t, err, "already referenced")
	require.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
}

func cherryPickRetryProps() iceberg.Properties {
	return iceberg.Properties{
		table.CommitNumRetriesKey:     "2",
		table.CommitMinRetryWaitMsKey: "1",
		table.CommitMaxRetryWaitMsKey: "2",
	}
}

func TestCherryPickSnapshotRejectsConcurrentTargetMove(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, cherryPickRetryProps()), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	stale := tbl.NewTransaction()
	require.NoError(t, stale.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))

	// A concurrent commit changes main after the cherry-pick validated its
	// duplicate-file set. The pinned head forbids an unsafe automatic rebase.
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)
	_, err := stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	assert.ElementsMatch(t, []int64{1, 3}, idsOnRef(t, tbl, table.MainBranch))
}

// A fast-forward followed by another append must never rebuild on the old
// main head when an unrelated branch takes the next table-wide sequence number.
func TestCherryPickSnapshotFastForwardThenAppendDoesNotLoseFilesOnRetry(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, cherryPickRetryProps()), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	stale := tbl.NewTransaction()
	require.NoError(t, stale.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	reader := idRecordReader(t, tbl, 3)
	require.NoError(t, stale.Append(t.Context(), reader, nil))
	reader.Release()

	// The competing writer changes another branch, not main.
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 4)
	_, err := stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	assert.ElementsMatch(t, []int64{1}, idsOnRef(t, tbl, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2, 4}, idsOnRef(t, tbl, branchPlanningRef))
}

func TestCherryPickSnapshotRejectsConcurrentReplayTargetMoveWithRetries(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, cherryPickRetryProps()), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)

	stale := tbl.NewTransaction()
	require.NoError(t, stale.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 4)
	_, err := stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	assert.ElementsMatch(t, []int64{1, 3, 4}, idsOnRef(t, tbl, table.MainBranch))
}

func TestCherryPickSnapshotRejectsConcurrentMainMoveForNewBranch(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, cherryPickRetryProps()), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)

	const newTarget = "cherry-target"
	stale, err := tbl.NewTransactionOnBranch(newTarget)
	require.NoError(t, err)
	require.NoError(t, stale.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))

	// The competing writer publishes exactly the same source to main.
	// A new branch inherits main's files, so an unpinned retry would
	// introduce two live entries for the source's data file.
	publish := tbl.NewTransaction()
	require.NoError(t, publish.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	tbl, err = publish.Commit(t.Context())
	require.NoError(t, err)

	_, err = stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.Nil(t, tbl.SnapshotByName(newTarget))
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, tbl, table.MainBranch))
}

func TestCherryPickSnapshotFastForwardsRootToEmptyMain(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 1)
	require.Nil(t, tbl.CurrentSnapshot())
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	require.Nil(t, source.ParentSnapshotID)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := txn.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, source.SnapshotID, result.CurrentSnapshot().SnapshotID)
	assert.ElementsMatch(t, []int64{1}, idsOnRef(t, result, table.MainBranch))
}

func TestCherryPickSnapshotFailedReplayDoesNotPinTransaction(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, cherryPickRetryProps()), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	dataPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	mainPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	var sourcePath string
	for _, path := range dataPaths {
		if path != mainPaths[0] {
			sourcePath = path
		}
	}
	require.NotEmpty(t, sourcePath)
	importTxn := tbl.NewTransaction()
	require.NoError(t, importTxn.AddDataFilesByPath(t.Context(), []string{sourcePath}, nil, false))
	tbl, err := importTxn.Commit(t.Context())
	require.NoError(t, err)

	stale := tbl.NewTransaction()
	err = stale.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorContains(t, err, "already referenced")
	require.NoError(t, stale.SetProperties(iceberg.Properties{"cherry-pick-failed-no-pin": "yes"}))
	appendRowsOnRef(t, tbl, table.MainBranch, 3)
	result, err := stale.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "yes", result.Metadata().Properties()["cherry-pick-failed-no-pin"])
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, result, table.MainBranch))
}

func TestCherryPickSnapshotRejectsV3RowLineageReuse(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{table.PropertyFormatVersion: "3"})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	// Diverge the target: a direct-child source is safely fast-forwardable.
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)

	tx := tbl.NewTransaction()
	err := tx.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	assert.Contains(t, err.Error(), "row ID")
}

func TestCherryPickSnapshotV3FastForwardPreservesLineage(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{table.PropertyFormatVersion: "3"})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, source.SnapshotID, result.CurrentSnapshot().SnapshotID)
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, table.MainBranch))
}

func TestCherryPickSnapshotTracksWAPPublication(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	branch, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	reader := idRecordReader(t, tbl, 2)
	require.NoError(t, branch.Append(t.Context(), reader, iceberg.Properties{"wap.id": "batch-1"}))
	reader.Release()
	tbl, err = branch.Commit(t.Context())
	require.NoError(t, err)
	source := tbl.SnapshotByName(branchPlanningRef)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID,
		iceberg.Properties{"wap.id": "forged", "published-wap-id": "forged"}))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "batch-1", tbl.CurrentSnapshot().Summary.Properties["published-wap-id"])
	assert.NotContains(t, tbl.CurrentSnapshot().Summary.Properties, "wap.id")

	branch, err = tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	reader = idRecordReader(t, tbl, 4)
	require.NoError(t, branch.Append(t.Context(), reader, iceberg.Properties{"wap.id": "batch-1"}))
	reader.Release()
	tbl, err = branch.Commit(t.Context())
	require.NoError(t, err)
	duplicate := tbl.NewTransaction()
	err = duplicate.CherryPickSnapshot(t.Context(), tbl.SnapshotByName(branchPlanningRef).SnapshotID, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "already been published")
}

// Java validates a staged WAP ID against all retained target ancestors, not
// only snapshots carrying the published-wap-id marker.
func TestCherryPickSnapshotRejectsWAPIDOnAncestor(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	main := tbl.NewTransaction()
	reader := idRecordReader(t, tbl, 1)
	require.NoError(t, main.Append(t.Context(), reader, iceberg.Properties{"wap.id": "already-published"}))
	reader.Release()
	tbl, err := main.Commit(t.Context())
	require.NoError(t, err)

	branchTxn, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	reader = idRecordReader(t, tbl, 2)
	require.NoError(t, branchTxn.Append(t.Context(), reader, iceberg.Properties{"wap.id": "already-published"}))
	reader.Release()
	tbl, err = branchTxn.Commit(t.Context())
	require.NoError(t, err)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)

	txn := tbl.NewTransaction()
	err = txn.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "already been published")
}

func TestCherryPickSnapshotMergeAppendUsesOnlySourceAdditions(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{
		table.ManifestMergeEnabledKey:  "true",
		table.ManifestMinMergeCountKey: "2",
	})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 3)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 4)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := txn.Commit(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 3, 4}, idsOnRef(t, result, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, result, branchPlanningRef))
}

// The source manifest keeps its original spec even when the default evolves.
// Replaying a file must use its registered spec ID and partition tuple.
func TestCherryPickSnapshotFromOlderPartitionSpec(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	specTxn := tbl.NewTransaction()
	require.NoError(t, table.NewUpdateSpec(specTxn, true).
		AddField("id", iceberg.IdentityTransform{}, "id_part").Commit())
	tbl, err := specTxn.Commit(t.Context())
	require.NoError(t, err)
	spec := tbl.Spec()
	olderSpec := spec.ID()

	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	specTxn = tbl.NewTransaction()
	require.NoError(t, table.NewUpdateSpec(specTxn, true).RemoveField("id_part").Commit())
	tbl, err = specTxn.Commit(t.Context())
	require.NoError(t, err)
	spec = tbl.Spec()
	require.NotEqual(t, olderSpec, spec.ID())
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := txn.Commit(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, result, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, branchPlanningRef))
}

func TestCherryPickSnapshotV1FromDivergentBranch(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{table.PropertyFormatVersion: "1"})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, table.OpAppend, result.CurrentSnapshot().Summary.Operation)
	require.Equal(t, strconv.FormatInt(source.SnapshotID, 10), result.CurrentSnapshot().Summary.Properties["source-snapshot-id"])
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, result, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, branchPlanningRef))
}
