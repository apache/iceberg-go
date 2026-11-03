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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestDiffSnapshotFilesWithRealBranchesAndManifestRewrite(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{
		table.ManifestMergeEnabledKey: "false",
	})
	tbl = seedDivergentBranch(t, tbl)
	branch := tbl.SnapshotByName(branchPlanningRef)
	main := tbl.SnapshotByName(table.MainBranch)
	require.NotNil(t, branch)
	require.NotNil(t, main)

	branchPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	mainPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	require.Len(t, branchPaths, 2)
	require.Len(t, mainPaths, 2)

	diff, err := tbl.Inspect().DiffSnapshotFiles(t.Context(), branch.SnapshotID, main.SnapshotID)
	require.NoError(t, err)
	require.Len(t, diff.AddedDataFiles, 1)
	require.Len(t, diff.RemovedDataFiles, 1)
	require.Contains(t, mainPaths, diff.AddedDataFiles[0].FilePath())
	require.NotContains(t, branchPaths, diff.AddedDataFiles[0].FilePath())
	require.Contains(t, branchPaths, diff.RemovedDataFiles[0].FilePath())
	require.NotContains(t, mainPaths, diff.RemovedDataFiles[0].FilePath())

	txn := tbl.NewTransaction()
	_, err = txn.RewriteManifests(t.Context())
	require.NoError(t, err)
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	rewritten := tbl.SnapshotByName(table.MainBranch)
	require.NotNil(t, rewritten)
	unchanged, err := tbl.Inspect().DiffSnapshotFiles(t.Context(), main.SnapshotID, rewritten.SnapshotID)
	require.NoError(t, err)
	require.False(t, unchanged.HasChanges(), "rewriting manifests must not change physical files")
}

func TestDiffSnapshotFilesWithBranchDelete(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
	before := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, before)
	beforePaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)

	txn, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.Delete(t.Context(),
		iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(3)), nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	after := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, after)
	diff, err := tbl.Inspect().DiffSnapshotFiles(t.Context(), before.SnapshotID, after.SnapshotID)
	require.NoError(t, err)
	require.Empty(t, diff.AddedDataFiles)
	require.Len(t, diff.RemovedDataFiles, 1)
	require.Contains(t, beforePaths, diff.RemovedDataFiles[0].FilePath())
	require.Empty(t, diff.AddedDeleteFiles)
	require.Empty(t, diff.RemovedDeleteFiles)
}
