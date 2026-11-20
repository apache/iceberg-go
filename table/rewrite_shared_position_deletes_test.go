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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// A position delete can cover several files. Worker-level SafePosDeletes
// does not mean that the coordinator may remove it after rewriting one file.
func setupSharedPositionDeleteRewrite(t *testing.T, tbl *table.Table) (*table.Table, []table.CompactionTaskGroup) {
	t.Helper()

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	paths := make([]string, 2)
	for i := range paths {
		paths[i] = fmt.Sprintf("%s/data/shared-%d.parquet", tbl.Location(), i)
		writeParquetFile(t, paths[i], arrowSchema, fmt.Sprintf(
			`[{"id":%d,"data":"deleted"},{"id":%d,"data":"visible"}]`, i*2+1, i*2+2))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{paths[i]}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	deletePath := tbl.Location() + "/data/shared-position-delete.parquet"
	writeParquetFile(t, deletePath, table.PositionalDeleteArrowSchema, fmt.Sprintf(
		`[{"file_path":%q,"pos":0},{"file_path":%q,"pos":0}]`, paths[0], paths[1]))
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes, deletePath, iceberg.ParquetFile,
		nil, nil, nil, 2, 128)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(builder.Build()).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{2, 4}, scanIDs(t, tbl))

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	groups := make([]table.CompactionTaskGroup, 0, 2)
	for _, path := range paths {
		for _, task := range tasks {
			if task.File.FilePath() != path {
				continue
			}
			require.Len(t, task.DeleteFiles, 1)
			groups = append(groups, table.CompactionTaskGroup{
				PartitionKey:   "unpartitioned",
				Tasks:          []table.FileScanTask{task},
				TotalSizeBytes: task.File.FileSizeBytes(),
			})
		}
	}
	require.Len(t, groups, 2)

	return tbl, groups
}

func TestRewriteCompactionKeepsSharedPositionDeleteForSurvivingFile(t *testing.T) {
	for _, mode := range []string{"atomic", "distributed"} {
		for _, groupCount := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/%d-groups", mode, groupCount), func(t *testing.T) {
				tbl, groups := setupSharedPositionDeleteRewrite(t, newRewriteTestTable(t))
				tx := tbl.NewTransaction()

				if mode == "atomic" {
					result, err := tx.RewriteDataFiles(t.Context(), groups[:groupCount],
						table.RewriteDataFilesOptions{})
					require.NoError(t, err)
					require.Equal(t, groupCount, result.RewrittenGroups)
					// Only the complete rewrite may remove the shared delete.
					wantRemoved := 0
					if groupCount == 2 {
						wantRemoved = 1
					}
					require.Equal(t, wantRemoved, result.RemovedPositionDeleteFiles)
				} else {
					rewrite := tx.NewRewrite(nil)
					for _, group := range groups[:groupCount] {
						output, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
						require.NoError(t, err)
						require.Len(t, output.SafePosDeletes, 1)
						rewrite.ApplyResult(output)
					}
					require.NoError(t, rewrite.Commit(t.Context()))
				}

				committed, err := tx.Commit(t.Context())
				require.NoError(t, err)
				require.ElementsMatch(t, []int64{2, 4}, scanIDs(t, committed),
					"compaction must not resurrect rows deleted from an unrewritten file")

				tasks, err := committed.Scan().PlanFiles(t.Context())
				require.NoError(t, err)
				filesWithDeletes := 0
				for _, task := range tasks {
					if len(task.DeleteFiles) > 0 {
						filesWithDeletes++
					}
				}
				if groupCount == 1 {
					require.Equal(t, 1, filesWithDeletes, "surviving file still requires the delete")
				} else {
					require.Zero(t, filesWithDeletes, "delete is dead after all source files are rewritten")
					require.Equal(t, "1", committed.CurrentSnapshot().Summary.Properties["removed-position-delete-files"],
						"the shared position delete must be removed exactly once")
				}
			})
		}
	}
}

func TestRewriteFilesRejectsMisclassifiedWorkerDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := addSortTestFile(t, newRewriteTestTable(t), "source", "original", 1, 2)
	deletePath := tbl.Location() + "/data/equality-delete.parquet"
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, deletePath, schema, `[{"id":1}]`)
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentEqDeletes, deletePath,
		iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	deleteFile := builder.EqualityFieldIDs([]int{1}).Build()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.NewRowDelta(nil).AddDeletes(deleteFile).Commit(ctx))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, scanIDs(t, tbl))

	for _, tc := range []struct {
		name   string
		result table.CompactionGroupResult
	}{
		{"position delete file", table.CompactionGroupResult{SafePosDeletes: []iceberg.DataFile{deleteFile}}},
		{"deletion vector", table.CompactionGroupResult{SafeDeletionVectors: []iceberg.DataFile{deleteFile}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rewrite := tbl.NewTransaction().NewRewrite(nil).ApplyResult(tc.result)
			err := rewrite.Commit(t.Context())
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			require.ErrorContains(t, err, "automatic delete removal expected a "+tc.name)
		})
	}
}

func TestRewriteFilesRejectsUnknownWorkerPositionDelete(t *testing.T) {
	ctx := t.Context()
	tbl, groups := setupSharedPositionDeleteRewrite(t, newRewriteTestTable(t))
	output, err := table.ExecuteCompactionGroup(ctx, tbl, groups[0])
	require.NoError(t, err)
	require.Len(t, output.SafePosDeletes, 1)

	missingPath := tbl.Location() + "/data/missing-position-delete.parquet"
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes, missingPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	output.SafePosDeletes = append(output.SafePosDeletes, builder.Build())

	txn := tbl.NewTransaction()
	err = txn.NewRewrite(nil).ApplyResult(output).Commit(ctx)
	require.ErrorContains(t, err, "do not belong to the table")
	require.ErrorContains(t, err, missingPath)
	unchanged, err := txn.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, unchanged.CurrentSnapshot().SnapshotID)
	require.ElementsMatch(t, []int64{2, 4}, scanIDs(t, unchanged))
}

func TestRewriteFilesWithAutomaticPositionRemovalRejectsStaleCommit(t *testing.T) {
	ctx := t.Context()
	tbl, catalog := newConcurrentRewriteTestTable(t)
	tbl, groups := setupSharedPositionDeleteRewrite(t, tbl)
	stale := tbl.NewTransaction()
	rewrite := stale.NewRewrite(nil)
	for _, group := range groups {
		result, err := table.ExecuteCompactionGroup(ctx, tbl, group)
		require.NoError(t, err)
		rewrite.ApplyResult(result)
	}
	require.NoError(t, rewrite.Commit(ctx))

	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	path := tbl.Location() + "/data/peer.parquet"
	writeParquetFile(t, path, schema, `[{"id":5,"data":"visible"}]`)
	peer := tbl.NewTransaction()
	require.NoError(t, peer.AddDataFilesByPath(ctx, []string{path}, nil, false))
	latest, err := peer.Commit(ctx)
	require.NoError(t, err)

	attempts := catalog.attempts.Load()
	_, err = stale.Commit(ctx)
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.Equal(t, attempts+1, catalog.attempts.Load())
	current, err := catalog.LoadTable(ctx, tbl.Identifier())
	require.NoError(t, err)
	require.Equal(t, latest.CurrentSnapshot().SnapshotID, current.CurrentSnapshot().SnapshotID)
	require.ElementsMatch(t, []int64{2, 4, 5}, scanIDs(t, current))
}

type deleteSelectionManifestHook struct {
	iceio.LocalFS
	manifestList string
	onOpen       func()
}

func (h *deleteSelectionManifestHook) Open(ctx context.Context, name string) (iceio.File, error) {
	if name == h.manifestList && h.onOpen != nil {
		hook := h.onOpen
		h.onOpen = nil
		hook()
	}

	return h.LocalFS.Open(ctx, name)
}

func TestRewriteFilesRejectsParentChangeDuringAutomaticDeleteSelection(t *testing.T) {
	ctx := t.Context()
	tbl, catalog := newConcurrentRewriteTestTable(t)
	tbl, groups := setupSharedPositionDeleteRewrite(t, tbl)
	hook := &deleteSelectionManifestHook{manifestList: tbl.CurrentSnapshot().ManifestList}
	wrapped := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return hook, nil }, catalog)
	tx := wrapped.NewTransaction()
	rewrite := tx.NewRewrite(nil)
	for _, group := range groups {
		result, err := table.ExecuteCompactionGroup(ctx, tbl, group)
		require.NoError(t, err)
		rewrite.ApplyResult(result)
	}

	var appendedSnapshotID int64
	hook.onOpen = func() {
		reader := rowsReader(t, tbl, `[{"id":5,"data":"visible"}]`)
		defer reader.Release()
		require.NoError(t, tx.Append(ctx, reader, nil))
		staged, err := tx.StagedTable()
		require.NoError(t, err)
		appendedSnapshotID = staged.CurrentSnapshot().SnapshotID
	}

	err := rewrite.Commit(ctx)
	require.NotZero(t, appendedSnapshotID)
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.ErrorContains(t, err, "selecting automatic delete removals")
	committed, err := tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, appendedSnapshotID, committed.CurrentSnapshot().SnapshotID)
	require.ElementsMatch(t, []int64{2, 4, 5}, scanIDs(t, committed))
}

func TestRewriteFilesValidatesWorkerDeletionVectors(t *testing.T) {
	for _, tc := range []struct {
		name          string
		rewriteTarget bool
	}{
		{name: "keep vector for surviving file"},
		{name: "remove duplicate vector candidates once", rewriteTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, target := seedV3TableWithDV(t)
			ctx := t.Context()
			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 2)

			var targetDV iceberg.DataFile
			var task table.FileScanTask
			for _, candidate := range tasks {
				isTarget := candidate.File.FilePath() == target
				if isTarget {
					require.Len(t, candidate.DeletionVectorFiles, 1)
					targetDV = candidate.DeletionVectorFiles[0]
				}
				if isTarget == tc.rewriteTarget {
					task = candidate
				}
			}
			require.NotNil(t, targetDV)
			require.NotNil(t, task.File)
			if !tc.rewriteTarget {
				wrongTask := task
				wrongTask.DeletionVectorFiles = []iceberg.DataFile{targetDV}
				require.Empty(t, table.CollectSafeDeletionVectors([]table.FileScanTask{wrongTask}))
			}

			result, err := table.ExecuteCompactionGroup(ctx, tbl,
				table.CompactionTaskGroup{Tasks: []table.FileScanTask{task}})
			require.NoError(t, err)
			result.SafeDeletionVectors = []iceberg.DataFile{targetDV, targetDV}

			tx := tbl.NewTransaction()
			require.NoError(t, tx.NewRewrite(nil).ApplyResult(result).Commit(ctx))
			committed, err := tx.Commit(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []int64{2, 3, 4}, scanIDs(t, committed))
			if tc.rewriteTarget {
				require.Equal(t, "1", committed.CurrentSnapshot().Summary.Properties["removed-dvs"])
			} else {
				require.NotContains(t, committed.CurrentSnapshot().Summary.Properties, "removed-dvs")
			}
		})
	}
}

func TestRewriteCompactionRetainsSharedDeleteWhenLastGroupOnlyIsRewritten(t *testing.T) {
	for _, mode := range []string{"atomic", "distributed"} {
		t.Run(mode, func(t *testing.T) {
			tbl, groups := setupSharedPositionDeleteRewrite(t, newRewriteTestTable(t))
			tx := tbl.NewTransaction()

			if mode == "atomic" {
				result, err := tx.RewriteDataFiles(t.Context(), groups[1:], table.RewriteDataFilesOptions{})
				require.NoError(t, err)
				require.Equal(t, 1, result.RewrittenGroups)
				require.Zero(t, result.RemovedPositionDeleteFiles,
					"rewriting the second target cannot remove a delete needed by the first")
			} else {
				result, err := table.ExecuteCompactionGroup(t.Context(), tbl, groups[1])
				require.NoError(t, err)
				require.Len(t, result.SafePosDeletes, 1)
				require.NoError(t, tx.NewRewrite(nil).ApplyResult(result).Commit(t.Context()))
			}

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.ElementsMatch(t, []int64{2, 4}, scanIDs(t, committed))

			tasks, err := committed.Scan().PlanFiles(t.Context())
			require.NoError(t, err)
			require.Len(t, tasks, 2)
			filesWithDeletes := 0
			for _, task := range tasks {
				if len(task.DeleteFiles) > 0 {
					filesWithDeletes++
				}
			}
			require.Equal(t, 1, filesWithDeletes,
				"the first source file must still have its shared delete attached")
			require.NotContains(t, committed.CurrentSnapshot().Summary.Properties,
				"removed-position-delete-files")
		})
	}
}

func TestRewriteCompactionDropsSharedDeleteWithOnlyNewerSurvivor(t *testing.T) {
	for _, mode := range []string{"atomic", "distributed"} {
		t.Run(mode, func(t *testing.T) {
			tbl, groups := setupSharedPositionDeleteRewrite(t, newRewriteTestTable(t))
			tbl = appendRows(t, tbl, `[{"id":5,"data":"newer"}]`)
			tx := tbl.NewTransaction()
			if mode == "atomic" {
				result, err := tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{})
				require.NoError(t, err)
				require.Equal(t, 1, result.RemovedPositionDeleteFiles)
			} else {
				rewrite := tx.NewRewrite(nil)
				for _, group := range groups {
					result, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
					require.NoError(t, err)
					require.Len(t, result.SafePosDeletes, 1)
					rewrite.ApplyResult(result)
				}
				require.NoError(t, rewrite.Commit(t.Context()))
			}

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.ElementsMatch(t, []int64{2, 4, 5}, scanIDs(t, committed))
			require.Equal(t, "1", committed.CurrentSnapshot().Summary.Properties["removed-position-delete-files"])
		})
	}
}
