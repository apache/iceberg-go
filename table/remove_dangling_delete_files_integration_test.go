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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestRemoveDanglingDeleteFilesCommit(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, iceberg.Properties{
		table.PropertyFormatVersion:        "2",
		table.CommitNumRetriesKey:          "2",
		table.CommitMinRetryWaitMsKey:      "1",
		table.CommitMaxRetryWaitMsKey:      "2",
		table.CommitTotalRetryTimeoutMsKey: "1000",
	})
	require.NoError(t, err)
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: location + "/metadata/v1.metadata.json", fsF: fsF}
	tbl := table.New(table.Identifier{"db", "test"}, meta, cat.location, fsF, cat)
	partition := map[int]any{1000: int64(7)}
	data, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, location+"/data/live.parquet", iceberg.ParquetFile, partition, nil, nil, 1, 128)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(data.Build()).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// With no delete files, planning must leave the table unchanged.
	before, err := tbl.PlanDanglingDeleteFiles(t.Context())
	require.NoError(t, err)
	require.Empty(t, before)
	noOp, err := tbl.RemoveDanglingDeleteFiles(t.Context())
	require.NoError(t, err)
	require.Zero(t, noOp.RemovedDeleteFiles)
	require.Equal(t, tbl.Metadata(), noOp.Table.Metadata())

	live, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, location+"/data/live-delete.parquet", iceberg.ParquetFile, partition, nil, nil, 1, 128)
	require.NoError(t, err)
	dead, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, location+"/data/dead-delete.parquet", iceberg.ParquetFile, map[int]any{1000: int64(8)}, nil, nil, 1, 128)
	require.NoError(t, err)
	liveFile := live.ReferencedDataFile(data.Build().FilePath()).Build()
	deadFile := dead.Build()
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(liveFile, deadFile).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	planned, err := tbl.PlanDanglingDeleteFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, planned, 1)
	require.Equal(t, deadFile.FilePath(), planned[0].FilePath())
	result, err := tbl.RemoveDanglingDeleteFiles(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.RemovedDeleteFiles)
	updated := result.Table
	require.Empty(t, mustPlanDangling(t, updated))
	tasks, err := updated.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeleteFiles, 1)
	require.Equal(t, liveFile.FilePath(), tasks[0].DeleteFiles[0].FilePath())
	// The stale table must not replay the same cleanup on a moved branch,
	// even with retries enabled.
	attemptsBefore := cat.attempts.Load()
	_, err = tbl.RemoveDanglingDeleteFiles(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.Equal(t, attemptsBefore+1, cat.attempts.Load(),
		"cleanup must not retry after a pinned branch head moves")
}

func TestRemoveDanglingDeleteFilesUnpartitionedCleanup(t *testing.T) {
	ctx := t.Context()
	tbl, _ := newConcurrentRewriteTestTable(t)
	dataPath := tbl.Location() + "/data/live.parquet"
	missingPath := tbl.Location() + "/data/missing.parquet"
	posPath := tbl.Location() + "/data/dangling-pos.parquet"
	eqPath := tbl.Location() + "/data/dangling-eq.parquet"

	data, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		dataPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	pos, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	eq, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	// An explicit position-delete target is absent, and an equality delete
	// does not apply to data from the same sequence number.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).
		AddRows(data.Build()).
		AddDeletes(pos.ReferencedDataFile(missingPath).Build(), eq.EqualityFieldIDs([]int{1}).Build()).
		Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	planned, err := tbl.PlanDanglingDeleteFiles(ctx)
	require.NoError(t, err)
	var paths []string
	for _, file := range planned {
		paths = append(paths, file.FilePath())
	}
	require.ElementsMatch(t, []string{posPath, eqPath}, paths)

	result, err := tbl.RemoveDanglingDeleteFiles(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, result.RemovedDeleteFiles)
	updated := result.Table
	require.Empty(t, mustPlanDangling(t, updated))

	tasks, err := updated.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, dataPath, tasks[0].File.FilePath())
	require.Empty(t, tasks[0].DeleteFiles)
	require.Empty(t, tasks[0].EqualityDeleteFiles)
}

func mustPlanDangling(t *testing.T, tbl *table.Table) []iceberg.DataFile {
	t.Helper()
	files, err := tbl.PlanDanglingDeleteFiles(t.Context())
	require.NoError(t, err)

	return files
}

func TestRemoveDanglingDeleteFilesPrunesEqualityBounds(t *testing.T) {
	ctx := t.Context()
	tbl, _ := newConcurrentRewriteTestTable(t)
	makeFile := func(content iceberg.ManifestEntryContent, name, value string) iceberg.DataFile {
		t.Helper()
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, content, tbl.Location()+"/data/"+name,
			iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		builder.LowerBoundValues(map[int][]byte{2: []byte(value)})
		builder.UpperBoundValues(map[int][]byte{2: []byte(value)})
		builder.ValueCounts(map[int]int64{2: 1})
		builder.NullValueCounts(map[int]int64{2: 0})
		if content == iceberg.EntryContentEqDeletes {
			builder.EqualityFieldIDs([]int{2})
		}

		return builder.Build()
	}

	data := makeFile(iceberg.EntryContentData, "live.parquet", "a")
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(data).Commit(ctx))
	var err error
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	liveEquality := makeFile(iceberg.EntryContentEqDeletes, "live-eq.parquet", "a")
	danglingEquality := makeFile(iceberg.EntryContentEqDeletes, "dangling-eq.parquet", "z")
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(liveEquality, danglingEquality).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].EqualityDeleteFiles, 1)
	require.Equal(t, liveEquality.FilePath(), tasks[0].EqualityDeleteFiles[0].FilePath())

	planned, err := tbl.PlanDanglingDeleteFiles(ctx)
	require.NoError(t, err)
	require.Len(t, planned, 1)
	require.Equal(t, danglingEquality.FilePath(), planned[0].FilePath())

	result, err := tbl.RemoveDanglingDeleteFiles(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.RemovedDeleteFiles)
	updated := result.Table
	require.Empty(t, mustPlanDangling(t, updated))
}

func TestRemoveDanglingDeleteFilesTransactionStagesAfterRowDelta(t *testing.T) {
	ctx := t.Context()
	tbl, _ := newConcurrentRewriteTestTable(t)
	data := newDataFile(t, tbl.Location()+"/data/live.parquet")
	posBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/dangling.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).
		AddRows(data).
		AddDeletes(posBuilder.ReferencedDataFile(tbl.Location()+"/data/missing.parquet").Build()).
		Commit(ctx))

	result, err := tx.RemoveDanglingDeleteFiles(ctx)
	require.NoError(t, err)
	require.Nil(t, result.Table, "transaction action stages rather than commits")
	require.Equal(t, 1, result.RemovedDeleteFiles)
	require.Nil(t, tbl.CurrentSnapshot(), "base table must remain unchanged")

	committed, err := tx.Commit(ctx)
	require.NoError(t, err)
	require.Empty(t, mustPlanDangling(t, committed))
	tasks, err := committed.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, data.FilePath(), tasks[0].File.FilePath())
	require.Empty(t, tasks[0].DeleteFiles)
}

func TestRemoveDanglingDeleteFilesTransactionTargetsBranch(t *testing.T) {
	ctx := t.Context()
	tbl, _ := newConcurrentRewriteTestTable(t)
	data := newDataFile(t, tbl.Location()+"/data/main.parquet")
	posBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/main-delete.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	liveDelete := posBuilder.ReferencedDataFile(data.FilePath()).Build()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(data).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(liveDelete).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// Fork from main, then remove the target data only on the branch.
	branchTx, err := tbl.NewTransactionOnBranch("cleanup")
	require.NoError(t, err)
	rewrite := branchTx.NewRewrite(nil)
	rewrite.DeleteFile(data)
	require.NoError(t, rewrite.Commit(ctx))
	tbl, err = branchTx.Commit(ctx)
	require.NoError(t, err)
	require.Empty(t, mustPlanDangling(t, tbl), "delete remains live on main")

	branchTx, err = tbl.NewTransactionOnBranch("cleanup")
	require.NoError(t, err)
	require.NoError(t, branchTx.SetProperties(iceberg.Properties{"dangling-cleanup-test": "yes"}))
	planned, err := branchTx.PlanDanglingDeleteFiles(ctx)
	require.NoError(t, err)
	require.Len(t, planned, 1)
	require.Equal(t, liveDelete.FilePath(), planned[0].FilePath())
	result, err := branchTx.RemoveDanglingDeleteFiles(ctx)
	require.NoError(t, err)
	require.Nil(t, result.Table)
	require.Equal(t, 1, result.RemovedDeleteFiles)

	updated, err := branchTx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, "yes", updated.Properties()["dangling-cleanup-test"])
	mainData, mainDeletes := liveFilesOnRef(t, updated, table.MainBranch)
	require.Contains(t, mainData, data.FilePath())
	require.Contains(t, mainDeletes, liveDelete.FilePath())
	_, branchDeletes := liveFilesOnRef(t, updated, "cleanup")
	require.Empty(t, branchDeletes)
}
