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

package compaction_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// TestCollectDeadPositionDeletes drives the full manifest walk: a
// partition-scoped position delete (no referenced_data_file, no file_path
// bounds — Spark's default granularity) covering three data files is dead only
// when every file it covers is rewritten, and retained the moment one is not.
func TestCollectDeadPositionDeletes(t *testing.T) {
	ctx := t.Context()
	fs := iceio.LocalFS{}
	tbl := newCDCStressTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 3 {
		p := tbl.Location() + fmt.Sprintf("/data/d-%d.parquet", i)
		writeParquet(t, p, arrowSc, fmt.Sprintf(`[{"id": %d, "data": "r%d"}]`, i+1, i+1))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(ctx, []string{p}, nil, false))
		tbl, err = tx.Commit(ctx)
		require.NoError(t, err)
	}

	dataPaths := dataFilePaths(t, tbl)
	require.Len(t, dataPaths, 3)

	// A partition-scoped position delete: unpartitioned spec, no ref, no
	// bounds. Committed after the data files, so its sequence number exceeds
	// theirs and it applies to all three.
	delDF, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/pos-del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(delDF.Build())
	require.NoError(t, rd.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	t.Run("all covered files rewritten is dead", func(t *testing.T) {
		rewritten := pathSet(dataPaths...)
		dead, err := compaction.CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), rewritten)
		require.NoError(t, err)
		require.Len(t, dead, 1, "a partition-scoped delete whose every covered file is rewritten must be expunged")
	})

	t.Run("one covered file surviving is retained", func(t *testing.T) {
		rewritten := pathSet(dataPaths[0], dataPaths[1])
		dead, err := compaction.CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), rewritten)
		require.NoError(t, err)
		require.Empty(t, dead, "a surviving covered data file must keep the partition-scoped delete alive")
	})
}

// TestCollectDeadPositionDeletesPartitioned guards the (specID, partition)
// keying of the survivor check: a partition-scoped delete stays alive only
// through a same-partition survivor that predates it — survivors in other
// partitions or with newer sequence numbers must not retain it.
func TestCollectDeadPositionDeletesPartitioned(t *testing.T) {
	ctx := t.Context()
	fs := iceio.LocalFS{}
	tbl := newPartitionedTable(t)
	spec := tbl.Spec()

	addDataFile := func(path, part string) {
		df, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			path, iceberg.ParquetFile, map[int]any{1000: part}, nil, nil, 1, 128)
		require.NoError(t, err)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).AddRows(df.Build()).Commit(ctx))
		tbl, err = tx.Commit(ctx)
		require.NoError(t, err)
	}

	aOld := tbl.Location() + "/data/data=a/old.parquet"
	bOld := tbl.Location() + "/data/data=b/old.parquet"
	aNew := tbl.Location() + "/data/data=a/new.parquet"

	addDataFile(aOld, "a")
	addDataFile(bOld, "b")

	// Partition-scoped delete in partition "a": applies to aOld only.
	delDF, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/data=a/pos-del.parquet", iceberg.ParquetFile,
		map[int]any{1000: "a"}, nil, nil, 1, 128)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(delDF.Build()).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// Same partition, but sequenced after the delete — must not retain it.
	addDataFile(aNew, "a")

	t.Run("same-partition predating survivor retains", func(t *testing.T) {
		dead, err := compaction.CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), pathSet(bOld))
		require.NoError(t, err)
		require.Empty(t, dead, "a same-partition survivor with seq <= the delete's must keep it alive")
	})

	t.Run("cross-partition and newer survivors do not retain", func(t *testing.T) {
		dead, err := compaction.CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), pathSet(aOld))
		require.NoError(t, err)
		require.Len(t, dead, 1, "a survivor in another partition or sequenced after the delete must not retain it")
	})
}

// TestDeadPositionDeleteRoundTrip drives the full integration path the
// collector is documented for: Collect → ExtraDeleteFilesToRemove →
// RewriteDataFiles → commit → re-scan. It pins that a merge-on-read-deleted
// row stays deleted after the expunge, that the delete file disappears from
// scan planning, and that it is removed and counted exactly once even though
// the per-group staging and the collector both report it.
func TestDeadPositionDeleteRoundTrip(t *testing.T) {
	ctx := t.Context()
	fs := iceio.LocalFS{}
	tbl := newCDCStressTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPaths := make([]string, 3)
	for i := range dataPaths {
		dataPaths[i] = tbl.Location() + fmt.Sprintf("/data/d-%d.parquet", i)
		writeParquet(t, dataPaths[i], arrowSc, fmt.Sprintf(`[{"id": %d, "data": "r%d"}]`, i+1, i+1))
	}
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(ctx, dataPaths, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// A real partition-scoped position delete (no ref, no bounds) killing the
	// single row of dataPaths[1] (id 2).
	delPath := tbl.Location() + "/data/pos-del.parquet"
	writeParquet(t, delPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": %q, "pos": 0}]`, dataPaths[1]))
	delInfo, err := os.Stat(delPath)
	require.NoError(t, err)
	delDF, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		delPath, iceberg.ParquetFile, nil, nil, nil, 1, delInfo.Size())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(delDF.Build()).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	require.Equal(t, []int64{1, 3}, scanIDs(t, tbl), "merge-on-read delete must hide id 2")

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	cfg := compaction.Config{
		TargetFileSizeBytes: 64 * 1024 * 1024,
		MinFileSizeBytes:    32 * 1024 * 1024,
		MaxFileSizeBytes:    128 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 1,
		PackingLookback:     compaction.DefaultPackingLookback,
	}
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	rewritten := make(map[string]struct{})
	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		}
		for _, task := range g.Tasks {
			rewritten[task.File.FilePath()] = struct{}{}
		}
	}
	require.Len(t, rewritten, len(dataPaths), "plan must rewrite every data file")

	dead, err := compaction.CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), rewritten)
	require.NoError(t, err)
	require.Len(t, dead, 1, "the delete's every covered file is rewritten, so it must be dead")

	tx = tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{
		ExtraDeleteFilesToRemove: dead,
	})
	require.NoError(t, err)
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	require.Equal(t, 1, result.RemovedPositionDeleteFiles,
		"the dead delete must be removed and counted exactly once despite being both group-staged and collected")
	require.Equal(t, 0, result.RemovedEqualityDeleteFiles,
		"a dead position delete must not be miscounted as an equality delete")

	require.Equal(t, []int64{1, 3}, scanIDs(t, tbl), "id 2 must stay deleted after the expunge")

	postTasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	for _, task := range postTasks {
		require.Empty(t, task.DeleteFiles, "no position delete may survive the rewrite")
	}
}

func scanIDs(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()

	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(0).(*array.Int64)
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
	}
	slices.Sort(ids)

	return ids
}

func newPartitionedTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "data",
	})

	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "pos_delete_collect_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&stubCatalog{metadata: meta},
	)
}

func dataFilePaths(t *testing.T, tbl *table.Table) []string {
	t.Helper()

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	seen := make(map[string]struct{})
	var paths []string
	for _, task := range tasks {
		p := task.File.FilePath()
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		paths = append(paths, p)
	}

	return paths
}

func pathSet(paths ...string) map[string]struct{} {
	s := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		s[p] = struct{}{}
	}

	return s
}
