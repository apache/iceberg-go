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
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRewriteTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "rewrite_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

// toTaskGroups converts compaction.Plan groups to table.CompactionTaskGroup.
// This bridge is needed because table/ cannot import table/compaction/
// (circular dependency).
func toTaskGroups(groups []compaction.Group) []table.CompactionTaskGroup {
	out := make([]table.CompactionTaskGroup, len(groups))
	for i, g := range groups {
		out[i] = table.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		}
	}

	return out
}

var defaultTestCompactionCfg = compaction.Config{
	TargetFileSizeBytes: 10 * 1024 * 1024,
	MinFileSizeBytes:    5 * 1024 * 1024,
	MaxFileSizeBytes:    20 * 1024 * 1024,
	MinInputFiles:       2,
	DeleteFileThreshold: 5,
	PackingLookback:     compaction.DefaultPackingLookback,
}

func TestRewriteDataFiles_SmallFiles(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 5 small data files, one per commit.
	for i := range 5 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row-%d"}]`, i+1, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	assertRowCount(t, tbl, 5)

	// Plan compaction.
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 5)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	// Execute compaction.
	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), toTaskGroups(plan.Groups), false, nil)
	require.NoError(t, err)

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Verify: same rows, fewer files.
	assertRowCount(t, tbl, 5)
	assert.Equal(t, 5, result.RemovedDataFiles)
	assert.Equal(t, 1, result.AddedDataFiles)
	assert.Equal(t, 1, result.RewrittenGroups)
	assert.Greater(t, result.BytesBefore, int64(0))
	assert.Greater(t, result.BytesAfter, int64(0))

	// Verify new snapshot has fewer files.
	newTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	assert.Len(t, newTasks, 1)
}

func TestRewriteDataFiles_WithPositionDeletes(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 3 data files with 2 rows each.
	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}, {"id": %d, "data": "b"}]`, i*2+1, i*2+2))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	assertRowCount(t, tbl, 6)

	// Add a position delete targeting the first data file.
	firstDataPath := tbl.Location() + "/data/file-0.parquet"
	posDelPath := tbl.Location() + "/data/pos-del-001.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 0}]`, firstDataPath))

	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(posDelBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	assertRowCount(t, tbl, 5) // one row deleted

	// Plan and execute compaction with low delete threshold.
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1 // force compaction due to deletes

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	tx2 := tbl.NewTransaction()
	result, err := tx2.RewriteDataFiles(t.Context(), toTaskGroups(plan.Groups), false, nil)
	require.NoError(t, err)

	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	// After compaction: delete is applied, 5 rows remain, delete file removed.
	assertRowCount(t, tbl, 5)
	assert.Greater(t, result.RemovedDeleteFiles, 0)

	// Verify no delete files remain.
	newTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	for _, task := range newTasks {
		assert.Empty(t, task.DeleteFiles, "no position deletes should remain after compaction")
	}
}

func TestRewriteDataFiles_EmptyPlan(t *testing.T) {
	tbl := newRewriteTestTable(t)

	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), nil, false, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, result.RewrittenGroups)
	assert.Equal(t, 0, result.AddedDataFiles)
	assert.Equal(t, 0, result.RemovedDataFiles)
	assert.Equal(t, int64(0), result.BytesBefore)
}

func TestRewriteDataFiles_EmptyGroupSkipped(t *testing.T) {
	tbl := newRewriteTestTable(t)

	groups := []table.CompactionTaskGroup{
		{PartitionKey: "empty", Tasks: nil, TotalSizeBytes: 0},
	}

	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), groups, false, nil)
	require.NoError(t, err)

	assert.Equal(t, 0, result.RewrittenGroups)
}

func TestRewriteDataFiles_PartialProgress(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 6 small files.
	for i := range 6 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row"}]`, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), toTaskGroups(plan.Groups), true, nil)
	require.NoError(t, err)

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	assertRowCount(t, tbl, 6)
	assert.Greater(t, result.RewrittenGroups, 0)
	assert.Equal(t, 6, result.RemovedDataFiles)
}

func TestRewriteDataFiles_ContextCancellation(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 3 files.
	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row"}]`, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	// Cancel context before execution.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	tx := tbl.NewTransaction()
	_, err = tx.RewriteDataFiles(ctx, toTaskGroups(plan.Groups), false, nil)
	require.Error(t, err)
}

func TestRewriteDataFiles_EqualityDeletesSurviveCompaction(t *testing.T) {
	tbl := newRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write 3 data files with 2 rows each.
	for i := range 3 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeParquetFile(t, dataPath, arrowSc, fmt.Sprintf(
			`[{"id": %d, "data": "a"}, {"id": %d, "data": "b"}]`, i*2+1, i*2+2))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	assertRowCount(t, tbl, 6)

	// Add an equality delete that removes id=2.
	delSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	delArrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, delArrowSc,
		strings.NewReader(`[{"id": 2}]`))
	require.NoError(t, err)

	records := func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}

	tx := tbl.NewTransaction()
	deleteFiles, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
	require.NoError(t, err)
	rec.Release()

	rd := tx.NewRowDelta(nil)
	for _, df := range deleteFiles {
		rd.AddDeletes(df)
	}
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	assertRowCount(t, tbl, 5) // id=2 deleted

	// Plan and execute compaction.
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	cfg := defaultTestCompactionCfg
	cfg.DeleteFileThreshold = 1

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	tx2 := tbl.NewTransaction()
	result, err := tx2.RewriteDataFiles(t.Context(), toTaskGroups(plan.Groups), false, nil)
	require.NoError(t, err)

	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	// Equality deletes are applied during the read phase of compaction,
	// so compacted files don't contain the deleted row. The equality
	// delete file is NOT removed by collectSafePositionDeletes (which
	// only handles position deletes). It remains in the manifest but
	// no longer matches the new data files (higher sequence number).
	assertRowCount(t, tbl, 5)
	assert.Equal(t, 0, result.RemovedDeleteFiles,
		"equality delete files should NOT be removed by compaction")
	assert.Greater(t, result.RemovedDataFiles, 0, "compaction should have rewritten data files")

	// The deleted row (id=2) must not reappear after compaction.
	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}

	assert.NotContains(t, ids, int64(2), "deleted row id=2 must not reappear after compaction")
}
