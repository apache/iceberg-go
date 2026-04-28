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
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCDCStress emulates sustained Postgres → Iceberg CDC replication:
// many small commits, each adding one data file and one equality
// delete. This is the workload from issue apache/iceberg-go#946.
//
// Without dead-eq-delete cleanup, every preserved eq-delete sits in
// every future snapshot's manifests forever, and scan latency degrades
// linearly with the number of accumulated commits. With cleanup
// (compaction.CollectDeadEqualityDeletes threaded through
// table.RewriteDataFilesOptions.ExtraDeleteFilesToRemove), a single
// compaction collapses both data files and eq-deletes back to
// near-baseline.
//
// The test asserts:
//  1. Pre-compaction: data files and eq-delete files both equal
//     numCDCCycles (we wrote one of each per cycle).
//  2. Post-compaction: a single data file remains and zero eq-deletes
//     are referenced from any surviving scan task.
//  3. Scan correctness: every row that was deleted stays deleted, and
//     every row that wasn't survives.
//
// Skipped under -short.
func TestCDCStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CDC stress test in short mode")
	}

	const (
		numCDCCycles = 80   // ~one snapshot per CDC tick
		baselineRows = 1000 // initial table size
	)

	tbl := newCDCStressTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	delSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	delArrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(t, err)

	// Step 1: seed the table with baseline rows in a single data file.
	seedPath := tbl.Location() + "/data/seed.parquet"
	writeParquet(t, seedPath, arrowSc, generateRowsJSON(baselineRows))
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{seedPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Step 2: simulate the CDC replication stream. Each cycle commits
	// (1) a tiny data file and (2) one eq-delete file in separate
	// snapshots — this is the worst case for manifest fanout.
	deletedIDs := make(map[int64]struct{}, numCDCCycles)
	for cycle := range numCDCCycles {
		// (a) Append one small data file (e.g. INSERT replica).
		extraID := int64(baselineRows + cycle + 1)
		dataPath := tbl.Location() + fmt.Sprintf("/data/cdc-%05d.parquet", cycle)
		writeParquet(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "cdc-%d"}]`, extraID, cycle))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)

		// (b) Commit one eq-delete (UPDATE/DELETE replica). Cycle
		// through the seeded baseline rows so each eq-delete actually
		// applies to a real surviving row.
		victim := int64((cycle % baselineRows) + 1)
		deletedIDs[victim] = struct{}{}

		rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, delArrowSc,
			strings.NewReader(fmt.Sprintf(`[{"id": %d}]`, victim)))
		require.NoError(t, err)

		records := func(yield func(arrow.RecordBatch, error) bool) {
			yield(rec, nil)
		}

		tx = tbl.NewTransaction()
		deleteFiles, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
		rec.Release()
		require.NoError(t, err)

		rd := tx.NewRowDelta(nil)
		for _, df := range deleteFiles {
			rd.AddDeletes(df)
		}
		require.NoError(t, rd.Commit(t.Context()))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	// Step 3: pre-compaction observations.
	preTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	preDataFiles := len(preTasks)
	preEqDeleteRefs := countDistinctEqDeletePaths(preTasks)
	t.Logf("pre-compaction: %d data files, %d distinct eq-deletes referenced",
		preDataFiles, preEqDeleteRefs)
	require.Equal(t, 1+numCDCCycles, preDataFiles,
		"every CDC cycle should have produced one new data file (plus seed)")
	require.Equal(t, numCDCCycles, preEqDeleteRefs,
		"every CDC cycle should have produced one eq-delete referenced by the surviving data files")

	// Step 4: plan + compact. DeleteFileThreshold=1 forces compaction
	// even though the data files are tiny.
	cfg := compaction.Config{
		TargetFileSizeBytes: 64 * 1024 * 1024,
		MinFileSizeBytes:    32 * 1024 * 1024,
		MaxFileSizeBytes:    128 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 1,
		PackingLookback:     compaction.DefaultPackingLookback,
	}
	plan, err := cfg.PlanCompaction(preTasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups, "planner must select something to compact")

	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		}
	}

	// Step 5: compute dead eq-deletes against the pre-rewrite snapshot.
	// This is the orchestration the CLI / library caller does.
	rewrittenSet := make(map[string]struct{})
	for _, g := range plan.Groups {
		for _, task := range g.Tasks {
			rewrittenSet[task.File.FilePath()] = struct{}{}
		}
	}
	fs := iceio.LocalFS{}
	deadEqDeletes, err := compaction.CollectDeadEqualityDeletes(
		t.Context(), fs, tbl.CurrentSnapshot(), rewrittenSet)
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	result, err := tx.RewriteDataFiles(t.Context(), groups, table.RewriteDataFilesOptions{
		PartialProgress:          false,
		ExtraDeleteFilesToRemove: deadEqDeletes,
	})
	require.NoError(t, err)
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	t.Logf("compaction: %d groups, +%d / -%d data files, removed %d position + %d equality delete files",
		result.RewrittenGroups, result.AddedDataFiles, result.RemovedDataFiles,
		result.RemovedPositionDeleteFiles, result.RemovedEqualityDeleteFiles)

	// Step 6: post-compaction observations.
	postTasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	postDataFiles := len(postTasks)
	postEqDeleteRefs := countDistinctEqDeletePaths(postTasks)
	t.Logf("post-compaction: %d data files, %d distinct eq-deletes referenced",
		postDataFiles, postEqDeleteRefs)

	assert.Less(t, postDataFiles, preDataFiles,
		"compaction must reduce data file count")
	assert.Equal(t, 0, postEqDeleteRefs,
		"all dead eq-deletes must be expunged after CDC compaction")
	assert.GreaterOrEqual(t, result.RemovedEqualityDeleteFiles, numCDCCycles,
		"every CDC cycle's eq-delete should be reported as removed")

	// Step 7: row-level correctness. Every deleted id must stay deleted.
	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	survivors := make(map[int64]struct{})
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			survivors[col.Value(i)] = struct{}{}
		}
		rec.Release()
	}

	for id := range deletedIDs {
		_, present := survivors[id]
		assert.False(t, present, "deleted row id=%d must not reappear after compaction", id)
	}
	for i := int64(1); i <= int64(baselineRows); i++ {
		if _, deleted := deletedIDs[i]; deleted {
			continue
		}
		_, present := survivors[i]
		assert.True(t, present, "surviving baseline row id=%d must be visible", i)
	}
}

// stubCatalog is a minimal catalog implementation that holds metadata
// in memory for the stress test's commits. The CDC test does many
// small commits but never hits the catalog from outside.
type stubCatalog struct {
	metadata table.Metadata
}

func (s *stubCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (s *stubCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(s.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	s.metadata = meta

	return meta, "", nil
}

func newCDCStressTable(t *testing.T) *table.Table {
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
		table.Identifier{"db", "cdc_stress_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&stubCatalog{metadata: meta},
	)
}

func writeParquet(t testing.TB, path string, sc *arrow.Schema, jsonData string) {
	t.Helper()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(jsonData))
	require.NoError(t, err)
	defer rec.Release()

	fs := iceio.LocalFS{}
	fw, err := fs.Create(path)
	require.NoError(t, err)

	tbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
}

func generateRowsJSON(n int) string {
	var b strings.Builder
	b.WriteByte('[')
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id": %d, "data": "row-%06d"}`, i+1, i+1)
	}
	b.WriteByte(']')

	return b.String()
}

func countDistinctEqDeletePaths(tasks []table.FileScanTask) int {
	seen := make(map[string]struct{})
	for _, task := range tasks {
		for _, df := range task.EqualityDeleteFiles {
			seen[df.FilePath()] = struct{}{}
		}
	}

	return len(seen)
}
