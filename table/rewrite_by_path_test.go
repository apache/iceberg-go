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

	"github.com/apache/arrow-go/v18/parquet"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPathRewriteFixture(t *testing.T) (*table.Table, string, string) {
	t.Helper()

	return newPathRewriteFixtureVersion(t, 2)
}

func newPathRewriteFixtureVersion(t *testing.T, version int) (*table.Table, string, string) {
	t.Helper()

	tbl, _ := newConcurrentRewriteTestTableVersion(t, version)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	oldPath := tbl.Location() + "/data/original.parquet"
	replacementPath := tbl.Location() + "/data/replacement.parquet"
	rows := `[{"id":1,"data":"first"},{"id":2,"data":"second"}]`
	writeParquetFile(t, oldPath, sc, rows)
	writeParquetFile(t, replacementPath, sc, rows)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFilesByPath(t.Context(), []string{oldPath}, nil, false))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	return tbl, oldPath, replacementPath
}

func TestStageRewrittenDataFilesByPathProducesReplaceSnapshot(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	before := tbl.CurrentSnapshot()
	require.NotNil(t, before)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath},
		iceberg.Properties{"reason": "compaction"}))

	tbl, err := txn.Commit(t.Context())
	require.NoError(t, err)

	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, table.OpReplace, snapshot.Summary.Operation)
	assert.Equal(t, "compaction", snapshot.Summary.Properties["reason"])
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
	dataPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	assert.Equal(t, []string{replacementPath}, dataPaths)
	assert.NotEqual(t, before.SnapshotID, snapshot.SnapshotID)
}

func TestStageRewrittenDataFilesByPathAllowsNewColumnMetrics(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTableVersion(t, 2)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	originalPath := tbl.Location() + "/data/limited-metrics.parquet"
	replacementPath := tbl.Location() + "/data/full-metrics.parquet"
	rows := `[{"id":1,"data":"first"},{"id":2,"data":"second"}]`
	writeParquetFile(t, originalPath, sc, rows)
	writeParquetFile(t, replacementPath, sc, rows)

	const metricProperty = "write.metadata.metrics.column.data"
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{metricProperty: "none"}))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{originalPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotContains(t, tasks[0].File.ValueCounts(), 2)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.RemoveProperties([]string{metricProperty}))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{originalPath}, []string{replacementPath}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err = tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].File.ValueCounts(), 2)
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
	assert.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)
}

func TestStageRewrittenDataFilesByPathAllowsDisabledReplacementMetrics(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{
		"write.metadata.metrics.column.data": "none",
	}))
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].File.ValueCounts(), 2)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err = tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotContains(t, tasks[0].File.ValueCounts(), 2)
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
	assert.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)
}

func TestStageRewrittenDataFilesByPathAllowsMissingReplacementNullStats(t *testing.T) {
	tbl, oldPath, _ := newPathRewriteFixture(t)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	replacementPath := tbl.Location() + "/data/no-parquet-stats.parquet"
	writeParquetFileWithProperties(t, replacementPath, sc,
		`[{"id":1,"data":"first"},{"id":2,"data":"second"}]`, 0,
		parquet.NewWriterProperties(parquet.WithStats(false)))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Contains(t, tasks[0].File.ValueCounts(), 2)
	require.NotContains(t, tasks[0].File.NullValueCounts(), 2)
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
	assert.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)
}

func TestStageRewrittenDataFilesByPathAllowsPartialSourceMetrics(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTableVersion(t, 2)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	firstPath := tbl.Location() + "/data/partially-measured-first.parquet"
	secondPath := tbl.Location() + "/data/partially-measured-second.parquet"
	replacementPath := tbl.Location() + "/data/partially-measured-merged.parquet"
	writeParquetFile(t, firstPath, sc, `[{"id":1,"data":"first"}]`)
	writeParquetFile(t, secondPath, sc, `[{"id":2,"data":"second"}]`)
	writeParquetFile(t, replacementPath, sc, `[{"id":1,"data":"first"},{"id":2,"data":"second"}]`)

	const metricProperty = "write.metadata.metrics.column.data"
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{metricProperty: "none"}))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{firstPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.RemoveProperties([]string{metricProperty}))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{secondPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	measured := 0
	for _, task := range tasks {
		if _, ok := task.File.ValueCounts()[2]; ok {
			measured++
		}
	}
	require.Equal(t, 1, measured, "the sources must have different metrics coverage")

	tx = tbl.NewTransaction()
	require.NoError(t, tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{firstPath, secondPath}, []string{replacementPath}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
	assert.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)
}

func TestStageRewrittenDataFilesByPathRejectsInvalidRecordCounts(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows string
	}{
		{"drops rows without deletes", `[{"id":1,"data":"first"}]`},
		{"adds rows", `[{"id":1,"data":"first"},{"id":2,"data":"second"},{"id":3,"data":"extra"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, oldPath, _ := newPathRewriteFixture(t)
			sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
			require.NoError(t, err)
			replacementPath := tbl.Location() + "/data/invalid-replacement.parquet"
			writeParquetFile(t, replacementPath, sc, tc.rows)

			txn := tbl.NewTransaction()
			err = txn.StageRewrittenDataFilesByPath(t.Context(),
				[]string{oldPath}, []string{replacementPath}, nil)
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			require.ErrorContains(t, err, "record count")
		})
	}
}

// A delete file elsewhere in the table does not prove that fewer physical
// rows in the replacement are valid. Only the explicit rewrite builder may
// handle delete-aware compaction with different physical row counts.
func TestStageRewrittenDataFilesByPathRejectsReducedRowsWithExistingDeleteFile(t *testing.T) {
	tbl, oldPath, _ := newPathRewriteFixture(t)
	deletes := newEqDeleteFile(t, tbl.Location()+"/data/rewrite-count-delete.parquet")
	peer := tbl.NewTransaction()
	require.NoError(t, peer.NewRowDelta(nil).AddDeletes(deletes).Commit(t.Context()))
	var err error
	tbl, err = peer.Commit(t.Context())
	require.NoError(t, err)

	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	replacementPath := tbl.Location() + "/data/short-replacement.parquet"
	writeParquetFile(t, replacementPath, sc, `[{"id":1,"data":"first"}]`)

	txn := tbl.NewTransaction()
	err = txn.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "applicable delete files")
}

func TestStageRewrittenDataFilesByPathValidatesPaths(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	peerPath := tbl.Location() + "/data/live-peer.parquet"
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	writeParquetFile(t, peerPath, sc, "[{\"id\":3,\"data\":\"peer\"}]")
	peer := tbl.NewTransaction()
	require.NoError(t, peer.AddDataFilesByPath(t.Context(), []string{peerPath}, nil, false))
	tbl, err = peer.Commit(t.Context())
	require.NoError(t, err)

	cases := []struct {
		name    string
		removed []string
		added   []string
	}{
		{"no removed files", nil, []string{replacementPath}},
		{"no added files", []string{oldPath}, nil},
		{"blank removed path", []string{""}, []string{replacementPath}},
		{"blank added path", []string{oldPath}, []string{""}},
		{"duplicate removed path", []string{oldPath, oldPath}, []string{replacementPath}},
		{"duplicate added path", []string{oldPath}, []string{replacementPath, replacementPath}},
		{"overlapping path sets", []string{oldPath}, []string{oldPath}},
		{"unknown removed path", []string{tbl.Location() + "/unknown.parquet"}, []string{replacementPath}},
		{"existing added path", []string{oldPath}, []string{peerPath}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txn := tbl.NewTransaction()
			err := txn.StageRewrittenDataFilesByPath(t.Context(), tc.removed, tc.added, nil)
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			if tc.name == "existing added path" {
				require.ErrorContains(t, err, peerPath)
			}
			if tc.name == "unknown removed path" {
				require.ErrorContains(t, err, "unknown.parquet")
			}
		})
	}

	t.Run("missing replacement parquet", func(t *testing.T) {
		txn := tbl.NewTransaction()
		err := txn.StageRewrittenDataFilesByPath(t.Context(), []string{oldPath},
			[]string{tbl.Location() + "/missing.parquet"}, nil)
		require.Error(t, err)
	})

	t.Run("no snapshot", func(t *testing.T) {
		empty, _ := newConcurrentRewriteTestTable(t)
		txn := empty.NewTransaction()
		err := txn.StageRewrittenDataFilesByPath(t.Context(), []string{"old"}, []string{"new"}, nil)
		require.ErrorIs(t, err, table.ErrInvalidOperation)
	})
}

func TestStageRewrittenDataFilesByPathRespectsCanceledContext(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	txn := tbl.NewTransaction()
	err := txn.StageRewrittenDataFilesByPath(ctx, []string{oldPath}, []string{replacementPath}, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestStageRewrittenDataFilesByPathRejectsConcurrentEqualityDelete(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	leader := tbl.NewTransaction()
	require.NoError(t, leader.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil))

	peer := tbl.NewTransaction()
	deletes := newEqDeleteFile(t, tbl.Location()+"/data/concurrent-eq-delete.parquet")
	require.NoError(t, peer.NewRowDelta(nil).AddDeletes(deletes).Commit(t.Context()))
	_, err := peer.Commit(t.Context())
	require.NoError(t, err)

	_, err = leader.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
}

func TestStageRewrittenDataFilesByPathTargetsBranch(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 3, 4)
	require.Nil(t, tbl.CurrentSnapshot())

	oldPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	require.Len(t, oldPaths, 1)

	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	replacement := tbl.Location() + "/data/branch-replacement.parquet"
	writeParquetFile(t, replacement, sc, `[{"id":3},{"id":4}]`)

	txn, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.StageRewrittenDataFilesByPath(t.Context(), oldPaths,
		[]string{replacement}, nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	require.Nil(t, tbl.CurrentSnapshot(), "rewriting a branch must not create a main snapshot")
	assert.Equal(t, []int64{3, 4}, idsOnRef(t, tbl, branchPlanningRef))
	dataPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	assert.Equal(t, []string{replacement}, dataPaths)
	branchHead := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, branchHead)
	assert.Equal(t, table.OpReplace, branchHead.Summary.Operation)
}

func TestStageRewrittenDataFilesByPathRejectsV3(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{table.PropertyFormatVersion: "3"})
	txn := tbl.NewTransaction()
	err := txn.StageRewrittenDataFilesByPath(t.Context(), []string{"old.parquet"}, []string{"new.parquet"}, nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	require.ErrorContains(t, err, "row lineage")
}

func TestStageRewrittenDataFilesByPathRejectsPartitionedSpec(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}})
	location := t.TempDir()
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	tbl := table.New(branchPlanningIdent, meta, location+"/metadata/v1.metadata.json", branchPlanningFS, nil)
	err = tbl.NewTransaction().StageRewrittenDataFilesByPath(t.Context(), []string{"old.parquet"}, []string{"new.parquet"}, nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	require.ErrorContains(t, err, "partitioned tables")
}

func TestStageRewrittenDataFilesByPathV1(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixtureVersion(t, 1)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.StageRewrittenDataFilesByPath(t.Context(), []string{oldPath}, []string{replacementPath}, nil))
	result, err := txn.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, table.OpReplace, result.CurrentSnapshot().Summary.Operation)
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, result, table.MainBranch))
}

func TestStageRewrittenDataFilesByPathRejectsRealDeleteFiles(t *testing.T) {
	for _, kind := range []string{"equality", "position"} {
		t.Run(kind, func(t *testing.T) {
			tbl, oldPath, replacementPath := newPathRewriteFixture(t)
			deletePath := tbl.Location() + "/data/rewrite-" + kind + "-delete.parquet"

			var deleted iceberg.DataFile
			switch kind {
			case "equality":
				deleteSchema := iceberg.NewSchema(0, iceberg.NestedField{
					ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
				})
				sc, err := table.SchemaToArrowSchema(deleteSchema, nil, true, false)
				require.NoError(t, err)
				writeParquetFile(t, deletePath, sc, "[{\"id\":2}]")
				builder, err := iceberg.NewDataFileBuilder(
					*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
					deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
				require.NoError(t, err)
				deleted = builder.EqualityFieldIDs([]int{1}).Build()
			case "position":
				writeParquetFile(t, deletePath, table.PositionalDeleteArrowSchema,
					fmt.Sprintf("[{\"file_path\":%q,\"pos\":1}]", oldPath))
				builder, err := iceberg.NewDataFileBuilder(
					*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
					deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
				require.NoError(t, err)
				deleted = builder.ReferencedDataFile(oldPath).Build()
			}

			peer := tbl.NewTransaction()
			require.NoError(t, peer.NewRowDelta(nil).AddDeletes(deleted).Commit(t.Context()))
			tbl, err := peer.Commit(t.Context())
			require.NoError(t, err)
			assert.Equal(t, []int64{1}, idsOnRef(t, tbl, table.MainBranch))

			before := tbl.CurrentSnapshot()
			txn := tbl.NewTransaction()
			err = txn.StageRewrittenDataFilesByPath(t.Context(),
				[]string{oldPath}, []string{replacementPath}, nil)
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			require.ErrorContains(t, err, "applicable delete files")
			require.ErrorContains(t, err, oldPath)

			tbl, err = txn.Commit(t.Context())
			require.NoError(t, err)
			assert.Equal(t, before.SnapshotID, tbl.CurrentSnapshot().SnapshotID)
			assert.Equal(t, []int64{1}, idsOnRef(t, tbl, table.MainBranch))
		})
	}
}

func TestStageRewrittenDataFilesByPathAllowsUnrelatedPositionDelete(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	peerPath := tbl.Location() + "/data/unrelated-data.parquet"
	writeParquetFile(t, peerPath, sc, "[{\"id\":9,\"data\":\"other\"}]")
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{peerPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	deletePath := tbl.Location() + "/data/unrelated-position-delete.parquet"
	writeParquetFile(t, deletePath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf("[{\"file_path\":%q,\"pos\":0}]", peerPath))
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(builder.ReferencedDataFile(peerPath).Build()).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))

	t.Run("rejects reusing another file's delete path", func(t *testing.T) {
		collision := tbl.NewTransaction()
		err := collision.StageRewrittenDataFilesByPath(t.Context(),
			[]string{oldPath}, []string{deletePath}, nil)
		require.ErrorIs(t, err, table.ErrInvalidOperation)
		require.ErrorContains(t, err, deletePath)
		require.ErrorContains(t, err, "already referenced by the table as a delete file")
	})

	tx = tbl.NewTransaction()
	require.NoError(t, tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
	paths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	assert.ElementsMatch(t, []string{replacementPath, peerPath}, paths)
}

func TestStageRewrittenDataFilesByPathManyToMany(t *testing.T) {
	cases := []struct {
		name         string
		sources      []string
		replacements []string
	}{
		{
			name: "two to one",
			sources: []string{
				"[{\"id\":1,\"data\":\"first\"}]",
				"[{\"id\":2,\"data\":\"second\"}]",
			},
			replacements: []string{
				"[{\"id\":1,\"data\":\"first\"},{\"id\":2,\"data\":\"second\"}]",
			},
		},
		{
			name: "one to two",
			sources: []string{
				"[{\"id\":1,\"data\":\"first\"},{\"id\":2,\"data\":\"second\"}]",
			},
			replacements: []string{
				"[{\"id\":1,\"data\":\"first\"}]",
				"[{\"id\":2,\"data\":\"second\"}]",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl, _ := newConcurrentRewriteTestTableVersion(t, 2)
			sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
			require.NoError(t, err)

			sourcePaths := make([]string, 0, len(tc.sources))
			for i, rows := range tc.sources {
				path := fmt.Sprintf("%s/data/source-%d.parquet", tbl.Location(), i)
				writeParquetFile(t, path, sc, rows)
				sourcePaths = append(sourcePaths, path)
			}
			replacementPaths := make([]string, 0, len(tc.replacements))
			for i, rows := range tc.replacements {
				path := fmt.Sprintf("%s/data/replacement-%d.parquet", tbl.Location(), i)
				writeParquetFile(t, path, sc, rows)
				replacementPaths = append(replacementPaths, path)
			}
			untouched := tbl.Location() + "/data/untouched.parquet"
			writeParquetFile(t, untouched, sc, "[{\"id\":9,\"data\":\"untouched\"}]")

			initial := append(append([]string{}, sourcePaths...), untouched)
			tx := tbl.NewTransaction()
			require.NoError(t, tx.AddDataFilesByPath(t.Context(), initial, nil, false))
			tbl, err = tx.Commit(t.Context())
			require.NoError(t, err)

			tx = tbl.NewTransaction()
			require.NoError(t, tx.StageRewrittenDataFilesByPath(t.Context(),
				sourcePaths, replacementPaths, nil))
			tbl, err = tx.Commit(t.Context())
			require.NoError(t, err)
			assert.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)
			assert.ElementsMatch(t, []int64{1, 2, 9}, idsOnRef(t, tbl, table.MainBranch))
			paths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
			expectedPaths := append(append([]string{}, replacementPaths...), untouched)
			assert.ElementsMatch(t, expectedPaths, paths)
		})
	}
}

func TestStageRewrittenDataFilesByPathRejectsLostOptionalColumn(t *testing.T) {
	tbl, oldPath, _ := newPathRewriteFixture(t)
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	sc, err := table.SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	replacementPath := tbl.Location() + "/data/lost-optional-column.parquet"
	writeParquetFile(t, replacementPath, sc, "[{\"id\":1},{\"id\":2}]")

	tx := tbl.NewTransaction()
	err = tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "value count")
}

func TestStageRewrittenDataFilesByPathDeduplicatesSplitTasks(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTableVersion(t, 2)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	rows := "[{\"id\":1,\"data\":\"first\"},{\"id\":2,\"data\":\"second\"},{\"id\":3,\"data\":\"third\"},{\"id\":4,\"data\":\"fourth\"}]"
	sourcePath := tbl.Location() + "/data/multiple-row-groups.parquet"
	replacementPath := tbl.Location() + "/data/merged-row-groups.parquet"
	writeParquetFileWithProperties(t, sourcePath, sc, rows, 1, nil)
	writeParquetFile(t, replacementPath, sc, rows)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.ReadSplitTargetSizeKey: "1"}))
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{sourcePath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Greater(t, len(tasks), 1, "the fixture must split one physical data file")

	tx = tbl.NewTransaction()
	require.NoError(t, tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{sourcePath}, []string{replacementPath}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)
	assert.ElementsMatch(t, []int64{1, 2, 3, 4}, idsOnRef(t, tbl, table.MainBranch))
	paths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	assert.Equal(t, []string{replacementPath}, paths)
}

func TestStageRewrittenDataFilesByPathRejectsNullifiedColumn(t *testing.T) {
	tbl, oldPath, _ := newPathRewriteFixture(t)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	replacementPath := tbl.Location() + "/data/nullified-column.parquet"
	writeParquetFile(t, replacementPath, sc, `[{"id":1,"data":null},{"id":2,"data":null}]`)

	before := tbl.CurrentSnapshot()
	tx := tbl.NewTransaction()
	err = tx.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "null count")

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, before.SnapshotID, result.CurrentSnapshot().SnapshotID)
}

func TestStageRewrittenDataFilesByPathRejectsConcurrentPositionDelete(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)

	leader := tbl.NewTransaction()
	require.NoError(t, leader.StageRewrittenDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil))

	deletePath := tbl.Location() + "/data/concurrent-position-delete.parquet"
	writeParquetFile(t, deletePath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path":%q,"pos":1}]`, oldPath))
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	peer := tbl.NewTransaction()
	require.NoError(t, peer.NewRowDelta(nil).
		AddDeletes(builder.ReferencedDataFile(oldPath).Build()).Commit(t.Context()))
	_, err = peer.Commit(t.Context())
	require.NoError(t, err)

	_, err = leader.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
}

func TestStageRewrittenDataFilesByPathRejectsHistoricalPartitionedSource(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTableVersion(t, 2)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSpec(false).
		AddField("id", iceberg.IdentityTransform{}, "id_identity").Commit())
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	oldPath := tbl.Location() + "/data/old-partition-spec.parquet"
	replacementPath := tbl.Location() + "/data/new-unpartitioned.parquet"
	rows := `[{"id":1,"data":"first"}]`
	writeParquetFile(t, oldPath, sc, rows)
	writeParquetFile(t, replacementPath, sc, rows)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{oldPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpdateSpec(false).RemoveField("id_identity").Commit())
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	spec := tbl.Spec()
	require.Zero(t, spec.NumFields())

	before := tbl.CurrentSnapshot()
	tx = tbl.NewTransaction()
	err = tx.StageRewrittenDataFilesByPath(t.Context(), []string{oldPath}, []string{replacementPath}, nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	require.ErrorContains(t, err, "partitioned spec")

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, before.SnapshotID, result.CurrentSnapshot().SnapshotID)
}
