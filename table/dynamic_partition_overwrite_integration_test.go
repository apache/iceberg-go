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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDynamicOverwriteTable(t *testing.T) (*table.Table, *concurrentTestCatalog) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}})

	return newDynamicOverwriteTableWithSpec(t, spec)
}

func newDynamicOverwriteTableWithSpec(t *testing.T, spec iceberg.PartitionSpec) (*table.Table, *concurrentTestCatalog) {
	t.Helper()

	return newDynamicOverwriteTableWithFormat(t, spec, "2")
}

func newDynamicOverwriteTableWithFormat(t *testing.T, spec iceberg.PartitionSpec, formatVersion string) (*table.Table, *concurrentTestCatalog) {
	t.Helper()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})

	return newDynamicOverwriteTableWithSchema(t, schema, spec, formatVersion)
}

func newDynamicOverwriteTableWithSchema(t *testing.T, schema *iceberg.Schema, spec iceberg.PartitionSpec, formatVersion string) (*table.Table, *concurrentTestCatalog) {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, iceberg.Properties{
		table.PropertyFormatVersion:        formatVersion,
		table.CommitNumRetriesKey:          "2",
		table.CommitMinRetryWaitMsKey:      "1",
		table.CommitMaxRetryWaitMsKey:      "2",
		table.CommitTotalRetryTimeoutMsKey: "1000",
	})
	require.NoError(t, err)
	metaLoc := location + "/metadata/v1.metadata.json"
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: branchPlanningFS}

	return table.New(branchPlanningIdent, meta, metaLoc, branchPlanningFS, cat), cat
}

func newDynamicOverwriteTableWithUnpartitionedHistory(t *testing.T, formatVersion string) *table.Table {
	t.Helper()

	tbl, _ := newDynamicOverwriteTableWithFormat(t, *iceberg.UnpartitionedSpec, formatVersion)
	evolve := tbl.NewTransaction()
	require.NoError(t, evolve.UpdateSpec(true).AddIdentity("id").Commit())
	tbl, err := evolve.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

func appendGlobalEqualityDelete(t *testing.T, tbl *table.Table, id int64) *table.Table {
	t.Helper()

	deletePath := tbl.Location() + "/data/global-equality-delete.parquet"
	require.NoError(t, os.MkdirAll(filepath.Dir(deletePath), 0o755))
	deleteSchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{
			ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
		}), nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, deletePath, deleteSchema, fmt.Sprintf("[{\"id\":%d}]", id))
	deleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 256)
	require.NoError(t, err)
	deleteFile := deleteBuilder.EqualityFieldIDs([]int{1}).Build()

	peer := tbl.NewTransaction()
	require.NoError(t, peer.NewRowDelta(nil).AddDeletes(deleteFile).Commit(t.Context()))
	updated, err := peer.Commit(t.Context())
	require.NoError(t, err)

	return updated
}

func TestDynamicPartitionOverwriteArrowAndConcurrentCommit(t *testing.T) {
	base, cat := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	txn := tbl.NewTransaction()
	reader := idRecordReader(t, tbl, 1, 3)
	require.NoError(t, txn.DynamicPartitionOverwrite(t.Context(), reader, nil))
	reader.Release()
	updated, err := txn.Commit(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, updated, table.MainBranch))

	stale := updated.NewTransaction()
	reader = idRecordReader(t, updated, 1)
	require.NoError(t, stale.DynamicPartitionOverwrite(t.Context(), reader, nil))
	reader.Release()
	updated = appendRowsOnRef(t, updated, table.MainBranch, 1)
	attemptsBefore := cat.attempts.Load()
	_, err = stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrConflictingDataFiles)
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
		"touched partition conflict should fail before another catalog submission")
	assert.ElementsMatch(t, []int64{1, 1, 2, 3}, idsOnRef(t, updated, table.MainBranch))
}

func TestDynamicPartitionOverwriteRejectsOldSpecGlobalEqualityDelete(t *testing.T) {
	for _, formatVersion := range []string{"2", "3"} {
		t.Run("v"+formatVersion, func(t *testing.T) {
			tbl := newDynamicOverwriteTableWithUnpartitionedHistory(t, formatVersion)
			tbl = appendRowsOnRef(t, tbl, table.MainBranch, 7)
			currentSpec := tbl.Spec()
			require.Equal(t, 1, currentSpec.ID())
			oldSpec := tbl.Metadata().PartitionSpecByID(0)
			require.NotNil(t, oldSpec)
			require.True(t, oldSpec.IsUnpartitioned())

			leader := tbl.NewTransaction()
			replacement := idRecordReader(t, tbl, 7)
			require.NoError(t, leader.DynamicPartitionOverwrite(t.Context(), replacement, nil))
			replacement.Release()

			peer := appendGlobalEqualityDelete(t, tbl, 7)
			assert.Empty(t, idsOnRef(t, peer, table.MainBranch))

			_, err := leader.Commit(t.Context())
			require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
		})
	}
}

func TestDynamicPartitionOverwriteAllowsOldSpecGlobalDeleteOutsideTouchedPartition(t *testing.T) {
	for _, formatVersion := range []string{"2", "3"} {
		t.Run("v"+formatVersion, func(t *testing.T) {
			tbl := newDynamicOverwriteTableWithUnpartitionedHistory(t, formatVersion)
			tbl = appendRowsOnRef(t, tbl, table.MainBranch, 7)

			leader := tbl.NewTransaction()
			replacement := idRecordReader(t, tbl, 8)
			require.NoError(t, leader.DynamicPartitionOverwrite(t.Context(), replacement, nil))
			replacement.Release()

			peer := appendGlobalEqualityDelete(t, tbl, 7)
			assert.Empty(t, idsOnRef(t, peer, table.MainBranch))

			committed, err := leader.Commit(t.Context())
			require.NoError(t, err)
			assert.ElementsMatch(t, []int64{8}, idsOnRef(t, committed, table.MainBranch))
		})
	}
}

func TestDynamicPartitionOverwriteAllowsRemovalOfOldSpecGlobalDelete(t *testing.T) {
	for _, formatVersion := range []string{"2", "3"} {
		t.Run("v"+formatVersion, func(t *testing.T) {
			tbl := newDynamicOverwriteTableWithUnpartitionedHistory(t, formatVersion)
			tbl = appendRowsOnRef(t, tbl, table.MainBranch, 7)
			tbl = appendGlobalEqualityDelete(t, tbl, 9)

			leader := tbl.NewTransaction()
			replacement := idRecordReader(t, tbl, 7)
			require.NoError(t, leader.DynamicPartitionOverwrite(t.Context(), replacement, nil))
			replacement.Release()

			builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
				tbl.Location()+"/data/global-equality-delete.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 256)
			require.NoError(t, err)
			peer := tbl.NewTransaction()
			require.NoError(t, peer.ReplaceFiles(t.Context(), nil, nil,
				[]iceberg.DataFile{builder.EqualityFieldIDs([]int{1}).Build()}, nil))
			_, err = peer.Commit(t.Context())
			require.NoError(t, err)

			committed, err := leader.Commit(t.Context())
			require.NoError(t, err)
			assert.Equal(t, []int64{7}, idsOnRef(t, committed, table.MainBranch))
		})
	}
}

// An append staged in the same transaction must be visible to dynamic
// overwrite planning: touched rows are replaced, and other rows survive.
func TestDynamicPartitionOverwriteAfterAppendInSameTransaction(t *testing.T) {
	base, _ := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	txn := tbl.NewTransaction()

	appended := idRecordReader(t, tbl, 1, 3)
	require.NoError(t, txn.Append(t.Context(), appended, nil))
	appended.Release()

	replacement := idRecordReader(t, tbl, 1)
	require.NoError(t, txn.DynamicPartitionOverwrite(t.Context(), replacement, nil))
	replacement.Release()

	updated, err := txn.Commit(t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, updated, table.MainBranch))
}

func TestDynamicPartitionOverwriteTable(t *testing.T) {
	base, _ := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{`[{"id":1},{"id":3}]`})
	require.NoError(t, err)
	defer data.Release()

	updated, err := tbl.DynamicPartitionOverwriteTable(t.Context(), data, 1, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, updated, table.MainBranch))
}

func TestDynamicPartitionOverwriteAllowsConcurrentOtherPartition(t *testing.T) {
	base, cat := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	stale := tbl.NewTransaction()
	reader := idRecordReader(t, tbl, 1, 3)
	require.NoError(t, stale.DynamicPartitionOverwrite(t.Context(), reader, nil))
	reader.Release()

	peer := appendRowsOnRef(t, tbl, table.MainBranch, 4)
	attemptsBefore := cat.attempts.Load()
	result, err := stale.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, attemptsBefore+2, cat.attempts.Load(), "different-partition append should allow refresh and retry")
	assert.ElementsMatch(t, []int64{1, 2, 3, 4}, idsOnRef(t, result, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2, 4}, idsOnRef(t, peer, table.MainBranch))
}

// Overwriting an existing branch must plan against that branch's files,
// rather than replacing files that only main references.
func TestDynamicPartitionOverwriteExistingBranchKeepsMainUnchanged(t *testing.T) {
	base, _ := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 1, 3)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 4)

	require.ElementsMatch(t, []int64{1, 1, 2, 3}, idsOnRef(t, tbl, branchPlanningRef))
	require.ElementsMatch(t, []int64{1, 2, 4}, idsOnRef(t, tbl, table.MainBranch))

	txn, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	reader := idRecordReader(t, tbl, 1, 5)
	require.NoError(t, txn.DynamicPartitionOverwrite(t.Context(), reader, nil))
	reader.Release()
	updated, err := txn.Commit(t.Context())
	require.NoError(t, err)

	require.ElementsMatch(t, []int64{1, 2, 3, 5}, idsOnRef(t, updated, branchPlanningRef))
	require.ElementsMatch(t, []int64{1, 2, 4}, idsOnRef(t, updated, table.MainBranch))
}

func TestDynamicPartitionOverwriteMissingBranchPinsMain(t *testing.T) {
	base, _ := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	stale, err := tbl.NewTransactionOnBranch("audit")
	require.NoError(t, err)
	reader := idRecordReader(t, tbl, 1)
	require.NoError(t, stale.DynamicPartitionOverwrite(t.Context(), reader, nil))
	reader.Release()

	peer := appendRowsOnRef(t, tbl, table.MainBranch, 1)
	_, err = stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	assert.ElementsMatch(t, []int64{1, 1, 2}, idsOnRef(t, peer, table.MainBranch))
}

func TestDynamicPartitionOverwriteBucketTransformWithFiles(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id_bucket", Transform: iceberg.BucketTransform{NumBuckets: 256},
	})
	base, _ := newDynamicOverwriteTableWithSpec(t, spec)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	reader := idRecordReader(t, tbl, 1, 3)
	updated, err := tbl.DynamicPartitionOverwrite(t.Context(), reader, nil)
	reader.Release()
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, updated, table.MainBranch))
}

func TestDynamicPartitionOverwriteV3ArrowInheritsRowIDs(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	base, _ := newDynamicOverwriteTableWithFormat(t, spec, "3")
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)

	reader := idRecordReader(t, tbl, 1, 3)
	updated, err := tbl.DynamicPartitionOverwrite(t.Context(), reader, nil)
	reader.Release()
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, updated, table.MainBranch))

	head := updated.SnapshotByName(table.MainBranch)
	require.NotNil(t, head)
	require.NotNil(t, head.FirstRowID)
	require.NotNil(t, head.AddedRows)
	assert.Positive(t, *head.AddedRows)
}

func dynamicOverwriteJSONReader(t *testing.T, tbl *table.Table, rows string) array.RecordReader {
	t.Helper()
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{rows})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	return array.NewTableReader(data, -1)
}

// A null source value must be stored as a null partition tuple, and a
// replacement of that partition must leave non-null partitions untouched.
func TestDynamicPartitionOverwriteNullIdentityPartition(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "group", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "group", Transform: iceberg.IdentityTransform{},
	})
	tbl, _ := newDynamicOverwriteTableWithSchema(t, schema, spec, "2")

	seed := dynamicOverwriteJSONReader(t, tbl, `[{"id":1,"group":null},{"id":2,"group":7},{"id":3,"group":null}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.Append(t.Context(), seed, nil))
	seed.Release()
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, tbl, table.MainBranch))

	replacement := dynamicOverwriteJSONReader(t, tbl, `[{"id":4,"group":null},{"id":5,"group":null}]`)
	updated, err := tbl.DynamicPartitionOverwrite(t.Context(), replacement, nil)
	replacement.Release()
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{2, 4, 5}, idsOnRef(t, updated, table.MainBranch))
}

// The manifest decoder and Arrow writer can represent day partitions with
// different Go types. An overwrite must still match their logical values.
func TestDynamicPartitionOverwriteDatePartition(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "event_date", Type: iceberg.PrimitiveTypes.Date, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "event_day", Transform: iceberg.DayTransform{},
	})
	tbl, _ := newDynamicOverwriteTableWithSchema(t, schema, spec, "2")

	seed := dynamicOverwriteJSONReader(t, tbl, `[{"id":1,"event_date":"2026-02-10"},{"id":2,"event_date":"2026-02-11"},{"id":3,"event_date":"2026-02-10"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.Append(t.Context(), seed, nil))
	seed.Release()
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, tbl, table.MainBranch))

	replacement := dynamicOverwriteJSONReader(t, tbl, `[{"id":4,"event_date":"2026-02-10"}]`)
	updated, err := tbl.DynamicPartitionOverwrite(t.Context(), replacement, nil)
	replacement.Release()
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{2, 4}, idsOnRef(t, updated, table.MainBranch))
}

// newDynamicOverwriteV3MoRTable writes two rows in each partition so a
// partial DELETE creates a deletion vector instead of dropping a whole file.
func newDynamicOverwriteV3MoRTable(t *testing.T) (*table.Table, *concurrentTestCatalog) {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "group", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "group", Transform: iceberg.IdentityTransform{},
	})
	tbl, cat := newDynamicOverwriteTableWithSchema(t, schema, spec, "3")
	setMode := tbl.NewTransaction()
	require.NoError(t, setMode.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead}))
	var err error
	tbl, err = setMode.Commit(t.Context())
	require.NoError(t, err)
	seed := dynamicOverwriteJSONReader(t, tbl,
		`[{"id":1,"group":10},{"id":2,"group":10},{"id":3,"group":20},{"id":4,"group":20}]`)
	tbl, err = tbl.Append(t.Context(), seed, nil)
	seed.Release()
	require.NoError(t, err)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2, "each partition needs its own data file")

	return tbl, cat
}

// The overwrite must remove the DV tied to the replaced data file, preserve
// the DV for the untouched partition, and preserve row IDs for surviving rows.
func TestDynamicPartitionOverwriteV3RemovesOnlyTouchedDeletionVectors(t *testing.T) {
	tbl, _ := newDynamicOverwriteV3MoRTable(t)
	originalRowIDs := rowIDByID(t, tbl)
	var err error
	for _, id := range []int64{1, 3} {
		tbl, err = tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), id), nil)
		require.NoError(t, err)
	}
	require.Equal(t, 2, liveDVCountOnRef(t, tbl, table.MainBranch))
	require.ElementsMatch(t, []int64{2, 4}, idsOnRef(t, tbl, table.MainBranch))
	beforeData, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	require.Len(t, beforeData, 2)
	replacement := dynamicOverwriteJSONReader(t, tbl, `[{"id":5,"group":10}]`)
	updated, err := tbl.DynamicPartitionOverwrite(t.Context(), replacement, nil)
	replacement.Release()
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{4, 5}, idsOnRef(t, updated, table.MainBranch))
	require.Equal(t, 1, liveDVCountOnRef(t, updated, table.MainBranch))
	afterData, _ := liveFilesOnRef(t, updated, table.MainBranch)
	require.Len(t, afterData, 2)
	var retained, replaced int
	for _, path := range beforeData {
		refs := map[string]struct{}{path: {}}
		if slices.Contains(afterData, path) {
			retained++
			require.Len(t, deleteEntriesReferencing(t, updated, refs), 1,
				"the untouched data file must keep its deletion vector")
		} else {
			replaced++
			require.Empty(t, deleteEntriesReferencing(t, updated, refs),
				"the replaced data file must not keep its deletion vector")
		}
	}
	require.Equal(t, 1, retained)
	require.Equal(t, 1, replaced)
	rowIDs := rowIDByID(t, updated)
	require.Equal(t, originalRowIDs[4], rowIDs[4], "untouched rows must keep their _row_id")
	require.Greater(t, rowIDs[5],
		max(originalRowIDs[1], originalRowIDs[2], originalRowIDs[3], originalRowIDs[4]),
		"the replacement row must receive a fresh _row_id")
}

// Concurrent deletion vectors in replaced partitions must conflict, while
// ones in other partitions must survive a refresh-and-replay of the overwrite.
func TestDynamicPartitionOverwriteConcurrentDeletionVectors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deleteID int64
		conflict bool
	}{
		{name: "touched partition", deleteID: 1, conflict: true},
		{name: "other partition", deleteID: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, cat := newDynamicOverwriteV3MoRTable(t)
			stale := tbl.NewTransaction()
			replacement := dynamicOverwriteJSONReader(t, tbl, `[{"id":5,"group":10}]`)
			require.NoError(t, stale.DynamicPartitionOverwrite(t.Context(), replacement, nil))
			replacement.Release()
			peer, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), tc.deleteID), nil)
			require.NoError(t, err)
			require.Equal(t, 1, liveDVCountOnRef(t, peer, table.MainBranch),
				"concurrent writer must add a deletion vector")
			attemptsBefore := cat.attempts.Load()
			updated, err := stale.Commit(t.Context())
			if tc.conflict {
				require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
				require.Equal(t, attemptsBefore+1, cat.attempts.Load())
				require.ElementsMatch(t, []int64{2, 3, 4}, idsOnRef(t, peer, table.MainBranch))

				return
			}
			require.NoError(t, err)
			require.Equal(t, attemptsBefore+2, cat.attempts.Load(),
				"the independent delete must survive a refresh and retry")
			require.ElementsMatch(t, []int64{4, 5}, idsOnRef(t, updated, table.MainBranch))
			require.Equal(t, 1, liveDVCountOnRef(t, updated, table.MainBranch))
		})
	}
}

// Count data files recursively to catch writes in existing partition
// subdirectories as well as new ones.
func dynamicOverwriteDataPaths(t *testing.T, tbl *table.Table) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(filepath.Join(tbl.Location(), "data"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			paths = append(paths, filepath.ToSlash(path))
		}

		return nil
	})
	require.NoError(t, err)
	slices.Sort(paths)

	return paths
}

// Existing files use their own spec ID in the partition key, so an Arrow
// overwrite with the current spec leaves old-spec rows untouched.
func TestDynamicPartitionOverwriteArrowKeepsOldSpecFiles(t *testing.T) {
	base, _ := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	evolve := tbl.NewTransaction()
	require.NoError(t, evolve.UpdateSpec(true).
		RemoveField("id").
		AddField("id", iceberg.BucketTransform{NumBuckets: 16}, "id_bucket").
		Commit())
	var err error
	tbl, err = evolve.Commit(t.Context())
	require.NoError(t, err)
	before := dynamicOverwriteDataPaths(t, tbl)
	require.NotEmpty(t, before, "test requires already-written data files")
	replacement := idRecordReader(t, tbl, 3)
	updated, err := tbl.DynamicPartitionOverwrite(t.Context(), replacement, nil)
	replacement.Release()
	require.NoError(t, err)
	require.Len(t, dynamicOverwriteDataPaths(t, updated), len(before)+1)
	require.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, updated, table.MainBranch))
}
