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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestConvertEqualityDeletesToDeletionVectors(t *testing.T) {
	ctx := t.Context()
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`)
	beforeDelete := tbl.CurrentSnapshot().SnapshotID
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2},{"id":4}]`)
	deleteSnapshot := tbl.CurrentSnapshot().SnapshotID

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.NotNil(t, result.Table)
	require.Equal(t, 1, result.ConvertedEqualityDeleteFiles)
	require.Equal(t, 1, result.AddedDeletionVectors)
	require.Equal(t, []int64{1, 3, 5}, idsInTable(t, result.Table))
	require.Equal(t, 1, liveDVCount(t, result.Table))
	require.Equal(t, 0, liveDeleteFileCount(t, result.Table, iceberg.EntryContentEqDeletes))
	require.Equal(t, table.OpReplace, result.Table.CurrentSnapshot().Summary.Operation)

	// The equality-delete snapshot emits the deletes once. The conversion
	// snapshot is maintenance and must not emit them again.
	deleteRows := readChangelog(t, ctx,
		tbl.NewIncrementalChangelogScan().FromSnapshotExclusive(beforeDelete).ToSnapshot(deleteSnapshot))
	require.Equal(t, []int64{2, 4}, changelogIDs(deleteRows, table.ChangelogOpDelete))
	conversionRows := readChangelog(t, ctx,
		result.Table.NewIncrementalChangelogScan().FromSnapshotExclusive(deleteSnapshot))
	require.Empty(t, conversionRows)
}

func TestConvertEqualityDeletesMergesExistingPositionDeletes(t *testing.T) {
	ctx := t.Context()
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "2"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`)
	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDeleteFileCount(t, tbl, iceberg.EntryContentPosDeletes))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":4}]`)

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.RemovedPositionDeleteFiles)
	require.Zero(t, liveDeleteFileCount(t, result.Table, iceberg.EntryContentPosDeletes))
	require.Equal(t, 1, liveDVCount(t, result.Table))
	require.Equal(t, []int64{1, 3, 5}, idsInTable(t, result.Table),
		"the old position delete and the new equality delete must both remain applied")
}

func TestConvertEqualityDeletesMergesExistingDeletionVector(t *testing.T) {
	ctx := t.Context()
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`)
	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":4}]`)

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.ReplacedDeletionVectors)
	require.Equal(t, 1, liveDVCount(t, result.Table))
	require.Equal(t, []int64{1, 3, 5}, idsInTable(t, result.Table),
		"merging a new equality delete must not resurrect rows from the old vector")
}

func TestConvertEqualityDeletesWritesOneVectorPerPartitionFile(t *testing.T) {
	ctx := t.Context()
	tbl := newPartitionedMergeOnReadTestTable(t)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"category":"x"},{"id":2,"category":"x"},{"id":1,"category":"y"},{"id":3,"category":"y"}]`,
	})
	require.NoError(t, err)
	defer data.Release()
	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	tbl = appendEqualityDelete(t, tbl, []int{1, 2},
		`[{"id":1,"category":"x"},{"id":1,"category":"y"}]`)

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, result.ConvertedEqualityDeleteFiles)
	require.Equal(t, 2, result.AddedDeletionVectors)
	require.Equal(t, 2, liveDVCount(t, result.Table))
	require.Equal(t, []int64{2, 3}, idsInTable(t, result.Table))
}

func TestConvertEqualityDeletesAcrossDataFileSplits(t *testing.T) {
	ctx := t.Context()
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},{"id":9,"data":"i"},{"id":10,"data":"j"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.ReadSplitTargetSizeKey: "1"}))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2},{"id":7}]`)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "the data file has two row groups and each must be planned as a split")
	require.Equal(t, tasks[0].File.FilePath(), tasks[1].File.FilePath())

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.AddedDeletionVectors, "all split positions must form one vector for the data file")
	require.Equal(t, 1, liveDVCount(t, result.Table))
	require.Equal(t, []int64{1, 3, 4, 5, 6, 8, 9, 10}, idsInTable(t, result.Table))
}

func TestConvertEqualityDeletesRemovesDeletesWithNoMatches(t *testing.T) {
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":99}]`)

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, result.ConvertedEqualityDeleteFiles)
	require.Zero(t, result.AddedDeletionVectors)
	require.Zero(t, liveDeleteFileCount(t, result.Table, iceberg.EntryContentEqDeletes))
	require.Empty(t, liveDVCount(t, result.Table))
	require.Equal(t, []int64{1, 2}, idsInTable(t, result.Table))
}

func TestConvertEqualityDeletesPreservesSameSnapshotSequenceSemantics(t *testing.T) {
	ctx := t.Context()
	tbl := newMergeOnReadTestTableVersion(t, "3")
	dataPath := filepath.ToSlash(tbl.Location()) + "/data/same-snapshot-data.parquet"
	equalityPath := filepath.ToSlash(tbl.Location()) + "/data/same-snapshot-equality.parquet"
	dataSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, dataPath, dataSchema, `[{"id":7,"data":"same snapshot"}]`)
	equalitySchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, equalityPath, equalitySchema, `[{"id":7}]`)
	info, err := os.Stat(equalityPath)
	require.NoError(t, err)
	equalityBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		equalityPath, iceberg.ParquetFile, nil, nil, nil, 1, info.Size())
	require.NoError(t, err)
	equalityBuilder.EqualityFieldIDs([]int{1})

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).
		AddRows(changelogDataFile(t, dataPath, 1)).
		AddDeletes(equalityBuilder.Build()).
		Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{7}, idsInTable(t, tbl), "same-snapshot equality deletes do not apply to same-snapshot data")

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.ConvertedEqualityDeleteFiles)
	require.Zero(t, result.AddedDeletionVectors)
	require.Equal(t, []int64{7}, idsInTable(t, result.Table))
}

func TestConvertEqualityDeletesRequiresV3(t *testing.T) {
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "2"),
		`[{"id":1,"data":"a"}]`)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":1}]`)

	_, err := tbl.ConvertEqualityDeletesToDeletionVectors(t.Context())
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

func appendEqualityConversionRows(t *testing.T, tbl *table.Table, rowsJSON string) *table.Table {
	t.Helper()

	schema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, schema, []string{rowsJSON})
	require.NoError(t, err)
	defer data.Release()

	reader := array.NewTableReader(data, -1)
	defer reader.Release()

	out, err := tbl.Append(t.Context(), reader, nil)
	require.NoError(t, err)

	return out
}

func liveDeleteFileCount(t *testing.T, tbl *table.Table, content iceberg.ManifestEntryContent) int {
	t.Helper()

	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)
	snapshot := tbl.CurrentSnapshot()
	manifests, err := snapshot.Manifests(fs)
	require.NoError(t, err)

	count := 0
	for _, manifest := range manifests {
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(fs, false) {
			require.NoError(t, err)
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			file := entry.DataFile()
			if file.ContentType() == content && !(content == iceberg.EntryContentPosDeletes && table.IsDeletionVector(file)) {
				count++
			}
		}
	}

	return count
}

func TestConvertEqualityDeletesTransactionStagesOneReplace(t *testing.T) {
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2}]`)

	tx := tbl.NewTransaction()
	result, err := tx.ConvertEqualityDeletesToDeletionVectors(t.Context())
	require.NoError(t, err)
	require.Nil(t, result.Table)
	require.Equal(t, 1, result.AddedDeletionVectors)

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, table.OpReplace, committed.CurrentSnapshot().Summary.Operation)
	require.Equal(t, []int64{1}, idsInTable(t, committed))
}

func TestConvertEqualityDeletesRejectsStaleTransactionAtCommit(t *testing.T) {
	ctx := t.Context()
	tbl := appendEqualityConversionRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2}]`)

	tx := tbl.NewTransaction()
	_, err := tx.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)

	concurrent := appendEqualityConversionRows(t, tbl, `[{"id":3,"data":"concurrent"}]`)
	_, err = tx.Commit(ctx)
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.Equal(t, []int64{1, 3}, idsInTable(t, concurrent),
		"the stale conversion must not commit over a newer table snapshot")
}
