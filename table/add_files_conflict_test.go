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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// newAddFilesConflictTable builds a table, seeded with one data file holding
// id 0, whose catalog checks commit requirements, so a writer that loses the
// race refreshes and replays. It returns the table and the path of a parquet
// file holding ids 1 and 2 that is not yet in the table.
func newAddFilesConflictTable(t *testing.T, version int, numRetries string) (*table.Table, *concurrentTestCatalog, string) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:        strconv.Itoa(version),
			table.CommitNumRetriesKey:          numRetries,
			table.CommitMinRetryWaitMsKey:      "1",
			table.CommitMaxRetryWaitMsKey:      "2",
			table.CommitTotalRetryTimeoutMsKey: "1000",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "add_files_conflict"}, meta, metaLoc, fsF, cat)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	seed := location + "/data/a.parquet"
	writeParquetFile(t, seed, arrowSc, `[{"id": 0, "data": "a"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{seed}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	imported := location + "/data/imported.parquet"
	writeParquetFile(t, imported, arrowSc, `[{"id": 1, "data": "b"}, {"id": 2, "data": "c"}]`)

	return tbl, cat, imported
}

// loadCurrent returns the table as the catalog currently has it.
func loadCurrent(t *testing.T, cat *concurrentTestCatalog) *table.Table {
	t.Helper()

	tbl, err := cat.LoadTable(t.Context(), table.Identifier{"db", "add_files_conflict"})
	require.NoError(t, err)

	return tbl
}

// newImportedDataFile describes the parquet file at path as an
// iceberg.DataFile, the input that AddDataFiles takes.
func newImportedDataFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, 2, mustFileSize(t, path))
	require.NoError(t, err)

	return b.Build()
}

// TestAddDataFilesByPath_ConcurrentDuplicateRejected proves the duplicate
// check covers files a concurrent commit added. Both writers plan against
// the same snapshot, so both pass the plan-time check; without the
// commit-time check, the loser's retry would add the file a second time
// and its rows would be read twice.
func TestAddDataFilesByPath_ConcurrentDuplicateRejected(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run("v"+strconv.Itoa(version), func(t *testing.T) {
			ctx := t.Context()
			tbl, cat, imported := newAddFilesConflictTable(t, version, "4")

			txB := tbl.NewTransaction()
			require.NoError(t, txB.AddDataFilesByPath(ctx, []string{imported}, nil, false))

			txA := tbl.NewTransaction()
			require.NoError(t, txA.AddDataFilesByPath(ctx, []string{imported}, nil, false))
			_, err := txA.Commit(ctx)
			require.NoError(t, err)

			_, err = txB.Commit(ctx)
			require.ErrorIs(t, err, table.ErrDuplicateDataFiles)
			require.ErrorIs(t, err, table.ErrTransactionUnusable)
			require.Equal(t, []int64{0, 1, 2}, idsInTable(t, loadCurrent(t, cat)))
		})
	}
}

// TestAddDataFiles_ConcurrentDuplicateRejected is the AddDataFiles
// counterpart of TestAddDataFilesByPath_ConcurrentDuplicateRejected.
func TestAddDataFiles_ConcurrentDuplicateRejected(t *testing.T) {
	ctx := t.Context()
	tbl, cat, imported := newAddFilesConflictTable(t, 2, "4")
	df := newImportedDataFile(t, imported)

	txB := tbl.NewTransaction()
	require.NoError(t, txB.AddDataFiles(ctx, []iceberg.DataFile{df}, nil))

	txA := tbl.NewTransaction()
	require.NoError(t, txA.AddDataFiles(ctx, []iceberg.DataFile{df}, nil))
	_, err := txA.Commit(ctx)
	require.NoError(t, err)

	_, err = txB.Commit(ctx)
	require.ErrorIs(t, err, table.ErrTransactionUnusable)
	require.ErrorIs(t, err, table.ErrDuplicateDataFiles)
	require.Equal(t, []int64{0, 1, 2}, idsInTable(t, loadCurrent(t, cat)))
}

// TestAddDataFilesByPath_ConcurrentDistinctFilesCommit is the control: a
// concurrent commit that added a different file must not trip the duplicate
// check, so the replayed commit lands with both files.
func TestAddDataFilesByPath_ConcurrentDistinctFilesCommit(t *testing.T) {
	ctx := t.Context()
	tbl, _, imported := newAddFilesConflictTable(t, 2, "4")

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	other := tbl.Location() + "/data/other.parquet"
	writeParquetFile(t, other, arrowSc, `[{"id": 3, "data": "d"}]`)

	txB := tbl.NewTransaction()
	require.NoError(t, txB.AddDataFilesByPath(ctx, []string{other}, nil, false))

	txA := tbl.NewTransaction()
	require.NoError(t, txA.AddDataFilesByPath(ctx, []string{imported}, nil, false))
	_, err = txA.Commit(ctx)
	require.NoError(t, err)

	committed, err := txB.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3}, idsInTable(t, committed))
}

// TestAddDataFilesSkippedDuplicateCheckAllowsConcurrentDuplicate pins the
// opt-outs: ignoreDuplicates and WithoutDuplicateCheck disable the
// commit-time check as well as the plan-time one.
func TestAddDataFilesSkippedDuplicateCheckAllowsConcurrentDuplicate(t *testing.T) {
	t.Run("AddDataFilesByPath", func(t *testing.T) {
		ctx := t.Context()
		tbl, _, imported := newAddFilesConflictTable(t, 2, "4")

		txB := tbl.NewTransaction()
		require.NoError(t, txB.AddDataFilesByPath(ctx, []string{imported}, nil, true))

		txA := tbl.NewTransaction()
		require.NoError(t, txA.AddDataFilesByPath(ctx, []string{imported}, nil, false))
		_, err := txA.Commit(ctx)
		require.NoError(t, err)

		_, err = txB.Commit(ctx)
		require.NoError(t, err)
	})

	t.Run("AddDataFiles", func(t *testing.T) {
		ctx := t.Context()
		tbl, _, imported := newAddFilesConflictTable(t, 2, "4")
		df := newImportedDataFile(t, imported)

		txB := tbl.NewTransaction()
		require.NoError(t, txB.AddDataFiles(ctx, []iceberg.DataFile{df}, nil, table.WithoutDuplicateCheck()))

		txA := tbl.NewTransaction()
		require.NoError(t, txA.AddDataFiles(ctx, []iceberg.DataFile{df}, nil))
		_, err := txA.Commit(ctx)
		require.NoError(t, err)

		_, err = txB.Commit(ctx)
		require.NoError(t, err)
	})
}

// TestAddDataFilesByPath_RecommitAfterFailedCommitRejected covers a caller
// that retries by calling Commit again. With no retries configured, the first
// Commit fails the stale branch assertion; the second resumes from a refresh
// and must still reject the duplicate.
func TestAddDataFilesByPath_RecommitAfterFailedCommitRejected(t *testing.T) {
	ctx := t.Context()
	tbl, cat, imported := newAddFilesConflictTable(t, 2, "0")

	txB := tbl.NewTransaction()
	require.NoError(t, txB.AddDataFilesByPath(ctx, []string{imported}, nil, false))

	txA := tbl.NewTransaction()
	require.NoError(t, txA.AddDataFilesByPath(ctx, []string{imported}, nil, false))
	_, err := txA.Commit(ctx)
	require.NoError(t, err)

	_, err = txB.Commit(ctx)
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.NotErrorIs(t, err, table.ErrDuplicateDataFiles)

	_, err = txB.Commit(ctx)
	require.ErrorIs(t, err, table.ErrDuplicateDataFiles)
	require.Equal(t, []int64{0, 1, 2}, idsInTable(t, loadCurrent(t, cat)))
}

// TestAddDataFilesByPath_ConcurrentRollbackCommits checks that a concurrent
// rollback past the writer's base does not fail the commit: the retry
// rebuilds on the rolled-back head, where the path is not present.
func TestAddDataFilesByPath_ConcurrentRollbackCommits(t *testing.T) {
	ctx := t.Context()
	tbl, cat, imported := newAddFilesConflictTable(t, 2, "4")
	seedID := tbl.CurrentSnapshot().SnapshotID

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	extra := tbl.Location() + "/data/extra.parquet"
	writeParquetFile(t, extra, arrowSc, `[{"id": 9, "data": "z"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(ctx, []string{extra}, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	txB := tbl.NewTransaction()
	require.NoError(t, txB.AddDataFilesByPath(ctx, []string{imported}, nil, false))

	rollback := loadCurrent(t, cat).NewTransaction()
	require.NoError(t, rollback.RollbackToSnapshot(seedID))
	_, err = rollback.Commit(ctx)
	require.NoError(t, err)

	_, err = txB.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, idsInTable(t, loadCurrent(t, cat)))
}

// TestAddDataFilesByPath_ConcurrentExpiryCommits checks that expiring the
// snapshots committed after the writer's base does not fail the commit.
func TestAddDataFilesByPath_ConcurrentExpiryCommits(t *testing.T) {
	ctx := t.Context()
	tbl, cat, imported := newAddFilesConflictTable(t, 2, "4")

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	txB := tbl.NewTransaction()
	require.NoError(t, txB.AddDataFilesByPath(ctx, []string{imported}, nil, false))

	for i, name := range []string{"x", "y"} {
		path := tbl.Location() + "/data/" + name + ".parquet"
		writeParquetFile(t, path, arrowSc, `[{"id": `+strconv.Itoa(3+i)+`, "data": "q"}]`)
		tx := loadCurrent(t, cat).NewTransaction()
		require.NoError(t, tx.AddDataFilesByPath(ctx, []string{path}, nil, false))
		_, err := tx.Commit(ctx)
		require.NoError(t, err)
	}

	expire := loadCurrent(t, cat).NewTransaction()
	require.NoError(t, expire.ExpireSnapshots(table.WithOlderThan(0), table.WithRetainLast(1)))
	_, err = expire.Commit(ctx)
	require.NoError(t, err)
	require.Len(t, loadCurrent(t, cat).Metadata().Snapshots(), 1)

	_, err = txB.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, idsInTable(t, loadCurrent(t, cat)))
}

// TestAddDataFilesByPath_ConcurrentAddThenDeleteCommits checks that a path a
// concurrent commit added and a later commit deleted is not a duplicate: it is
// no longer live, so the retry adds it.
func TestAddDataFilesByPath_ConcurrentAddThenDeleteCommits(t *testing.T) {
	ctx := t.Context()
	tbl, cat, imported := newAddFilesConflictTable(t, 2, "4")

	txB := tbl.NewTransaction()
	require.NoError(t, txB.AddDataFilesByPath(ctx, []string{imported}, nil, false))

	txA := tbl.NewTransaction()
	require.NoError(t, txA.AddDataFilesByPath(ctx, []string{imported}, nil, false))
	_, err := txA.Commit(ctx)
	require.NoError(t, err)

	del := loadCurrent(t, cat).NewTransaction()
	require.NoError(t, del.Delete(ctx, iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(1)), nil))
	_, err = del.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, idsInTable(t, loadCurrent(t, cat)))

	_, err = txB.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, idsInTable(t, loadCurrent(t, cat)))
}
