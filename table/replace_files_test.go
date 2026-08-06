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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReplaceFilesTestTable(t *testing.T) *table.Table {
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
		table.Identifier{"db", "replace_files_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func TestReplaceFiles_DataAndDeleteFiles(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Step 1: Write and commit a data file with 3 rows
	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 2, "data": "beta"},
		{"id": 3, "data": "gamma"}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 3)

	// Step 2: Add a position delete file via RowDelta
	posDelPath := tbl.Location() + "/data/pos-del-001.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 1}]`, dataPath))

	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	posDelFile := posDelBuilder.Build()

	tx2 := tbl.NewTransaction()
	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(posDelFile)
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 2) // beta deleted

	// Step 3: Get existing data + delete files from scan tasks
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	oldDataFile := tasks[0].File
	var deleteFilesToRemove []iceberg.DataFile
	deleteFilesToRemove = append(deleteFilesToRemove, tasks[0].DeleteFiles...)
	require.Len(t, deleteFilesToRemove, 1)

	// Step 4: Write a compacted data file (without deleted row)
	newDataPath := tbl.Location() + "/data/data-compacted.parquet"
	writeParquetFile(t, newDataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 3, "data": "gamma"}
	]`)

	// Build new DataFile directly (not via AddFiles which would commit it)
	newDataFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		newDataPath, iceberg.ParquetFile, nil, nil, nil, 2, 512)
	require.NoError(t, err)
	newDataFile := newDataFileBuilder.Build()

	// Step 5: ReplaceFiles — swap old data + remove delete file
	tx3 := tbl.NewTransaction()
	err = tx3.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{oldDataFile},
		[]iceberg.DataFile{newDataFile},
		deleteFilesToRemove,
		nil,
	)
	require.NoError(t, err)

	tbl, err = tx3.Commit(t.Context())
	require.NoError(t, err)

	// Verify: 2 rows, snapshot committed
	assertRowCount(t, tbl, 2)

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpOverwrite, snap.Summary.Operation)
}

func TestReplaceFilesWithDeleteFilesPreservesDataSequenceNumber(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	oldDeletePath := tbl.Location() + "/data/old-pos-delete.parquet"
	writeParquetFile(t, oldDeletePath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path":%q,"pos":0}]`, dataPath))
	oldDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		oldDeletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(oldDeleteBuilder.Build()).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	oldDelete := oldDeleteBuilder.Build()
	oldSequence := deleteFileSequence(t, tbl, oldDelete.FilePath())
	newDeletePath := tbl.Location() + "/data/new-pos-delete.parquet"
	writeParquetFile(t, newDeletePath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path":%q,"pos":0}]`, dataPath))
	newDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		newDeletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{oldDelete},
		[]table.DeleteFileAddition{{
			File:               newDeleteBuilder.Build(),
			DataSequenceNumber: oldSequence,
		}}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, oldSequence, deleteFileSequence(t, tbl, newDeletePath),
		"rewritten delete files must retain the replaced data sequence number")
	assert.NotEqual(t, tbl.CurrentSnapshot().SequenceNumber, oldSequence,
		"the new snapshot sequence must not replace the delete data sequence")
	assert.Equal(t, []int64{2}, scanIDs(t, tbl),
		"the replacement must preserve delete applicability")
}

func TestReplaceFilesWithDeleteFilesRejectsDVOnV2(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)
	ref := tbl.Location() + "/data/data.parquet"
	dvBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/delete-vector.puffin", iceberg.PuffinFile,
		nil, nil, nil, 1, 1)
	require.NoError(t, err)
	dv := dvBuilder.ReferencedDataFile(ref).Build()
	oldDelete := newPosDeleteFile(t, tbl.Location()+"/data/old-pos-delete.parquet")

	tx := tbl.NewTransaction()
	err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{oldDelete},
		[]table.DeleteFileAddition{{File: dv, DataSequenceNumber: 0}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires table format version >= 3")
}

func deleteFileSequence(t *testing.T, tbl *table.Table, path string) int64 {
	t.Helper()
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	manifests, err := snap.Manifests(iceio.LocalFS{})
	require.NoError(t, err)
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(iceio.LocalFS{}, false) {
			require.NoError(t, err)
			if entry.DataFile().FilePath() == path {
				return entry.SequenceNum()
			}
		}
	}
	t.Fatalf("delete file %q not found in current snapshot", path)
	return -1
}

func scanIDs(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()
	_, records, err := tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)
	var ids []int64
	for record, err := range records {
		require.NoError(t, err)
		values := record.Column(record.Schema().FieldIndices("id")[0]).(*array.Int64)
		for i := 0; i < values.Len(); i++ {
			ids = append(ids, values.Value(i))
		}
		record.Release()
	}

	return ids
}

func TestReplaceFiles_DelegatesToReplaceDataFilesWhenNoDeleteFiles(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	oldDataFile := tasks[0].File

	newDataPath := tbl.Location() + "/data/data-new.parquet"
	writeParquetFile(t, newDataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	newBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		newDataPath, iceberg.ParquetFile, nil, nil, nil, 1, 256)
	require.NoError(t, err)

	tx2 := tbl.NewTransaction()
	err = tx2.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{oldDataFile},
		[]iceberg.DataFile{newBuilder.Build()},
		nil, // no delete files
		nil,
	)
	require.NoError(t, err)

	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 1)
}

func TestReplaceFiles_ValidationErrors(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	t.Run("nil delete file", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{nil},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil delete file")
	})

	t.Run("duplicate delete file paths", func(t *testing.T) {
		posDelBuilder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			"s3://bucket/del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		df := posDelBuilder.Build()

		tx := tbl.NewTransaction()
		err = tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{df, df},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unique")
	})

	t.Run("delete file not in table", func(t *testing.T) {
		posDelBuilder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			"s3://bucket/nonexistent-del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)

		tx := tbl.NewTransaction()
		err = tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{posDelBuilder.Build()},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot remove delete files")
	})

	newDV := func(path, ref string) iceberg.DataFile {
		b, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			path, iceberg.PuffinFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		if ref != "" {
			b.ReferencedDataFile(ref)
		}

		return b.Build()
	}

	t.Run("deletion vector missing referenced_data_file", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{newDV("s3://bucket/dv-0001.puffin", "")},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing referenced_data_file")
	})

	t.Run("deletion vectors referencing the same data file", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{
				newDV("s3://bucket/dv-a.puffin", "s3://bucket/data-001.parquet"),
				newDV("s3://bucket/dv-b.puffin", "s3://bucket/data-001.parquet"),
			},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "distinct data files")
	})

	t.Run("deletion vector not in table", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{newDV("s3://bucket/dv-0001.puffin", "s3://bucket/nonexistent-data.parquet")},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot remove deletion vectors that do not belong to the table")
	})
}
