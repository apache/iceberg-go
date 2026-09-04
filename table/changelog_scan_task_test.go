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

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func changelogTestDataFile(t *testing.T, path string, content iceberg.ManifestEntryContent, format iceberg.FileFormat) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		content, path, format, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return b.Build()
}

func TestAddedRowsScanTaskAppliesSameSnapshotDeletes(t *testing.T) {
	data := changelogTestDataFile(t, "data/f1.parquet", iceberg.EntryContentData, iceberg.ParquetFile)
	posDel := changelogTestDataFile(t, "deletes/d1.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)
	eqDel := changelogTestDataFile(t, "deletes/d2.parquet", iceberg.EntryContentEqDeletes, iceberg.ParquetFile)
	dv := changelogTestDataFile(t, "deletes/d3.puffin", iceberg.EntryContentPosDeletes, iceberg.PuffinFile)

	task, err := NewAddedRowsScanTask(data, []iceberg.DataFile{eqDel, dv, posDel}, 0, 42)
	require.NoError(t, err)

	require.Equal(t, ChangelogOpInsert, task.Operation())
	require.Equal(t, 0, task.ChangeOrdinal())
	require.Equal(t, int64(42), task.CommitSnapshotID())
	require.Equal(t, data.FilePath(), task.File.FilePath())
	require.Equal(t, []iceberg.DataFile{posDel}, task.DeleteFiles)
	require.Equal(t, []iceberg.DataFile{eqDel}, task.EqualityDeleteFiles)
	require.Equal(t, []iceberg.DataFile{dv}, task.DeletionVectorFiles)
	require.Equal(t, []iceberg.DataFile{posDel, eqDel, dv}, task.Deletes())
}

func TestDeletedDataFileScanTaskKeepsExistingDeletes(t *testing.T) {
	data := changelogTestDataFile(t, "data/f2.parquet", iceberg.EntryContentData, iceberg.ParquetFile)
	existing := changelogTestDataFile(t, "deletes/d1.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)

	task, err := NewDeletedDataFileScanTask(data, []iceberg.DataFile{existing}, 1, 43)
	require.NoError(t, err)

	require.Equal(t, ChangelogOpDelete, task.Operation())
	require.Equal(t, 1, task.ChangeOrdinal())
	require.Equal(t, int64(43), task.CommitSnapshotID())
	require.Equal(t, []iceberg.DataFile{existing}, task.ExistingDeletes())
	require.Equal(t, []iceberg.DataFile{existing}, task.DeleteFiles)
}

func TestDeletedRowsScanTaskSeparatesAddedAndExistingDeletes(t *testing.T) {
	data := changelogTestDataFile(t, "data/f2.parquet", iceberg.EntryContentData, iceberg.ParquetFile)
	added := changelogTestDataFile(t, "deletes/d2.parquet", iceberg.EntryContentEqDeletes, iceberg.ParquetFile)
	existing := changelogTestDataFile(t, "deletes/d1.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)

	task, err := NewDeletedRowsScanTask(data, []iceberg.DataFile{added}, []iceberg.DataFile{existing}, 2, 44)
	require.NoError(t, err)

	require.Equal(t, ChangelogOpDelete, task.Operation())
	require.Equal(t, 2, task.ChangeOrdinal())
	require.Equal(t, int64(44), task.CommitSnapshotID())
	require.Equal(t, []iceberg.DataFile{added}, task.AddedDeletes())
	require.Equal(t, []iceberg.DataFile{existing}, task.ExistingDeletes())
	require.Equal(t, existing.FilePath(), task.DeleteFiles[0].FilePath())
	require.Empty(t, task.EqualityDeleteFiles)
}

func TestChangelogScanTaskInterface(t *testing.T) {
	data := changelogTestDataFile(t, "data/f1.parquet", iceberg.EntryContentData, iceberg.ParquetFile)

	added, err := NewAddedRowsScanTask(data, nil, 0, 1)
	require.NoError(t, err)
	deletedFile, err := NewDeletedDataFileScanTask(data, nil, 1, 2)
	require.NoError(t, err)
	deletedRows, err := NewDeletedRowsScanTask(data, nil, nil, 2, 3)
	require.NoError(t, err)

	tasks := []ChangelogScanTask{added, deletedFile, deletedRows}
	require.Equal(t, ChangelogOpInsert, tasks[0].Operation())
	require.Equal(t, ChangelogOpDelete, tasks[1].Operation())
	require.Equal(t, ChangelogOpDelete, tasks[2].Operation())
}

func TestClassifyDeleteFiles(t *testing.T) {
	posDel := changelogTestDataFile(t, "deletes/d1.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)
	eqDel := changelogTestDataFile(t, "deletes/d2.parquet", iceberg.EntryContentEqDeletes, iceberg.ParquetFile)
	dv := changelogTestDataFile(t, "deletes/d3.puffin", iceberg.EntryContentPosDeletes, iceberg.PuffinFile)
	data := changelogTestDataFile(t, "data/f1.parquet", iceberg.EntryContentData, iceberg.ParquetFile)

	got, err := classifyDeleteFiles([]iceberg.DataFile{eqDel, dv, posDel})
	require.NoError(t, err)
	require.Equal(t, []iceberg.DataFile{posDel}, got.pos)
	require.Equal(t, []iceberg.DataFile{eqDel}, got.eq)
	require.Equal(t, []iceberg.DataFile{dv}, got.dv)

	_, err = classifyDeleteFiles([]iceberg.DataFile{data})
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "expected delete file")
}
