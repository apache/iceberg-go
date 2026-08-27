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

func changelogTestDataFile(t *testing.T, path string, content iceberg.ManifestEntryContent) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		content, path, iceberg.ParquetFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return b.Build()
}

func TestAddedRowsScanTaskAppliesSameSnapshotDeletes(t *testing.T) {
	data := changelogTestDataFile(t, "data/f1.parquet", iceberg.EntryContentData)
	posDel := changelogTestDataFile(t, "deletes/d1.parquet", iceberg.EntryContentPosDeletes)

	task := NewAddedRowsScanTask(data, []iceberg.DataFile{posDel}, 0, 42)

	require.Equal(t, 0, task.ChangeOrdinal())
	require.Equal(t, int64(42), task.CommitSnapshotID())
	require.Equal(t, data.FilePath(), task.File.FilePath())
	require.Equal(t, []iceberg.DataFile{posDel}, task.Deletes())
}

func TestDeletedDataFileScanTaskKeepsExistingDeletes(t *testing.T) {
	data := changelogTestDataFile(t, "data/f2.parquet", iceberg.EntryContentData)
	existing := changelogTestDataFile(t, "deletes/d1.parquet", iceberg.EntryContentPosDeletes)

	task := NewDeletedDataFileScanTask(data, []iceberg.DataFile{existing}, 1, 43)

	require.Equal(t, 1, task.ChangeOrdinal())
	require.Equal(t, int64(43), task.CommitSnapshotID())
	require.Equal(t, []iceberg.DataFile{existing}, task.ExistingDeletes())
}

func TestDeletedRowsScanTaskSeparatesAddedAndExistingDeletes(t *testing.T) {
	data := changelogTestDataFile(t, "data/f2.parquet", iceberg.EntryContentData)
	added := changelogTestDataFile(t, "deletes/d2.parquet", iceberg.EntryContentEqDeletes)
	existing := changelogTestDataFile(t, "deletes/d1.parquet", iceberg.EntryContentPosDeletes)

	task := NewDeletedRowsScanTask(data, []iceberg.DataFile{added}, []iceberg.DataFile{existing}, 2, 44)

	require.Equal(t, 2, task.ChangeOrdinal())
	require.Equal(t, int64(44), task.CommitSnapshotID())
	require.Equal(t, []iceberg.DataFile{added}, task.AddedDeletes())
	require.Equal(t, []iceberg.DataFile{existing}, task.ExistingDeletes())
	require.Equal(t, existing.FilePath(), task.DeleteFiles[0].FilePath())
}
