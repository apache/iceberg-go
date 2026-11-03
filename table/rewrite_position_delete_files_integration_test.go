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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestRewritePositionDeleteFilesCommit(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTable(t)
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	a, b := tbl.Location()+"/data/a.parquet", tbl.Location()+"/data/b.parquet"
	writeParquetFile(t, a, schema, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	writeParquetFile(t, b, schema, `[{"id":3,"data":"c"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{a, b}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	var deletes []iceberg.DataFile
	for i := range 2 {
		path := fmt.Sprintf("%s/data/delete-%d.parquet", tbl.Location(), i)
		// The second row is outside the file's referenced-data-file scope.
		writeParquetFile(t, path, table.PositionalDeleteArrowSchema, fmt.Sprintf(`[{"file_path":%q,"pos":0},{"file_path":%q,"pos":0}]`, a, b))
		builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, path, iceberg.ParquetFile, nil, nil, nil, 2, 128)
		require.NoError(t, err)
		deletes = append(deletes, builder.ReferencedDataFile(a).Build())
	}
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(deletes...).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	sequence := currentManifestEntry(t, tbl, deletes[0].FilePath()).DataSequenceNumber()
	require.ElementsMatch(t, []int64{2, 3}, scanIDs(t, tbl))
	updated, count, err := tbl.RewritePositionDeleteFiles(t.Context(), table.PositionDeleteRewriteOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.ElementsMatch(t, []int64{2, 3}, scanIDs(t, updated))
	tasks, err := updated.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	var replacement iceberg.DataFile
	for _, task := range tasks {
		if task.File.FilePath() == a {
			require.Len(t, task.DeleteFiles, 1)
			replacement = task.DeleteFiles[0]
		}
	}
	require.NotNil(t, replacement)
	require.Equal(t, sequence, currentManifestEntry(t, updated, replacement.FilePath()).DataSequenceNumber())
	require.Equal(t, int64(1), replacement.Count())
	_, _, err = tbl.RewritePositionDeleteFiles(t.Context(), table.PositionDeleteRewriteOptions{})
	require.ErrorIs(t, err, table.ErrCommitFailed)
}
