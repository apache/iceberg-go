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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func newMultiTargetConflictFixture(t *testing.T, props iceberg.Properties) (*table.Table, iceberg.DataFile, string, string) {
	t.Helper()
	tbl, _ := newConcurrentRewriteTestTableVersion(t, 2)
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	pathA := tbl.Location() + "/data/a.parquet"
	pathB := tbl.Location() + "/data/b.parquet"
	writeParquetFile(t, pathA, schema, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	writeParquetFile(t, pathB, schema, `[{"id":3,"data":"c"},{"id":4,"data":"d"}]`)

	seed := tbl.NewTransaction()
	if len(props) > 0 {
		require.NoError(t, seed.SetProperties(props))
	}
	require.NoError(t, seed.AddDataFilesByPath(t.Context(), []string{pathA, pathB}, nil, false))
	tbl, err = seed.Commit(t.Context())
	require.NoError(t, err)
	files, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, files, 2)
	for _, task := range files {
		if task.File.FilePath() == pathA {
			return tbl, task.File, pathA, pathB
		}
	}
	t.Fatal("missing data file A")

	return nil, nil, "", ""
}

func rewriteMultiTargetConflictFile(t *testing.T, tbl *table.Table, oldFile iceberg.DataFile) {
	t.Helper()
	committed := rewriteConflictFile(t, tbl, oldFile, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	require.ElementsMatch(t, []int64{1, 2, 3, 4}, scanIDs(t, committed))
}

func rewriteConflictFile(t *testing.T, tbl *table.Table, oldFile iceberg.DataFile, rows string) *table.Table {
	t.Helper()
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	name := strings.TrimSuffix(strings.TrimPrefix(oldFile.FilePath(), tbl.Location()+"/data/"), ".parquet")
	path := tbl.Location() + "/data/replacement-" + name + ".parquet"
	writeParquetFile(t, path, schema, rows)
	info, err := os.Stat(path)
	require.NoError(t, err)
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, 2, info.Size())
	require.NoError(t, err)
	peer := tbl.NewTransaction()
	require.NoError(t, peer.NewRewrite(nil).DeleteFile(oldFile).AddDataFile(builder.Build()).Commit(t.Context()))
	committed, err := peer.Commit(t.Context())
	require.NoError(t, err)

	return committed
}

func TestMergeOnReadDeleteRejectsRewriteOfMultiTargetPositionDelete(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)
	tbl, oldFile, _, _ := newMultiTargetConflictFixture(t, iceberg.Properties{
		table.WriteDeleteModeKey:           table.WriteModeMergeOnRead,
		table.WriteDeleteIsolationLevelKey: string(table.IsolationSnapshot),
	})
	leader := tbl.NewTransaction()
	filter := iceberg.IsIn(iceberg.Reference("id"), int64(1), int64(3))
	require.NoError(t, leader.Delete(ctx, filter, nil, table.WithDeleteConcurrency(1)))

	staged, err := leader.StagedTable()
	require.NoError(t, err)
	manifests, err := staged.CurrentSnapshot().Manifests(t.Context(), iceio.LocalFS{})
	require.NoError(t, err)
	var deletes []iceberg.DataFile
	for _, manifest := range manifests {
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(t.Context(), iceio.LocalFS{}, true) {
			require.NoError(t, err)
			deletes = append(deletes, entry.DataFile())
		}
	}
	require.Len(t, deletes, 1)
	require.Nil(t, deletes[0].ReferencedDataFile())
	require.Equal(t, int64(2), deletes[0].Count())
	rewriteMultiTargetConflictFile(t, tbl, oldFile)

	_, err = leader.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrDataFilesMissing)
}
