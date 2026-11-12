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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func replacementConflictFixture(t *testing.T, version int) (*table.Table, iceberg.DataFile) {
	t.Helper()

	tbl, _ := newConcurrentRewriteTestTableVersion(t, version)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	path := tbl.Location() + "/data/original.parquet"
	writeParquetFile(t, path, arrowSchema,
		`[{"id":5,"data":"a"},{"id":6,"data":"b"},{"id":7,"data":"c"},{"id":8,"data":"d"},{"id":9,"data":"e"}]`)
	seed := tbl.NewTransaction()
	require.NoError(t, seed.AddDataFilesByPath(t.Context(), []string{path}, nil, false))
	tbl, err = seed.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return tbl, tasks[0].File
}

func replacementDataFile(t *testing.T, tbl *table.Table, oldFile iceberg.DataFile, path, rows string, recordCount int64) iceberg.DataFile {
	t.Helper()

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	writeParquetFile(t, path, arrowSchema, rows)
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, recordCount, 512)
	require.NoError(t, err)
	if firstRowID := oldFile.FirstRowID(); firstRowID != nil {
		builder.FirstRowID(*firstRowID)
	}

	return builder.Build()
}

func TestReplaceDataFiles_RejectsConcurrentEqualityDelete(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			tbl, oldFile := replacementConflictFixture(t, version)
			replacement := replacementDataFile(t, tbl, oldFile, tbl.Location()+"/data/replacement.parquet",
				`[{"id":5,"data":"a"},{"id":6,"data":"b"},{"id":7,"data":"c"},{"id":8,"data":"d"},{"id":9,"data":"e"}]`, 5)

			leader := tbl.NewTransaction()
			require.NoError(t, leader.ReplaceDataFiles(t.Context(),
				[]iceberg.DataFile{oldFile}, []iceberg.DataFile{replacement}, nil))

			peer := appendEqualityDelete(t, tbl, []int{1}, `[{"id":7}]`)
			assert.Equal(t, []int64{5, 6, 8, 9}, scanIDs(t, peer))

			_, err := leader.Commit(t.Context())
			require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
		})
	}
}

func TestReplaceFiles_RejectsConcurrentEqualityDelete(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			tbl, oldFile := replacementConflictFixture(t, version)
			replacement := replacementDataFile(t, tbl, oldFile, tbl.Location()+"/data/replacement.parquet",
				`[{"id":5,"data":"a"},{"id":6,"data":"b"},{"id":7,"data":"c"},{"id":8,"data":"d"},{"id":9,"data":"e"}]`, 5)
			leader := tbl.NewTransaction()
			require.NoError(t, leader.ReplaceFiles(t.Context(),
				[]iceberg.DataFile{oldFile}, []iceberg.DataFile{replacement}, nil, nil))

			peer := appendEqualityDelete(t, tbl, []int{1}, `[{"id":7}]`)
			assert.Equal(t, []int64{5, 6, 8, 9}, scanIDs(t, peer))

			_, err := leader.Commit(t.Context())
			require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
		})
	}
}
