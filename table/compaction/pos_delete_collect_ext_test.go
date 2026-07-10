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

package compaction_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// TestCollectDeadPositionDeletes drives the full manifest walk: a
// partition-scoped position delete (no referenced_data_file, no file_path
// bounds — Spark's default granularity) covering three data files is dead only
// when every file it covers is rewritten, and retained the moment one is not.
func TestCollectDeadPositionDeletes(t *testing.T) {
	ctx := t.Context()
	fs := iceio.LocalFS{}
	tbl := newCDCStressTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 3 {
		p := tbl.Location() + fmt.Sprintf("/data/d-%d.parquet", i)
		writeParquet(t, p, arrowSc, fmt.Sprintf(`[{"id": %d, "data": "r%d"}]`, i+1, i+1))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(ctx, []string{p}, nil, false))
		tbl, err = tx.Commit(ctx)
		require.NoError(t, err)
	}

	dataPaths := dataFilePaths(t, tbl)
	require.Len(t, dataPaths, 3)

	// A partition-scoped position delete: unpartitioned spec, no ref, no
	// bounds. Committed after the data files, so its sequence number exceeds
	// theirs and it applies to all three.
	delDF, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/pos-del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(delDF.Build())
	require.NoError(t, rd.Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	t.Run("all covered files rewritten is dead", func(t *testing.T) {
		rewritten := pathSet(dataPaths...)
		dead, err := compaction.CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), rewritten)
		require.NoError(t, err)
		require.Len(t, dead, 1, "a partition-scoped delete whose every covered file is rewritten must be expunged")
	})

	t.Run("one covered file surviving is retained", func(t *testing.T) {
		rewritten := pathSet(dataPaths[0], dataPaths[1])
		dead, err := compaction.CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), rewritten)
		require.NoError(t, err)
		require.Empty(t, dead, "a surviving covered data file must keep the partition-scoped delete alive")
	})
}

func dataFilePaths(t *testing.T, tbl *table.Table) []string {
	t.Helper()

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	seen := make(map[string]struct{})
	var paths []string
	for _, task := range tasks {
		p := task.File.FilePath()
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		paths = append(paths, p)
	}

	return paths
}

func pathSet(paths ...string) map[string]struct{} {
	s := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		s[p] = struct{}{}
	}

	return s
}
