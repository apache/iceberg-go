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

package maintenance

import (
	"context"
	stdfs "io/fs"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func orphanCleanupRefsTestTable(t *testing.T, fs *iceio.MemFS, refsOnly bool) (*table.Table, *orphanCleanupTestCatalog, string) {
	t.Helper()
	const location = "mem://refs/table"
	metadataPath := location + "/metadata/v1.metadata.json"
	orphan := location + "/data/orphan.parquet"
	meta, err := table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, location, nil)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(t.Context(), metadataPath, nil))
	require.NoError(t, fs.WriteFile(t.Context(), orphan, []byte("orphan")))

	fsF := testFSF(fs)
	catalog := &orphanCleanupTestCatalog{metadata: meta, location: metadataPath, fsF: fsF, refsOnly: refsOnly}

	return table.New(table.Identifier{"db", "refs"}, meta, metadataPath, fsF, catalog,
		table.WithSnapshotHistoryLimitedToRefs(refsOnly)), catalog, orphan
}

func TestOrphanCleanupRejectsRefsOnlySnapshotHistory(t *testing.T) {
	fs := iceio.NewMemFS()
	tbl, _, orphan := orphanCleanupRefsTestTable(t, fs, true)

	_, err := PlanOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour))
	require.ErrorContains(t, err, "snapshot-loading-mode=all")
	_, err = DeleteOrphanFiles(t.Context(), tbl, WithCleanupDryRun(true), WithCleanupFilesOlderThan(time.Hour))
	require.ErrorContains(t, err, "snapshot-loading-mode=all")

	var calls atomic.Int32
	_, err = DeleteOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour),
		WithCleanupDeleteFunc(func(ctx context.Context, path string) error {
			calls.Add(1)

			return fs.Remove(ctx, path)
		}))
	require.ErrorContains(t, err, "snapshot-loading-mode=all")
	require.Zero(t, calls.Load())

	file, err := fs.Open(t.Context(), orphan)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestOrphanCleanupAllowsFullSnapshotHistory(t *testing.T) {
	fs := iceio.NewMemFS()
	tbl, _, orphan := orphanCleanupRefsTestTable(t, fs, false)

	plan, err := PlanOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour))
	require.NoError(t, err)
	require.Equal(t, []string{orphan}, plan.Files())
	result, err := DeleteOrphanFiles(t.Context(), tbl, WithCleanupDryRun(true), WithCleanupFilesOlderThan(time.Hour))
	require.NoError(t, err)
	require.Len(t, result.OrphanFiles, 1)
	require.Empty(t, result.DeletedFiles)
	result, err = DeleteOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour))
	require.NoError(t, err)
	require.Equal(t, []string{orphan}, result.DeletedFiles)
	_, err = fs.Open(t.Context(), orphan)
	require.ErrorIs(t, err, stdfs.ErrNotExist)
}

func TestDeleteOrphanFilesRejectsRefsOnlyRefresh(t *testing.T) {
	fs := iceio.NewMemFS()
	tbl, catalog, orphan := orphanCleanupRefsTestTable(t, fs, false)
	catalog.refsOnly = true

	_, err := DeleteOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour))
	require.ErrorContains(t, err, "snapshot-loading-mode=all")

	file, err := fs.Open(t.Context(), orphan)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}
