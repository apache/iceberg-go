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
	"errors"
	stdfs "io/fs"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func orphanCleanupGCTestTable(t *testing.T, fs *iceio.MemFS, props iceberg.Properties) (*table.Table, string) {
	t.Helper()
	const location = "mem://gc/table"
	metadataPath := location + "/metadata/v1.metadata.json"
	orphan := location + "/data/orphan.parquet"
	meta, err := table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(t.Context(), metadataPath, nil))
	require.NoError(t, fs.WriteFile(t.Context(), orphan, []byte("orphan")))

	return table.New(table.Identifier{"db", "gc"}, meta, metadataPath,
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil), orphan
}

func TestOrphanCleanupRespectsGCDisabled(t *testing.T) {
	for _, value := range []string{"false", "FALSE", "1", "true "} {
		t.Run(value, func(t *testing.T) {
			fs := iceio.NewMemFS()
			tbl, orphan := orphanCleanupGCTestTable(t, fs, iceberg.Properties{table.GCEnabledKey: value})
			var calls atomic.Int32
			_, err := DeleteOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour),
				WithCleanupDeleteFunc(func(ctx context.Context, path string) error {
					calls.Add(1)

					return fs.Remove(ctx, path)
				}))
			require.Zero(t, calls.Load())
			require.ErrorContains(t, err, "GC is disabled")
			_, err = DeleteOrphanFiles(t.Context(), tbl, WithCleanupDryRun(true), WithCleanupFilesOlderThan(0))
			require.ErrorContains(t, err, "GC is disabled")
			_, err = PlanOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(0))
			require.ErrorContains(t, err, "GC is disabled")
			file, err := fs.Open(t.Context(), orphan)
			require.NoError(t, err)
			require.NoError(t, file.Close())
		})
	}
}

func TestOrphanCleanupPlanRejectsExecutionAfterGCIsDisabled(t *testing.T) {
	fs := iceio.NewMemFS()
	enabledTable, orphan := orphanCleanupGCTestTable(t, fs, nil)
	plan, err := PlanOrphanFiles(t.Context(), enabledTable, WithCleanupFilesOlderThan(time.Hour))
	require.NoError(t, err)
	require.Equal(t, []string{orphan}, plan.Files())
	builder, err := table.MetadataBuilderFromBase(enabledTable.Metadata(), enabledTable.MetadataLocation())
	require.NoError(t, err)
	require.NoError(t, builder.SetProperties(iceberg.Properties{table.GCEnabledKey: "false"}))
	meta, err := builder.Build()
	require.NoError(t, err)
	disabledTable := table.New(enabledTable.Identifier(), meta, enabledTable.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

	_, err = ExecuteOrphanCleanup(t.Context(), disabledTable, plan, WithCleanupDryRun(true))
	require.ErrorContains(t, err, "GC is disabled")

	_, err = ExecuteOrphanCleanup(t.Context(), disabledTable, plan)
	require.ErrorContains(t, err, "GC is disabled")
	file, err := fs.Open(t.Context(), orphan)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestOrphanCleanupChecksGCBeforeDeletingOrphans(t *testing.T) {
	meta, err := table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder,
		"mem://gc/table", iceberg.Properties{table.GCEnabledKey: "false"})
	require.NoError(t, err)
	var calls atomic.Int32
	tbl := table.New(table.Identifier{"db", "gc"}, meta, "mem://gc/table/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) {
			calls.Add(1)

			return nil, errors.New("unexpected filesystem access")
		}, nil)
	_, err = PlanOrphanFiles(t.Context(), tbl)
	require.ErrorContains(t, err, "GC is disabled")
	_, err = DeleteOrphanFiles(t.Context(), tbl, WithCleanupDryRun(true))
	require.ErrorContains(t, err, "GC is disabled")
	_, err = DeleteOrphanFiles(t.Context(), tbl)
	require.ErrorContains(t, err, "GC is disabled")
	_, err = ExecuteOrphanCleanup(t.Context(), tbl, OrphanCleanupPlan{}, WithCleanupDryRun(true))
	require.ErrorContains(t, err, "GC is disabled")
	_, err = ExecuteOrphanCleanup(t.Context(), tbl, OrphanCleanupPlan{})
	require.ErrorContains(t, err, "GC is disabled")
	require.Zero(t, calls.Load())
}

func TestOrphanCleanupAllowsGCEnabled(t *testing.T) {
	for _, props := range []iceberg.Properties{nil, {table.GCEnabledKey: "true"}, {table.GCEnabledKey: "TrUe"}} {
		t.Run(props[table.GCEnabledKey], func(t *testing.T) {
			fs := iceio.NewMemFS()
			tbl, orphan := orphanCleanupGCTestTable(t, fs, props)
			plan, err := PlanOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour))
			require.NoError(t, err)
			require.Equal(t, []string{orphan}, plan.Files())
			result, err := DeleteOrphanFiles(t.Context(), tbl, WithCleanupDryRun(true), WithCleanupFilesOlderThan(time.Hour))
			require.NoError(t, err)
			require.Empty(t, result.DeletedFiles)
			result, err = ExecuteOrphanCleanup(t.Context(), tbl, plan, WithCleanupDryRun(true))
			require.NoError(t, err)
			require.Empty(t, result.DeletedFiles)
			result, err = DeleteOrphanFiles(t.Context(), tbl, WithCleanupFilesOlderThan(time.Hour))
			require.NoError(t, err)
			require.Equal(t, []string{orphan}, result.DeletedFiles)
			_, err = fs.Open(t.Context(), orphan)
			require.ErrorIs(t, err, stdfs.ErrNotExist)
		})
	}
}
