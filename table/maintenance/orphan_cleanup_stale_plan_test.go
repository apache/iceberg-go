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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdfs "io/fs"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestExecuteOrphanCleanupRejectsStalePlanWhenFileBecomesLive(t *testing.T) {
	ctx := context.Background()
	fs := iceio.NewMemFS()
	location := "mem://stale-plan/table"
	metadataLocation := location + "/metadata/v1.metadata.json"
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	fsF := testFSF(fs)
	catalog := &orphanCleanupTestCatalog{metadata: meta, location: metadataLocation, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "stale_plan"}, meta, metadataLocation, fsF, catalog)

	dataPath := location + "/data/newly-live.parquet"
	require.NoError(t, fs.WriteFile(ctx, dataPath, []byte("data")))
	dataLocation := location + "/data"
	plan, err := PlanOrphanFiles(ctx, tbl,
		WithCleanupLocation(dataLocation),
		WithCleanupFilesOlderThan(0),
	)
	require.NoError(t, err)
	require.Equal(t, []string{dataPath}, plan.Files())

	loadErr := errors.New("catalog unavailable")
	catalog.beforeLoad = func() error { return loadErr }
	_, err = ExecuteOrphanCleanup(ctx, tbl, plan)
	require.ErrorIs(t, err, loadErr)
	file, err := fs.Open(ctx, dataPath)
	require.NoError(t, err, "failed refresh must leave the plan's files intact")
	require.NoError(t, file.Close())

	fileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		dataPath,
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		int64(len("data")),
	)
	require.NoError(t, err)
	peer := tbl.NewTransaction()
	require.NoError(t, peer.AddDataFiles(ctx, []iceberg.DataFile{fileBuilder.Build()}, nil))
	committed, err := peer.Commit(ctx)
	require.NoError(t, err)
	require.NotEqual(t, plan.metadataLocation, committed.MetadataLocation())

	_, err = ExecuteOrphanCleanup(ctx, tbl, plan)
	require.ErrorIs(t, err, ErrStaleOrphanCleanupPlan)

	file, err = fs.Open(ctx, dataPath)
	require.NoError(t, err, "a file committed after planning must not be deleted")
	require.NoError(t, file.Close())

	// Execution refreshes a copy, so planning again on tbl still sees the old
	// metadata and produces another stale plan.
	staleAgain, err := PlanOrphanFiles(ctx, tbl,
		WithCleanupLocation(dataLocation),
		WithCleanupFilesOlderThan(0),
	)
	require.NoError(t, err)
	require.Equal(t, []string{dataPath}, staleAgain.Files())
	_, err = ExecuteOrphanCleanup(ctx, tbl, staleAgain)
	require.ErrorIs(t, err, ErrStaleOrphanCleanupPlan)

	// Refresh the caller's table before planning again. The committed file is
	// now referenced, so the new plan is empty and execution is safe.
	require.NoError(t, tbl.Refresh(ctx))
	freshPlan, err := PlanOrphanFiles(ctx, tbl,
		WithCleanupLocation(dataLocation),
		WithCleanupFilesOlderThan(0),
	)
	require.NoError(t, err)
	require.Empty(t, freshPlan.Files())
	result, err := ExecuteOrphanCleanup(ctx, tbl, freshPlan)
	require.NoError(t, err)
	require.Empty(t, result.DeletedFiles)
}

func TestOrphanCleanupRejectsRefsOnlyHistoryAfterRefresh(t *testing.T) {
	ctx := t.Context()
	fs := iceio.NewMemFS()
	const location = "mem://stale-plan/refs-only"
	const metadataLocation = location + "/metadata/v1.metadata.json"
	const orphanPath = location + "/data/orphan.parquet"
	meta, err := table.NewMetadata(iceberg.NewSchema(0), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, nil)
	require.NoError(t, err)
	fsF := testFSF(fs)
	catalog := &orphanCleanupTestCatalog{metadata: meta, location: metadataLocation, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "refs_only_refresh"}, meta, metadataLocation, fsF, catalog)
	require.NoError(t, fs.WriteFile(ctx, orphanPath, []byte("orphan")))
	plan, err := PlanOrphanFiles(ctx, tbl, WithCleanupFilesOlderThan(0))
	require.NoError(t, err)
	require.Equal(t, []string{orphanPath}, plan.Files())

	for _, name := range []string{"saved plan", "before planning", "after planning"} {
		t.Run(name, func(t *testing.T) {
			catalog.refsOnly = false
			switchCatalogToRefs := func() error {
				// The metadata location stays the same when only the load mode changes.
				catalog.refsOnly = true

				return nil
			}
			catalog.beforeLoad = switchCatalogToRefs
			if name == "after planning" {
				catalog.beforeLoad = func() error {
					catalog.beforeLoad = switchCatalogToRefs

					return nil
				}
			}

			var cleanupErr error
			if name == "saved plan" {
				_, cleanupErr = ExecuteOrphanCleanup(ctx, tbl, plan)
			} else {
				_, cleanupErr = DeleteOrphanFiles(ctx, tbl, WithCleanupFilesOlderThan(0))
			}
			require.ErrorIs(t, cleanupErr, table.ErrInvalidOperation)
			require.ErrorContains(t, cleanupErr, "snapshot-loading-mode=refs")
			file, err := fs.Open(ctx, orphanPath)
			require.NoError(t, err)
			require.NoError(t, file.Close())
			require.False(t, tbl.SnapshotHistoryLimitedToRefs())
		})
	}
}

func TestDeleteOrphanFilesRefreshesAroundPlanning(t *testing.T) {
	for _, afterPlanning := range []bool{false, true} {
		name := "peer commits before cleanup"
		if afterPlanning {
			name = "peer commits after planning"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			fs := iceio.NewMemFS()
			const location = "mem://stale-plan/one-shot"
			const metadataLocation = location + "/metadata/v1.metadata.json"
			const dataPath = location + "/data/newly-live.parquet"
			const orphanPath = location + "/data/orphan.parquet"
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			)
			meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
				iceberg.Properties{table.PropertyFormatVersion: "2"})
			require.NoError(t, err)
			fsF := testFSF(fs)
			catalog := &orphanCleanupTestCatalog{metadata: meta, location: metadataLocation, fsF: fsF}
			tbl := table.New(table.Identifier{"db", "one_shot_stale_plan"}, meta, metadataLocation, fsF, catalog)

			require.NoError(t, fs.WriteFile(ctx, dataPath, []byte("data")))
			require.NoError(t, fs.WriteFile(ctx, orphanPath, []byte("orphan")))
			fileBuilder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec, iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
				nil, nil, nil, 1, int64(len("data")),
			)
			require.NoError(t, err)

			commitPeer := func() error {
				peer := tbl.NewTransaction()
				if err := peer.AddDataFiles(ctx, []iceberg.DataFile{fileBuilder.Build()}, nil); err != nil {
					return err
				}
				_, err := peer.Commit(ctx)

				return err
			}
			if afterPlanning {
				catalog.beforeLoad = func() error {
					catalog.beforeLoad = commitPeer

					return nil
				}
			} else {
				require.NoError(t, commitPeer())
			}

			result, err := DeleteOrphanFiles(ctx, tbl, WithCleanupLocation(location+"/data"), WithCleanupFilesOlderThan(0))
			if afterPlanning {
				require.ErrorIs(t, err, ErrStaleOrphanCleanupPlan)
				orphan, err := fs.Open(ctx, orphanPath)
				require.NoError(t, err, "a stale plan must leave all of its files intact")
				require.NoError(t, orphan.Close())
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{orphanPath}, result.DeletedFiles)
				_, err := fs.Open(ctx, orphanPath)
				require.ErrorIs(t, err, stdfs.ErrNotExist)
			}

			file, err := fs.Open(ctx, dataPath)
			require.NoError(t, err, "cleanup must preserve the committed file")
			require.NoError(t, file.Close())
			require.Equal(t, metadataLocation, tbl.MetadataLocation(), "cleanup refreshes a copy")
		})
	}
}

func TestExecuteOrphanCleanupRejectsPlanForDifferentTable(t *testing.T) {
	ctx := context.Background()
	fs := iceio.NewMemFS()
	location := "mem://stale-plan/same-location"
	metadataLocation := location + "/metadata/v1.metadata.json"
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	firstMeta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	fsF := testFSF(fs)
	firstCatalog := &orphanCleanupTestCatalog{metadata: firstMeta, location: metadataLocation, fsF: fsF}
	firstTable := table.New(table.Identifier{"db", "first"}, firstMeta, metadataLocation, fsF, firstCatalog)

	dataPath := location + "/data/shared.parquet"
	require.NoError(t, fs.WriteFile(ctx, dataPath, []byte("data")))
	plan, err := PlanOrphanFiles(ctx, firstTable, WithCleanupFilesOlderThan(0))
	require.NoError(t, err)
	require.Equal(t, []string{dataPath}, plan.Files())

	secondMeta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	require.NotEqual(t, firstMeta.TableUUID(), secondMeta.TableUUID())
	secondCatalog := &orphanCleanupTestCatalog{metadata: secondMeta, location: metadataLocation, fsF: fsF}
	secondTable := table.New(table.Identifier{"db", "second"}, secondMeta, metadataLocation, fsF, secondCatalog)

	_, err = ExecuteOrphanCleanup(ctx, secondTable, plan)
	require.ErrorIs(t, err, ErrStaleOrphanCleanupPlan)

	file, err := fs.Open(ctx, dataPath)
	require.NoError(t, err, "a plan from another table must not delete this file")
	require.NoError(t, file.Close())
}

func TestExecuteOrphanCleanupChecksMetadataWhenV1TableHasNoUUID(t *testing.T) {
	ctx := t.Context()
	fs := iceio.NewMemFS()
	const location = "mem://stale-plan/legacy-v1"
	const metadataLocation = location + "/metadata/v1.metadata.json"
	const dataPath = location + "/data/live.parquet"
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "1"})
	require.NoError(t, err)
	withoutUUID := func(meta table.Metadata) table.Metadata {
		t.Helper()
		data, err := json.Marshal(meta)
		require.NoError(t, err)
		var object map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &object))
		delete(object, "table-uuid")
		data, err = json.Marshal(object)
		require.NoError(t, err)
		parsed, err := table.ParseMetadataBytes(data)
		require.NoError(t, err)
		require.Equal(t, uuid.Nil, parsed.TableUUID())

		return parsed
	}

	fsF := testFSF(fs)
	catalog := &orphanCleanupTestCatalog{metadata: withoutUUID(meta), location: metadataLocation, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "legacy"}, catalog.metadata, metadataLocation, fsF, catalog)
	require.NoError(t, fs.WriteFile(ctx, dataPath, []byte("data")))
	plan, err := PlanOrphanFiles(ctx, tbl, WithCleanupFilesOlderThan(0))
	require.NoError(t, err)
	require.Equal(t, []string{dataPath}, plan.Files())

	// The catalog may parse a new metadata object on each load.
	catalog.metadata = withoutUUID(meta)
	var deleted []string
	deleteFile := WithCleanupDeleteFunc(func(_ context.Context, path string) error {
		deleted = append(deleted, path)

		return nil
	})
	_, err = ExecuteOrphanCleanup(ctx, tbl, plan, deleteFile)
	require.NoError(t, err)
	require.Equal(t, []string{dataPath}, deleted)

	fileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, dataPath,
		iceberg.ParquetFile, nil, nil, nil, 1, int64(len("data")),
	)
	require.NoError(t, err)
	peer := tbl.NewTransaction()
	require.NoError(t, peer.AddDataFiles(ctx, []iceberg.DataFile{fileBuilder.Build()}, nil))
	committed, err := peer.Commit(ctx)
	require.NoError(t, err)

	// Simulate a legacy catalog reusing the metadata location.
	catalog.metadata = withoutUUID(committed.Metadata())
	catalog.location = metadataLocation
	_, err = ExecuteOrphanCleanup(ctx, tbl, plan, deleteFile)
	require.ErrorIs(t, err, ErrStaleOrphanCleanupPlan)
	require.Equal(t, []string{dataPath}, deleted, "a stale plan must not invoke deletion")
}

func TestOrphanCleanupRequiresCatalogForDeletion(t *testing.T) {
	ctx := context.Background()
	fs := iceio.NewMemFS()
	location := "mem://stale-plan/offline/table"
	metadataLocation := location + "/metadata/v1.metadata.json"
	meta, err := table.NewMetadata(iceberg.NewSchema(0), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, nil)
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"db", "offline"}, meta, metadataLocation, testFSF(fs), nil)

	dataPath := location + "/data/orphan.parquet"
	require.NoError(t, fs.WriteFile(ctx, dataPath, []byte("data")))
	plan, err := PlanOrphanFiles(ctx, tbl, WithCleanupFilesOlderThan(0))
	require.NoError(t, err)
	require.Equal(t, []string{dataPath}, plan.Files())

	_, err = ExecuteOrphanCleanup(ctx, tbl, plan)
	require.ErrorIs(t, err, table.ErrInvalidOperation)

	dryRun, err := DeleteOrphanFiles(ctx, tbl,
		WithCleanupFilesOlderThan(0), WithCleanupDryRun(true))
	require.NoError(t, err, "planning and dry-run remain available without a catalog")
	require.Len(t, dryRun.OrphanFiles, 1)
	require.Equal(t, dataPath, dryRun.OrphanFiles[0].Path)

	deleteCalls := 0
	_, err = DeleteOrphanFiles(ctx, tbl,
		WithCleanupFilesOlderThan(0),
		WithCleanupDeleteFunc(func(context.Context, string) error {
			deleteCalls++

			return nil
		}))
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.Zero(t, deleteCalls, "a custom delete function must not bypass metadata refresh")

	file, err := fs.Open(ctx, dataPath)
	require.NoError(t, err, "execution must fail before deleting files without a catalog")
	require.NoError(t, file.Close())
}

type orphanCleanupTestCatalog struct {
	metadata   table.Metadata
	location   string
	fsF        table.FSysF
	nextFile   int
	beforeLoad func() error
	refsOnly   bool
}

func (c *orphanCleanupTestCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	if c.beforeLoad != nil {
		beforeLoad := c.beforeLoad
		c.beforeLoad = nil
		if err := beforeLoad(); err != nil {
			return nil, err
		}
	}

	return table.New(ident, c.metadata, c.location, c.fsF, c, table.WithSnapshotHistoryLimitedToRefs(c.refsOnly)), nil
}

func (c *orphanCleanupTestCatalog) CommitTable(
	_ context.Context,
	_ table.Identifier,
	reqs []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	for _, req := range reqs {
		if err := req.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
		}
	}

	meta, err := table.UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta
	c.nextFile++
	c.location = fmt.Sprintf("%s/metadata/v%d.metadata.json", meta.Location(), c.nextFile+1)

	return meta, c.location, nil
}
