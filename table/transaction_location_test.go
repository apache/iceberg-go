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
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionSetLocationStagesMetadataUpdate(t *testing.T) {
	tbl := newCommitTestTable(t)
	original := tbl.Location()
	tx := tbl.NewTransaction()

	newLocation := "s3://bucket/new-location///"
	require.NoError(t, tx.SetLocation(newLocation))

	staged, err := tx.StagedTable()
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/new-location", staged.Location())
	assert.Equal(t, original, tbl.Location(), "the original table stays unchanged until commit")

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Len(t, commit.Updates, 1)
	assert.Equal(t, table.UpdateSetLocation, commit.Updates[0].Action())
	updateJSON, err := json.Marshal(commit.Updates[0])
	require.NoError(t, err)
	assert.JSONEq(t, `{"action":"set-location","location":"s3://bucket/new-location"}`, string(updateJSON))
	require.Len(t, commit.Requirements, 2)
	assert.Equal(t, "assert-ref-snapshot-id", commit.Requirements[0].GetType())
	assert.Equal(t, "assert-table-uuid", commit.Requirements[1].GetType())
}

func TestTransactionSetLocationNoOp(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetLocation(tbl.Location()))

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	assert.Empty(t, commit.Updates)
}

func TestTransactionSetLocationRepeatedSameLocation(t *testing.T) {
	tx := newCommitTestTable(t).NewTransaction()
	require.NoError(t, tx.SetLocation("s3://bucket/new"))
	require.NoError(t, tx.SetLocation("s3://bucket/new"))

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Len(t, commit.Updates, 1)
}

func TestTransactionSetLocationStripsTrailingSlash(t *testing.T) {
	tx := newCommitTestTable(t).NewTransaction()
	require.NoError(t, tx.SetLocation("s3://bucket/new/"))

	staged, err := tx.StagedTable()
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/new", staged.Location())
}

func TestTransactionSetLocationRejectsEmptyLocation(t *testing.T) {
	for _, location := range []string{"", " ", "\t\n", "///"} {
		t.Run("location="+location, func(t *testing.T) {
			tx := newCommitTestTable(t).NewTransaction()
			err := tx.SetLocation(location)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

			commit, err := tx.TableCommit()
			require.NoError(t, err)
			assert.Empty(t, commit.Updates)
		})
	}
}

func TestTransactionSetLocationTrailingSlashNoOp(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetLocation(tbl.Location()+"///"))

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	assert.Empty(t, commit.Updates)
}

func TestTransactionSetLocationRejectsCrossStorageBoundary(t *testing.T) {
	for _, location := range []string{
		"s3://another-bucket/relocated",
		"gs://bucket/relocated",
		"/tmp/relocated",
	} {
		t.Run(location, func(t *testing.T) {
			tx := newCommitTestTable(t).NewTransaction()
			require.ErrorIs(t, tx.SetLocation(location), iceberg.ErrNotImplemented)

			commit, err := tx.TableCommit()
			require.NoError(t, err)
			assert.Empty(t, commit.Updates)
		})
	}
}

func TestTransactionSetLocationRejectsCrossAzureContainers(t *testing.T) {
	const location = "abfss://container-one@account.dfs.core.windows.net/source"

	meta, err := table.NewMetadata(newCommitTestTable(t).Schema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"db", "azure_location_test"},
		meta, location+"/metadata/v1.metadata.json", nil, nil)

	tx := tbl.NewTransaction()
	require.ErrorIs(t, tx.SetLocation("abfss://container-two@account.dfs.core.windows.net/target"),
		iceberg.ErrNotImplemented)
	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Empty(t, commit.Updates)

	sameContainer := tbl.NewTransaction()
	const newLocation = "abfss://container-one@account.dfs.core.windows.net/target"
	require.NoError(t, sameContainer.SetLocation(newLocation))
	staged, err := sameContainer.StagedTable()
	require.NoError(t, err)
	require.Equal(t, newLocation, staged.Location())
}

func TestTransactionSetLocationPreservesOtherUpdates(t *testing.T) {
	tx := newCommitTestTable(t).NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{"test.property": "value"}))
	require.NoError(t, tx.SetLocation("s3://bucket/relocated"))

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Len(t, commit.Updates, 2)
	assert.Equal(t, table.UpdateSetProperties, commit.Updates[0].Action())
	assert.Equal(t, table.UpdateSetLocation, commit.Updates[1].Action())
}

func TestTransactionSetLocationAfterCommitIsRejected(t *testing.T) {
	tx := newCommitTestTable(t).NewTransaction()
	tx.MarkCommitted()

	err := tx.SetLocation("s3://bucket/new")
	require.ErrorContains(t, err, "already been committed")
}

func TestTransactionSetLocationWritesToStagedPaths(t *testing.T) {
	for _, tc := range []struct {
		name          string
		formatVersion int
		overridePaths bool
	}{
		{name: "v2", formatVersion: 2},
		{name: "v3", formatVersion: 3},
		{name: "staged_write_properties", formatVersion: 2, overridePaths: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, _ := newConcurrentRewriteTestTableVersion(t, tc.formatVersion)
			originalLocation := tbl.Location()
			newLocation := filepath.ToSlash(filepath.Join(t.TempDir(), "relocated"))
			dataPath := newLocation + "/data"
			metadataPath := newLocation + "/metadata"

			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetLocation(newLocation))
			if tc.overridePaths {
				dataPath = filepath.ToSlash(filepath.Join(t.TempDir(), "custom-data"))
				metadataPath = filepath.ToSlash(filepath.Join(t.TempDir(), "custom-metadata"))
				require.NoError(t, tx.SetProperties(iceberg.Properties{
					table.WriteDataPathKey:     dataPath,
					table.WriteMetadataPathKey: metadataPath,
				}))
			}

			arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
			require.NoError(t, err)
			records, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
				[]string{`[{"id": 1, "data": "row"}]`})
			require.NoError(t, err)
			defer records.Release()

			require.NoError(t, tx.AppendTable(t.Context(), records, 1, nil))
			staged, err := tx.StagedTable()
			require.NoError(t, err)
			require.Equal(t, newLocation, staged.Location())
			require.Equal(t, originalLocation, tbl.Location())

			snapshot := staged.CurrentSnapshot()
			require.NotNil(t, snapshot)
			require.True(t, strings.HasPrefix(snapshot.ManifestList, metadataPath+"/"), snapshot.ManifestList)

			fsys, err := staged.FS(t.Context())
			require.NoError(t, err)
			manifests, err := snapshot.Manifests(t.Context(), fsys)
			require.NoError(t, err)
			require.Len(t, manifests, 1)
			require.True(t, strings.HasPrefix(manifests[0].FilePath(), metadataPath+"/"),
				manifests[0].FilePath())

			var dataFiles int
			for entry, err := range manifests[0].Entries(t.Context(), fsys, false) {
				require.NoError(t, err)
				require.True(t, strings.HasPrefix(entry.DataFile().FilePath(), dataPath+"/"),
					entry.DataFile().FilePath())
				dataFiles++
			}
			require.Equal(t, 1, dataFiles)

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.Equal(t, newLocation, committed.Location())
		})
	}
}

// Equality-delete writing is a separate path from Append. It must use the
// staged table location and any staged write.data.path override as well.
func TestTransactionSetLocationWritesEqualityDeletesToStagedPath(t *testing.T) {
	for _, tc := range []struct {
		name         string
		overridePath bool
	}{
		{name: "new table location"},
		{name: "custom data path", overridePath: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := newEqDeleteTestTable(t, "2")
			location := filepath.ToSlash(filepath.Join(t.TempDir(), "relocated"))
			dataPath := location + "/data"
			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetLocation(location))
			if tc.overridePath {
				dataPath = filepath.ToSlash(filepath.Join(t.TempDir(), "override-data"))
				require.NoError(t, tx.SetProperties(iceberg.Properties{
					table.WriteDataPathKey: dataPath,
				}))
			}

			deleteSchema, err := table.SchemaToArrowSchema(
				iceberg.NewSchema(0, iceberg.NestedField{
					ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
				}), nil, true, false)
			require.NoError(t, err)
			records, release := makeEqDeleteRecords(t, deleteSchema, `[{"id": 7}]`)
			defer release()

			files, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
			require.NoError(t, err)
			require.Len(t, files, 1)
			require.True(t, strings.HasPrefix(files[0].FilePath(), dataPath+"/"),
				"equality deletes must use the staged data directory: %s", files[0].FilePath())
		})
	}
}

func TestTransactionSetLocationOverwriteAndDeletesWriteToStagedPaths(t *testing.T) {
	cases := []struct {
		name        string
		version     string
		operation   string
		wantContent iceberg.ManifestContent
		wantIDs     []int64
	}{
		{name: "overwrite", version: "2", operation: "overwrite", wantContent: iceberg.ManifestContentData, wantIDs: []int64{2, 3}},
		{name: "copy-on-write rewrite", version: "2", operation: "cow", wantContent: iceberg.ManifestContentData, wantIDs: []int64{2}},
		{name: "position delete v2", version: "2", operation: "mor", wantContent: iceberg.ManifestContentDeletes, wantIDs: []int64{2}},
		{name: "deletion vector v3", version: "3", operation: "mor", wantContent: iceberg.ManifestContentDeletes, wantIDs: []int64{2}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := newMergeOnReadTestTableVersion(t, tc.version)
			tbl = appendRows(t, tbl, `[{"id":1,"data":"one"},{"id":2,"data":"two"}]`)
			originalLocation := tbl.Location()
			newLocation := filepath.ToSlash(filepath.Join(t.TempDir(), "relocated"))

			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetLocation(newLocation))
			filter := iceberg.EqualTo(iceberg.Reference("id"), int64(1))
			switch tc.operation {
			case "overwrite":
				rdr := rowsReader(t, tbl, `[{"id":3,"data":"new"}]`)
				defer rdr.Release()
				require.NoError(t, tx.Overwrite(t.Context(), rdr, nil, table.WithOverwriteFilter(filter)))
			case "cow":
				require.NoError(t, tx.SetProperties(iceberg.Properties{
					table.WriteDeleteModeKey: table.WriteModeCopyOnWrite,
				}))
				require.NoError(t, tx.Delete(t.Context(), filter, nil))
			case "mor":
				require.NoError(t, tx.Delete(t.Context(), filter, nil))
			}

			staged, err := tx.StagedTable()
			require.NoError(t, err)
			require.Equal(t, newLocation, staged.Location())
			fs, err := staged.FS(t.Context())
			require.NoError(t, err)
			manifests, err := staged.CurrentSnapshot().Manifests(t.Context(), fs)
			require.NoError(t, err)

			var newFiles int
			for _, manifest := range manifests {
				if manifest.ManifestContent() != tc.wantContent {
					continue
				}
				for entry, err := range manifest.Entries(t.Context(), fs, true) {
					require.NoError(t, err)
					file := entry.DataFile()
					if !strings.HasPrefix(file.FilePath(), newLocation+"/data/") {
						continue
					}
					newFiles++
					if tc.operation == "mor" {
						require.Equal(t, iceberg.EntryContentPosDeletes, file.ContentType())
						if tc.version == "3" {
							require.Equal(t, iceberg.PuffinFile, file.FileFormat())
						} else {
							require.Equal(t, iceberg.ParquetFile, file.FileFormat())
						}
					}
				}
			}
			require.NotZero(t, newFiles, "the operation must write at least one file in the staged location")

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.Equal(t, originalLocation, tbl.Location())
			require.Equal(t, newLocation, committed.Location())
			require.Equal(t, tc.wantIDs, idsInTable(t, committed))
		})
	}
}

func TestTransactionSetLocationRewriteDataFilesUsesStagedLocation(t *testing.T) {
	tbl := newRewriteTestTable(t)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for _, data := range []string{
		`[{"id": 1, "data": "one"}]`,
		`[{"id": 2, "data": "two"}]`,
	} {
		records, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{data})
		require.NoError(t, err)
		tx := tbl.NewTransaction()
		err = tx.AppendTable(t.Context(), records, 1, nil)
		records.Release()
		require.NoError(t, err)
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	originalLocation := tbl.Location()
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	newLocation := filepath.ToSlash(filepath.Join(t.TempDir(), "relocated"))
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetLocation(newLocation+"/"))
	result, err := tx.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{{
		PartitionKey: "all",
		Tasks:        tasks,
	}}, table.RewriteDataFilesOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, result.RewrittenGroups)

	staged, err := tx.StagedTable()
	require.NoError(t, err)
	require.Equal(t, newLocation, staged.Location())
	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, newLocation, committed.Location())
	require.Equal(t, originalLocation, tbl.Location())

	newTasks, err := committed.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, newTasks)
	for _, task := range newTasks {
		require.True(t, strings.HasPrefix(task.File.FilePath(), newLocation+"/data/"),
			"compacted file must use staged location: %s", task.File.FilePath())
	}

	rows, err := committed.Scan().ToArrowTable(t.Context())
	require.NoError(t, err)
	defer rows.Release()
	require.EqualValues(t, 2, rows.NumRows())
}

func TestTransactionSetLocationDoesNotMoveExistingFiles(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTable(t)
	originalLocation := tbl.Location()

	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	records, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{`[{"id": 1, "data": "existing"}]`})
	require.NoError(t, err)
	defer records.Release()

	seed := tbl.NewTransaction()
	require.NoError(t, seed.AppendTable(t.Context(), records, 1, nil))
	tbl, err = seed.Commit(t.Context())
	require.NoError(t, err)

	originalManifestList := tbl.CurrentSnapshot().ManifestList
	require.True(t, strings.HasPrefix(originalManifestList, originalLocation+"/metadata/"))

	newLocation := filepath.ToSlash(filepath.Join(t.TempDir(), "relocated"))
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetLocation(newLocation))
	relocated, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, newLocation, relocated.Location())
	require.Equal(t, originalManifestList, relocated.CurrentSnapshot().ManifestList)

	// The relocated table still reads data through its original file paths.
	rows, err := relocated.Scan().ToArrowTable(t.Context())
	require.NoError(t, err)
	defer rows.Release()
	require.EqualValues(t, 1, rows.NumRows())
}

func TestTransactionSetLocationReadsOldAndNewFilesAfterReload(t *testing.T) {
	tbl, cat := newConcurrentRewriteTestTable(t)
	originalLocation := tbl.Location()

	initial := rowsReader(t, tbl, `[{"id":1,"data":"old"}]`)
	defer initial.Release()
	seed := tbl.NewTransaction()
	require.NoError(t, seed.Append(t.Context(), initial, nil))
	tbl, err := seed.Commit(t.Context())
	require.NoError(t, err)

	newLocation := filepath.ToSlash(filepath.Join(t.TempDir(), "relocated"))
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetLocation(newLocation))
	added := rowsReader(t, tbl, `[{"id":2,"data":"new"}]`)
	defer added.Release()
	require.NoError(t, tx.Append(t.Context(), added, nil))
	_, err = tx.Commit(t.Context())
	require.NoError(t, err)

	reloaded, err := cat.LoadTable(t.Context(), tbl.Identifier())
	require.NoError(t, err)
	require.Equal(t, newLocation, reloaded.Location())

	tasks, err := reloaded.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	var oldFiles, newFiles int
	for _, task := range tasks {
		switch {
		case strings.HasPrefix(task.File.FilePath(), originalLocation+"/data/"):
			oldFiles++
		case strings.HasPrefix(task.File.FilePath(), newLocation+"/data/"):
			newFiles++
		default:
			t.Fatalf("unexpected data file location: %s", task.File.FilePath())
		}
	}
	require.Equal(t, 1, oldFiles)
	require.Equal(t, 1, newFiles)
	require.Equal(t, []int64{1, 2}, idsInTable(t, reloaded))
}
