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
	"errors"
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
	assert.Equal(t, newLocation, staged.Location())
	assert.Equal(t, original, tbl.Location(), "the original table stays unchanged until commit")

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Len(t, commit.Updates, 1)
	assert.Equal(t, table.UpdateSetLocation, commit.Updates[0].Action())
	updateJSON, err := json.Marshal(commit.Updates[0])
	require.NoError(t, err)
	assert.JSONEq(t, "{\"action\":\"set-location\",\"location\":\"s3://bucket/new-location///\"}", string(updateJSON))
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

func TestTransactionSetLocationPreservesTrailingSlash(t *testing.T) {
	tx := newCommitTestTable(t).NewTransaction()
	require.NoError(t, tx.SetLocation("s3://bucket/new/"))

	staged, err := tx.StagedTable()
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/new/", staged.Location())
}

func TestTransactionSetLocationRejectsEmptyLocation(t *testing.T) {
	for _, location := range []string{"", " ", "\t\n"} {
		t.Run("location="+location, func(t *testing.T) {
			tx := newCommitTestTable(t).NewTransaction()
			err := tx.SetLocation(location)
			require.Error(t, err)
			assert.True(t, errors.Is(err, iceberg.ErrInvalidArgument))

			commit, err := tx.TableCommit()
			require.NoError(t, err)
			assert.Empty(t, commit.Updates)
		})
	}
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
			manifests, err := snapshot.Manifests(fsys)
			require.NoError(t, err)
			require.Len(t, manifests, 1)
			require.True(t, strings.HasPrefix(manifests[0].FilePath(), metadataPath+"/"),
				manifests[0].FilePath())

			var dataFiles int
			for entry, err := range manifests[0].Entries(fsys, false) {
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
