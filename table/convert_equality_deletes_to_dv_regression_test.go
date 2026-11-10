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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// The same row may match several live equality-delete files. Each matching
// row position should appear only once in the converted vector's cardinality.
func TestConvertEqualityDeletesDeduplicatesOverlappingKeys(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"}]`)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2},{"id":3}]`)
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2}]`)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataPath := tasks[0].File.FilePath()

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, result.ConvertedEqualityDeleteFiles)
	require.Equal(t, 1, result.AddedDeletionVectors)
	require.Zero(t, liveDeleteFileCount(t, result.Table, iceberg.EntryContentEqDeletes))
	require.Equal(t, []int64{1, 4}, idsInTable(t, result.Table))

	fs, err := result.Table.FS(ctx)
	require.NoError(t, err)
	manifests, err := result.Table.CurrentSnapshot().Manifests(ctx, fs)
	require.NoError(t, err)
	var vectors int
	for _, manifest := range manifests {
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(ctx, fs, true) {
			require.NoError(t, err)
			file := entry.DataFile()
			if !table.IsDeletionVector(file) {
				continue
			}
			vectors++
			require.Equal(t, int64(2), file.Count(), "overlapping equality deletes must not double-count a row")
			require.NotNil(t, file.ReferencedDataFile())
			require.Equal(t, dataPath, *file.ReferencedDataFile())
		}
	}
	require.Equal(t, 1, vectors)
}

// Converting one data file must not replace a DV on another data file that
// has no applicable equality deletes.
func TestConvertEqualityDeletesPreservesUnrelatedDeletionVector(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tbl = appendRows(t, tbl, `[{"id":3,"data":"c"},{"id":4,"data":"d"}]`)

	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCount(t, tbl))

	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2}]`)
	require.Equal(t, []int64{1, 3}, idsInTable(t, tbl))

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.ConvertedEqualityDeleteFiles)
	require.Equal(t, 1, result.AddedDeletionVectors)
	require.Zero(t, result.ReplacedDeletionVectors)
	require.Equal(t, 2, liveDVCount(t, result.Table))
	require.Zero(t, liveDeleteFileCount(t, result.Table, iceberg.EntryContentEqDeletes))
	require.Equal(t, []int64{1, 3}, idsInTable(t, result.Table))
}

// An equality delete targeting a row already deleted by a DV should not
// create a redundant replacement vector.
func TestConvertEqualityDeletesAlreadyDeletedRow(t *testing.T) {
	ctx := t.Context()
	tbl := appendRows(t, newMergeOnReadTestTableVersion(t, "3"),
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"}]`)

	var err error
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCount(t, tbl))
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2}]`)

	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.ConvertedEqualityDeleteFiles)
	require.Zero(t, result.AddedDeletionVectors)
	require.Zero(t, result.ReplacedDeletionVectors)
	require.Equal(t, 1, liveDVCount(t, result.Table))
	require.Zero(t, liveDeleteFileCount(t, result.Table, iceberg.EntryContentEqDeletes))
	require.Equal(t, []int64{1, 3}, idsInTable(t, result.Table))
}
