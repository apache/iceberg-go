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
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestTransactionSetLocationDynamicOverwriteKeepsUntouchedPartitions(t *testing.T) {
	for _, version := range []string{"2", "3"} {
		t.Run("v"+version, func(t *testing.T) {
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
			})
			base, cat := newDynamicOverwriteTableWithFormat(t, spec, version)
			tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2, 4)
			oldPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
			require.Len(t, oldPaths, 3)

			newLocation := filepath.ToSlash(filepath.Join(t.TempDir(), "relocated"))
			tx := tbl.NewTransaction()
			require.NoError(t, tx.SetLocation(newLocation+"/"))

			rdr := idRecordReader(t, tbl, 1, 3)
			err := tx.DynamicPartitionOverwrite(t.Context(), rdr, nil)
			rdr.Release()
			require.NoError(t, err)

			staged, err := tx.StagedTable()
			require.NoError(t, err)
			require.Equal(t, newLocation, staged.Location())
			snapshot := staged.CurrentSnapshot()
			require.NotNil(t, snapshot)
			fs, err := staged.FS(t.Context())
			require.NoError(t, err)
			manifests, err := snapshot.Manifests(t.Context(), fs)
			require.NoError(t, err)

			var addedPaths []string
			for _, manifest := range manifests {
				for entry, err := range manifest.Entries(t.Context(), fs, true) {
					require.NoError(t, err)
					if entry.Status() != iceberg.EntryStatusADDED || entry.SnapshotID() != snapshot.SnapshotID {
						continue
					}
					path := entry.DataFile().FilePath()
					require.True(t, strings.HasPrefix(path, newLocation+"/data/"),
						"dynamic overwrite must add files under the staged location: %s", path)
					addedPaths = append(addedPaths, path)
				}
			}
			require.Len(t, addedPaths, 2, "both touched partitions must produce a new data file")

			_, err = tx.Commit(t.Context())
			require.NoError(t, err)
			reloaded, err := cat.LoadTable(t.Context(), tbl.Identifier())
			require.NoError(t, err)
			require.Equal(t, newLocation, reloaded.Location())
			require.Equal(t, []int64{1, 2, 3, 4}, idsOnRef(t, reloaded, table.MainBranch))

			livePaths, _ := liveFilesOnRef(t, reloaded, table.MainBranch)
			require.Len(t, livePaths, 4)
			original := make(map[string]struct{}, len(oldPaths))
			for _, path := range oldPaths {
				original[path] = struct{}{}
			}
			var retainedOld, relocatedNew int
			for _, path := range livePaths {
				if _, ok := original[path]; ok {
					retainedOld++
				} else {
					require.Contains(t, addedPaths, path)
					relocatedNew++
				}
			}
			require.Equal(t, 2, retainedOld, "untouched partitions must retain their original files")
			require.Equal(t, 2, relocatedNew, "both replacement files must remain live after reload")
		})
	}
}
