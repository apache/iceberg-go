// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestManifestProviderUniqueManifestsFromAnotherPackage(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 2)
	snapshots := tbl.Metadata().Snapshots()
	require.Len(t, snapshots, 2)
	provider := tbl.ManifestProvider()
	var expected []string
	seen := make(map[string]bool)
	for _, snapshot := range snapshots {
		manifests, err := provider.Manifests(t.Context(), snapshot)
		require.NoError(t, err)
		for manifest := range manifests {
			if !seen[manifest.FilePath()] {
				expected = append(expected, manifest.FilePath())
				seen[manifest.FilePath()] = true
			}
		}
	}
	var actual []string
	for manifest, err := range provider.UniqueManifests(t.Context(), snapshots) {
		require.NoError(t, err)
		actual = append(actual, manifest.FilePath())
	}
	require.NotEmpty(t, expected)
	require.Equal(t, expected, actual)
}
