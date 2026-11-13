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

package table

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddSnapshotRequiresManifestListForV2AndV3(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			builder := builderWithoutChanges(version)
			snapshot := freshBuilderSnapshot(101, nil, 1, time.Now().UnixMilli())
			snapshot.ManifestList = ""
			// A v1 embedded-manifest fallback is not valid for a new v2/v3 snapshot.
			snapshot.ManifestLocations = []string{"s3://bucket/old-manifest.avro"}

			err := builder.AddSnapshot(&snapshot)
			require.ErrorIs(t, err, ErrInvalidMetadata)
			require.ErrorContains(t, err, "manifest-list is required")
			require.Empty(t, builder.snapshotList)
			require.Empty(t, builder.updates)
		})
	}
}

func TestAddSnapshotAcceptsManifestListForV2AndV3(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			builder := builderWithoutChanges(version)
			snapshot := freshBuilderSnapshot(101, nil, 1, time.Now().UnixMilli())
			if version == 3 {
				firstRowID, addedRows := int64(0), int64(0)
				snapshot.FirstRowID = &firstRowID
				snapshot.AddedRows = &addedRows
			}

			require.NoError(t, builder.AddSnapshot(&snapshot))
			require.Len(t, builder.snapshotList, 1)
		})
	}
}

func TestAddSnapshotKeepsV1EmbeddedManifests(t *testing.T) {
	builder := builderWithoutChanges(1)
	snapshot := freshBuilderSnapshot(101, nil, 1, time.Now().UnixMilli())
	snapshot.ManifestList = ""
	snapshot.ManifestLocations = []string{"s3://bucket/old-manifest.avro"}

	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.Len(t, builder.snapshotList, 1)
}

func TestAddSnapshotRequiresManifestSourceForV1(t *testing.T) {
	builder := builderWithoutChanges(1)
	snapshot := freshBuilderSnapshot(101, nil, 1, time.Now().UnixMilli())
	snapshot.ManifestList = ""
	snapshot.ManifestLocations = nil

	err := builder.AddSnapshot(&snapshot)
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "snapshot 101 requires manifest-list or manifests")
	require.Empty(t, builder.snapshotList)
	require.Empty(t, builder.updates)
}

func TestAddSnapshotAcceptsEmptyEmbeddedManifestsForV1(t *testing.T) {
	builder := builderWithoutChanges(1)
	snapshot := freshBuilderSnapshot(101, nil, 1, time.Now().UnixMilli())
	snapshot.ManifestList = ""
	snapshot.ManifestLocations = []string{}

	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.Len(t, builder.snapshotList, 1)
}
