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
	"context"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestFastAppendInheritsEmbeddedV1ManifestWithLength(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 1

	const parentSnapshotID int64 = 101
	inherited := writeTestManifestFileWithVersion(
		t, memIO, spec, simpleSchema(), parentSnapshotID, 1, 1,
	)
	parent := Snapshot{
		SnapshotID:        parentSnapshotID,
		TimestampMs:       time.Now().UnixMilli(),
		ManifestLocations: []string{inherited.FilePath()},
		Summary:           &Summary{Operation: OpAppend},
	}
	require.NoError(t, txn.meta.AddSnapshot(&parent))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, parentSnapshotID, BranchRef))

	producer := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	producer.appendDataFile(newTestDataFile(t, spec, "file://new-data.parquet", nil))

	updates, _, err := producer.commit(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, updates)

	addSnapshot, ok := updates[0].(*addSnapshotUpdate)
	require.True(t, ok, "first update must add the new snapshot")
	manifests := readManifestListFromPath(t, memIO, addSnapshot.Snapshot.ManifestList)
	require.Len(t, manifests, 2)

	for _, manifest := range manifests {
		if manifest.FilePath() == inherited.FilePath() {
			require.Equal(t, inherited.Length(), manifest.Length())
			require.Positive(t, manifest.Length())

			return
		}
	}

	require.Failf(t, "missing inherited manifest", "path=%s", inherited.FilePath())
}
