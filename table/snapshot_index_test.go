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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotByIDUsesReadOnlyFallbackAfterSliceReplacement(t *testing.T) {
	metadata := commonMetadata{
		SnapshotList: []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}},
	}
	metadata.snapshotIndex = buildSnapshotIndex(metadata.SnapshotList)
	originalIndex := metadata.snapshotIndex

	snapshot := metadata.SnapshotByID(2)
	require.NotNil(t, snapshot)
	require.Equal(t, int64(2), snapshot.SnapshotID)
	require.Same(t, originalIndex, metadata.snapshotIndex)

	// Keep the slice length unchanged so the lookup must validate the cached
	// position instead of relying only on ensureSnapshotIndex.
	metadata.SnapshotList = []Snapshot{{SnapshotID: 3}, {SnapshotID: 4}}

	snapshot = metadata.SnapshotByID(4)
	require.NotNil(t, snapshot)
	require.Equal(t, int64(4), snapshot.SnapshotID)
	require.Same(t, originalIndex, metadata.snapshotIndex)
	require.Nil(t, metadata.SnapshotByID(2))
	require.Equal(t, map[int64]int{1: 0, 2: 1}, metadata.snapshotIndex.positions)

	metadata.SnapshotList = []Snapshot{{SnapshotID: 5}}
	snapshot = metadata.SnapshotByID(5)
	require.NotNil(t, snapshot)
	require.Equal(t, int64(5), snapshot.SnapshotID)
	require.Same(t, originalIndex, metadata.snapshotIndex)
}

func TestMetadataBuilderSnapshotByIDUsesReadOnlyFallbackAfterSliceReplacement(t *testing.T) {
	builder := builderWithoutChanges(2)
	builder.snapshotList = []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}}
	builder.snapshotIndex = buildSnapshotIndex(builder.snapshotList)
	originalIndex := builder.snapshotIndex

	// Package-level fixtures can replace the snapshot slice directly. The
	// cached index still has the same length, so the lookup must fall back.
	builder.snapshotList = []Snapshot{{SnapshotID: 3}, {SnapshotID: 4}}

	snapshot, err := builder.SnapshotByID(4)
	require.NoError(t, err)
	require.Equal(t, int64(4), snapshot.SnapshotID)
	require.Same(t, originalIndex, builder.snapshotIndex)
	require.Equal(t, map[int64]int{1: 0, 2: 1}, builder.snapshotIndex.positions)
	_, err = builder.SnapshotByID(2)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	require.Same(t, originalIndex, builder.snapshotIndex)
	require.Equal(t, map[int64]int{1: 0, 2: 1}, builder.snapshotIndex.positions)
}

func TestMetadataBuilderSnapshotIndexFollowsUpdates(t *testing.T) {
	builder := builderWithoutChanges(2)
	baseTimestamp := builder.base.LastUpdatedMillis()
	parentID := int64(1)

	first := Snapshot{SnapshotID: 1, TimestampMs: baseTimestamp + 1}
	second := Snapshot{SnapshotID: 2, ParentSnapshotID: &parentID, SequenceNumber: 1, TimestampMs: baseTimestamp + 2}
	require.NoError(t, builder.AddSnapshot(&first))
	require.NoError(t, builder.AddSnapshot(&second))

	snapshot, err := builder.SnapshotByID(second.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, second.SnapshotID, snapshot.SnapshotID)

	require.NoError(t, builder.RemoveSnapshots([]int64{first.SnapshotID}, false))
	require.NotContains(t, builder.snapshotIndex.positions, first.SnapshotID)
	require.Equal(t, 0, builder.snapshotIndex.positions[second.SnapshotID])
	_, err = builder.SnapshotByID(first.SnapshotID)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	snapshot, err = builder.SnapshotByID(second.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, second.SnapshotID, snapshot.SnapshotID)
}

func TestMetadataBuilderFromBaseBuildsSnapshotIndex(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.Len(t, builder.snapshotIndex.positions, len(builder.snapshotList))

	id := builder.snapshotList[len(builder.snapshotList)-1].SnapshotID
	snapshot, err := builder.SnapshotByID(id)
	require.NoError(t, err)
	require.Equal(t, id, snapshot.SnapshotID)
}

func TestMetadataBuilderBuildIncludesSnapshotIndex(t *testing.T) {
	builder := freshMetadataBuilder(t, 2)
	snapshot := freshBuilderSnapshot(1, nil, 0, time.Now().UnixMilli())
	require.NoError(t, builder.AddSnapshot(&snapshot))

	metadata, err := builder.Build()
	require.NoError(t, err)
	require.Same(t, builder.snapshotIndex, metadataCommon(metadata).snapshotIndex)

	found := metadata.SnapshotByID(snapshot.SnapshotID)
	require.NotNil(t, found)
	require.Equal(t, snapshot.SnapshotID, found.SnapshotID)

	parentID := snapshot.SnapshotID
	next := freshBuilderSnapshot(2, &parentID, 1, snapshot.TimestampMs+1)
	require.NoError(t, builder.AddSnapshot(&next))
	require.Nil(t, metadata.SnapshotByID(next.SnapshotID))
	found, err = builder.SnapshotByID(next.SnapshotID)
	require.NoError(t, err)
	require.Equal(t, next.SnapshotID, found.SnapshotID)
}

func TestMetadataDecodeBuildsSnapshotIndex(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)

	common := metadataCommon(metadata)
	require.Len(t, common.snapshotIndex.positions, len(common.SnapshotList))
	for i, snapshot := range common.SnapshotList {
		require.Equal(t, i, common.snapshotIndex.positions[snapshot.SnapshotID])
	}
}

func TestMetadataBuilderCloneSharesSnapshotIndexUntilSnapshotMutation(t *testing.T) {
	builder := builderWithoutChanges(2)
	baseTimestamp := builder.base.LastUpdatedMillis()
	first := freshBuilderSnapshot(1, nil, 0, baseTimestamp+1)
	require.NoError(t, builder.AddSnapshot(&first))

	cloned := builder.clone()
	require.Same(t, builder.snapshotIndex, cloned.snapshotIndex)

	second := freshBuilderSnapshot(2, &first.SnapshotID, 1, baseTimestamp+2)
	require.NoError(t, cloned.AddSnapshot(&second))

	require.NotSame(t, builder.snapshotIndex, cloned.snapshotIndex)
	require.NotContains(t, builder.snapshotIndex.positions, second.SnapshotID)
	require.Equal(t, 1, cloned.snapshotIndex.positions[second.SnapshotID])
}

func TestCommonMetadataSnapshotLookupsConcurrent(t *testing.T) {
	currentSnapshotID := int64(2)
	metadata := &commonMetadata{
		SnapshotList:      []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}},
		CurrentSnapshotID: &currentSnapshotID,
		SnapshotRefs: map[string]SnapshotRef{
			MainBranch: {SnapshotID: currentSnapshotID, SnapshotRefType: BranchRef},
		},
	}
	metadata.snapshotIndex = buildSnapshotIndex(metadata.SnapshotList)

	const (
		goroutineCount = 8
		lookupCount    = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutineCount)
	for i := range goroutineCount {
		go func(i int) {
			defer wg.Done()

			for range lookupCount {
				var snapshot *Snapshot
				var expectedID int64
				switch i % 3 {
				case 0:
					snapshot = metadata.SnapshotByID(1)
					expectedID = 1
				case 1:
					snapshot = metadata.SnapshotByName(MainBranch)
					expectedID = currentSnapshotID
				default:
					snapshot = metadata.CurrentSnapshot()
					expectedID = currentSnapshotID
				}

				if snapshot == nil || snapshot.SnapshotID != expectedID {
					t.Errorf("expected snapshot %d, got %v", expectedID, snapshot)

					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestMetadataBuilderSnapshotByIDConcurrent(t *testing.T) {
	builder := builderWithoutChanges(2)
	builder.snapshotList = []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}}
	builder.snapshotIndex = buildSnapshotIndex(builder.snapshotList)

	const (
		goroutineCount = 8
		lookupCount    = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutineCount)
	for range goroutineCount {
		go func() {
			defer wg.Done()

			for range lookupCount {
				snapshot, err := builder.SnapshotByID(2)
				if err != nil || snapshot == nil || snapshot.SnapshotID != 2 {
					t.Errorf("expected snapshot 2, got snapshot=%v err=%v", snapshot, err)

					return
				}
			}
		}()
	}
	wg.Wait()
}
