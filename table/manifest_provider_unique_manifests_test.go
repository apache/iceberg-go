// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestManifestProviderUniqueManifestsPreservesOrderAndDeduplicates(t *testing.T) {
	fs := iceio.NewMemFS()
	shared := iceberg.NewManifestBuilder(2, "mem://provider/shared.avro", 10, 0, 1).SequenceNumber(1, 1).Build()
	first := iceberg.NewManifestBuilder(2, "mem://provider/first.avro", 10, 0, 1).SequenceNumber(1, 1).Build()
	second := iceberg.NewManifestBuilder(2, "mem://provider/second.avro", 10, 0, 1).SequenceNumber(1, 1).Build()

	writeList := func(snapshotID int64, path string, manifests []iceberg.ManifestFile) {
		t.Helper()
		var buf bytes.Buffer
		sequenceNumber := snapshotID
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil, &sequenceNumber, 0, manifests))
		require.NoError(t, fs.WriteFile(t.Context(), path, buf.Bytes()))
	}

	const firstList = "mem://provider/snap-1.avro"
	const secondList = "mem://provider/snap-2.avro"
	writeList(1, firstList, []iceberg.ManifestFile{shared, first})
	writeList(2, secondList, []iceberg.ManifestFile{shared, second})

	provider := ManifestProvider{
		cache: newSnapshotManifestCache(),
		fsF:   testFSF(fs),
	}
	snapshots := []Snapshot{
		{SnapshotID: 1, ManifestList: firstList},
		{SnapshotID: 2, ManifestList: secondList},
	}
	var paths []string
	for manifest, err := range provider.UniqueManifests(t.Context(), snapshots) {
		require.NoError(t, err)
		paths = append(paths, manifest.FilePath())
	}
	require.Equal(t, []string{shared.FilePath(), first.FilePath(), second.FilePath()}, paths)
}

func TestManifestProviderUniqueManifestsCancelledBeforeRead(t *testing.T) {
	provider := ManifestProvider{cache: newSnapshotManifestCache(), fsF: testFSF(iceio.NewMemFS())}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	count := 0
	for manifest, err := range provider.UniqueManifests(ctx, []Snapshot{{SnapshotID: 1}}) {
		require.Nil(t, manifest)
		require.ErrorIs(t, err, context.Canceled)
		count++
	}
	require.Equal(t, 1, count)
}

func TestStreamSnapshotManifestsStopsOnCancellation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		snapshots []Snapshot
		cachedIDs map[int64]bool
	}{
		{"single uncached", []Snapshot{{SnapshotID: 1}}, nil},
		{"single cached", []Snapshot{{SnapshotID: 1}}, map[int64]bool{1: true}},
		{"all cached", []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}}, map[int64]bool{1: true, 2: true}},
		{"mixed", []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}}, map[int64]bool{1: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifests := []iceberg.ManifestFile{
				iceberg.NewManifestBuilder(1, "mem://a", 1, 0, 1).Build(),
				iceberg.NewManifestBuilder(1, "mem://b", 1, 0, 1).Build(),
			}
			set := newSnapshotManifestSet(manifests)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var paths []string
			var gotErr error
			cached := func(snapshot Snapshot) (snapshotManifestSet, bool) { return set, tc.cachedIDs[snapshot.SnapshotID] }
			load := func(context.Context, Snapshot) (snapshotManifestSet, error) { return set, nil }
			streamSnapshotManifests(ctx, tc.snapshots, cached, load, func(manifest iceberg.ManifestFile, err error) bool {
				if err != nil {
					gotErr = err

					return true
				}
				paths = append(paths, manifest.FilePath())
				cancel()

				return true
			})
			require.Equal(t, []string{"mem://a"}, paths)
			require.ErrorIs(t, gotErr, context.Canceled)
		})
	}
}

func TestStreamSnapshotManifestsStopsWhenConsumerStops(t *testing.T) {
	set := newSnapshotManifestSet([]iceberg.ManifestFile{
		iceberg.NewManifestBuilder(1, "mem://a", 1, 0, 1).Build(),
		iceberg.NewManifestBuilder(1, "mem://b", 1, 0, 1).Build(),
	})
	calls := 0
	streamSnapshotManifests(t.Context(), []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}}, nil,
		func(context.Context, Snapshot) (snapshotManifestSet, error) { return set, nil },
		func(_ iceberg.ManifestFile, err error) bool {
			require.NoError(t, err)
			calls++

			return false
		})
	require.Equal(t, 1, calls)
}
