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
//
//
package table

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestInspectSnapshotManifestPrefetchPreservesOrderAndBoundsReads(t *testing.T) {
	snapshots := make([]Snapshot, 25)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan int64, inspectSnapshotManifestPrefetch)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	var active, maxActive atomic.Int32
	resultCh := make(chan []string, 1)
	go func() {
		var paths []string
		streamSnapshotManifests(ctx, snapshots,
			func(ctx context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
				current := active.Add(1)
				defer active.Add(-1)
				for {
					previous := maxActive.Load()
					if current <= previous || maxActive.CompareAndSwap(previous, current) {
						break
					}
				}

				if snapshot.SnapshotID <= inspectSnapshotManifestPrefetch {
					started <- snapshot.SnapshotID
				}
				select {
				case <-release:
				case <-ctx.Done():
					return snapshotManifestSet{}, ctx.Err()
				}

				path := fmt.Sprintf("manifest-%d.avro", snapshot.SnapshotID)
				manifest := iceberg.NewManifestFile(2, path, 10, 0, 1).Build()

				return snapshotManifestSet{all: []iceberg.ManifestFile{manifest}}, nil
			},
			func(manifest iceberg.ManifestFile, err error) bool {
				if err != nil {
					paths = append(paths, "ERROR: "+err.Error())

					return false
				}
				paths = append(paths, manifest.FilePath())

				return true
			})
		resultCh <- paths
	}()

	var firstWindow []int64
	for range inspectSnapshotManifestPrefetch {
		select {
		case id := <-started:
			firstWindow = append(firstWindow, id)
		case <-time.After(5 * time.Second):
			t.Fatal("initial manifest reads did not start concurrently")
		}
	}
	slices.Sort(firstWindow)
	for i, id := range firstWindow {
		require.Equal(t, int64(i+1), id)
	}
	require.EqualValues(t, inspectSnapshotManifestPrefetch, maxActive.Load())
	close(release)

	select {
	case paths := <-resultCh:
		require.Len(t, paths, len(snapshots))
		for i, path := range paths {
			require.Equal(t, fmt.Sprintf("manifest-%d.avro", i+1), path)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("manifest prefetch failed to finish")
	}
	require.EqualValues(t, 0, active.Load())
	require.LessOrEqual(t, maxActive.Load(), int32(inspectSnapshotManifestPrefetch))
}

func TestInspectSnapshotManifestPrefetchStopsReadersOnEarlyExit(t *testing.T) {
	snapshots := make([]Snapshot, 30)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}

	var active atomic.Int32
	var yielded int
	streamSnapshotManifests(context.Background(), snapshots,
		func(ctx context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			active.Add(1)
			defer active.Add(-1)

			if snapshot.SnapshotID == 1 {
				manifest := iceberg.NewManifestFile(2, "first.avro", 10, 0, 1).Build()

				return snapshotManifestSet{all: []iceberg.ManifestFile{manifest}}, nil
			}
			<-ctx.Done()

			return snapshotManifestSet{}, ctx.Err()
		},
		func(_ iceberg.ManifestFile, _ error) bool {
			yielded++

			return false
		})
	require.Equal(t, 1, yielded)
	require.EqualValues(t, 0, active.Load(), "all workers must finish before return")
}

func TestInspectSnapshotManifestPrefetchReportsFirstOrderedError(t *testing.T) {
	failure := errors.New("manifest load failed")
	snapshots := []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}, {SnapshotID: 3}}
	var paths []string
	var gotErr error

	streamSnapshotManifests(context.Background(), snapshots,
		func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			if snapshot.SnapshotID == 2 {
				return snapshotManifestSet{}, failure
			}
			path := fmt.Sprintf("manifest-%d.avro", snapshot.SnapshotID)
			manifest := iceberg.NewManifestFile(2, path, 10, 0, 1).Build()

			return snapshotManifestSet{all: []iceberg.ManifestFile{manifest}}, nil
		},
		func(manifest iceberg.ManifestFile, err error) bool {
			if err != nil {
				gotErr = err

				return false
			}
			paths = append(paths, manifest.FilePath())

			return true
		})

	require.Equal(t, []string{"manifest-1.avro"}, paths)
	require.ErrorIs(t, gotErr, failure)
	require.ErrorContains(t, gotErr, "snapshot 2")
}

func BenchmarkInspectSnapshotManifestListPrefetch(b *testing.B) {
	const snapshotCount = 64
	snapshots := make([]Snapshot, snapshotCount)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}

	// Model an object-store manifest-list read with 1 ms of latency. This
	// measures the I/O overlap, not end-to-end metadata-table throughput.
	load := func(ctx context.Context, _ Snapshot) (snapshotManifestSet, error) {
		select {
		case <-time.After(time.Millisecond):
			return snapshotManifestSet{}, nil
		case <-ctx.Done():
			return snapshotManifestSet{}, ctx.Err()
		}
	}

	b.Run("sequential", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(snapshotCount, "lists/op")
		for b.Loop() {
			for _, snapshot := range snapshots {
				if _, err := load(b.Context(), snapshot); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("prefetch8", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(snapshotCount, "lists/op")
		for b.Loop() {
			streamSnapshotManifests(b.Context(), snapshots, load,
				func(_ iceberg.ManifestFile, err error) bool {
					if err != nil {
						b.Fatal(err)
					}

					return true
				})
		}
	})
}
