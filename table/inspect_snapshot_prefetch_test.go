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

	ctx := t.Context()
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
		streamSnapshotManifests(ctx, snapshots, nil,
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
				manifest := iceberg.NewManifestBuilder(2, path, 10, 0, 1).Build()

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

func TestInspectSnapshotManifestPrefetchUsesCachedSets(t *testing.T) {
	snapshots := make([]Snapshot, 12)
	cachedSets := make(map[int64]snapshotManifestSet)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
		if i%2 == 0 {
			path := fmt.Sprintf("manifest-%d.avro", i+1)
			manifest := iceberg.NewManifestBuilder(2, path, 10, 0, 1).Build()
			cachedSets[int64(i+1)] = snapshotManifestSet{all: []iceberg.ManifestFile{manifest}}
		}
	}

	var loaded atomic.Int32
	var paths []string
	streamSnapshotManifests(context.Background(), snapshots,
		func(snapshot Snapshot) (snapshotManifestSet, bool) {
			set, ok := cachedSets[snapshot.SnapshotID]

			return set, ok
		},
		func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			loaded.Add(1)
			path := fmt.Sprintf("manifest-%d.avro", snapshot.SnapshotID)
			manifest := iceberg.NewManifestBuilder(2, path, 10, 0, 1).Build()

			return snapshotManifestSet{all: []iceberg.ManifestFile{manifest}}, nil
		},
		func(manifest iceberg.ManifestFile, err error) bool {
			require.NoError(t, err)
			paths = append(paths, manifest.FilePath())

			return true
		})

	require.EqualValues(t, len(snapshots)/2, loaded.Load())
	require.Len(t, paths, len(snapshots))
	for i, path := range paths {
		require.Equal(t, fmt.Sprintf("manifest-%d.avro", i+1), path)
	}
}

func TestInspectSnapshotManifestPrefetchDeduplicatesPaths(t *testing.T) {
	snapshots := []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}, {SnapshotID: 3}}
	manifests := [][]string{{"a.avro", "shared.avro"}, {"shared.avro", "b.avro"}, {"a.avro", "c.avro"}}
	var paths []string
	streamSnapshotManifests(context.Background(), snapshots, nil,
		func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			set := snapshotManifestSet{}
			for _, path := range manifests[snapshot.SnapshotID-1] {
				set.all = append(set.all, iceberg.NewManifestBuilder(2, path, 10, 0, 1).Build())
			}

			return set, nil
		},
		func(manifest iceberg.ManifestFile, err error) bool {
			require.NoError(t, err)
			paths = append(paths, manifest.FilePath())

			return true
		})

	require.Equal(t, []string{"a.avro", "shared.avro", "b.avro", "c.avro"}, paths)
}

func TestInspectSnapshotManifestPrefetchStopsReadersOnEarlyExit(t *testing.T) {
	snapshots := make([]Snapshot, 30)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}

	var active atomic.Int32
	var yielded int
	streamSnapshotManifests(context.Background(), snapshots, nil,
		func(ctx context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			active.Add(1)
			defer active.Add(-1)

			if snapshot.SnapshotID == 1 {
				manifest := iceberg.NewManifestBuilder(2, "first.avro", 10, 0, 1).Build()

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

	streamSnapshotManifests(context.Background(), snapshots, nil,
		func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			if snapshot.SnapshotID == 2 {
				return snapshotManifestSet{}, failure
			}
			path := fmt.Sprintf("manifest-%d.avro", snapshot.SnapshotID)
			manifest := iceberg.NewManifestBuilder(2, path, 10, 0, 1).Build()

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
			streamSnapshotManifests(b.Context(), snapshots, nil, load,
				func(_ iceberg.ManifestFile, err error) bool {
					if err != nil {
						b.Fatal(err)
					}

					return true
				})
		}
	})
}

func BenchmarkInspectSnapshotManifestListCacheHit(b *testing.B) {
	cache := newSnapshotManifestCache()

	for _, count := range []int{1, 64} {
		b.Run(fmt.Sprintf("snapshots=%d", count), func(b *testing.B) {
			snapshots := make([]Snapshot, count)
			sets := make([]snapshotManifestSet, count)
			for i := range snapshots {
				snapshots[i].SnapshotID = int64(i + 1)
				manifest := iceberg.NewManifestBuilder(2, fmt.Sprintf("manifest-%d.avro", i+1), 10, 0, 1).Build()
				sets[i] = snapshotManifestSet{all: []iceberg.ManifestFile{manifest}}
				_, err := cache.get(context.Background(), snapshots[i], func(context.Context) (snapshotManifestSet, error) {
					return sets[i], nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}

			load := func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
				return sets[snapshot.SnapshotID-1], nil
			}
			b.Run("sequential", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var yielded int
					seen := make(map[string]struct{})
					for _, snapshot := range snapshots {
						value, err := cache.get(b.Context(), snapshot, func(context.Context) (snapshotManifestSet, error) {
							return sets[snapshot.SnapshotID-1], nil
						})
						if err != nil {
							b.Fatal(err)
						}
						for _, manifest := range value.borrowAllManifests() {
							path := manifest.FilePath()
							if _, ok := seen[path]; ok {
								continue
							}
							seen[path] = struct{}{}
							yielded++
						}
					}
					if yielded != count {
						b.Fatal(yielded)
					}
				}
			})
			b.Run("prefetch", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var yielded int
					streamSnapshotManifests(b.Context(), snapshots, cache.tryGetComplete, load,
						func(_ iceberg.ManifestFile, err error) bool {
							if err != nil {
								b.Fatal(err)
							}
							yielded++

							return true
						})
					if yielded != count {
						b.Fatal(yielded)
					}
				}
			})
		})
	}
}
