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
	"fmt"
	"sync"

	"github.com/apache/iceberg-go"
)

const inspectSnapshotManifestPrefetch = 8

type inspectSnapshotManifestRead struct {
	set snapshotManifestSet
	err error
}

// streamSnapshotManifests loads historical manifest lists concurrently while
// yielding them in snapshot order. Only a small window of lists can be held
// by in-flight reads, even for tables with many snapshots.
func streamSnapshotManifests(
	ctx context.Context,
	snapshots []Snapshot,
	cached func(Snapshot) (snapshotManifestSet, bool),
	load func(context.Context, Snapshot) (snapshotManifestSet, error),
	yield func(iceberg.ManifestFile, error) bool,
) {
	if len(snapshots) == 0 {
		return
	}
	if err := ctx.Err(); err != nil {
		yield(nil, err)

		return
	}
	seen := make(map[string]struct{})
	if len(snapshots) == 1 {
		set, ok := snapshotManifestSet{}, false
		if cached != nil {
			set, ok = cached(snapshots[0])
		}
		if !ok {
			var err error
			set, err = load(ctx, snapshots[0])
			if err != nil {
				yield(nil, fmt.Errorf("read snapshot %d manifests: %w", snapshots[0].SnapshotID, err))

				return
			}
		}
		for _, manifest := range set.borrowAllManifests() {
			path := manifest.FilePath()
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}
			if !yield(manifest, nil) {
				return
			}
		}

		return
	}
	if cached != nil {
		start := 0
		for start < len(snapshots) {
			count := min(inspectSnapshotManifestPrefetch, len(snapshots)-start)
			var sets [inspectSnapshotManifestPrefetch]snapshotManifestSet
			allCached := true
			for i := range count {
				set, ok := cached(snapshots[start+i])
				if !ok {
					allCached = false

					break
				}
				sets[i] = set
			}
			if !allCached {
				break
			}

			for i := range count {
				if err := ctx.Err(); err != nil {
					yield(nil, err)

					return
				}
				for _, manifest := range sets[i].borrowAllManifests() {
					path := manifest.FilePath()
					if _, ok := seen[path]; ok {
						continue
					}
					seen[path] = struct{}{}
					if !yield(manifest, nil) {
						return
					}
				}
			}
			start += count
		}
		if start == len(snapshots) {
			return
		}
		// The first cache miss starts the concurrent path. Recheck this small
		// window there so cached and newly loaded results share one ordered path.
		snapshots = snapshots[start:]
	}

	readCtx := ctx
	var cancel context.CancelFunc
	var workers sync.WaitGroup
	// Cancel first, then wait for all readers before returning. This also
	// handles a consumer that stops iterating partway through a snapshot.
	defer func() {
		if cancel != nil {
			cancel()
		}
		workers.Wait()
	}()

	window := min(len(snapshots), inspectSnapshotManifestPrefetch)
	results := make([]chan inspectSnapshotManifestRead, window)
	ready := make([]bool, window)
	readyResults := make([]inspectSnapshotManifestRead, window)

	startRead := func(index int) {
		if cancel == nil {
			readCtx, cancel = context.WithCancel(ctx)
		}
		slot := index % window
		if results[slot] == nil {
			// Buffer one result so a cancelled worker never blocks sending it.
			results[slot] = make(chan inspectSnapshotManifestRead, 1)
		}
		ready[slot] = false
		workers.Go(func() {
			set, err := load(readCtx, snapshots[index])
			results[slot] <- inspectSnapshotManifestRead{set: set, err: err}
		})
	}

	for i := range window {
		if cached != nil {
			if set, ok := cached(snapshots[i]); ok {
				ready[i] = true
				readyResults[i] = inspectSnapshotManifestRead{set: set}

				continue
			}
		}
		startRead(i)
	}

	for i, snapshot := range snapshots {
		if err := readCtx.Err(); err != nil {
			yield(nil, err)

			return
		}

		slot := i % window
		var result inspectSnapshotManifestRead
		if ready[slot] {
			result = readyResults[slot]
			ready[slot] = false
		} else {
			select {
			case <-readCtx.Done():
				yield(nil, readCtx.Err())

				return
			case result = <-results[slot]:
			}
		}

		if result.err != nil {
			yield(nil, fmt.Errorf("read snapshot %d manifests: %w", snapshot.SnapshotID, result.err))

			return
		}

		// Reuse the completed slot before yielding. The consumer may spend a
		// long time decoding these manifests while the next list is fetched.
		if next := i + window; next < len(snapshots) {
			if cached != nil {
				if set, ok := cached(snapshots[next]); ok {
					ready[slot] = true
					readyResults[slot] = inspectSnapshotManifestRead{set: set}
				} else {
					startRead(next)
				}
			} else {
				startRead(next)
			}
		}

		for _, manifest := range result.set.borrowAllManifests() {
			path := manifest.FilePath()
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}
			if !yield(manifest, nil) {
				return
			}
		}
	}
}
