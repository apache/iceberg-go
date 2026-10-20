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

	readCtx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	// Cancel first, then wait for all readers before returning. This also
	// handles a consumer that stops iterating partway through a snapshot.
	defer workers.Wait()
	defer cancel()

	window := min(len(snapshots), inspectSnapshotManifestPrefetch)
	results := make([]chan inspectSnapshotManifestRead, window)
	for i := range results {
		// Buffer one result so a cancelled worker never blocks sending it.
		results[i] = make(chan inspectSnapshotManifestRead, 1)
	}

	startRead := func(index int) {
		workers.Add(1)
		go func() {
			defer workers.Done()

			set, err := load(readCtx, snapshots[index])
			results[index%window] <- inspectSnapshotManifestRead{set: set, err: err}
		}()
	}

	for i := range window {
		startRead(i)
	}

	for i, snapshot := range snapshots {
		if err := readCtx.Err(); err != nil {
			yield(nil, err)

			return
		}

		var result inspectSnapshotManifestRead
		select {
		case <-readCtx.Done():
			yield(nil, readCtx.Err())

			return
		case result = <-results[i%window]:
		}

		if result.err != nil {
			yield(nil, fmt.Errorf("read snapshot %d manifests: %w", snapshot.SnapshotID, result.err))

			return
		}

		// Reuse the completed slot before yielding. The consumer may spend a
		// long time decoding these manifests while the next list is fetched.
		if next := i + window; next < len(snapshots) {
			startRead(next)
		}

		for _, manifest := range result.set.borrowAllManifests() {
			if !yield(manifest, nil) {
				return
			}
		}
	}
}
