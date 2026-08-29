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
	"slices"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// snapshotManifestSet keeps the complete manifest list and its content
// partitions together. Manifest descriptors are immutable for an Iceberg
// snapshot, so a table can safely share this decoded result across scans.
type snapshotManifestSet struct {
	all     []iceberg.ManifestFile
	data    []iceberg.ManifestFile
	deletes []iceberg.ManifestFile
}

func newSnapshotManifestSet(manifests []iceberg.ManifestFile) snapshotManifestSet {
	set := snapshotManifestSet{all: slices.Clone(manifests)}
	if len(manifests) == 0 {
		return set
	}

	set.data = make([]iceberg.ManifestFile, 0, len(manifests))
	set.deletes = make([]iceberg.ManifestFile, 0, len(manifests))
	for _, manifest := range set.all {
		if manifest.ManifestContent() == iceberg.ManifestContentDeletes {
			set.deletes = append(set.deletes, manifest)
		} else {
			set.data = append(set.data, manifest)
		}
	}

	return set
}

func (s snapshotManifestSet) allManifests() []iceberg.ManifestFile {
	return slices.Clone(s.all)
}

func (s snapshotManifestSet) dataManifests() []iceberg.ManifestFile {
	return slices.Clone(s.data)
}

func (s snapshotManifestSet) deleteManifests() []iceberg.ManifestFile {
	return slices.Clone(s.deletes)
}

type snapshotManifestCacheKey struct {
	snapshotID              int64
	manifestList            string
	hasEmbeddedSources      bool
	embeddedManifestSources string
}

func snapshotManifestCacheKeyFor(snapshot Snapshot) snapshotManifestCacheKey {
	var (
		hasEmbeddedSources      bool
		embeddedManifestSources string
	)
	if snapshot.ManifestList == "" {
		hasEmbeddedSources = snapshot.ManifestLocations != nil
		embeddedManifestSources = strings.Join(snapshot.ManifestLocations, "\x00")
	}

	return snapshotManifestCacheKey{
		snapshotID:              snapshot.SnapshotID,
		manifestList:            snapshot.ManifestList,
		hasEmbeddedSources:      hasEmbeddedSources,
		embeddedManifestSources: embeddedManifestSources,
	}
}

type snapshotManifestCacheEntry struct {
	ready     chan struct{}
	manifests snapshotManifestSet
	err       error
}

// snapshotManifestCache memoizes successful manifest-list reads and shares an
// in-flight read with concurrent scans. Failed reads are removed so a
// transient object-store error does not poison the cache.
type snapshotManifestCache struct {
	mu      sync.Mutex
	entries map[snapshotManifestCacheKey]*snapshotManifestCacheEntry
}

func newSnapshotManifestCache() *snapshotManifestCache {
	return &snapshotManifestCache{
		entries: make(map[snapshotManifestCacheKey]*snapshotManifestCacheEntry),
	}
}

func (c *snapshotManifestCache) get(
	ctx context.Context,
	snapshot Snapshot,
	fio iceio.IO,
) (snapshotManifestSet, error) {
	if c == nil {
		manifests, err := snapshot.Manifests(fio)

		return newSnapshotManifestSet(manifests), err
	}

	key := snapshotManifestCacheKeyFor(snapshot)
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok {
		c.mu.Unlock()

		select {
		case <-entry.ready:
			return entry.manifests, entry.err
		default:
			select {
			case <-entry.ready:
				return entry.manifests, entry.err
			case <-ctx.Done():
				return snapshotManifestSet{}, ctx.Err()
			}
		}
	}

	entry := &snapshotManifestCacheEntry{ready: make(chan struct{})}
	c.entries[key] = entry
	c.mu.Unlock()

	manifests, err := snapshot.Manifests(fio)
	value := newSnapshotManifestSet(manifests)

	c.mu.Lock()
	entry.manifests = value
	entry.err = err
	if err != nil {
		delete(c.entries, key)
	}
	close(entry.ready)
	c.mu.Unlock()

	return value, err
}
