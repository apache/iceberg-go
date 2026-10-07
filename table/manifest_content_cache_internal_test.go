// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableScansReuseManifestContent(t *testing.T) {
	fs := newTrackingCallsIO()
	tbl, manifestListPath, manifestPath := newManifestContentCacheTestTable(t, fs,
		iceberg.Properties{IOManifestCacheEnabledKey: "true"})

	for range 2 {
		tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 1)
	}

	assert.Equal(t, 1, fs.openCount[manifestListPath], "manifest list should use the decoded list cache")
	assert.Equal(t, 1, fs.openCount[manifestPath], "manifest bytes should be read once across scans")
}

func TestManifestContentCacheSkipsOversizedManifest(t *testing.T) {
	fs := newTrackingCallsIO()
	tbl, _, manifestPath := newManifestContentCacheTestTable(t, fs, iceberg.Properties{
		IOManifestCacheEnabledKey:          "true",
		IOManifestCacheMaxContentLengthKey: "1",
	})

	for range 2 {
		tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 1)
	}

	assert.Equal(t, 2, fs.openCount[manifestPath], "oversized manifests must bypass the content cache")
}

func TestManifestContentCacheEvictsLeastRecentlyUsed(t *testing.T) {
	fs := newTrackingCallsIO()
	const (
		firstPath  = "mem://manifest-content-cache/first.avro"
		secondPath = "mem://manifest-content-cache/second.avro"
	)
	firstBytes := []byte("aaaa")
	secondBytes := []byte("bbbb")
	require.NoError(t, fs.WriteFile(firstPath, firstBytes))
	require.NoError(t, fs.WriteFile(secondPath, secondBytes))

	first := iceberg.NewManifestFile(2, firstPath, int64(len(firstBytes)), 0, 1).Build()
	second := iceberg.NewManifestFile(2, secondPath, int64(len(secondBytes)), 0, 1).Build()
	cache := newManifestContentCache(0, 4, 4)

	readCachedManifest(t, cache, fs, first)
	readCachedManifest(t, cache, fs, second)
	readCachedManifest(t, cache, fs, first)

	assert.Equal(t, 2, fs.openCount[firstPath])
	assert.Equal(t, 1, fs.openCount[secondPath])
}

func TestManifestContentCacheSharesInFlightRead(t *testing.T) {
	const manifestPath = "mem://manifest-content-cache/shared.avro"
	content := []byte("manifest")
	base := iceio.NewMemFS()
	require.NoError(t, base.WriteFile(manifestPath, content))

	fs := &blockingSnapshotManifestIO{
		IO:          base,
		blockedPath: manifestPath,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
		opens:       make(map[string]int),
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })

	manifest := iceberg.NewManifestFile(2, manifestPath, int64(len(content)), 0, 1).Build()
	cache := newManifestContentCache(0, 1<<20, 1<<20)
	const callers = 16
	errs := make(chan error, callers)
	for range callers {
		go func() {
			file, err := cache.open(t.Context(), fs, manifest)
			if err == nil {
				_, err = io.ReadAll(file)
				if closeErr := file.Close(); err == nil {
					err = closeErr
				}
			}
			errs <- err
		}()
	}

	select {
	case <-fs.started:
	case <-time.After(time.Second):
		t.Fatal("manifest read did not start")
	}
	release.Do(func() { close(fs.release) })

	for range callers {
		require.NoError(t, <-errs)
	}

	fs.mu.Lock()
	openCount := fs.opens[manifestPath]
	fs.mu.Unlock()
	assert.Equal(t, 1, openCount, "concurrent misses should share one backend read")
}

func newManifestContentCacheTestTable(
	t testing.TB,
	fs *trackingCallsIO,
	cacheProperties iceberg.Properties,
) (*Table, string, string) {
	t.Helper()

	const tableLocation = "mem://manifest-content-cache"
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
		tableLocation, iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	const snapshotID = int64(1)
	manifestPath := tableLocation + "/metadata/manifest.avro"
	manifest := writeManifest(t, fs.trackingIO, snapshotID, 1, manifestPath,
		tableLocation+"/data/file.parquet")
	manifestListPath := tableLocation + "/metadata/snap.avro"
	writeManifestList(t, fs.trackingIO, snapshotID, manifestListPath, []iceberg.ManifestFile{manifest})

	schemaID := meta.CurrentSchema().ID
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       &schemaID,
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	built, err := builder.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "manifest-content-cache"}, built, tableLocation+"/metadata/metadata.json",
		testFSF(fs), nil, WithSavedConfig(cacheProperties))

	return tbl, manifestListPath, manifestPath
}

func readCachedManifest(
	t testing.TB,
	cache *manifestContentCache,
	fs iceio.IO,
	manifest iceberg.ManifestFile,
) {
	t.Helper()

	file, err := cache.open(context.Background(), fs, manifest)
	require.NoError(t, err)
	_, err = io.ReadAll(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}
