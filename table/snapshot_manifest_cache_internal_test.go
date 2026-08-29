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
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableScansReuseSnapshotManifestList(t *testing.T) {
	fs := newTrackingCallsIO()
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://snapshot-manifest-cache", iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	const snapshotID = int64(1)
	manifestPath := "mem://snapshot-manifest-cache/metadata/manifest.avro"
	manifest := writeManifest(t, fs.trackingIO, snapshotID, 1, manifestPath,
		"mem://snapshot-manifest-cache/data/file.parquet")
	manifestListPath := "mem://snapshot-manifest-cache/metadata/snap.avro"
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

	tbl := New(Identifier{"db", "snapshot-manifest-cache"}, built, "metadata.json", testFSF(fs), nil)
	for range 2 {
		tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(context.Background())
		require.NoError(t, err)
		require.Len(t, tasks, 1)
	}

	assert.Equal(t, 1, fs.openCount[manifestListPath], "manifest list should be decoded once across scans")
	assert.Equal(t, 2, fs.openCount[manifestPath], "manifest entries still need to be read for each plan")
}

func TestSnapshotManifestCacheSeparatesContentAndProtectsSlices(t *testing.T) {
	data := iceberg.NewManifestFile(2, "data.avro", 10, 0, 1).Build()
	deleteManifest := iceberg.NewManifestFile(2, "delete.avro", 20, 0, 1).
		Content(iceberg.ManifestContentDeletes).
		Build()
	cache := newSnapshotManifestCache()

	fs := iceio.NewMemFS()
	var list bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{data, deleteManifest}))
	const listPath = "mem://snapshot-manifest-cache/separate.avro"
	require.NoError(t, fs.WriteFile(listPath, list.Bytes()))

	set, err := cache.get(context.Background(), Snapshot{SnapshotID: 1, ManifestList: listPath}, fs)
	require.NoError(t, err)
	require.Len(t, set.allManifests(), 2)
	require.Len(t, set.dataManifests(), 1)
	require.Len(t, set.deleteManifests(), 1)

	all := set.allManifests()
	all[0] = nil
	assert.Equal(t, "data.avro", set.allManifests()[0].FilePath())
	dataManifests := set.dataManifests()
	deleteManifests := set.deleteManifests()
	assert.Equal(t, "data.avro", dataManifests[0].FilePath())
	assert.Equal(t, "delete.avro", deleteManifests[0].FilePath())
}

func TestSnapshotManifestCacheSharesInFlightRead(t *testing.T) {
	const listPath = "mem://snapshot-manifest-cache/concurrent.avro"
	base := iceio.NewMemFS()
	var list bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{}))
	require.NoError(t, base.WriteFile(listPath, list.Bytes()))

	fs := &blockingSnapshotManifestIO{
		IO:          base,
		blockedPath: listPath,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
		opens:       make(map[string]int),
	}
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: listPath}
	const callers = 16
	errs := make(chan error, callers)
	for range callers {
		go func() {
			_, err := cache.get(context.Background(), snapshot, fs)
			errs <- err
		}()
	}

	select {
	case <-fs.started:
	case <-time.After(time.Second):
		t.Fatal("manifest-list read did not start")
	}
	close(fs.release)

	for range callers {
		require.NoError(t, <-errs)
	}

	fs.mu.Lock()
	openCount := fs.opens[listPath]
	fs.mu.Unlock()
	assert.Equal(t, 1, openCount, "concurrent scans should share the first manifest-list read")
}

func TestSnapshotManifestCacheRetriesFailedRead(t *testing.T) {
	const listPath = "mem://snapshot-manifest-cache/retry.avro"
	fs := iceio.NewMemFS()
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: listPath}

	_, err := cache.get(context.Background(), snapshot, fs)
	require.Error(t, err)

	var list bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{}))
	require.NoError(t, fs.WriteFile(listPath, list.Bytes()))
	_, err = cache.get(context.Background(), snapshot, fs)
	require.NoError(t, err)
}

func TestTableRefreshReplacesSnapshotManifestCache(t *testing.T) {
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://snapshot-manifest-cache-refresh", nil)
	require.NoError(t, err)
	old := New(Identifier{"db", "snapshot-manifest-cache-refresh"}, meta, "old.json",
		testFSF(iceio.NewMemFS()), nil)
	fresh := New(Identifier{"db", "snapshot-manifest-cache-refresh"}, meta, "new.json",
		testFSF(iceio.NewMemFS()), nil)
	old.cat = &snapshotManifestRefreshCatalog{fresh: fresh}
	oldCache := old.manifestCache

	require.NoError(t, old.Refresh(context.Background()))
	assert.NotSame(t, oldCache, old.manifestCache)
}

type snapshotManifestRefreshCatalog struct {
	fresh *Table
}

func (c *snapshotManifestRefreshCatalog) LoadTable(context.Context, Identifier) (*Table, error) {
	return c.fresh, nil
}

func (c *snapshotManifestRefreshCatalog) CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error) {
	return nil, "", nil
}

type blockingSnapshotManifestIO struct {
	iceio.IO
	blockedPath string
	started     chan struct{}
	release     chan struct{}
	once        sync.Once

	mu    sync.Mutex
	opens map[string]int
}

func (fs *blockingSnapshotManifestIO) Open(name string) (iceio.File, error) {
	if name == fs.blockedPath {
		fs.once.Do(func() { close(fs.started) })
		<-fs.release
	}

	fs.mu.Lock()
	fs.opens[name]++
	fs.mu.Unlock()

	return fs.IO.Open(name)
}
