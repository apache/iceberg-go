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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSnapshotManifestLoader(snapshot Snapshot, fs iceio.IO) snapshotManifestLoader {
	return func(ctx context.Context) (snapshotManifestSet, error) {
		return readSnapshotManifestSet(ctx, snapshot, testFSF(fs))
	}
}

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

	snapshot := Snapshot{SnapshotID: 1, ManifestList: listPath}
	set, err := cache.get(context.Background(), snapshot, testSnapshotManifestLoader(snapshot, fs))
	require.NoError(t, err)
	require.Len(t, set.allManifests(), 2)
	require.Len(t, set.dataManifests(), 1)

	all := set.allManifests()
	all[0] = nil
	assert.Equal(t, "data.avro", set.allManifests()[0].FilePath())
	dataManifests := set.dataManifests()
	assert.Equal(t, "data.avro", dataManifests[0].FilePath())
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
	waitersStarted := make(chan struct{}, callers)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })
	for range callers {
		ctx := &countingContext{Context: t.Context(), entered: waitersStarted}
		go func() {
			_, err := cache.get(ctx, snapshot, testSnapshotManifestLoader(snapshot, fs))
			errs <- err
		}()
	}

	select {
	case <-fs.started:
	case <-time.After(time.Second):
		t.Fatal("manifest-list read did not start")
	}
	for range callers - 1 {
		select {
		case <-waitersStarted:
		case <-time.After(time.Second):
			t.Fatal("all concurrent callers did not wait on the shared read")
		}
	}
	release.Do(func() { close(fs.release) })

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

	_, err := cache.get(context.Background(), snapshot, testSnapshotManifestLoader(snapshot, fs))
	require.Error(t, err)

	var list bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{}))
	require.NoError(t, fs.WriteFile(listPath, list.Bytes()))
	_, err = cache.get(context.Background(), snapshot, testSnapshotManifestLoader(snapshot, fs))
	require.NoError(t, err)
}

func TestSnapshotManifestCacheBoundsCompletedEntries(t *testing.T) {
	fs := newTrackingCallsIO()
	cache := newSnapshotManifestCache()
	sequenceNumber := int64(1)
	snapshots := make([]Snapshot, snapshotManifestCacheSize+1)
	for i := range snapshots {
		var list bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &list, int64(i), nil, &sequenceNumber, 0, nil))
		path := fmt.Sprintf("mem://snapshot-manifest-cache/bounded-%d.avro", i)
		require.NoError(t, fs.WriteFile(path, list.Bytes()))
		snapshots[i] = Snapshot{SnapshotID: int64(i), ManifestList: path}
	}

	for i := range snapshotManifestCacheSize {
		_, err := cache.get(context.Background(), snapshots[i], testSnapshotManifestLoader(snapshots[i], fs))
		require.NoError(t, err)
	}
	_, err := cache.get(context.Background(), snapshots[0], testSnapshotManifestLoader(snapshots[0], fs))
	require.NoError(t, err)
	_, err = cache.get(context.Background(), snapshots[snapshotManifestCacheSize], testSnapshotManifestLoader(snapshots[snapshotManifestCacheSize], fs))
	require.NoError(t, err)

	assert.Equal(t, 1, fs.openCount[snapshots[0].ManifestList])
	assert.Equal(t, 1, fs.openCount[snapshots[1].ManifestList])
	_, err = cache.get(context.Background(), snapshots[0], testSnapshotManifestLoader(snapshots[0], fs))
	require.NoError(t, err)
	_, err = cache.get(context.Background(), snapshots[1], testSnapshotManifestLoader(snapshots[1], fs))
	require.NoError(t, err)
	assert.Equal(t, 1, fs.openCount[snapshots[0].ManifestList])
	assert.Equal(t, 2, fs.openCount[snapshots[1].ManifestList])
	assert.LessOrEqual(t, cache.complete.Len(), snapshotManifestCacheSize)
}

func TestSnapshotManifestCacheBoundsManifestDescriptors(t *testing.T) {
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: "mem://snapshot-manifest-cache/large.avro"}
	largeSet := snapshotManifestSet{
		all: make([]iceberg.ManifestFile, snapshotManifestCacheManifestLimit+1),
	}

	_, err := cache.get(context.Background(), snapshot, func(context.Context) (snapshotManifestSet, error) {
		return largeSet, nil
	})
	require.NoError(t, err)
	assert.Zero(t, cache.complete.Len(), "an oversized manifest set should not be retained")
	assert.Zero(t, cache.completeManifestCount)
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

type notifyingContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

type countingContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *notifyingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })

	return c.Context.Done()
}

func (c *countingContext) Done() <-chan struct{} {
	c.once.Do(func() { c.entered <- struct{}{} })

	return c.Context.Done()
}

type producerContextKey struct{}

func TestSnapshotManifestCacheProducerCancellationDoesNotCancelSharedRead(t *testing.T) {
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: "mem://snapshot-manifest-cache/detached-context.avro"}
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	load := func(loadCtx context.Context) (snapshotManifestSet, error) {
		close(started)
		select {
		case <-release:
			return snapshotManifestSet{}, nil
		case <-loadCtx.Done():
			return snapshotManifestSet{}, loadCtx.Err()
		}
	}

	producerDone := make(chan error, 1)
	go func() {
		_, err := cache.get(ctx, snapshot, load)
		producerDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manifest-list read did not start")
	}
	waiterEntered := make(chan struct{}, 1)
	waiterDone := make(chan error, 1)
	waiterCtx := &countingContext{Context: t.Context(), entered: waiterEntered}
	go func() {
		_, err := cache.get(waiterCtx, snapshot, load)
		waiterDone <- err
	}()
	select {
	case <-waiterEntered:
	case <-time.After(time.Second):
		t.Fatal("waiter did not start waiting on the shared read")
	}
	cancel()

	select {
	case err := <-producerDone:
		t.Fatalf("producer stopped after its context was canceled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-producerDone)
	require.NoError(t, <-waiterDone)
}

func TestReadSnapshotManifestSetKeepsCompletedReadAfterCancellation(t *testing.T) {
	fs := iceio.NewMemFS()
	var list bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0, nil))
	const listPath = "mem://snapshot-manifest-cache/late-cancel.avro"
	require.NoError(t, fs.WriteFile(listPath, list.Bytes()))

	snapshot := Snapshot{SnapshotID: 1, ManifestList: listPath}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	set, err := readSnapshotManifestSet(ctx, snapshot, func(context.Context) (iceio.IO, error) {
		cancel()

		return fs, nil
	})

	require.NoError(t, err)
	assert.Empty(t, set.allManifests())
}

func TestSnapshotManifestCacheCanceledWaiterDoesNotCancelSharedRead(t *testing.T) {
	const listPath = "mem://snapshot-manifest-cache/canceled-waiter.avro"
	base := iceio.NewMemFS()
	var list bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0, nil))
	require.NoError(t, base.WriteFile(listPath, list.Bytes()))
	fs := &blockingSnapshotManifestIO{
		IO: base, blockedPath: listPath, started: make(chan struct{}),
		release: make(chan struct{}), opens: make(map[string]int),
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: listPath}
	first := make(chan error, 1)
	go func() {
		_, err := cache.get(t.Context(), snapshot, testSnapshotManifestLoader(snapshot, fs))
		first <- err
	}()
	select {
	case <-fs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("manifest-list read did not start")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := cache.get(ctx, snapshot, testSnapshotManifestLoader(snapshot, fs))
	require.ErrorIs(t, err, context.Canceled)
	retry := make(chan error, 1)
	go func() {
		_, err := cache.get(t.Context(), snapshot, testSnapshotManifestLoader(snapshot, fs))
		retry <- err
	}()
	select {
	case err := <-retry:
		t.Fatalf("retry returned before the shared producer finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	release.Do(func() { close(fs.release) })
	require.NoError(t, <-first)
	require.NoError(t, <-retry)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	assert.Equal(t, 1, fs.opens[listPath])
}

func TestSnapshotManifestCachePanickingProducerUnblocksWaiters(t *testing.T) {
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: "mem://snapshot-manifest-cache/panic.avro"}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	load := func(context.Context) (snapshotManifestSet, error) {
		close(started)
		<-release
		panic("manifest-list producer failed")
	}

	producerDone := make(chan any, 1)
	go func() {
		defer func() { producerDone <- recover() }()
		_, _ = cache.get(t.Context(), snapshot, load)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manifest-list producer did not start")
	}

	waiterDone := make(chan error, 1)
	waiterCtx := &notifyingContext{Context: t.Context(), entered: make(chan struct{})}
	go func() {
		_, err := cache.get(waiterCtx, snapshot, load)
		waiterDone <- err
	}()
	select {
	case <-waiterCtx.entered:
	case <-time.After(time.Second):
		t.Fatal("waiter did not start waiting on the producer")
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-waiterDone:
		require.Error(t, err)
		require.ErrorContains(t, err, "panic while reading snapshot 1 manifest list")
	case <-time.After(time.Second):
		t.Fatal("waiter remained blocked after producer panic")
	}
	assert.Equal(t, "manifest-list producer failed", <-producerDone)

	_, err := cache.get(t.Context(), snapshot, func(context.Context) (snapshotManifestSet, error) {
		return snapshotManifestSet{}, nil
	})
	require.NoError(t, err)
}

func TestSnapshotManifestCacheUsesDetachedProducerContext(t *testing.T) {
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: "mem://snapshot-manifest-cache/context.avro"}
	ctx := context.WithValue(t.Context(), producerContextKey{}, "producer")
	seen := make(chan context.Context, 1)

	_, err := cache.get(ctx, snapshot, func(loadCtx context.Context) (snapshotManifestSet, error) {
		seen <- loadCtx

		return snapshotManifestSet{}, nil
	})
	require.NoError(t, err)
	loadCtx := <-seen
	assert.Equal(t, "producer", loadCtx.Value(producerContextKey{}))
	assert.Nil(t, loadCtx.Done())
}

func TestSnapshotManifestCacheDoesNotClassifyUnknownContentAsData(t *testing.T) {
	unknown := iceberg.NewManifestFile(2, "unknown.avro", 1, 0, 1).
		Content(iceberg.ManifestContent(2)).
		Build()
	set := newSnapshotManifestSet([]iceberg.ManifestFile{
		iceberg.NewManifestFile(2, "data.avro", 1, 0, 1).Build(),
		iceberg.NewManifestFile(2, "delete.avro", 1, 0, 1).
			Content(iceberg.ManifestContentDeletes).
			Build(),
		unknown,
	})

	assert.Len(t, set.allManifests(), 3)
	assert.Len(t, set.dataManifests(), 1)
}

func TestTransactionScanUsesAnIsolatedSnapshotManifestCache(t *testing.T) {
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://snapshot-manifest-cache-transaction", nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "snapshot-manifest-cache-transaction"}, meta, "metadata.json",
		testFSF(iceio.NewMemFS()), nil)
	txn := tbl.NewTransaction()

	scan, err := txn.Scan()
	require.NoError(t, err)
	assert.NotSame(t, tbl.manifestCache, scan.manifestCache)
}
