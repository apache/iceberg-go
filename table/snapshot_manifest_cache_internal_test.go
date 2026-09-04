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
	"errors"
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

func TestSnapshotManifestCacheRetriesAfterProducerCancellation(t *testing.T) {
	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: "mem://snapshot-manifest-cache/detached-context.avro"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
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
		_, err := cache.get(waiterCtx, snapshot, func(context.Context) (snapshotManifestSet, error) {
			return snapshotManifestSet{}, nil
		})
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
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("producer did not stop after its context was canceled")
	}

	select {
	case err := <-waiterDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("healthy waiter did not retry the canceled read")
	}
}

func TestPlanFilesSnapshotManifestFactoryHonorsDeadline(t *testing.T) {
	scan, _ := scanWithManifestCount(t, 1)
	scan.manifestCache = newSnapshotManifestCache()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	scan.ioF = func(ctx context.Context) (iceio.IO, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return nil, errors.New("released stalled factory")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := scan.PlanFiles(ctx)
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("manifest IO factory outlived the scan deadline")
	}
}

func TestSnapshotManifestCacheKeepsCompletedReadAfterCancellation(t *testing.T) {
	fs := iceio.NewMemFS()
	var list bytes.Buffer
	sequenceNumber := int64(1)
	manifest := iceberg.NewManifestFile(2, "data.avro", 10, 0, 1).Build()
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0, []iceberg.ManifestFile{manifest}))
	const listPath = "mem://snapshot-manifest-cache/late-cancel.avro"
	require.NoError(t, fs.WriteFile(listPath, list.Bytes()))

	cache := newSnapshotManifestCache()
	snapshot := Snapshot{SnapshotID: 1, ManifestList: listPath}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	set, err := cache.get(ctx, snapshot, func(loadCtx context.Context) (snapshotManifestSet, error) {
		return readSnapshotManifestSet(loadCtx, snapshot, func(context.Context) (iceio.IO, error) {
			cancel()

			return fs, nil
		})
	})
	require.NoError(t, err)
	require.Len(t, set.allManifests(), 1)

	cached, err := cache.get(t.Context(), snapshot, func(context.Context) (snapshotManifestSet, error) {
		t.Error("completed manifest list was discarded after cancellation")

		return snapshotManifestSet{}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, set, cached)
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
	defer cancel()
	waiterCtx := &notifyingContext{Context: ctx, entered: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() {
		_, err := cache.get(waiterCtx, snapshot, testSnapshotManifestLoader(snapshot, fs))
		waiterDone <- err
	}()
	select {
	case <-waiterCtx.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelable waiter did not join the shared read")
	}
	cancel()
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter did not stop")
	}
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

func TestSnapshotManifestCacheUsesCallerContext(t *testing.T) {
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
	assert.Same(t, ctx, loadCtx)
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

func TestSnapshotManifestCacheCountsDataDescriptorsOnce(t *testing.T) {
	cache := newSnapshotManifestCache()
	manifests := make([]iceberg.ManifestFile, snapshotManifestCacheManifestLimit)
	for i := range manifests {
		manifests[i] = iceberg.NewManifestFile(2, fmt.Sprintf("data-%d.avro", i), 10, 0, 1).Build()
	}
	set := newSnapshotManifestSet(manifests)
	snapshot := Snapshot{SnapshotID: 1, ManifestList: "full.avro"}
	_, err := cache.get(t.Context(), snapshot, func(context.Context) (snapshotManifestSet, error) {
		return set, nil
	})
	require.NoError(t, err)
	assert.Equal(t, snapshotManifestCacheManifestLimit, cache.completeManifestCount)
	assert.Equal(t, 1, cache.complete.Len())

	_, err = cache.get(t.Context(), snapshot, func(context.Context) (snapshotManifestSet, error) {
		t.Error("data partition counted the same manifest descriptors twice")

		return snapshotManifestSet{}, nil
	})
	require.NoError(t, err)

	_, err = cache.get(t.Context(), Snapshot{SnapshotID: 2, ManifestList: "next.avro"},
		func(context.Context) (snapshotManifestSet, error) {
			return newSnapshotManifestSet(manifests[:1]), nil
		})
	require.NoError(t, err)
	assert.Equal(t, 1, cache.completeManifestCount)
	assert.Equal(t, 1, cache.complete.Len())
	assert.False(t, cache.complete.Contains(snapshotManifestCacheKeyFor(snapshot)))
}

func TestTableManifestListCacheProperty(t *testing.T) {
	for _, value := range []string{"", "true", "false", "invalid"} {
		t.Run("value="+value, func(t *testing.T) {
			scan, base := scanWithManifestCount(t, 1)
			builder, err := MetadataBuilderFromBase(scan.metadata, "")
			require.NoError(t, err)
			if value != "" {
				require.NoError(t, builder.SetProperties(iceberg.Properties{ReadManifestListCacheEnabledKey: value}))
			}
			meta, err := builder.Build()
			require.NoError(t, err)
			fs := &snapshotManifestCacheBenchmarkIO{IO: base, opens: make(map[string]int)}
			tbl := New(Identifier{"db", "cache-property"}, meta, "metadata.json", testFSF(fs), nil)
			for range 2 {
				tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(t.Context())
				require.NoError(t, err)
				require.Len(t, tasks, 1)
			}
			wantOpens := 1
			if value == "false" {
				wantOpens = 2
				assert.Nil(t, tbl.manifestCache)
			}
			assert.Equal(t, wantOpens, fs.opens[meta.CurrentSnapshot().ManifestList])
		})
	}
}

func TestTableRefreshHonorsManifestListCacheProperty(t *testing.T) {
	scan, fs := scanWithManifestCount(t, 1)
	builder, err := MetadataBuilderFromBase(scan.metadata, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetProperties(iceberg.Properties{ReadManifestListCacheEnabledKey: "false"}))
	disabled, err := builder.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "cache-refresh"}, scan.metadata, "old.json", testFSF(fs), nil)
	cat := &snapshotManifestRefreshCatalog{
		fresh: New(tbl.Identifier(), disabled, "disabled.json", testFSF(fs), nil),
	}
	tbl.cat = cat
	require.NoError(t, tbl.Refresh(t.Context()))
	assert.Nil(t, tbl.manifestCache)
	assert.Nil(t, tbl.Scan().manifestCache)

	cat.fresh = New(tbl.Identifier(), scan.metadata, "enabled.json", testFSF(fs), nil)
	require.NoError(t, tbl.Refresh(t.Context()))
	assert.NotNil(t, tbl.manifestCache)
	assert.NotSame(t, cat.fresh.manifestCache, tbl.manifestCache)
}

func TestTransactionScanHonorsManifestListCacheProperty(t *testing.T) {
	scan, fs := scanWithManifestCount(t, 1)
	tbl := New(Identifier{"db", "cache-transaction"}, scan.metadata, "metadata.json", testFSF(fs), nil)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{ReadManifestListCacheEnabledKey: "false"}))
	disabled, err := txn.Scan()
	require.NoError(t, err)
	assert.Nil(t, disabled.manifestCache)
	assert.NotNil(t, tbl.manifestCache)

	require.NoError(t, txn.SetProperties(iceberg.Properties{ReadManifestListCacheEnabledKey: "true"}))
	enabled, err := txn.Scan()
	require.NoError(t, err)
	assert.NotNil(t, enabled.manifestCache)
	assert.NotSame(t, tbl.manifestCache, enabled.manifestCache)
}

func TestAllManifestsSkipsFileIOForEmptyTable(t *testing.T) {
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://empty-manifests", nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "empty-manifests"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) {
			t.Error("an empty table should not resolve file IO")

			return nil, errors.New("factory failed")
		}, nil)
	for manifest, err := range tbl.AllManifests(t.Context()) {
		t.Fatalf("empty table yielded %v, %v", manifest, err)
	}
}

type contextBoundSnapshotManifestIO struct {
	iceio.IO
	ctx         context.Context
	started     chan struct{}
	release     chan struct{}
	blockedPath string
}

func (fs *contextBoundSnapshotManifestIO) Open(name string) (iceio.File, error) {
	if fs.blockedPath != "" && name != fs.blockedPath {
		return fs.IO.Open(name)
	}
	close(fs.started)
	select {
	case <-fs.ctx.Done():
		return nil, fs.ctx.Err()
	case <-fs.release:
		return nil, errors.New("released stalled read")
	}
}

func TestPlanFilesRetriesCanceledSnapshotManifestProducer(t *testing.T) {
	for _, source := range []string{"scan", "all-manifests"} {
		for _, phase := range []string{"factory", "read"} {
			t.Run(source+"/"+phase, func(t *testing.T) {
				scan, fs := scanWithManifestCount(t, 1)
				started := make(chan struct{})
				release := make(chan struct{})
				t.Cleanup(func() { close(release) })
				factory := func(ctx context.Context) (iceio.IO, error) {
					if ctx.Value(producerContextKey{}) != true {
						return fs, nil
					}
					if phase == "read" {
						return &contextBoundSnapshotManifestIO{IO: fs, ctx: ctx, started: started, release: release}, nil
					}
					close(started)
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-release:
						return nil, errors.New("released stalled factory")
					}
				}
				tbl := New(Identifier{"db", "canceled-producer"}, scan.metadata, "metadata.json", factory, nil)
				ctx, cancel := context.WithCancel(context.WithValue(t.Context(), producerContextKey{}, true))
				defer cancel()
				producerDone := make(chan error, 1)
				go func() {
					if source == "scan" {
						_, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(ctx)
						producerDone <- err

						return
					}
					for _, err := range tbl.AllManifests(ctx) {
						if err != nil {
							producerDone <- err

							return
						}
					}
					producerDone <- nil
				}()
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("manifest producer did not start")
				}

				waiterCtx := &notifyingContext{Context: t.Context(), entered: make(chan struct{})}
				waiterDone := make(chan error, 1)
				go func() {
					tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(waiterCtx)
					if err == nil && len(tasks) != 1 {
						err = fmt.Errorf("expected one scan task, got %d", len(tasks))
					}
					waiterDone <- err
				}()
				select {
				case <-waiterCtx.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("scan did not wait on the shared read")
				}
				cancel()
				select {
				case err := <-producerDone:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(5 * time.Second):
					t.Fatal("canceled producer did not stop")
				}
				select {
				case err := <-waiterDone:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("healthy scan did not retry")
				}
			})
		}
	}
}

func TestSnapshotManifestCacheSharesErrorsFromLiveProducer(t *testing.T) {
	for _, loadErr := range []error{errors.New("read failed"), context.DeadlineExceeded} {
		t.Run(loadErr.Error(), func(t *testing.T) {
			cache := newSnapshotManifestCache()
			snapshot := Snapshot{SnapshotID: 1, ManifestList: "error.avro"}
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			producerDone := make(chan error, 1)
			go func() {
				_, err := cache.get(t.Context(), snapshot, func(context.Context) (snapshotManifestSet, error) {
					close(started)
					<-release

					return snapshotManifestSet{}, loadErr
				})
				producerDone <- err
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("producer did not start")
			}
			ctx := &notifyingContext{Context: t.Context(), entered: make(chan struct{})}
			waiterDone := make(chan error, 1)
			go func() {
				_, err := cache.get(ctx, snapshot, func(context.Context) (snapshotManifestSet, error) {
					return snapshotManifestSet{}, errors.New("unexpected retry")
				})
				waiterDone <- err
			}()
			select {
			case <-ctx.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("waiter did not start")
			}
			once.Do(func() { close(release) })
			require.ErrorIs(t, <-producerDone, loadErr)
			require.ErrorIs(t, <-waiterDone, loadErr)
		})
	}
}

func TestAllManifestsEarlyStopDoesNotFailConcurrentScan(t *testing.T) {
	scan, fs := scanWithManifestCount(t, 1)
	previous := scan.metadata.CurrentSnapshot()
	manifests, err := previous.Manifests(fs)
	require.NoError(t, err)
	const currentList = "mem://planning/table/metadata/snap-2.avro"
	var list bytes.Buffer
	sequenceNumber := int64(2)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 2, &previous.SnapshotID, &sequenceNumber, 0, manifests))
	require.NoError(t, fs.WriteFile(currentList, list.Bytes()))
	builder, err := MetadataBuilderFromBase(scan.metadata, "")
	require.NoError(t, err)
	current := *previous
	current.SnapshotID = 2
	current.ParentSnapshotID = &previous.SnapshotID
	current.SequenceNumber = 2
	current.TimestampMs++
	current.ManifestList = currentList
	require.NoError(t, builder.AddSnapshot(&current))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 2, BranchRef))
	meta, err := builder.Build()
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	tbl := New(Identifier{"db", "early-stop"}, meta, "metadata.json", func(ctx context.Context) (iceio.IO, error) {
		if ctx.Value(producerContextKey{}) == true {
			return &contextBoundSnapshotManifestIO{
				IO: fs, ctx: ctx, started: started, release: release, blockedPath: currentList,
			}, nil
		}

		return fs, nil
	}, nil)
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), producerContextKey{}, true))
	defer cancel()
	all := tbl.AllManifests(ctx)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("current snapshot read did not start")
	}
	waiterCtx := &notifyingContext{Context: t.Context(), entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(waiterCtx)
		if err == nil && len(tasks) != 1 {
			err = fmt.Errorf("expected one scan task, got %d", len(tasks))
		}
		done <- err
	}()
	select {
	case <-waiterCtx.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not wait for the current snapshot")
	}
	count := 0
	for _, err := range all {
		require.NoError(t, err)
		count++

		break
	}
	require.Equal(t, 1, count)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not recover from early iterator stop")
	}
}
