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
	"bytes"
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

func TestManifestContentCacheBoundsPopulationAndPreservesFullReads(t *testing.T) {
	const location = "mem://manifest-content-cache/length.avro"
	content := bytes.Repeat([]byte("x"), 32)
	for _, tt := range []struct {
		name           string
		expectedLength int64
		wantOpens      int
		firstReadBytes int
	}{
		{name: "matching length", expectedLength: 32, wantOpens: 1, firstReadBytes: 32},
		{name: "longer content", expectedLength: 8, wantOpens: 2, firstReadBytes: 9},
		{name: "shorter content", expectedLength: 64, wantOpens: 2, firstReadBytes: 32},
		{name: "unknown length", expectedLength: 0, wantOpens: 1, firstReadBytes: 32},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base := iceio.NewMemFS()
			require.NoError(t, base.WriteFile(location, content))
			fs := &manifestContentCountingIO{IO: base}
			cache := newManifestContentCache(0, 128, 64)
			manifest := iceberg.NewManifestFile(2, location, tt.expectedLength, 0, 1).Build()

			file, err := cache.open(t.Context(), fs, manifest)
			require.NoError(t, err)
			got, err := io.ReadAll(file)
			require.NoError(t, err)
			require.NoError(t, file.Close())
			assert.Equal(t, content, got, "cache population must not truncate the ordinary read")
			require.Len(t, fs.files, tt.wantOpens)
			assert.Equal(t, tt.firstReadBytes, fs.files[0].bytesRead)
			if tt.expectedLength == int64(len(content)) {
				require.Contains(t, cache.entries, location)
				assert.Equal(t, len(content), cap(cache.entries[location].content))
				assert.Equal(t, int64(len(content)), cache.totalBytes)
			} else {
				assert.Empty(t, cache.entries)
				assert.Zero(t, cache.totalBytes)
			}
		})
	}
}

type manifestContentCountingIO struct {
	iceio.IO
	files []*manifestContentCountingFile
}

func (f *manifestContentCountingIO) Open(name string) (iceio.File, error) {
	file, err := f.IO.Open(name)
	if err != nil {
		return nil, err
	}
	counted := &manifestContentCountingFile{File: file}
	f.files = append(f.files, counted)

	return counted, nil
}

type manifestContentCountingFile struct {
	iceio.File
	bytesRead int
}

func (f *manifestContentCountingFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.bytesRead += n

	return n, err
}

func TestManifestContentCacheEvictsLeastRecentlyUsed(t *testing.T) {
	fs := newTrackingCallsIO()
	const (
		firstPath  = "mem://manifest-content-cache/first.avro"
		secondPath = "mem://manifest-content-cache/second.avro"
		thirdPath  = "mem://manifest-content-cache/third.avro"
	)
	firstBytes := []byte("aaaa")
	secondBytes := []byte("bbbb")
	require.NoError(t, fs.WriteFile(firstPath, firstBytes))
	require.NoError(t, fs.WriteFile(secondPath, secondBytes))
	require.NoError(t, fs.WriteFile(thirdPath, []byte("cccc")))

	first := iceberg.NewManifestFile(2, firstPath, int64(len(firstBytes)), 0, 1).Build()
	second := iceberg.NewManifestFile(2, secondPath, int64(len(secondBytes)), 0, 1).Build()
	third := iceberg.NewManifestFile(2, thirdPath, 4, 0, 1).Build()
	cache := newManifestContentCache(0, 8, 4)

	readCachedManifest(t, cache, fs, first)
	readCachedManifest(t, cache, fs, second)
	readCachedManifest(t, cache, fs, first)
	readCachedManifest(t, cache, fs, third)

	assert.Contains(t, cache.entries, firstPath, "access must refresh LRU order")
	assert.NotContains(t, cache.entries, secondPath)
	assert.Equal(t, 1, fs.openCount[firstPath])
	assert.Equal(t, 1, fs.openCount[secondPath])
	assert.Equal(t, int64(8), cache.totalBytes)
}

func TestManifestContentCacheExpiresIdleEntries(t *testing.T) {
	const location = "mem://manifest-content-cache/expired.avro"
	fs := newTrackingCallsIO()
	require.NoError(t, fs.WriteFile(location, []byte("aaaa")))
	manifest := iceberg.NewManifestFile(2, location, 4, 0, 1).Build()
	cache := newManifestContentCache(60_000, 8, 4)

	now := time.Now()
	cache.now = func() time.Time { return now }
	readCachedManifest(t, cache, fs, manifest)
	now = now.Add(time.Hour)
	readCachedManifest(t, cache, fs, manifest)

	assert.Equal(t, 2, fs.openCount[location])
	assert.Equal(t, int64(4), cache.totalBytes)
	assert.Len(t, cache.entries, 1)
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
	joined := make(chan struct{}, callers)
	for range callers {
		go func() {
			// Only followers call Done() while awaiting the blocked load.
			ctx := &countingContext{Context: t.Context(), entered: joined}
			file, err := cache.open(ctx, fs, manifest)
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
	case <-time.After(5 * time.Second):
		t.Fatal("manifest read did not start")
	}
	// All followers must enter the shared-load wait path. Otherwise late
	// callers could hit the populated cache and mask broken single-flight.
	for range callers - 1 {
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Fatal("not all callers joined the in-flight manifest read")
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiterCtx := &notifyingContext{Context: ctx, entered: make(chan struct{})}
	waiterErr := make(chan error, 1)
	go func() {
		file, err := cache.open(waiterCtx, fs, manifest)
		if file != nil {
			file.Close()
		}
		waiterErr <- err
	}()
	select {
	case <-waiterCtx.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not join the manifest read")
	}
	cancel()
	select {
	case err := <-waiterErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter did not return")
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

func TestManifestContentCacheInFlightLengthMismatch(t *testing.T) {
	const location = "mem://manifest-content-cache/inflight-length.avro"
	content := []byte("manifest")
	base := iceio.NewMemFS()
	require.NoError(t, base.WriteFile(location, content))
	fs := &blockingSnapshotManifestIO{
		IO: base, blockedPath: location, started: make(chan struct{}),
		release: make(chan struct{}), opens: make(map[string]int),
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })

	cache := newManifestContentCache(0, 1024, 1024)
	actual := iceberg.NewManifestFile(2, location, int64(len(content)), 0, 1).Build()
	wrongLength := iceberg.NewManifestFile(2, location, int64(len(content)-1), 0, 1).Build()
	type result struct {
		content []byte
		err     error
	}
	read := func(ctx context.Context, manifest iceberg.ManifestFile) <-chan result {
		out := make(chan result, 1)
		go func() {
			file, err := cache.open(ctx, fs, manifest)
			if err != nil {
				out <- result{err: err}

				return
			}
			got, err := io.ReadAll(file)
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
			out <- result{content: got, err: err}
		}()

		return out
	}

	producer := read(t.Context(), actual)
	select {
	case <-fs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("manifest read did not start")
	}

	waiterCtx := &notifyingContext{Context: t.Context(), entered: make(chan struct{})}
	waiter := read(waiterCtx, wrongLength)
	select {
	case <-waiterCtx.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not join the manifest read")
	}
	release.Do(func() { close(fs.release) })

	for _, got := range []result{<-producer, <-waiter} {
		require.NoError(t, got.err)
		assert.Equal(t, content, got.content)
	}
	fs.mu.Lock()
	opens := fs.opens[location]
	fs.mu.Unlock()
	assert.Equal(t, 2, opens, "a descriptor with a different length must bypass in-flight bytes")
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

func TestManifestContentCacheReuseAcrossCompatibleConfig(t *testing.T) {
	config := iceberg.Properties{IOManifestCacheEnabledKey: "true"}
	previous := newManifestContentCacheForConfig(config)
	require.Same(t, previous, reuseManifestContentCache(previous, newManifestContentCacheForConfig(config)))

	changed := iceberg.Properties{IOManifestCacheEnabledKey: "true", IOManifestCacheMaxTotalBytesKey: "12345"}
	require.NotSame(t, previous, reuseManifestContentCache(previous, newManifestContentCacheForConfig(changed)))
	require.Nil(t, reuseManifestContentCache(previous, newManifestContentCacheForConfig(nil)))
}

func TestManifestContentCacheInvalidNumericConfig(t *testing.T) {
	for _, bad := range []string{"bad", "-1", "0"} {
		config := iceberg.Properties{IOManifestCacheEnabledKey: "true"}
		config[IOManifestCacheMaxTotalBytesKey] = bad
		assert.Nil(t, newManifestContentCacheForConfig(config))
	}
}

type manifestContentCacheFailOnceIO struct {
	iceio.IO
	failed bool
}

func (f *manifestContentCacheFailOnceIO) Open(name string) (iceio.File, error) {
	if !f.failed {
		f.failed = true

		return nil, io.ErrUnexpectedEOF
	}

	return f.IO.Open(name)
}

func TestManifestContentCacheFailedPopulationFallsBackWithoutCaching(t *testing.T) {
	const location = "mem://manifest-content-cache/fallback.avro"
	base := iceio.NewMemFS()
	require.NoError(t, base.WriteFile(location, []byte("test")))
	fs := &manifestContentCacheFailOnceIO{IO: base}
	cache := newManifestContentCache(0, 16, 16)
	manifest := iceberg.NewManifestFile(2, location, 4, 0, 1).Build()

	readCachedManifest(t, cache, fs, manifest)
	assert.Empty(t, cache.entries, "ordinary-open fallback must not populate the cache")
	readCachedManifest(t, cache, fs, manifest)
	assert.Contains(t, cache.entries, location)
}

type manifestContentCachePanicIO struct {
	iceio.IO
}

func (manifestContentCachePanicIO) Open(string) (iceio.File, error) {
	panic("unexpected backend panic")
}

func TestManifestContentCachePanicReleasesInFlightLoad(t *testing.T) {
	cache := newManifestContentCache(0, 16, 16)
	const location = "mem://manifest-content-cache/panic.avro"
	manifest := iceberg.NewManifestFile(2, location, 4, 0, 1).Build()
	assert.Panics(t, func() {
		_, _ = cache.open(t.Context(), manifestContentCachePanicIO{}, manifest)
	})
	assert.Empty(t, cache.loads)
}

func TestTableRefreshKeepsWarmManifestCacheForMatchingConfig(t *testing.T) {
	config := iceberg.Properties{IOManifestCacheEnabledKey: "true"}
	fs := newTrackingCallsIO()
	tbl, _, manifestPath := newManifestContentCacheTestTable(t, fs, config)
	tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, 1, fs.openCount[manifestPath])
	oldCache := tbl.manifestContentCache

	newTable := func(config iceberg.Properties) *Table {
		return New(tbl.identifier, tbl.metadata, tbl.metadataLocation,
			tbl.fsF, nil, WithSavedConfig(config))
	}
	cat := &snapshotManifestRefreshCatalog{fresh: newTable(config)}
	tbl.cat = cat

	require.NoError(t, tbl.Refresh(t.Context()))
	require.Same(t, oldCache, tbl.manifestContentCache)
	tasks, err = tbl.Scan(WithMaxConcurrency(1)).PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, 1, fs.openCount[manifestPath], "refresh must reuse immutable manifest content")

	changed := iceberg.Properties{
		IOManifestCacheEnabledKey:          "true",
		IOManifestCacheMaxContentLengthKey: "1",
	}
	cat.fresh = newTable(changed)
	require.NoError(t, tbl.Refresh(t.Context()))
	require.NotSame(t, oldCache, tbl.manifestContentCache)
	tasks, err = tbl.Scan(WithMaxConcurrency(1)).PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, 2, fs.openCount[manifestPath], "changed limits must bypass the old content cache")

	cat.fresh = newTable(nil)
	require.NoError(t, tbl.Refresh(t.Context()))
	require.Nil(t, tbl.manifestContentCache)
}

func TestManifestContentCacheReclaimsExpiredEntriesOnInsert(t *testing.T) {
	fs := newTrackingCallsIO()
	const (
		firstPath  = "mem://manifest-content-cache/expired-1.avro"
		secondPath = "mem://manifest-content-cache/expired-2.avro"
		thirdPath  = "mem://manifest-content-cache/new.avro"
	)
	for _, name := range []string{firstPath, secondPath, thirdPath} {
		require.NoError(t, fs.WriteFile(name, []byte("test")))
	}
	cache := newManifestContentCache(1000, 16, 4)
	now := time.Now()
	cache.now = func() time.Time { return now }
	for _, name := range []string{firstPath, secondPath} {
		readCachedManifest(t, cache, fs, iceberg.NewManifestFile(2, name, 4, 0, 1).Build())
	}
	require.Equal(t, int64(8), cache.totalBytes)

	now = now.Add(2 * time.Second)
	readCachedManifest(t, cache, fs, iceberg.NewManifestFile(2, thirdPath, 4, 0, 1).Build())
	assert.NotContains(t, cache.entries, firstPath)
	assert.NotContains(t, cache.entries, secondPath)
	assert.Contains(t, cache.entries, thirdPath)
	assert.Equal(t, int64(4), cache.totalBytes)
}

type manifestContentFailingBlockedIO struct {
	iceio.IO
	started chan struct{}
	release chan struct{}

	mu    sync.Mutex
	opens int
}

func (f *manifestContentFailingBlockedIO) Open(name string) (iceio.File, error) {
	f.mu.Lock()
	f.opens++
	attempt := f.opens
	f.mu.Unlock()
	if attempt == 1 {
		close(f.started)
		<-f.release

		return nil, io.ErrUnexpectedEOF
	}

	return f.IO.Open(name)
}

func TestManifestContentCacheSharesFailedLoadWithoutRetryStorm(t *testing.T) {
	const path = "mem://manifest-content-cache/failed-shared.avro"
	base := iceio.NewMemFS()
	require.NoError(t, base.WriteFile(path, []byte("test")))
	fs := &manifestContentFailingBlockedIO{
		IO: base, started: make(chan struct{}), release: make(chan struct{}),
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })
	cache := newManifestContentCache(0, 16, 16)
	manifest := iceberg.NewManifestFile(2, path, 4, 0, 1).Build()

	producerErr := make(chan error, 1)
	go func() {
		file, err := cache.open(t.Context(), fs, manifest)
		if err == nil {
			_, err = io.ReadAll(file)
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}
		producerErr <- err
	}()
	select {
	case <-fs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("cache producer did not start")
	}

	const followers = 8
	entered := make(chan struct{}, followers)
	waiterErrors := make(chan error, followers)
	for range followers {
		go func() {
			ctx := &countingContext{Context: t.Context(), entered: entered}
			file, err := cache.open(ctx, fs, manifest)
			if file != nil {
				_ = file.Close()
			}
			waiterErrors <- err
		}()
	}
	for range followers {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("waiter did not join failed shared load")
		}
	}
	release.Do(func() { close(fs.release) })
	require.NoError(t, <-producerErr, "the producer should use one ordinary-open fallback")
	for range followers {
		require.ErrorIs(t, <-waiterErrors, io.ErrUnexpectedEOF)
	}
	fs.mu.Lock()
	opens := fs.opens
	fs.mu.Unlock()
	assert.Equal(t, 2, opens, "one failed population plus one producer fallback")
	assert.Empty(t, cache.entries, "failed population must not retain fallback bytes")

	readCachedManifest(t, cache, fs, manifest)
	assert.Contains(t, cache.entries, path, "the next independent scan can populate the cache")
}

func TestManifestContentCacheRetriesAfterProducerCancellation(t *testing.T) {
	const path = "mem://manifest-content-cache/canceled-producer.avro"
	base := iceio.NewMemFS()
	require.NoError(t, base.WriteFile(path, []byte("test")))
	fs := &manifestContentFailingBlockedIO{
		IO: base, started: make(chan struct{}), release: make(chan struct{}),
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })
	cache := newManifestContentCache(0, 16, 16)
	manifest := iceberg.NewManifestFile(2, path, 4, 0, 1).Build()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	producerErr := make(chan error, 1)
	go func() {
		file, err := cache.open(ctx, fs, manifest)
		if file != nil {
			_ = file.Close()
		}
		producerErr <- err
	}()
	select {
	case <-fs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("cache producer did not start")
	}

	joined := make(chan struct{}, 1)
	waiterErr := make(chan error, 1)
	go func() {
		waiterCtx := &countingContext{Context: t.Context(), entered: joined}
		file, err := cache.open(waiterCtx, fs, manifest)
		if err == nil {
			_, err = io.ReadAll(file)
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}
		waiterErr <- err
	}()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not join canceled producer")
	}
	cancel()
	release.Do(func() { close(fs.release) })
	require.ErrorIs(t, <-producerErr, context.Canceled)
	require.NoError(t, <-waiterErr)
	fs.mu.Lock()
	opens := fs.opens
	fs.mu.Unlock()
	assert.Equal(t, 2, opens, "a live waiter should retry the canceled producer just once")
	assert.Contains(t, cache.entries, path)
}
