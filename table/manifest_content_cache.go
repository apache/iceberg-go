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
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// manifestContentCache stores immutable manifest bytes so repeated scans can
// reuse the same object-store read while still decoding and filtering entries
// independently for each scan.
type manifestContentCache struct {
	mu sync.Mutex

	expirationIntervalMs int64
	now                  func() time.Time
	maxTotalBytes        int64
	maxContentLength     int64
	totalBytes           int64

	entries map[string]*manifestContentCacheEntry
	lru     list.List
	loads   map[string]*manifestContentCacheLoad
}

type manifestContentCacheEntry struct {
	location     string
	content      []byte
	lastAccess   time.Time
	element      *list.Element
}

type manifestContentCacheLoad struct {
	ready    chan struct{}
	content  []byte
	err      error
	canceled bool
}

func newManifestContentCache(expirationIntervalMs, maxTotalBytes, maxContentLength int64) *manifestContentCache {
	return &manifestContentCache{
		expirationIntervalMs: expirationIntervalMs,
		now:                  time.Now,
		maxTotalBytes:        maxTotalBytes,
		maxContentLength:     maxContentLength,
		entries:              make(map[string]*manifestContentCacheEntry),
		loads:                make(map[string]*manifestContentCacheLoad),
	}
}

func newManifestContentCacheForConfig(config iceberg.Properties) *manifestContentCache {
	if !config.GetBool(IOManifestCacheEnabledKey, IOManifestCacheEnabledDefault) {
		return nil
	}

	expirationIntervalMs, ok := manifestContentCacheConfigInt64(config,
		IOManifestCacheExpirationIntervalMsKey, IOManifestCacheExpirationIntervalMsDefault)
	if !ok {
		return nil
	}
	maxTotalBytes, ok := manifestContentCacheConfigInt64(config,
		IOManifestCacheMaxTotalBytesKey, IOManifestCacheMaxTotalBytesDefault)
	if !ok {
		return nil
	}
	maxContentLength, ok := manifestContentCacheConfigInt64(config,
		IOManifestCacheMaxContentLengthKey, IOManifestCacheMaxContentLengthDefault)
	if !ok {
		return nil
	}
	if expirationIntervalMs < 0 || maxTotalBytes <= 0 || maxContentLength <= 0 {
		log.Printf("Warning: disabling manifest content cache: invalid limits (expiration=%d, total=%d, content=%d)",
			expirationIntervalMs, maxTotalBytes, maxContentLength)

		return nil
	}

	return newManifestContentCache(expirationIntervalMs, maxTotalBytes, maxContentLength)
}

func manifestContentCacheConfigInt64(config iceberg.Properties, key string, fallback int64) (int64, bool) {
	raw, ok := config[key]
	if !ok {
		return fallback, true
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Printf("Warning: disabling manifest content cache: invalid %s=%q: %v", key, raw, err)

		return 0, false
	}

	return value, true
}

// Reuse retained immutable manifests when a table is refreshed or committed.
// Independent table handles still have independent byte budgets.
func reuseManifestContentCache(previous, configured *manifestContentCache) *manifestContentCache {
	if previous != nil && configured != nil &&
		previous.expirationIntervalMs == configured.expirationIntervalMs &&
		previous.maxTotalBytes == configured.maxTotalBytes &&
		previous.maxContentLength == configured.maxContentLength {
		return previous
	}
	return configured
}

func (c *manifestContentCache) wrap(
	ctx context.Context,
	base iceio.IO,
	manifest iceberg.ManifestFile,
) iceio.IO {
	if c == nil {
		return base
	}

	return &manifestContentCacheIO{
		ctx:      ctx,
		base:     base,
		cache:    c,
		manifest: manifest,
	}
}

func (c *manifestContentCache) open(
	ctx context.Context,
	base iceio.IO,
	manifest iceberg.ManifestFile,
) (iceio.File, error) {
	location := manifest.FilePath()
	expectedLength := manifest.Length()
	if expectedLength <= 0 || expectedLength > c.maxContentLength || expectedLength > c.maxTotalBytes {
		return base.Open(location)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		c.mu.Lock()
		if content, ok := c.getLocked(location, expectedLength); ok {
			c.mu.Unlock()

			return newCachedManifestFile(location, content), nil
		}
		if load, ok := c.loads[location]; ok {
			c.mu.Unlock()
			select {
			case <-load.ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if load.canceled {
				continue
			}
			// Concurrent callers may describe the same path with different lengths.
			// Never reuse in-flight bytes that do not match this caller's descriptor.
			if load.err != nil {
				// A failed shared population should not fan out into one
				// backend retry per waiter.
				return nil, load.err
			}
			if int64(len(load.content)) != expectedLength {
				return base.Open(location)
			}

			return newCachedManifestFile(location, load.content), nil
		}

		load := &manifestContentCacheLoad{ready: make(chan struct{})}
		c.loads[location] = load
		c.mu.Unlock()

		var content []byte
		var err error
		var canceled bool
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					// Always release the single-flight waiters, even if a
					// FileIO implementation panics during population.
					c.finishLoad(location, expectedLength, load, nil,
						fmt.Errorf("manifest content load panicked: %v", recovered), false)
					panic(recovered)
				}
				c.finishLoad(location, expectedLength, load, content, err, canceled)
			}()
			content, err = readManifestContent(base, location, expectedLength)
			canceled = err != nil && ctx.Err() != nil
		}()
		if err != nil {
			if canceled {
				return nil, ctx.Err()
			}

			// Preserve the existing ordinary-open fallback for the producer.
			file, fallbackErr := base.Open(location)
			if fallbackErr != nil {
				return nil, fmt.Errorf("manifest cache population failed: %w (fallback open failed: %v)",
					err, fallbackErr)
			}
			return file, nil
		}

		return newCachedManifestFile(location, content), nil
	}
}

func (c *manifestContentCache) getLocked(location string, expectedLength int64) ([]byte, bool) {
	entry, ok := c.entries[location]
	if !ok {
		return nil, false
	}
	if int64(len(entry.content)) != expectedLength {
		c.removeEntryLocked(entry)

		return nil, false
	}

	if c.expirationIntervalMs > 0 {
		now := c.now()
		if c.expired(entry, now) {
			c.removeEntryLocked(entry)
			return nil, false
		}
		entry.lastAccess = now
	}
	c.lru.MoveToFront(entry.element)

	return entry.content, true
}

func (c *manifestContentCache) expired(entry *manifestContentCacheEntry, now time.Time) bool {
	// Dividing a monotonic duration avoids overflowing on large configured
	// millisecond values and is immune to wall-clock jumps.
	return c.expirationIntervalMs > 0 &&
		now.Sub(entry.lastAccess)/time.Millisecond >= time.Duration(c.expirationIntervalMs)
}

func (c *manifestContentCache) finishLoad(
	location string,
	expectedLength int64,
	load *manifestContentCacheLoad,
	content []byte,
	err error,
	canceled bool,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.loads[location]; ok && current == load {
		delete(c.loads, location)
		if err == nil && int64(len(content)) == expectedLength {
			c.addEntryLocked(location, content)
		}
	}

	load.content = content
	load.err = err
	load.canceled = canceled
	close(load.ready)
}

func (c *manifestContentCache) addEntryLocked(location string, content []byte) {
	size := int64(len(content))
	if size > c.maxContentLength || size > c.maxTotalBytes {
		return
	}
	if previous, ok := c.entries[location]; ok {
		c.removeEntryLocked(previous)
	}
	if c.expirationIntervalMs > 0 {
		now := c.now()
		// The tail is the least recently accessed entry. Reclaim expired
		// entries when inserting, without a background expiration goroutine.
		for tail := c.lru.Back(); tail != nil; tail = c.lru.Back() {
			oldest, ok := tail.Value.(*manifestContentCacheEntry)
			if !ok {
				c.lru.Remove(tail)

				continue
			}
			if !c.expired(oldest, now) {
				break
			}
			c.removeEntryLocked(oldest)
		}
	}
	for c.totalBytes > c.maxTotalBytes-size && c.lru.Len() > 0 {
		tail := c.lru.Back()
		oldest, ok := tail.Value.(*manifestContentCacheEntry)
		if !ok {
			c.lru.Remove(tail)

			continue
		}
		c.removeEntryLocked(oldest)
	}

	entry := &manifestContentCacheEntry{
		location: location,
		content:  content,
	}
	if c.expirationIntervalMs > 0 {
		entry.lastAccess = c.now()
	}
	entry.element = c.lru.PushFront(entry)
	c.entries[location] = entry
	c.totalBytes += size
}

func (c *manifestContentCache) removeEntryLocked(entry *manifestContentCacheEntry) {
	delete(c.entries, entry.location)
	c.lru.Remove(entry.element)
	c.totalBytes -= int64(len(entry.content))
}

func readManifestContent(base iceio.IO, location string, expectedLength int64) (content []byte, err error) {
	file, err := base.Open(location)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}()

	// The advertised length has already passed the cache's size limits. Read
	// into an exact-sized buffer so retained capacity matches byte accounting
	// and a larger actual file cannot grow the population buffer without bound.
	content = make([]byte, expectedLength)
	if _, err = io.ReadFull(file, content); err != nil {
		return nil, err
	}

	var extra [1]byte
	if _, err = io.ReadFull(file, extra[:]); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("manifest content exceeds expected length %d", expectedLength)
		}
		return nil, err
	}

	return content, nil
}

// Used only while the owning scan goroutine is active. Manifest reads call
// Open; other optional FileIO interfaces are deliberately not forwarded.
type manifestContentCacheIO struct {
	ctx      context.Context
	base     iceio.IO
	cache    *manifestContentCache
	manifest iceberg.ManifestFile
}

func (m *manifestContentCacheIO) Open(name string) (iceio.File, error) {
	if name != m.manifest.FilePath() {
		return m.base.Open(name)
	}

	return m.cache.open(m.ctx, m.base, m.manifest)
}

func (m *manifestContentCacheIO) Remove(name string) error {
	return m.base.Remove(name)
}

type cachedManifestFile struct {
	*bytes.Reader
	location string
}

func newCachedManifestFile(location string, content []byte) *cachedManifestFile {
	return &cachedManifestFile{
		Reader:   bytes.NewReader(content),
		location: location,
	}
}

func (*cachedManifestFile) Close() error { return nil }

func (f *cachedManifestFile) Stat() (fs.FileInfo, error) {
	return cachedManifestFileInfo{
		name: path.Base(f.location),
		size: f.Size(),
	}, nil
}

type cachedManifestFileInfo struct {
	name string
	size int64
}

func (f cachedManifestFileInfo) Name() string     { return f.name }
func (f cachedManifestFileInfo) Size() int64      { return f.size }
func (cachedManifestFileInfo) Mode() fs.FileMode  { return 0o444 }
func (cachedManifestFileInfo) ModTime() time.Time { return time.Time{} }
func (cachedManifestFileInfo) IsDir() bool        { return false }
func (cachedManifestFileInfo) Sys() any           { return nil }
