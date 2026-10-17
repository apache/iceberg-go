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

package rest

import (
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	lru "github.com/hashicorp/golang-lru/v2"
)

// Catalog properties controlling the table cache used for freshness-aware
// table loading. The names and defaults match Iceberg Java's
// RESTCatalogProperties so the same configuration works across clients.
const (
	keyTableCacheMaxEntries         = "rest-table-cache.max-entries"
	keyTableCacheExpireAfterWriteMs = "rest-table-cache.expire-after-write-ms"

	defaultTableCacheMaxEntries       = 100
	defaultTableCacheExpireAfterWrite = 5 * time.Minute
)

// tableCacheEntry is a load response the server tagged with an ETag. The
// parsed response is kept rather than a *table.Table because Table.Refresh
// mutates its receiver: every 304 builds a fresh table from the response so
// callers never share one. The response's Labels are mutable through the
// table, so the entry holds its own copy and hands out a copy on every hit.
type tableCacheEntry struct {
	etag string
	// mode is the snapshot loading mode the response was loaded with. A refs
	// response replayed for an all request would silently drop snapshots, so
	// a load with another mode does not reuse the entry.
	mode    SnapshotMode
	rsp     loadTableResponse
	written time.Time
}

// tableCache maps a table identifier to its last ETag-tagged load response so
// LoadTable can send If-None-Match and reuse the response on 304 Not Modified.
// Entries expire a fixed time after they are written; reads do not extend
// them. A nil *tableCache is a disabled cache and every method is a no-op.
type tableCache struct {
	entries *lru.Cache[string, tableCacheEntry]
	ttl     time.Duration
	now     func() time.Time
}

// newTableCache builds the cache from the merged catalog properties. It
// returns nil when max-entries is 0, which disables freshness-aware loading.
func newTableCache(props iceberg.Properties) (*tableCache, error) {
	maxEntries := defaultTableCacheMaxEntries
	if v, ok := props[keyTableCacheMaxEntries]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("%w: invalid %s %q: want a non-negative integer",
				ErrRESTError, keyTableCacheMaxEntries, v)
		}
		maxEntries = n
	}

	ttl := defaultTableCacheExpireAfterWrite
	if v, ok := props[keyTableCacheExpireAfterWriteMs]; ok {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil || ms <= 0 {
			return nil, fmt.Errorf("%w: invalid %s %q: want a positive integer",
				ErrRESTError, keyTableCacheExpireAfterWriteMs, v)
		}
		ttl = time.Duration(ms) * time.Millisecond
	}

	if maxEntries == 0 {
		return nil, nil
	}

	entries, err := lru.New[string, tableCacheEntry](maxEntries)
	if err != nil {
		return nil, err
	}

	return &tableCache{entries: entries, ttl: ttl, now: time.Now}, nil
}

// tableCacheKey is unambiguous for any identifier, including parts that
// contain separators.
func tableCacheKey(ident table.Identifier) string {
	return fmt.Sprintf("%q", []string(ident))
}

func (c *tableCache) get(ident table.Identifier, mode SnapshotMode) (tableCacheEntry, bool) {
	if c == nil {
		return tableCacheEntry{}, false
	}

	key := tableCacheKey(ident)
	e, ok := c.entries.Get(key)
	if !ok || e.mode != mode {
		return tableCacheEntry{}, false
	}
	if c.now().Sub(e.written) >= c.ttl {
		c.entries.Remove(key)

		return tableCacheEntry{}, false
	}
	e.rsp.Labels = cloneLabels(e.rsp.Labels)

	return e, true
}

func (c *tableCache) put(ident table.Identifier, mode SnapshotMode, etag string, rsp loadTableResponse) {
	if c == nil {
		return
	}

	rsp.Labels = cloneLabels(rsp.Labels)
	c.entries.Add(tableCacheKey(ident), tableCacheEntry{etag: etag, mode: mode, rsp: rsp, written: c.now()})
}

// cloneLabels deep-copies l, so a table built from a cached response cannot
// change the labels of the cache entry or of another table.
func cloneLabels(l *iceberg.Labels) *iceberg.Labels {
	if l == nil {
		return nil
	}

	out := &iceberg.Labels{ObjectLabels: maps.Clone(l.ObjectLabels)}
	if l.Fields != nil {
		out.Fields = make([]iceberg.FieldLabel, len(l.Fields))
		for i, f := range l.Fields {
			out.Fields[i] = iceberg.FieldLabel{FieldID: f.FieldID, Labels: maps.Clone(f.Labels)}
		}
	}

	return out
}

func (c *tableCache) invalidate(ident table.Identifier) {
	if c == nil {
		return
	}

	c.entries.Remove(tableCacheKey(ident))
}

func (c *tableCache) purge() {
	if c == nil {
		return
	}

	c.entries.Purge()
}
