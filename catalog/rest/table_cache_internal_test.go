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
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableCacheDefaults(t *testing.T) {
	c, err := newTableCache(nil)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, 5*time.Minute, c.ttl)

	for i := range defaultTableCacheMaxEntries + 1 {
		c.put(table.Identifier{"ns", string(rune('a' + i%26)), string(rune('a' + i/26))}, SnapshotModeAll, "e", loadTableResponse{})
	}
	assert.Equal(t, defaultTableCacheMaxEntries, c.entries.Len())
}

func TestTableCacheExpiresAfterWrite(t *testing.T) {
	c, err := newTableCache(iceberg.Properties{keyTableCacheExpireAfterWriteMs: "1000"})
	require.NoError(t, err)

	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	ident := table.Identifier{"ns", "tbl"}

	c.put(ident, SnapshotModeAll, `"v1"`, loadTableResponse{})

	// Reads do not extend an entry's lifetime.
	now = now.Add(999 * time.Millisecond)
	e, ok := c.get(ident, SnapshotModeAll)
	require.True(t, ok)
	assert.Equal(t, `"v1"`, e.etag)

	now = now.Add(time.Millisecond)
	_, ok = c.get(ident, SnapshotModeAll)
	assert.False(t, ok)
	assert.Zero(t, c.entries.Len())
}

func TestTableCacheKeyIsUnambiguous(t *testing.T) {
	assert.NotEqual(t,
		tableCacheKey(table.Identifier{"a.b", "c"}),
		tableCacheKey(table.Identifier{"a", "b.c"}))
	assert.NotEqual(t,
		tableCacheKey(table.Identifier{`a" "b`, "c"}),
		tableCacheKey(table.Identifier{"a", "b", "c"}))
}

// TestTableCacheIgnoresEntryForOtherSnapshotMode checks that a response loaded
// with one snapshot mode is not reused for a load with another: a refs response
// replayed for an all request would silently drop snapshots.
func TestTableCacheIgnoresEntryForOtherSnapshotMode(t *testing.T) {
	c, err := newTableCache(nil)
	require.NoError(t, err)
	ident := table.Identifier{"ns", "tbl"}

	c.put(ident, SnapshotModeRefs, `"v1"`, loadTableResponse{})

	_, ok := c.get(ident, SnapshotModeAll)
	assert.False(t, ok)
	e, ok := c.get(ident, SnapshotModeRefs)
	require.True(t, ok)
	assert.Equal(t, `"v1"`, e.etag)
}
