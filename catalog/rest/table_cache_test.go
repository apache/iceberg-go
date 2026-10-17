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

package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// etagServer serves one table and answers loads conditionally: 304 when the
// request's If-None-Match equals the current ETag, otherwise 200 with the
// ETag. It records the If-None-Match of every load.
type etagServer struct {
	mu          sync.Mutex
	etag        string
	exists      bool
	body        string
	ifNoneMatch []string
}

func (s *etagServer) lastIfNoneMatch() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ifNoneMatch[len(s.ifNoneMatch)-1]
}

func newETagCatalog(t *testing.T, props iceberg.Properties) (*rest.Catalog, *etagServer) {
	t.Helper()

	s := &etagServer{etag: `"v1"`, exists: true, body: createTableRestExample}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
			"endpoints": rest.AllEndpointStrings,
		}))
	})
	mux.HandleFunc("/v1/namespaces/fokko/tables/table", func(w http.ResponseWriter, req *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()

		switch req.Method {
		case http.MethodGet:
			s.ifNoneMatch = append(s.ifNoneMatch, req.Header.Get("If-None-Match"))
		case http.MethodHead:
		case http.MethodDelete:
			s.exists = false
			w.WriteHeader(http.StatusNoContent)

			return
		}
		if !s.exists {
			w.WriteHeader(http.StatusNotFound)

			return
		}
		if req.Method == http.MethodHead {
			w.WriteHeader(http.StatusNoContent)

			return
		}
		if s.etag != "" && req.Header.Get("If-None-Match") == s.etag {
			w.WriteHeader(http.StatusNotModified)

			return
		}
		if s.etag != "" {
			w.Header().Set("ETag", s.etag)
		}
		_, err := w.Write([]byte(s.body))
		assert.NoError(t, err)
	})
	mux.HandleFunc("/v1/tables/rename", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/v1/namespaces/fokko/tables/renamed", func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte(createTableRestExample))
		assert.NoError(t, err)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL,
		rest.WithOAuthToken(TestToken), rest.WithAdditionalProps(props))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })

	return cat, s
}

var etagIdent = catalog.ToIdentifier("fokko", "table")

func TestLoadTableNotModifiedReusesCachedResponse(t *testing.T) {
	ctx := context.Background()
	cat, srv := newETagCatalog(t, nil)

	first, err := cat.LoadTable(ctx, etagIdent)
	require.NoError(t, err)
	assert.Empty(t, srv.lastIfNoneMatch())

	second, err := cat.LoadTable(ctx, etagIdent)
	require.NoError(t, err)
	assert.Equal(t, `"v1"`, srv.lastIfNoneMatch())

	// A 304 builds a new table from the cached response: Table.Refresh mutates
	// its receiver, so two loads must never share one.
	assert.NotSame(t, first, second)
	assert.True(t, first.Equals(*second))
	assert.Equal(t, first.MetadataLocation(), second.MetadataLocation())
	assert.Equal(t, first.Properties(), second.Properties())
}

// TestLoadTableNotModifiedDoesNotShareLabels checks that tables built from one
// cached response get independent labels. Labels are mutable through the table,
// so a change made through one table must not reach the cache or another table.
func TestLoadTableNotModifiedDoesNotShareLabels(t *testing.T) {
	ctx := context.Background()
	cat, srv := newETagCatalog(t, nil)

	var body map[string]any
	require.NoError(t, json.Unmarshal([]byte(createTableRestExample), &body))
	body["labels"] = map[string]any{
		"object-labels": map[string]string{"owner": "data-eng"},
		"fields":        []any{map[string]any{"field-id": 1, "labels": map[string]string{"pii": "no"}}},
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	srv.mu.Lock()
	srv.body = string(raw)
	srv.mu.Unlock()

	first, err := cat.LoadTable(ctx, etagIdent)
	require.NoError(t, err)
	first.Labels().Object()["owner"] = "changed"
	first.Labels().Field(1)["pii"] = "yes"

	for range 2 {
		next, err := cat.LoadTable(ctx, etagIdent)
		require.NoError(t, err)
		require.Equal(t, `"v1"`, srv.lastIfNoneMatch(), "the load must be served from the cache")
		assert.Equal(t, "data-eng", next.Labels().Object()["owner"])
		assert.Equal(t, "no", next.Labels().Field(1)["pii"])
		next.Labels().Object()["owner"] = "changed-again"
	}
}

func TestLoadTableModifiedReplacesCachedETag(t *testing.T) {
	ctx := context.Background()
	cat, srv := newETagCatalog(t, nil)

	_, err := cat.LoadTable(ctx, etagIdent)
	require.NoError(t, err)

	srv.mu.Lock()
	srv.etag = `"v2"`
	srv.mu.Unlock()

	_, err = cat.LoadTable(ctx, etagIdent)
	require.NoError(t, err)
	assert.Equal(t, `"v1"`, srv.lastIfNoneMatch())

	_, err = cat.LoadTable(ctx, etagIdent)
	require.NoError(t, err)
	assert.Equal(t, `"v2"`, srv.lastIfNoneMatch())
}

func TestLoadTableWithoutETagIsNotCached(t *testing.T) {
	ctx := context.Background()
	cat, srv := newETagCatalog(t, nil)
	srv.etag = ""

	for range 2 {
		_, err := cat.LoadTable(ctx, etagIdent)
		require.NoError(t, err)
		assert.Empty(t, srv.lastIfNoneMatch())
	}
}

func TestLoadTableCacheDisabled(t *testing.T) {
	ctx := context.Background()
	cat, srv := newETagCatalog(t, iceberg.Properties{"rest-table-cache.max-entries": "0"})

	for range 2 {
		_, err := cat.LoadTable(ctx, etagIdent)
		require.NoError(t, err)
		assert.Empty(t, srv.lastIfNoneMatch())
	}
}

func TestLoadTableUnsolicitedNotModified(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
			"endpoints": rest.AllEndpointStrings,
		}))
	})
	mux.HandleFunc("/v1/namespaces/fokko/tables/table", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL, rest.WithOAuthToken(TestToken))
	require.NoError(t, err)

	_, err = cat.LoadTable(context.Background(), etagIdent)
	require.ErrorIs(t, err, rest.ErrRESTError)
}

func TestTableCacheInvalidation(t *testing.T) {
	tests := []struct {
		name string
		op   func(ctx context.Context, cat *rest.Catalog, srv *etagServer) error
	}{
		{"drop", func(ctx context.Context, cat *rest.Catalog, srv *etagServer) error {
			if err := cat.DropTable(ctx, etagIdent); err != nil {
				return err
			}
			srv.mu.Lock()
			srv.exists = true
			srv.mu.Unlock()

			return nil
		}},
		{"purge", func(ctx context.Context, cat *rest.Catalog, srv *etagServer) error {
			if err := cat.PurgeTable(ctx, etagIdent); err != nil {
				return err
			}
			srv.mu.Lock()
			srv.exists = true
			srv.mu.Unlock()

			return nil
		}},
		{"rename", func(ctx context.Context, cat *rest.Catalog, _ *etagServer) error {
			_, err := cat.RenameTable(ctx, etagIdent, catalog.ToIdentifier("fokko", "renamed"))

			return err
		}},
		{"exists reports missing", func(ctx context.Context, cat *rest.Catalog, srv *etagServer) error {
			srv.mu.Lock()
			srv.exists = false
			srv.mu.Unlock()
			if ok, err := cat.CheckTableExists(ctx, etagIdent); err != nil || ok {
				return assert.AnError
			}
			srv.mu.Lock()
			srv.exists = true
			srv.mu.Unlock()

			return nil
		}},
		{"load reports missing", func(ctx context.Context, cat *rest.Catalog, srv *etagServer) error {
			srv.mu.Lock()
			srv.exists = false
			srv.mu.Unlock()
			if _, err := cat.LoadTable(ctx, etagIdent); !assert.ErrorIs(t, err, catalog.ErrNoSuchTable) {
				return assert.AnError
			}
			srv.mu.Lock()
			srv.exists = true
			srv.mu.Unlock()

			return nil
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cat, srv := newETagCatalog(t, nil)

			_, err := cat.LoadTable(ctx, etagIdent)
			require.NoError(t, err)
			require.NoError(t, tt.op(ctx, cat, srv))

			_, err = cat.LoadTable(ctx, etagIdent)
			require.NoError(t, err)
			assert.Empty(t, srv.lastIfNoneMatch())
		})
	}
}

func TestTableCacheInvalidProperties(t *testing.T) {
	for _, props := range []iceberg.Properties{
		{"rest-table-cache.max-entries": "-1"},
		{"rest-table-cache.max-entries": "many"},
		{"rest-table-cache.expire-after-write-ms": "0"},
		{"rest-table-cache.expire-after-write-ms": "-5"},
	} {
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"defaults": map[string]any{}, "overrides": map[string]any{},
			}))
		})
		srv := httptest.NewServer(mux)

		_, err := rest.NewCatalog(context.Background(), "rest", srv.URL,
			rest.WithOAuthToken(TestToken), rest.WithAdditionalProps(props))
		assert.ErrorIs(t, err, rest.ErrRESTError, "%v", props)
		srv.Close()
	}
}
