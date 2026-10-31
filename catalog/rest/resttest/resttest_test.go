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

package resttest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/catalogtest"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/catalog/rest/resttest"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCatalog(t *testing.T, url string) *rest.Catalog {
	t.Helper()

	cat, err := rest.NewCatalog(context.Background(), "resttest", url)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })

	return cat
}

func TestConformance(t *testing.T) {
	catalogtest.RunCatalogTests(t, catalogtest.Config{
		SupportsNamespaceProperties: true,
		SupportsRenameTable:         true,
		NewCatalog: func(t *testing.T) catalog.Catalog {
			return newCatalog(t, resttest.NewServer(t).URL)
		},
	})
}

// The prefix is only returned in the config response, so this also checks
// the client picks it up from there.
func TestConformanceWithPrefix(t *testing.T) {
	catalogtest.RunCatalogTests(t, catalogtest.Config{
		SupportsNamespaceProperties: true,
		SupportsRenameTable:         true,
		NewCatalog: func(t *testing.T) catalog.Catalog {
			return newCatalog(t, resttest.NewServer(t, resttest.WithPrefix("warehouses/wh1")).URL)
		},
	})
}

func createTable(t *testing.T, cat catalog.Catalog, props iceberg.Properties) (table.Identifier, *table.Table) {
	t.Helper()
	ctx := context.Background()

	ns := table.Identifier{"db"}
	if exists, err := cat.CheckNamespaceExists(ctx, ns); err == nil && !exists {
		require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	}
	ident := table.Identifier{"db", fmt.Sprintf("tbl_%d", tableSeq.Add(1))}
	tbl, err := cat.CreateTable(ctx, ident, catalogtest.Schema, catalog.WithProperties(props))
	require.NoError(t, err)

	return ident, tbl
}

var tableSeq atomic.Int64

func addColumn(t *testing.T, tbl *table.Table, name string) *table.Transaction {
	t.Helper()

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn([]string{name}, iceberg.PrimitiveTypes.String, "", false, nil).
		Commit())

	return txn
}

// Two writers start from the same metadata; the second commit's schema
// requirement no longer holds and fails as a conflict, and the table keeps
// the first writer's change.
func TestConcurrentSchemaCommitsConflict(t *testing.T) {
	ctx := context.Background()
	cat := newCatalog(t, resttest.NewServer(t).URL)
	ident, first := createTable(t, cat, iceberg.Properties{table.CommitNumRetriesKey: "0"})

	second, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)

	_, err = addColumn(t, first, "winner").Commit(ctx)
	require.NoError(t, err)

	_, err = addColumn(t, second, "loser").Commit(ctx)
	require.ErrorIs(t, err, table.ErrCommitFailed)

	current, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	_, ok := current.Schema().FindFieldByName("winner")
	assert.True(t, ok)
	_, ok = current.Schema().FindFieldByName("loser")
	assert.False(t, ok)
}

// Property updates carry no requirement that a concurrent writer breaks, so
// many writers committing at once must all land without losing an update.
func TestConcurrentPropertyCommits(t *testing.T) {
	ctx := context.Background()
	cat := newCatalog(t, resttest.NewServer(t).URL)
	ident, _ := createTable(t, cat, nil)

	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		wg.Go(func() {
			tbl, err := cat.LoadTable(ctx, ident)
			if err != nil {
				errs <- err

				return
			}
			txn := tbl.NewTransaction()
			if err := txn.SetProperties(iceberg.Properties{fmt.Sprintf("writer-%d", i): "1"}); err != nil {
				errs <- err

				return
			}
			_, err = txn.Commit(ctx)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	tbl, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	for i := range writers {
		assert.Equal(t, "1", tbl.Properties()[fmt.Sprintf("writer-%d", i)])
	}
}

func TestExistsChecksUseHead(t *testing.T) {
	var heads atomic.Int64
	h := resttest.NewHandler(resttest.WithWarehouse("file://" + t.TempDir()))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			heads.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	ctx := context.Background()
	cat := newCatalog(t, srv.URL)
	ident, _ := createTable(t, cat, nil)

	exists, err := cat.CheckNamespaceExists(ctx, table.Identifier{"missing"})
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = cat.CheckTableExists(ctx, ident)
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = cat.CheckTableExists(ctx, table.Identifier{"db", "missing"})
	require.NoError(t, err)
	assert.False(t, exists)

	assert.GreaterOrEqual(t, heads.Load(), int64(3))
}

func TestRegisterTable(t *testing.T) {
	ctx := context.Background()
	cat := newCatalog(t, resttest.NewServer(t).URL)
	ident, tbl := createTable(t, cat, nil)

	registered := table.Identifier{"db", "registered"}
	got, err := cat.RegisterTable(ctx, registered, tbl.MetadataLocation())
	require.NoError(t, err)
	assert.Equal(t, tbl.Metadata().TableUUID(), got.Metadata().TableUUID())
	assert.True(t, got.Schema().Equals(catalogtest.TableSchema))

	_, err = cat.RegisterTable(ctx, ident, tbl.MetadataLocation())
	require.ErrorIs(t, err, catalog.ErrTableAlreadyExists)

	_, err = cat.RegisterTable(ctx, table.Identifier{"missing", "t"}, tbl.MetadataLocation())
	require.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
}

func TestUnsupportedRoutesReturnNotImplemented(t *testing.T) {
	srv := resttest.NewServer(t)

	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1/transactions/commit"},
		{http.MethodGet, "/v1/namespaces/db/views"},
		{http.MethodPost, "/v1/namespaces/db/tables/t/plan"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), tc.method, srv.URL+tc.path, strings.NewReader("{}"))
			require.NoError(t, err)
			rsp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer rsp.Body.Close()

			assert.Equal(t, http.StatusNotImplemented, rsp.StatusCode)
			var body struct {
				Error struct {
					Type string `json:"type"`
					Code int    `json:"code"`
				} `json:"error"`
			}
			require.NoError(t, json.NewDecoder(rsp.Body).Decode(&body))
			assert.Equal(t, "NotImplementedException", body.Error.Type)
			assert.Equal(t, http.StatusNotImplemented, body.Error.Code)
		})
	}
}

func TestCreateTableWithoutWarehouseFails(t *testing.T) {
	srv := httptest.NewServer(resttest.NewHandler())
	t.Cleanup(srv.Close)

	ctx := context.Background()
	cat := newCatalog(t, srv.URL)
	require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{"db"}, nil))

	_, err := cat.CreateTable(ctx, table.Identifier{"db", "t"}, catalogtest.Schema)
	require.ErrorIs(t, err, rest.ErrBadRequest)

	loc := "file://" + t.TempDir() + "/t"
	tbl, err := cat.CreateTable(ctx, table.Identifier{"db", "t"}, catalogtest.Schema, catalog.WithLocation(loc))
	require.NoError(t, err)
	assert.Equal(t, loc, tbl.Location())
}
