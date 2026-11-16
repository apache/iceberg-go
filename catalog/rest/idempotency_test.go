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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMutationIdempotencyHeaders(t *testing.T) {
	for _, tc := range []struct {
		name, config string
		want         bool
	}{
		{"advertised", `"idempotency-key-lifetime":"PT30M"`, true},
		{"absent", `"other":"value"`, false},
		{"malformed", `"idempotency-key-lifetime":"not-a-duration"`, true},
		{"empty", `"idempotency-key-lifetime":""`, true},
		{"null", `"idempotency-key-lifetime":null`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var lastMethod, lastPath, lastKey string
			var endpoints []string
			for _, ep := range allEndpoints {
				endpoints = append(endpoints, ep.String())
			}
			eps, err := json.Marshal(endpoints)
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/config" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"endpoints":` + string(eps) + `,` + tc.config + `}`))

					return
				}
				mu.Lock()
				lastMethod, lastPath, lastKey = r.Method, r.URL.Path, r.Header.Get(headerIdempotencyKey)
				mu.Unlock()
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer server.Close()
			ctx := context.Background()
			c, err := newCatalogFromProps(ctx, "test", server.URL, nil)
			require.NoError(t, err)
			defer c.Close()
			id := table.Identifier{"ns", "name"}
			dst := table.Identifier{"ns", "other"}
			ns := table.Identifier{"ns"}
			tests := []struct {
				name, method, path string
				call               func()
			}{
				{"cancel planning", "DELETE", "/v1/namespaces/ns/tables/name/plan/plan-id", func() { _ = c.CancelPlanning(ctx, id, "plan-id") }},
				{"create namespace", "POST", "/v1/namespaces", func() { _ = c.CreateNamespace(ctx, ns, nil) }},
				{"drop namespace", "DELETE", "/v1/namespaces/ns", func() { _ = c.DropNamespace(ctx, ns) }},
				{"update namespace", "POST", "/v1/namespaces/ns/properties", func() { _, _ = c.UpdateNamespaceProperties(ctx, ns, nil, nil) }},
				{"create table", "POST", "/v1/namespaces/ns/tables", func() { _, _ = c.CreateTable(ctx, id, iceberg.NewSchema(0)) }},
				{"register table", "POST", "/v1/namespaces/ns/register", func() { _, _ = c.RegisterTable(ctx, id, "s3://metadata") }},
				{"commit table", "POST", "/v1/namespaces/ns/tables/name", func() { _, _, _ = c.CommitTable(ctx, id, nil, nil) }},
				{"update table", "POST", "/v1/namespaces/ns/tables/name", func() { _, _ = c.UpdateTable(ctx, id, nil, nil) }},
				{"drop table", "DELETE", "/v1/namespaces/ns/tables/name", func() { _ = c.DropTable(ctx, id) }},
				{"purge table", "DELETE", "/v1/namespaces/ns/tables/name", func() { _ = c.PurgeTable(ctx, id) }},
				{"rename table", "POST", "/v1/tables/rename", func() { _, _ = c.RenameTable(ctx, id, dst) }},
				{"commit transaction", "POST", "/v1/transactions/commit", func() { _ = c.CommitTransaction(ctx, []table.TableCommit{{Identifier: id}}) }},
				{"create view", "POST", "/v1/namespaces/ns/views", func() { _, _ = c.CreateView(ctx, id, &view.Version{}, iceberg.NewSchema(0)) }},
				{"update view", "POST", "/v1/namespaces/ns/views/name", func() { _, _ = c.UpdateView(ctx, id, nil, nil) }},
				{"drop view", "DELETE", "/v1/namespaces/ns/views/name", func() { _ = c.DropView(ctx, id) }},
				{"rename view", "POST", "/v1/views/rename", func() { _, _ = c.RenameView(ctx, id, dst) }},
				{"register view", "POST", "/v1/namespaces/ns/register-view", func() { _, _ = c.RegisterView(ctx, id, "s3://metadata") }},
			}
			seen := map[string]bool{}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					for range 2 {
						mu.Lock()
						lastMethod, lastPath, lastKey = "", "", ""
						mu.Unlock()
						test.call()
						mu.Lock()
						method, path, keyValue := lastMethod, lastPath, lastKey
						mu.Unlock()
						require.Equal(t, test.method, method)
						require.Equal(t, test.path, path)
						if !tc.want {
							require.Empty(t, keyValue)

							continue
						}
						key, err := uuid.Parse(keyValue)
						require.NoError(t, err)
						require.Equal(t, uuid.Version(7), key.Version())
						require.Equal(t, uuid.RFC4122, key.Variant())
						require.Equal(t, strings.ToLower(key.String()), keyValue)
						require.False(t, seen[keyValue], "key reused across operations")
						seen[keyValue] = true
					}
				})
			}
			for _, read := range []func(){func() { _, _ = c.LoadTable(ctx, id) }, func() { _, _ = c.ListNamespaces(ctx, nil) }} {
				mu.Lock()
				lastMethod, lastPath, lastKey = "", "", ""
				mu.Unlock()
				read()
				mu.Lock()
				require.NotEmpty(t, lastPath)
				require.Empty(t, lastKey)
				mu.Unlock()
			}
		})
	}
}

func TestDefaultIdempotencyHeader(t *testing.T) {
	for _, tc := range []struct {
		name, config string
		advertised   bool
	}{
		{"advertised", `{"idempotency-key-lifetime":"PT30M"}`, true},
		{"absent", `{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/config" {
					_, _ = w.Write([]byte(tc.config))

					return
				}
				keys <- r.Header.Get(headerIdempotencyKey)
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer server.Close()
			ctx := context.Background()
			c, err := NewCatalog(ctx, "test", server.URL, WithHeaders(map[string]string{"idempotency-key": "static"}))
			require.NoError(t, err)
			defer c.Close()
			_, _ = c.ListNamespaces(ctx, nil)
			_ = c.CreateNamespace(ctx, table.Identifier{"ns"}, nil)
			readKey, mutationKey := <-keys, <-keys
			if tc.advertised {
				require.Empty(t, readKey)
				key, err := uuid.Parse(mutationKey)
				require.NoError(t, err)
				require.Equal(t, uuid.Version(7), key.Version())
			} else {
				require.Equal(t, "static", readKey)
				require.Equal(t, "static", mutationKey)
			}
		})
	}
}

func TestMutationTransportReplay(t *testing.T) {
	for _, tc := range []struct {
		name, config string
		advertised   bool
	}{
		{"advertised", `{"idempotency-key-lifetime":"PT30M"}`, true},
		{"zero lifetime", `{"idempotency-key-lifetime":"PT0S"}`, true},
		{"absent", `{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type attempt struct {
				key  string
				body []byte
				err  error
			}
			var mu sync.Mutex
			var attempts []attempt
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/v1/config":
					_, _ = io.WriteString(w, tc.config)
				case r.Method == http.MethodGet:
					_, _ = io.WriteString(w, `{"namespaces":[]}`)
				default:
					body, err := io.ReadAll(r.Body)
					mu.Lock()
					attempts = append(attempts, attempt{r.Header.Get(headerIdempotencyKey), body, err})
					count := len(attempts)
					mu.Unlock()
					if count == 1 {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err == nil {
							_ = conn.Close()
						}

						return
					}
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			defer server.Close()
			ctx := context.Background()
			c, err := newCatalogFromProps(ctx, "test", server.URL, nil)
			require.NoError(t, err)
			defer c.Close()
			_, err = c.ListNamespaces(ctx, nil)
			require.NoError(t, err)
			err = c.CreateNamespace(ctx, table.Identifier{"ns"}, nil)
			mu.Lock()
			got := append([]attempt(nil), attempts...)
			mu.Unlock()
			if !tc.advertised {
				require.Error(t, err)
				require.Len(t, got, 1)
				require.NoError(t, got[0].err)
				require.Empty(t, got[0].key)
				require.NotEmpty(t, got[0].body)

				return
			}
			require.NoError(t, err)
			require.Len(t, got, 2)
			require.NoError(t, got[0].err)
			require.NoError(t, got[1].err)
			require.NotEmpty(t, got[0].key)
			require.NotEmpty(t, got[0].body)
			require.Equal(t, got[0].key, got[1].key)
			require.Equal(t, got[0].body, got[1].body)
		})
	}
}
