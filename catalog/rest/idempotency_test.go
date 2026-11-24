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
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		replays      bool
	}{
		{"advertised", `{"idempotency-key-lifetime":"PT30M"}`, true, true},
		{"zero lifetime", `{"idempotency-key-lifetime":"PT0S"}`, true, false},
		{"malformed lifetime", `{"idempotency-key-lifetime":"soon"}`, true, false},
		{"absent", `{}`, false, false},
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
			if !tc.replays {
				require.Error(t, err)
				require.Len(t, got, 1)
				require.NoError(t, got[0].err)
				require.Equal(t, tc.advertised, got[0].key != "")
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

type keyedAttempt struct {
	method, path, key, body string
}

// newKeyedTestCatalog serves /v1/config with every endpoint plus config (a JSON
// object fragment such as `"idempotency-key-lifetime":"PT30M"`) and sends all
// other requests to h with their 1-based count.
func newKeyedTestCatalog(t *testing.T, config string, h func(n int, w http.ResponseWriter, r *http.Request)) (*Catalog, func() []keyedAttempt) {
	t.Helper()
	var endpoints []string
	for _, ep := range allEndpoints {
		endpoints = append(endpoints, ep.String())
	}
	eps, err := json.Marshal(endpoints)
	require.NoError(t, err)
	if config != "" {
		config = "," + config
	}
	var mu sync.Mutex
	var got []keyedAttempt
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/config" {
			_, _ = io.WriteString(w, `{"endpoints":`+string(eps)+config+`}`)

			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, keyedAttempt{r.Method, r.URL.Path, r.Header.Get(headerIdempotencyKey), string(body)})
		n := len(got)
		mu.Unlock()
		h(n, w, r)
	}))
	t.Cleanup(server.Close)
	c, err := newCatalogFromProps(t.Context(), "test", server.URL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c, func() []keyedAttempt {
		mu.Lock()
		defer mu.Unlock()

		return append([]keyedAttempt(nil), got...)
	}
}

func lifetimeConfig(v string) string { return `"idempotency-key-lifetime":"` + v + `"` }

func dropConnection(w http.ResponseWriter) {
	if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
		_ = conn.Close()
	}
}

func uuidV7At(created time.Time) string {
	var id uuid.UUID
	ms := created.UnixMilli()
	for i := range 6 {
		id[i] = byte(ms >> (8 * (5 - i)))
	}
	id[6], id[8] = 0x70, 0x80

	return id.String()
}

func TestKeyedResendReusesKeyAndBody(t *testing.T) {
	t.Parallel()
	ns := table.Identifier{"ns"}
	for _, status := range []int{500, 502, 503, 504} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT30M"), func(n int, w http.ResponseWriter, _ *http.Request) {
				if n == 1 {
					w.WriteHeader(status)

					return
				}
				_, _ = io.WriteString(w, `{}`)
			})
			require.NoError(t, c.CreateNamespace(t.Context(), ns, nil))
			got := attempts()
			require.Len(t, got, 2)
			require.NotEmpty(t, got[0].key)
			require.Equal(t, got[0], got[1])
			require.NotEmpty(t, got[0].body)
		})
	}
	t.Run("delete", func(t *testing.T) {
		t.Parallel()
		c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT30M"), func(n int, w http.ResponseWriter, _ *http.Request) {
			if n == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)

				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		require.NoError(t, c.DropNamespace(t.Context(), ns))
		got := attempts()
		require.Len(t, got, 2)
		require.NotEmpty(t, got[0].key)
		require.Equal(t, got[0], got[1])
	})
	t.Run("connection error", func(t *testing.T) {
		t.Parallel()
		// No warm connection, so net/http does not replay: this is the explicit resend.
		c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT30M"), func(n int, w http.ResponseWriter, _ *http.Request) {
			if n == 1 {
				dropConnection(w)

				return
			}
			_, _ = io.WriteString(w, `{}`)
		})
		require.NoError(t, c.CreateNamespace(t.Context(), ns, nil))
		got := attempts()
		require.Len(t, got, 2)
		require.NotEmpty(t, got[0].key)
		require.Equal(t, got[0], got[1])
	})
}

func TestKeyedSeparateCallsUseNewKeys(t *testing.T) {
	t.Parallel()
	id := table.Identifier{"ns", "name"}
	c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT30M"), func(_ int, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})
	// A transaction refreshed after a 409 is a new call: it must not reuse the key.
	for range 2 {
		_, _, err := c.CommitTable(t.Context(), id, nil, nil)
		require.ErrorIs(t, err, ErrCommitFailed)
	}
	got := attempts()
	require.Len(t, got, 2)
	require.NotEmpty(t, got[0].key)
	require.NotEqual(t, got[0].key, got[1].key)
}

func TestCommitResendConflictAfterExpiry(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{http.StatusConflict, `{"error":{"message":"requirement failed","type":"CommitFailedException","code":409}}`},
		{http.StatusBadRequest, `{"error":{"message":"requirement failed","type":"BadRequestException","code":400}}`},
	} {
		var applied atomic.Int32
		c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT1S"), func(n int, w http.ResponseWriter, _ *http.Request) {
			if n == 1 {
				applied.Add(1)
				dropConnection(w)
				return
			}
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, tc.body)
		})
		transport := c.cl.Transport
		sends := 0
		c.cl.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			sends++
			if sends == 2 {
				time.Sleep(time.Second)
			}
			return transport.RoundTrip(r)
		})
		_, _, err := c.CommitTable(t.Context(), table.Identifier{"ns", "name"}, nil, nil)
		require.ErrorIs(t, err, ErrCommitStateUnknown, tc.status)
		// else the table commit loop replays the commit or cleans up its files
		require.NotErrorIs(t, err, table.ErrCommitFailed, tc.status)
		require.NotErrorIs(t, err, table.ErrCommitRejected, tc.status)
		require.ErrorContains(t, err, "requirement failed")
		got := attempts()
		require.Len(t, got, 2)
		require.EqualValues(t, 1, applied.Load())
		require.NotEmpty(t, got[0].key)
		require.Equal(t, got[0], got[1])
	}
}

func TestKeyedResendTimerRechecksExpiry(t *testing.T) {
	for _, commit := range []bool{true, false} {
		start := time.Now()
		clockReads := 0
		kc := &keyedCall{policy: &idempotencyPolicy{}, commit: commit, key: "key", start: start, lifetime: time.Second,
			now: func() time.Time {
				clockReads++
				if clockReads > 1 {
					return start.Add(time.Second)
				}
				return start
			},
		}
		attempts := 0
		cl := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			return nil, io.EOF
		})}
		_, err := send(t.Context(), cl, kc, &reqConfig{}, nil, func() (*http.Request, error) {
			return http.NewRequestWithContext(t.Context(), http.MethodPost, "http://catalog.test", nil)
		})
		require.ErrorIs(t, err, io.EOF)
		if commit {
			require.ErrorIs(t, err, ErrCommitStateUnknown)
		} else {
			require.NotErrorIs(t, err, ErrCommitStateUnknown)
		}
		require.Equal(t, 1, attempts, "commit=%v", commit)
		require.Equal(t, 2, clockReads)
	}
}

func TestKeyedNoResendOn4xx(t *testing.T) {
	t.Parallel()
	for _, status := range []int{400, 401, 403, 404, 408, 409, 429} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT30M"), func(_ int, w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			})
			require.Error(t, c.CreateNamespace(t.Context(), table.Identifier{"ns"}, nil))
			require.Len(t, attempts(), 1)
		})
	}
}

func TestKeyedNoResendWithoutUsableLifetime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, config string
		keyed        bool
	}{
		{"absent", ``, false},
		{"null", `"idempotency-key-lifetime":null`, false},
		{"zero", lifetimeConfig("PT0S"), true},
		{"negative", lifetimeConfig("-PT1S"), true},
		{"malformed", lifetimeConfig("soon"), true},
		{"empty", lifetimeConfig(""), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, attempts := newKeyedTestCatalog(t, tc.config, func(_ int, w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			require.Error(t, c.CreateNamespace(t.Context(), table.Identifier{"ns"}, nil))
			got := attempts()
			require.Len(t, got, 1)
			require.Equal(t, tc.keyed, got[0].key != "")
		})
	}
}

func TestKeyedResendBudget(t *testing.T) {
	t.Parallel()
	c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT30M"), func(_ int, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	err := c.CreateNamespace(t.Context(), table.Identifier{"ns"}, nil)
	require.ErrorIs(t, err, ErrServiceUnavailable)
	require.Len(t, attempts(), 1+keyedMaxRetries)
}

func TestKeyedResendStopsAtExpiry(t *testing.T) {
	t.Parallel()
	ns, id := table.Identifier{"ns"}, table.Identifier{"ns", "name"}
	t.Run("next delay would exceed the window", func(t *testing.T) {
		t.Parallel()
		c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT1S"), func(_ int, w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		start := time.Now()
		err := c.CreateNamespace(t.Context(), ns, nil)
		require.ErrorIs(t, err, ErrServiceUnavailable)
		require.Len(t, attempts(), 1)
		require.Less(t, time.Since(start), time.Second)
	})
	t.Run("slow first attempt", func(t *testing.T) {
		t.Parallel()
		c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT1S"), func(_ int, w http.ResponseWriter, _ *http.Request) {
			time.Sleep(1100 * time.Millisecond)
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		err := c.CreateNamespace(t.Context(), ns, nil)
		require.ErrorIs(t, err, ErrServiceUnavailable)
		require.Len(t, attempts(), 1)
	})
	t.Run("commit reports unknown state", func(t *testing.T) {
		t.Parallel()
		c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT1S"), func(_ int, w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		_, _, err := c.CommitTable(t.Context(), id, nil, nil)
		require.ErrorIs(t, err, ErrCommitStateUnknown)
		require.Len(t, attempts(), 1)
	})
	t.Run("commit connection error reports unknown state", func(t *testing.T) {
		t.Parallel()
		for _, config := range []string{lifetimeConfig("PT0S"), lifetimeConfig("PT1S")} {
			c, attempts := newKeyedTestCatalog(t, config, func(_ int, w http.ResponseWriter, _ *http.Request) {
				dropConnection(w)
			})
			_, _, err := c.CommitTable(t.Context(), id, nil, nil)
			require.ErrorIs(t, err, ErrCommitStateUnknown)
			require.NotEmpty(t, attempts()[0].key)
		}
	})
	t.Run("unkeyed commit connection error is unchanged", func(t *testing.T) {
		t.Parallel()
		c, attempts := newKeyedTestCatalog(t, ``, func(_ int, w http.ResponseWriter, _ *http.Request) {
			dropConnection(w)
		})
		_, _, err := c.CommitTable(t.Context(), id, nil, nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrCommitStateUnknown)
		require.Len(t, attempts(), 1)
	})
}

func TestKeyedResendHonorsCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT2H"), func(_ int, w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusServiceUnavailable)
		cancel()
	})
	start := time.Now()
	require.ErrorIs(t, c.CreateNamespace(ctx, table.Identifier{"ns"}, nil), context.Canceled)
	require.Less(t, time.Since(start), time.Second)
	require.Len(t, attempts(), 1)
}

func TestKeyedTransportReplayWindow(t *testing.T) {
	t.Parallel()
	ns := table.Identifier{"ns"}
	// replayHandler drops the first POST on a warmed connection, which net/http
	// replays by itself unless GetBody refuses.
	replayHandler := func(delay time.Duration) func(int, http.ResponseWriter, *http.Request) {
		return func(n int, w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				_, _ = io.WriteString(w, `{"namespaces":[]}`)

				return
			}
			if n == 2 { // n counts the warm-up GET
				time.Sleep(delay)
				dropConnection(w)

				return
			}
			_, _ = io.WriteString(w, `{}`)
		}
	}
	run := func(t *testing.T, lifetime string, delay time.Duration) ([]keyedAttempt, error) {
		c, attempts := newKeyedTestCatalog(t, lifetimeConfig(lifetime), replayHandler(delay))
		_, err := c.ListNamespaces(t.Context(), nil)
		require.NoError(t, err)

		err = c.CreateNamespace(t.Context(), ns, nil)

		return attempts()[1:], err
	}
	t.Run("inside the window", func(t *testing.T) {
		t.Parallel()
		got, err := run(t, "PT30M", 0)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, got[0], got[1])
	})
	t.Run("after a slow first attempt", func(t *testing.T) {
		t.Parallel()
		got, err := run(t, "PT1S", 1100*time.Millisecond)
		require.Error(t, err)
		require.Len(t, got, 1)
	})
	t.Run("zero lifetime", func(t *testing.T) {
		t.Parallel()
		got, err := run(t, "PT0S", 0)
		require.Error(t, err)
		require.Len(t, got, 1)
	})
}

func TestKeyedRedirectReplayWindow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		lifetime string
		followed bool
	}{{"PT30M", true}, {"PT0S", false}} {
		t.Run(tc.lifetime, func(t *testing.T) {
			t.Parallel()
			c, attempts := newKeyedTestCatalog(t, lifetimeConfig(tc.lifetime), func(_ int, w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/namespaces" {
					w.Header().Set("Location", "/v1/namespaces/moved")
					w.WriteHeader(http.StatusTemporaryRedirect)

					return
				}
				_, _ = io.WriteString(w, `{}`)
			})
			err := c.CreateNamespace(t.Context(), table.Identifier{"ns"}, nil)
			got := attempts()
			if !tc.followed {
				require.Error(t, err)
				require.Len(t, got, 1)

				return
			}
			require.NoError(t, err)
			require.Len(t, got, 2)
			require.Equal(t, got[0].key, got[1].key)
			require.Equal(t, got[0].body, got[1].body)
		})
	}
}

// A keyed DELETE has no body, so net/http replays it on a warmed connection
// without consulting GetBody and the window cannot bound it. This test records
// that documented gap.
func TestKeyedDeleteTransportReplayIsNotBounded(t *testing.T) {
	t.Parallel()
	c, attempts := newKeyedTestCatalog(t, lifetimeConfig("PT0S"), func(n int, w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet:
			_, _ = io.WriteString(w, `{"namespaces":[]}`)
		case n == 2:
			dropConnection(w)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	_, err := c.ListNamespaces(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, c.DropNamespace(t.Context(), table.Identifier{"ns"}))
	require.Len(t, attempts()[1:], 2)
}

func TestParseKeyLifetime(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]time.Duration{
		"PT30M":                 30 * time.Minute,
		"PT1H30M":               90 * time.Minute,
		"P1DT2H3M4.5S":          26*time.Hour + 3*time.Minute + 4500*time.Millisecond,
		"P2D":                   48 * time.Hour,
		"PT0.25S":               250 * time.Millisecond,
		"PT999999999999999999H": math.MaxInt64,
	} {
		got, ok := parseKeyLifetime(in)
		require.True(t, ok, in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "P", "PT", "P1DT", "PT0S", "P0D", "-PT1S", "PT-1S", "30m", "PT1.5H", "pt30m", "PT30", "soon"} {
		_, ok := parseKeyLifetime(in)
		require.False(t, ok, in)
	}
}
