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
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var scanPostEndpoints = []endpoint{endpointPlanTableScan, endpointFetchScanTasks}

// scanPOST calls PlanTableScan or FetchScanTasks, whichever operation names.
func scanPOST(t *testing.T, cat *Catalog, operation string, key *string) error {
	t.Helper()
	ident := table.Identifier{"db", "tbl"}
	if operation == "plan" {
		_, err := cat.PlanTableScan(t.Context(), ident, PlanTableScanRequest{IdempotencyKey: key})

		return err
	}
	_, err := cat.FetchScanTasks(t.Context(), ident, FetchScanTasksRequest{PlanTask: "opaque", IdempotencyKey: key})

	return err
}

func scanOK(w http.ResponseWriter, operation string) {
	if operation == "plan" {
		_, _ = w.Write([]byte(`{"status":"completed","plan-id":"p"}`))
	} else {
		_, _ = w.Write([]byte(`{}`))
	}
}

// scanRecorder is a plan/tasks handler that records each attempt's key and body
// and answers with respond(n), the 1-based attempt number.
type scanRecorder struct {
	mu   sync.Mutex
	got  []keyedAttempt
	resp func(n int, w http.ResponseWriter)
}

func (s *scanRecorder) register(operation string) func(*http.ServeMux) {
	return func(mux *http.ServeMux) {
		mux.HandleFunc("POST /v1/namespaces/db/tables/tbl/"+operation, func(w http.ResponseWriter, req *http.Request) {
			body, _ := io.ReadAll(req.Body)
			s.mu.Lock()
			s.got = append(s.got, keyedAttempt{req.Method, req.URL.Path, req.Header.Get(headerIdempotencyKey), string(body)})
			n := len(s.got)
			s.mu.Unlock()
			s.resp(n, w)
		})
	}
}

func (s *scanRecorder) attempts() []keyedAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]keyedAttempt(nil), s.got...)
}

func TestScanPlanningPOSTRetriesPreserveIdempotency(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"plan", "tasks"} {
		for _, status := range []int{500, 502, 503, 504} {
			t.Run(operation+"/"+strconv.Itoa(status), func(t *testing.T) {
				t.Parallel()
				rec := &scanRecorder{resp: func(n int, w http.ResponseWriter) {
					if n == 1 {
						w.WriteHeader(status)

						return
					}
					scanOK(w, operation)
				}}
				cat := newKeyedScanPlanningTestCatalog(t, "PT30M", scanPostEndpoints, rec.register(operation))
				require.NoError(t, scanPOST(t, cat, operation, nil))
				got := rec.attempts()
				require.Len(t, got, 2)
				parsed, err := uuid.Parse(got[0].key)
				require.NoError(t, err)
				assert.Equal(t, uuid.Version(7), parsed.Version())
				assert.Equal(t, got[0], got[1])
			})
		}
	}
}

func TestScanPlanningPOSTDoesNotResendClientErrorsOrTerminalResponses(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"plan", "tasks"} {
		for _, status := range []int{400, 401, 403, 404, 408, 429, 501} {
			t.Run(operation+"/"+strconv.Itoa(status), func(t *testing.T) {
				t.Parallel()
				rec := &scanRecorder{resp: func(_ int, w http.ResponseWriter) {
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`invalid json`))
				}}
				cat := newKeyedScanPlanningTestCatalog(t, "PT30M", scanPostEndpoints, rec.register(operation))
				require.Error(t, scanPOST(t, cat, operation, nil))
				assert.Len(t, rec.attempts(), 1)
			})
		}
	}
}

func TestScanPlanningPOSTDoesNotRetryMalformedSuccess(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"plan", "tasks"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			rec := &scanRecorder{resp: func(_ int, w http.ResponseWriter) { _, _ = w.Write([]byte(`invalid json`)) }}
			cat := newKeyedScanPlanningTestCatalog(t, "PT30M", scanPostEndpoints, rec.register(operation))
			err := scanPOST(t, cat, operation, nil)
			require.ErrorIs(t, err, ErrRESTError)
			require.ErrorContains(t, err, "error decoding json payload")
			var responseError errorResponse
			assert.False(t, errors.As(err, &responseError))
			assert.Len(t, rec.attempts(), 1)
		})
	}
}

func TestScanPlanningPOSTKeyWithoutAdvertisedLifetime(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"plan", "tasks"} {
		t.Run(operation+"/generated key is not sent", func(t *testing.T) {
			t.Parallel()
			rec := &scanRecorder{resp: func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) }}
			cat := newScanPlanningTestCatalog(t, scanPostEndpoints, rec.register(operation))
			require.Error(t, scanPOST(t, cat, operation, nil))
			got := rec.attempts()
			require.Len(t, got, 1)
			assert.Empty(t, got[0].key)
		})
		t.Run(operation+"/caller key is sent once", func(t *testing.T) {
			t.Parallel()
			key := uuidV7At(time.Now())
			rec := &scanRecorder{resp: func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) }}
			cat := newScanPlanningTestCatalog(t, scanPostEndpoints, rec.register(operation))
			require.Error(t, scanPOST(t, cat, operation, &key))
			got := rec.attempts()
			require.Len(t, got, 1)
			assert.Equal(t, key, got[0].key)
		})
	}
}

func TestScanPlanningPOSTCallerKeyAgeCountsAgainstLifetime(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"plan", "tasks"} {
		for _, tc := range []struct {
			name    string
			created time.Duration // offset from now
			resends bool
		}{
			{"fresh key", 0, true},
			{"key older than the lifetime", -2 * time.Hour, false},
			{"future timestamp does not lengthen the window", time.Hour, true},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				key := uuidV7At(time.Now().Add(tc.created))
				rec := &scanRecorder{resp: func(n int, w http.ResponseWriter) {
					if n == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)

						return
					}
					scanOK(w, operation)
				}}
				cat := newKeyedScanPlanningTestCatalog(t, "PT30M", scanPostEndpoints, rec.register(operation))
				err := scanPOST(t, cat, operation, &key)
				got := rec.attempts()
				require.NotEmpty(t, got)
				assert.Equal(t, key, got[0].key)
				if tc.resends {
					require.NoError(t, err)
					assert.Len(t, got, 2)
				} else {
					require.Error(t, err)
					assert.Len(t, got, 1)
				}
			})
		}
	}
}

func TestScanPlanningPOSTResendsConnectionError(t *testing.T) {
	t.Parallel()
	rec := &scanRecorder{resp: func(n int, w http.ResponseWriter) {
		if n == 1 {
			dropConnection(w)

			return
		}
		scanOK(w, "plan")
	}}
	cat := newKeyedScanPlanningTestCatalog(t, "PT30M", scanPostEndpoints, rec.register("plan"))
	require.NoError(t, scanPOST(t, cat, "plan", nil))
	got := rec.attempts()
	require.Len(t, got, 2)
	assert.Equal(t, got[0], got[1])
}

func TestScanPlanningRetryBoundsAndCancellation(t *testing.T) {
	t.Parallel()
	t.Run("bounded", func(t *testing.T) {
		t.Parallel()
		rec := &scanRecorder{resp: func(_ int, w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) }}
		cat := newKeyedScanPlanningTestCatalog(t, "PT30M", scanPostEndpoints, rec.register("plan"))
		require.ErrorIs(t, scanPOST(t, cat, "plan", nil), ErrServiceUnavailable)
		assert.Len(t, rec.attempts(), 4)
	})
	t.Run("cancel during retry after", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var attempts atomic.Int32
		cat := newKeyedScanPlanningTestCatalog(t, "PT2H", scanPostEndpoints, func(mux *http.ServeMux) {
			mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
				attempts.Add(1)
				w.Header().Set("Retry-After", "3600")
				w.WriteHeader(http.StatusServiceUnavailable)
				cancel()
			})
		})
		started := time.Now()
		_, err := cat.PlanTableScan(ctx, table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
		require.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(started), time.Second)
		assert.Equal(t, int32(1), attempts.Load())
	})
	t.Run("already cancelled", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		cat := newKeyedScanPlanningTestCatalog(t, "PT30M", scanPostEndpoints, func(mux *http.ServeMux) {
			mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(http.ResponseWriter, *http.Request) {
				t.Error("request after cancellation")
			})
		})
		_, err := cat.PlanTableScan(ctx, table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestScanPlanningRetryAfterTiming(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		deadline time.Duration
		wait     time.Duration
	}{
		{name: "capped without deadline", wait: 5 * time.Second},
		{name: "honored with deadline", deadline: 10 * time.Second, wait: 8 * time.Second},
		{name: "deadline interrupts wait", deadline: 2 * time.Second, wait: 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				if tc.deadline != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.deadline)
					defer cancel()
				}
				// An in-memory transport: synctest cannot advance time over real sockets.
				attempts := 0
				reply := func(status int, header http.Header, body string) (*http.Response, error) {
					return &http.Response{
						StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)),
						ContentLength: int64(len(body)),
					}, nil
				}
				cat, err := NewCatalog(ctx, "rest", "http://catalog.test", WithCustomTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/v1/config" {
						return reply(http.StatusOK, nil, `{"endpoints":["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"],"idempotency-key-lifetime":"PT1H"}`)
					}
					if attempts++; attempts == 1 {
						return reply(http.StatusServiceUnavailable, http.Header{"Retry-After": {"8"}}, "")
					}

					return reply(http.StatusOK, nil, `{"status":"completed","plan-id":"p"}`)
				})))
				require.NoError(t, err)
				started := time.Now()
				_, err = cat.PlanTableScan(ctx, table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
				assert.Equal(t, tc.wait, time.Since(started))
				if tc.deadline != 0 && tc.deadline < 8*time.Second {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					assert.Equal(t, 1, attempts)
				} else {
					require.NoError(t, err)
					assert.Equal(t, 2, attempts)
				}
			})
		})
	}
}
