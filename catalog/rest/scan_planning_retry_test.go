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
	"fmt"
	"io"
	"net/http"
	"strconv"
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

func TestScanPlanningPOSTRetriesPreserveIdempotency(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"plan", "tasks"} {
		for _, status := range []int{408, 429, 500, 502, 503, 504} {
			t.Run(fmt.Sprintf("%s/%d", operation, status), func(t *testing.T) {
				t.Parallel()
				var attempts atomic.Int32
				var firstKey, firstBody string
				var mu sync.Mutex
				cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan, endpointFetchScanTasks}, func(mux *http.ServeMux) {
					mux.HandleFunc("/v1/namespaces/db/tables/tbl/"+operation, func(w http.ResponseWriter, req *http.Request) {
						mu.Lock()
						defer mu.Unlock()
						key := req.Header.Get(headerIdempotencyKey)
						body, err := io.ReadAll(req.Body)
						require.NoError(t, err)
						parsed, err := uuid.Parse(key)
						require.NoError(t, err)
						assert.Equal(t, uuid.Version(7), parsed.Version())
						if attempts.Add(1) == 1 {
							firstKey, firstBody = key, string(body)
							w.WriteHeader(status)

							return
						}
						assert.Equal(t, firstKey, key)
						assert.Equal(t, firstBody, string(body))
						if operation == "plan" {
							_, _ = w.Write([]byte(`{"status":"completed","plan-id":"p"}`))
						} else {
							_, _ = w.Write([]byte(`{}`))
						}
					})
				})
				var err error
				if operation == "plan" {
					_, err = cat.PlanTableScan(t.Context(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
				} else {
					_, err = cat.FetchScanTasks(t.Context(), table.Identifier{"db", "tbl"}, FetchScanTasksRequest{PlanTask: "opaque"})
				}
				require.NoError(t, err)
				assert.Equal(t, int32(2), attempts.Load())
			})
		}
	}
}

func TestScanPlanningPOSTDoesNotRetryTerminalResponses(t *testing.T) {
	t.Parallel()
	for _, status := range []int{200, 400, 401, 403, 404, 501} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
					attempts.Add(1)
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`invalid json`))
				})
			})
			_, err := cat.PlanTableScan(t.Context(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
			require.Error(t, err)
			assert.Equal(t, int32(1), attempts.Load())
		})
	}
}

func TestScanPlanningRetryBoundsAndCancellation(t *testing.T) {
	t.Parallel()
	t.Run("bounded", func(t *testing.T) {
		t.Parallel()
		attempts := 0
		_, err := retryScanPlanning(t.Context(), "plan", func() (int, error) {
			attempts++

			return 0, errorResponse{statusCode: http.StatusServiceUnavailable, wrapping: ErrServiceUnavailable}
		})
		require.ErrorIs(t, err, ErrServiceUnavailable)
		assert.Equal(t, 4, attempts)
	})
	t.Run("transport failure", func(t *testing.T) {
		attempts := 0
		want := errors.New("connection reset after submission")
		_, err := retryScanPlanning(t.Context(), "plan", func() (int, error) {
			attempts++

			return 0, want
		})
		require.ErrorIs(t, err, want)
		assert.Equal(t, 1, attempts)
	})
	t.Run("transport sentinel without HTTP response", func(t *testing.T) {
		attempts := 0
		_, err := retryScanPlanning(t.Context(), "plan", func() (int, error) {
			attempts++

			return 0, ErrServiceUnavailable
		})
		require.ErrorIs(t, err, ErrServiceUnavailable)
		assert.Equal(t, 1, attempts)
	})
	t.Run("cancel during retry after", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var attempts atomic.Int32
		cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
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
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := retryScanPlanning(ctx, "plan", func() (int, error) {
			t.Fatal("request after cancellation")

			return 0, nil
		})
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
				started := time.Now()
				attempts := 0
				result, err := retryScanPlanning(ctx, "plan", func() (int, error) {
					attempts++
					if attempts == 1 {
						return 0, errorResponse{statusCode: http.StatusTooManyRequests, retryAfter: "8"}
					}

					return 42, nil
				})
				assert.Equal(t, tc.wait, time.Since(started))
				if tc.deadline != 0 && tc.deadline < 8*time.Second {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					assert.Equal(t, 1, attempts)
					assert.Zero(t, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, 2, attempts)
					assert.Equal(t, 42, result)
				}
			})
		})
	}
}
