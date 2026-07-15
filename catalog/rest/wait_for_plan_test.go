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
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastWaitOpts keeps the polling backoff sub-millisecond so multi-poll tests run
// quickly; the poll count is driven by the fake server's responses, not the delay.
var fastWaitOpts = WaitForPlanOptions{MinDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}

func TestWaitForPlanCompletesAfterPolling(t *testing.T) {
	t.Parallel()

	var polls atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			// Two "submitted" responses, then a "completed" one carrying tasks and
			// vended credentials.
			if polls.Add(1) <= 2 {
				_, err := w.Write([]byte(`{"status":"submitted"}`))
				require.NoError(t, err)

				return
			}
			_, err := w.Write([]byte(`{"status":"completed","plan-tasks":["t1","t2"],` +
				`"file-scan-tasks":[{}],"delete-files":[{}],` +
				`"storage-credentials":[{"prefix":"s3://bucket/","config":{"k":"v"}}]}`))
			require.NoError(t, err)
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.NoError(t, err)

	assert.Equal(t, PlanStatusCompleted, res.Status)
	assert.Equal(t, []string{"t1", "t2"}, res.PlanTasks)
	assert.Len(t, res.FileScanTasks, 1)
	assert.Len(t, res.DeleteFiles, 1)
	require.Len(t, res.StorageCredentials, 1)
	assert.Equal(t, "s3://bucket/", res.StorageCredentials[0].Prefix)
	// Two submitted polls plus the completing poll.
	assert.Equal(t, int32(3), polls.Load())
}

func TestWaitForPlanReturnsImmediatelyWhenCompleted(t *testing.T) {
	t.Parallel()

	var polls atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			polls.Add(1)
			_, err := w.Write([]byte(`{"status":"completed","plan-tasks":["t1"]}`))
			require.NoError(t, err)
		})
	})

	// Zero-value options: exercises the DefaultWaitForPlanOptions fallback. A
	// first-poll completion means no backoff sleep is incurred despite the
	// conservative default delays.
	res, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", WaitForPlanOptions{})
	require.NoError(t, err)

	assert.Equal(t, PlanStatusCompleted, res.Status)
	assert.Equal(t, []string{"t1"}, res.PlanTasks)
	assert.Equal(t, int32(1), polls.Load())
}

func TestWaitForPlanPropagatesFailed(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"failed","error":{"message":"boom","type":"ValidationException","code":400}}`))
			require.NoError(t, err)
		})
	})

	_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.ErrorIs(t, err, ErrPlanFailed)

	var pfe *PlanFailedError
	require.ErrorAs(t, err, &pfe)
	require.NotNil(t, pfe.Detail)
	assert.Equal(t, "boom", pfe.Detail.Message)
}

func TestWaitForPlanPropagatesFailedWithoutUsableError(t *testing.T) {
	t.Parallel()

	for _, payload := range []string{
		`{"status":"failed"}`,
		`{"status":"failed","error":"oops"}`,
	} {
		cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
			mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
				_, err := w.Write([]byte(payload))
				require.NoError(t, err)
			})
		})

		_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
		require.ErrorIs(t, err, ErrPlanFailed)
		var pfe *PlanFailedError
		require.ErrorAs(t, err, &pfe)
		assert.Nil(t, pfe.Detail)
	}
}

func TestWaitForPlanPropagatesCancelled(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"cancelled"}`))
			require.NoError(t, err)
		})
	})

	_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.ErrorIs(t, err, ErrPlanCancelled)
}

func TestWaitForPlanPropagatesExpired(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			writeRESTNotFound(t, w, errTypeNoSuchPlanID)
		})
	})

	_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.ErrorIs(t, err, ErrPlanExpired)
}

func TestWaitForPlanRetriesServiceUnavailable(t *testing.T) {
	t.Parallel()

	var polls atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			if polls.Add(1) <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)

				return
			}
			_, err := w.Write([]byte(`{"status":"completed","plan-tasks":["t1"]}`))
			require.NoError(t, err)
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.NoError(t, err)
	assert.Equal(t, PlanStatusCompleted, res.Status)
	assert.Equal(t, []string{"t1"}, res.PlanTasks)
	// Two 503s plus the completing poll.
	assert.Equal(t, int32(3), polls.Load())
}

func TestWaitForPlanRetriesJavaIdempotentGETStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range []int{
		http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()

			var polls atomic.Int32
			cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
					if polls.Add(1) == 1 {
						w.WriteHeader(status)

						return
					}
					_, err := w.Write([]byte(`{"status":"completed"}`))
					require.NoError(t, err)
				})
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
			require.NoError(t, err)
			assert.Equal(t, int32(2), polls.Load())
		})
	}
}

func TestWaitForPlanRetriesStatusWithMalformedErrorBody(t *testing.T) {
	t.Parallel()

	var polls atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			if polls.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, err := w.Write([]byte("not json"))
				require.NoError(t, err)

				return
			}
			_, err := w.Write([]byte(`{"status":"completed"}`))
			require.NoError(t, err)
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.NoError(t, err)
	assert.Equal(t, int32(2), polls.Load())
}

func TestWaitForPlanPropagatesTerminalServerStatuses(t *testing.T) {
	t.Parallel()

	// Statuses outside Java's idempotent retry set remain terminal.
	for _, tc := range []struct {
		status int
		want   error
	}{
		{http.StatusNotImplemented, iceberg.ErrNotImplemented},
		{http.StatusHTTPVersionNotSupported, ErrServerError},
	} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			t.Parallel()

			var polls atomic.Int32
			cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
					polls.Add(1)
					w.WriteHeader(tc.status)
				})
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
			require.ErrorIs(t, err, tc.want)
			assert.Equal(t, int32(1), polls.Load(), "a terminal status must not be retried")
		})
	}
}

func TestFetchPlanningResultPreservesRetryAfterForPoller(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusServiceUnavailable)
		})
	})

	_, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", FetchPlanningResultOptions{})
	require.Error(t, err)
	delay, retryable := scanPlanPollRetry(err)
	assert.True(t, retryable)
	assert.Equal(t, 2*time.Second, delay)
}

func TestApplyRetryAfter(t *testing.T) {
	t.Parallel()

	const (
		minD = time.Millisecond
		maxD = 5 * time.Second
	)
	backoff := 250 * time.Millisecond

	cases := []struct {
		name        string
		retryAfter  time.Duration
		hasDeadline bool
		want        time.Duration
	}{
		{"no hint keeps backoff", 0, false, backoff},
		{"no hint keeps backoff (deadline)", 0, true, backoff},
		{"hint within bounds is honoured", 2 * time.Second, false, 2 * time.Second},
		{"tiny hint floored at minDelay", time.Microsecond, false, minD},
		{"large hint clamped to maxDelay without deadline", 30 * time.Second, false, maxD},
		{"large hint honoured in full with deadline", 30 * time.Second, true, 30 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := applyRetryAfter(backoff, tc.retryAfter, minD, maxD, tc.hasDeadline)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 2*time.Second, parseRetryAfter(" 2 "))
	assert.Zero(t, parseRetryAfter("0"))
	assert.Zero(t, parseRetryAfter("invalid"))

	retryAt := time.Now().Add(10 * time.Second).UTC().Truncate(time.Second)
	delay := parseRetryAfter(retryAt.Format(http.TimeFormat))
	assert.Greater(t, delay, 8*time.Second)
	assert.LessOrEqual(t, delay, 10*time.Second)
}

func TestWaitForPlanServiceUnavailableUntilDeadline(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			// The server never recovers; the poll must ride the backoff until the
			// caller's context expires rather than surface the 503.
			w.WriteHeader(http.StatusServiceUnavailable)
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// Unlimited retries so this isolates the deadline path from the MaxRetries cap.
	opts := fastWaitOpts
	opts.MaxRetries = -1
	_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", opts)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWaitForPlanCancelsServerSideOnContextCancel(t *testing.T) {
	t.Parallel()

	var deleteHit atomic.Bool
	polled := make(chan struct{}, 8)
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult, endpointCancelPlanning}, func(mux *http.ServeMux) {
		// The plan never completes: it stays submitted until the caller's context
		// is cancelled.
		mux.HandleFunc("GET /v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			select {
			case polled <- struct{}{}:
			default:
			}
			_, err := w.Write([]byte(`{"status":"submitted"}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("DELETE /v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			deleteHit.Store(true)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-polled
		cancel()
	}()

	_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.ErrorIs(t, err, context.Canceled)
	// The cancel is synchronous: abandonPlan issues the DELETE and only then does
	// WaitForPlan return, so the server-side plan is released before a short-lived
	// caller could exit. It is guaranteed observed by now.
	assert.True(t, deleteHit.Load(), "expected a best-effort server-side cancel before return")
}

func TestWaitForPlanBoundsSlowServerSideCancel(t *testing.T) {
	t.Parallel()

	var cancelStarted atomic.Bool
	polled := make(chan struct{}, 8)
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult, endpointCancelPlanning}, func(mux *http.ServeMux) {
		mux.HandleFunc("GET /v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			select {
			case polled <- struct{}{}:
			default:
			}
			_, err := w.Write([]byte(`{"status":"submitted"}`))
			require.NoError(t, err)
		})
		// The cancel endpoint stalls: it never responds until the client (bounded
		// by the grace) gives up and drops the connection.
		mux.HandleFunc("DELETE /v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			cancelStarted.Store(true)
			<-req.Context().Done()
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-polled
		cancel()
	}()

	opts := fastWaitOpts
	opts.CancelGracePeriod = 100 * time.Millisecond

	// Run in a goroutine: without the grace bound a stalled cancel would hang
	// WaitForPlan forever (its DELETE runs on a context detached from the caller's
	// cancellation), so the failure mode is a hang, caught by the select timeout.
	done := make(chan error, 1)
	go func() {
		_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", opts)
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, cancelStarted.Load(), "expected the server-side cancel to be attempted")
	case <-time.After(3 * time.Second):
		t.Fatal("WaitForPlan hung on a stalled server-side cancel; CancelGracePeriod not enforced")
	}
}

func TestWaitForPlanReturnsDeadlineWhileSubmitted(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"submitted"}`))
			require.NoError(t, err)
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// Unlimited retries so this isolates the deadline path from the MaxRetries cap.
	opts := fastWaitOpts
	opts.MaxRetries = -1
	_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", opts)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWaitForPlanExhaustsMaxRetries(t *testing.T) {
	t.Parallel()

	var polls atomic.Int32
	var deleteHit atomic.Bool
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult, endpointCancelPlanning}, func(mux *http.ServeMux) {
		mux.HandleFunc("GET /v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			polls.Add(1)
			_, err := w.Write([]byte(`{"status":"submitted"}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("DELETE /v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			deleteHit.Store(true)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	// No context deadline: the MaxRetries cap is the only thing that stops an
	// endlessly-submitted plan from polling forever.
	opts := fastWaitOpts
	opts.MaxRetries = 3

	_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", opts)
	require.ErrorIs(t, err, ErrPlanPollExhausted)
	// The initial poll plus MaxRetries further attempts, then it gives up.
	assert.Equal(t, int32(4), polls.Load())
	// Exhaustion must free the still-active server plan, like the cancellation
	// path — the DELETE is synchronous, so it is observed by now.
	assert.True(t, deleteHit.Load(), "exhaustion must cancel the abandoned plan server-side")
}

func TestWaitForPlanDeadlineDisablesDefaultRetryCap(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"submitted"}`))
			require.NoError(t, err)
		})
	})

	// A generous deadline that comfortably outlasts the default 10-retry budget
	// (~20ms at this backoff). With default options the cap must be disabled by the
	// deadline, so polling runs to the deadline (DeadlineExceeded) rather than
	// stopping early with ErrPlanPollExhausted.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	_, err := cat.WaitForPlan(ctx, table.Identifier{"db", "tbl"}, "plan-1", fastWaitOpts)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrPlanPollExhausted)
}

func TestWaitForPlanRequiresFetchEndpoint(t *testing.T) {
	t.Parallel()

	// A server that does not advertise fetchPlanningResult cannot be polled.
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, nil)

	_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", WaitForPlanOptions{})
	require.ErrorIs(t, err, ErrEndpointNotSupported)
}

func TestWaitForPlanRejectsEmptyPlanID(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, nil)

	_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "", WaitForPlanOptions{})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestResolveWaitOptions(t *testing.T) {
	t.Parallel()

	def := DefaultWaitForPlanOptions
	cases := []struct {
		name            string
		in              WaitForPlanOptions
		hasDeadline     bool
		min, max, grace time.Duration
		retries         int
	}{
		{
			name:    "zero, no deadline: default cap applies",
			in:      WaitForPlanOptions{},
			min:     def.MinDelay,
			max:     def.MaxDelay,
			grace:   def.CancelGracePeriod,
			retries: def.MaxRetries,
		},
		{
			name:        "zero, with deadline: default cap disabled",
			in:          WaitForPlanOptions{},
			hasDeadline: true,
			min:         def.MinDelay,
			max:         def.MaxDelay,
			grace:       def.CancelGracePeriod,
			retries:     -1,
		},
		{
			name:    "negative durations use defaults; negative retries stays unlimited",
			in:      WaitForPlanOptions{MinDelay: -1, MaxDelay: -1, CancelGracePeriod: -1, MaxRetries: -1},
			min:     def.MinDelay,
			max:     def.MaxDelay,
			grace:   def.CancelGracePeriod,
			retries: -1,
		},
		{
			name:    "inverted clamps cap up to floor",
			in:      WaitForPlanOptions{MinDelay: 2 * time.Second, MaxDelay: time.Second},
			min:     2 * time.Second,
			max:     2 * time.Second,
			grace:   def.CancelGracePeriod,
			retries: def.MaxRetries,
		},
		{
			name:        "explicit positive retries honored even with a deadline",
			in:          WaitForPlanOptions{MinDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond, CancelGracePeriod: 2 * time.Second, MaxRetries: 7},
			hasDeadline: true,
			min:         5 * time.Millisecond,
			max:         50 * time.Millisecond,
			grace:       2 * time.Second,
			retries:     7,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			minDelay, maxDelay, grace, retries := resolveWaitOptions(tc.in, tc.hasDeadline)
			assert.Equal(t, tc.min, minDelay)
			assert.Equal(t, tc.max, maxDelay)
			assert.Equal(t, tc.grace, grace)
			assert.Equal(t, tc.retries, retries)
		})
	}
}

func TestNextScanPlanBackoffStaysInBoundsWithoutOverflow(t *testing.T) {
	t.Parallel()

	maxDur := time.Duration(math.MaxInt64)
	cases := []struct {
		name                   string
		prev, minDel, maxDelay time.Duration
	}{
		// prev*3 overflows int64; the ceiling must clamp to maxDelay before
		// sampling so rand.Int64N never sees a non-positive argument.
		{"all max", maxDur, maxDur, maxDur},
		{"max prev, small floor", maxDur, time.Millisecond, maxDur},
		{"half-max prev", maxDur / 2, time.Nanosecond, maxDur},
		// Ordinary tight bounds still hold their floor and cap.
		{"equal min and max", time.Millisecond, time.Millisecond, time.Millisecond},
		{"growing", 100 * time.Millisecond, 100 * time.Millisecond, 5 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Sample repeatedly: the bug is probabilistic across the RNG range.
			for range 1000 {
				got := nextScanPlanBackoff(tc.prev, tc.minDel, tc.maxDelay)
				assert.GreaterOrEqual(t, got, tc.minDel)
				assert.LessOrEqual(t, got, tc.maxDelay)
			}
		})
	}
}

func TestWaitForPlanForwardsAccessDelegation(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			assert.Equal(t, "remote-signing", req.Header.Get(headerIcebergAccessDelegation))
			_, err := w.Write([]byte(`{"status":"completed"}`))
			require.NoError(t, err)
		})
	})

	_, err := cat.WaitForPlan(context.Background(), table.Identifier{"db", "tbl"}, "plan-1", WaitForPlanOptions{
		AccessDelegation: stringPtr("remote-signing"),
	})
	require.NoError(t, err)
}
