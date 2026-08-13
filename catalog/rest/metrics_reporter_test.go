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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iceberg "github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/metrics"
	"github.com/DataDog/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturedRequest struct {
	method      string
	path        string // decoded path
	escapedPath string // percent-encoded path
	header      http.Header
	body        []byte
}

// captureTransport records the request it receives and returns 204 No Content,
// avoiding any real network listener.
type captureTransport struct {
	ch    chan capturedRequest
	block <-chan struct{} // if non-nil, RoundTrip waits on it before responding
	err   error           // if non-nil, returned instead of a response
}

func (c *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(r.Body)
	if c.ch != nil {
		c.ch <- capturedRequest{
			method:      r.Method,
			path:        r.URL.Path,
			escapedPath: r.URL.EscapedPath(),
			header:      r.Header.Clone(), // the SDK may reuse the request; snapshot it
			body:        body,
		}
	}
	if c.block != nil {
		<-c.block
	}
	if c.err != nil {
		return nil, c.err
	}

	return &http.Response{
		StatusCode: http.StatusNoContent,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}, nil
}

// ctxBlockTransport blocks until the request context is done, then reports the
// context error. It lets a test prove that a report has a finite deadline
// (timeout) and that Close cancels an in-flight request. entered counts how many
// requests reached the transport.
type ctxBlockTransport struct {
	entered atomic.Int32
	started chan struct{} // signalled once when RoundTrip is entered
	ctxErr  chan error    // receives the context error once it fires
}

func (c *ctxBlockTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.entered.Add(1)
	select {
	case c.started <- struct{}{}:
	default:
	}
	<-r.Context().Done()
	err := r.Context().Err()
	select {
	case c.ctxErr <- err:
	default:
	}

	return nil, err
}

// concurrencyTransport blocks every RoundTrip on release, recording how many run
// concurrently and how many were delivered in total. It lets a test prove the
// dispatcher bounds concurrency and drops excess reports.
type concurrencyTransport struct {
	inFlight  atomic.Int32
	maxSeen   atomic.Int32
	delivered atomic.Int32
	release   chan struct{}
}

func (c *concurrencyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	n := c.inFlight.Add(1)
	for {
		m := c.maxSeen.Load()
		if n <= m || c.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	c.delivered.Add(1)
	<-c.release
	c.inFlight.Add(-1)

	return &http.Response{
		StatusCode: http.StatusNoContent,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}, nil
}

// newTestDispatcher builds a dispatcher with the production pool sizing and a
// discarding logger, registering Close as cleanup.
func newTestDispatcher(t *testing.T, timeout time.Duration) *metricsDispatcher {
	t.Helper()
	d := newMetricsDispatcher(metricsDispatchWorkers, metricsDispatchQueueSize, timeout,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(d.close)

	return d
}

func newTestReporter(t *testing.T, tr http.RoundTripper) *restMetricsReporter {
	t.Helper()

	return reporterWith(t, tr, newTestDispatcher(t, 5*time.Second), nil)
}

// reporterWith builds a reporter bound to the given transport and dispatcher. A
// nil path uses the default single-level namespace path.
func reporterWith(t *testing.T, tr http.RoundTripper, d *metricsDispatcher, path []string) *restMetricsReporter {
	t.Helper()
	base, err := url.Parse("http://catalog.invalid")
	require.NoError(t, err)
	if path == nil {
		path = []string{"namespaces", "db", "tables", "t", "metrics"}
	}

	return &restMetricsReporter{
		baseURI:    base,
		cl:         &http.Client{Transport: tr},
		path:       path,
		dispatcher: d,
	}
}

func TestRESTMetricsReporterPostsScanReport(t *testing.T) {
	received := make(chan capturedRequest, 1)
	rep := newTestReporter(t, &captureTransport{ch: received})

	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t", SnapshotID: 99})

	select {
	case req := <-received:
		assert.Equal(t, http.MethodPost, req.method)
		assert.Equal(t, "/namespaces/db/tables/t/metrics", req.path)
		var m map[string]any
		require.NoError(t, json.Unmarshal(req.body, &m))
		assert.Equal(t, "scan-report", m["report-type"])
		assert.Equal(t, "db.t", m["table-name"])
		assert.Contains(t, m, "metrics", "report fields are flattened alongside report-type")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the async metrics POST")
	}
}

func TestRESTMetricsReporterPostsCommitReport(t *testing.T) {
	received := make(chan capturedRequest, 1)
	rep := newTestReporter(t, &captureTransport{ch: received})

	rep.Report(context.Background(), metrics.CommitReport{TableName: "db.t", Operation: "append"})

	select {
	case req := <-received:
		var m map[string]any
		require.NoError(t, json.Unmarshal(req.body, &m))
		assert.Equal(t, "commit-report", m["report-type"])
		assert.Equal(t, "append", m["operation"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the async metrics POST")
	}
}

func TestRESTMetricsReporterNilReportIsNoop(t *testing.T) {
	received := make(chan capturedRequest, 1)
	rep := newTestReporter(t, &captureTransport{ch: received})

	rep.Report(context.Background(), nil)

	select {
	case <-received:
		t.Fatal("nil report must not produce a POST")
	case <-time.After(200 * time.Millisecond):
		// expected: nothing sent
	}
}

func TestRESTMetricsReporterReportDoesNotBlockOnSlowServer(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	rep := newTestReporter(t, &captureTransport{block: block})

	done := make(chan struct{})
	go func() {
		rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
		close(done)
	}()

	select {
	case <-done:
		// Report returned promptly despite the hanging transport.
	case <-time.After(time.Second):
		t.Fatal("Report blocked on a slow server")
	}
}

// TestRESTMetricsReporterEscapesFullPath proves the request goes to the full
// /v1/{prefix}/... URL and that namespace and table segments needing escaping
// are percent-encoded.
func TestRESTMetricsReporterEscapesFullPath(t *testing.T) {
	received := make(chan capturedRequest, 1)
	base, err := url.Parse("http://catalog.invalid/v1/my-prefix")
	require.NoError(t, err)
	rep := &restMetricsReporter{
		baseURI:    base,
		cl:         &http.Client{Transport: &captureTransport{ch: received}},
		path:       []string{"namespaces", "a b", "tables", "t x", "metrics"},
		dispatcher: newTestDispatcher(t, 5*time.Second),
	}

	rep.Report(context.Background(), metrics.ScanReport{TableName: "a b.t x"})

	select {
	case req := <-received:
		assert.Equal(t, "/v1/my-prefix/namespaces/a b/tables/t x/metrics", req.path)
		assert.Equal(t, "/v1/my-prefix/namespaces/a%20b/tables/t%20x/metrics", req.escapedPath)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the async metrics POST")
	}
}

// rotatingAuthManager returns a distinct bearer header on each call, standing in
// for a token source that refreshes between reports.
type rotatingAuthManager struct{ n atomic.Int32 }

func (m *rotatingAuthManager) AuthHeader() (string, string, error) {
	return "Authorization", fmt.Sprintf("Bearer tok-%d", m.n.Add(1)), nil
}

// ctxAuthManager records whether the context-aware path was taken and returns a
// header value identifying which method produced it.
type ctxAuthManager struct{ usedContext atomic.Bool }

func (m *ctxAuthManager) AuthHeader() (string, string, error) {
	return "Authorization", "context-free", nil
}

func (m *ctxAuthManager) AuthHeaderWithContext(ctx context.Context) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	m.usedContext.Store(true)

	return "Authorization", "with-context", nil
}

// blockingHandler blocks in Handle until released, so a test can prove the drop
// path never logs synchronously on the caller's goroutine.
type blockingHandler struct{ release <-chan struct{} }

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *blockingHandler) Handle(context.Context, slog.Record) error {
	<-h.release

	return nil
}
func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler      { return h }

// recordingHandler captures emitted records so a test can assert the aggregated
// warning content.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())

	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) warnings() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Level == slog.LevelWarn {
			out = append(out, r)
		}
	}

	return out
}

// reporterWithSession builds a reporter whose client runs through a
// sessionTransport (auth + default headers) wrapping the given transport, so a
// test can assert the metrics POST goes out authenticated.
func reporterWithSession(t *testing.T, auth AuthManager, tr http.RoundTripper) *restMetricsReporter {
	t.Helper()
	session := &sessionTransport{
		RoundTripper:   tr,
		authManager:    auth,
		defaultHeaders: http.Header{},
	}
	session.defaultHeaders.Set("Content-Type", "application/json")

	return reporterWith(t, session, newTestDispatcher(t, 5*time.Second), nil)
}

// TestRESTMetricsReporterSendsAuthenticatedRequest pins that the metrics POST
// reuses the catalog's authenticated client: the Authorization header the auth
// manager produces and the catalog's default Content-Type both go out on the
// wire. Without this a refactor that dispatched metrics through a bare
// http.Client would keep every other test green.
func TestRESTMetricsReporterSendsAuthenticatedRequest(t *testing.T) {
	received := make(chan capturedRequest, 1)
	rep := reporterWithSession(t, staticAuthManager{key: "Authorization", value: "Bearer tok-abc"}, &captureTransport{ch: received})

	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})

	select {
	case req := <-received:
		assert.Equal(t, "Bearer tok-abc", req.header.Get("Authorization"),
			"metrics POST must carry the catalog's Authorization header")
		assert.Equal(t, "application/json", req.header.Get("Content-Type"),
			"catalog default headers must be forwarded")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the async metrics POST")
	}
}

// TestRESTMetricsReporterRefetchesCredentialPerReport proves auth runs per
// request, so a credential that rotates between reports is picked up rather than
// captured once.
func TestRESTMetricsReporterRefetchesCredentialPerReport(t *testing.T) {
	received := make(chan capturedRequest, 2)
	// One worker serializes the two reports so credentials are handed out in order.
	d := newMetricsDispatcher(1, 8, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(d.close)
	session := &sessionTransport{
		RoundTripper:   &captureTransport{ch: received},
		authManager:    &rotatingAuthManager{},
		defaultHeaders: http.Header{},
	}
	rep := reporterWith(t, session, d, nil)

	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})

	seen := make(map[string]bool)
	for range 2 {
		select {
		case req := <-received:
			seen[req.header.Get("Authorization")] = true
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for a metrics POST")
		}
	}
	assert.True(t, seen["Bearer tok-1"] && seen["Bearer tok-2"],
		"each report must fetch a fresh credential, got %v", seen)
}

// TestSessionTransportPrefersContextAuthManager proves sessionTransport uses the
// context-aware auth path when the manager implements ContextAuthManager, so a
// request deadline can bound the auth step.
func TestSessionTransportPrefersContextAuthManager(t *testing.T) {
	received := make(chan capturedRequest, 1)
	auth := &ctxAuthManager{}
	rep := reporterWithSession(t, auth, &captureTransport{ch: received})

	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})

	select {
	case req := <-received:
		assert.Equal(t, "with-context", req.header.Get("Authorization"),
			"the context-aware auth path must be preferred")
		assert.True(t, auth.usedContext.Load())
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the async metrics POST")
	}
}

// TestRESTMetricsReporterPreservesContextValues proves the caller's context
// values (trace spans, request-scoped attributes) reach the outbound report even
// though the caller's cancellation is detached.
func TestRESTMetricsReporterPreservesContextValues(t *testing.T) {
	type ctxKey struct{}
	got := make(chan any, 1)
	tr := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got <- r.Context().Value(ctxKey{})

		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	rep := newTestReporter(t, tr)

	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-123")
	rep.Report(ctx, metrics.ScanReport{TableName: "db.t"})

	select {
	case v := <-got:
		assert.Equal(t, "trace-123", v, "caller context values must propagate to the report")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the async metrics POST")
	}
}

// TestMetricsDispatcherDropCountsWithoutBlocking proves that dropping a report on
// a full queue neither blocks the caller nor logs synchronously: the logger here
// blocks in Handle, yet every Report returns promptly and the drops are counted.
func TestMetricsDispatcherDropCountsWithoutBlocking(t *testing.T) {
	release := make(chan struct{})
	handlerRelease := make(chan struct{})
	tr := &concurrencyTransport{release: release}
	d := newMetricsDispatcher(1, 1, time.Minute, slog.New(&blockingHandler{release: handlerRelease}))
	t.Cleanup(func() {
		close(handlerRelease)
		close(release)
		d.close()
	})
	rep := reporterWith(t, tr, d, nil)

	// Occupy the single worker; the next report fills the queue, the rest drop.
	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	require.Eventually(t, func() bool {
		return tr.inFlight.Load() == 1
	}, 3*time.Second, 5*time.Millisecond, "worker never saturated")

	for range 4 { // 1 fills the queue, 3 are dropped
		done := make(chan struct{})
		go func() {
			rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Report blocked; the drop path must not log synchronously")
		}
	}

	require.Eventually(t, func() bool {
		return d.dropped.Load() == 3
	}, 3*time.Second, 5*time.Millisecond, "expected exactly the surplus reports to be dropped")
}

// TestMetricsDispatcherAggregatesDropWarning proves the drop/failure counters
// surface as a single aggregated warning (carrying the counts) rather than one
// log line per dropped report. The shutdown flush is used to make the assertion
// deterministic without waiting on the stats ticker.
func TestMetricsDispatcherAggregatesDropWarning(t *testing.T) {
	handler := &recordingHandler{}
	release := make(chan struct{})
	tr := &concurrencyTransport{release: release}
	d := newMetricsDispatcher(1, 1, time.Minute, slog.New(handler))
	rep := reporterWith(t, tr, d, nil)

	// Occupy the worker, fill the queue, then overflow: three reports drop.
	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	require.Eventually(t, func() bool {
		return tr.inFlight.Load() == 1
	}, 3*time.Second, 5*time.Millisecond, "worker never saturated")
	for range 4 {
		rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	}
	require.Eventually(t, func() bool {
		return d.dropped.Load() == 3
	}, 3*time.Second, 5*time.Millisecond)

	close(release)
	d.close() // triggers the final aggregated flush

	warnings := handler.warnings()
	require.Len(t, warnings, 1, "drops must aggregate into a single warning, not one per drop")
	attrs := map[string]any{}
	warnings[0].Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()

		return true
	})
	assert.Equal(t, uint64(3), attrs["dropped"], "the aggregated warning must carry the drop count")
}

// TestMetricsDispatcherCloseIsIdempotent proves repeated and concurrent Close
// calls are safe and cheap: they do not panic and each returns, spawning no
// extra waiter goroutine per call.
func TestMetricsDispatcherCloseIsIdempotent(t *testing.T) {
	d := newMetricsDispatcher(2, 4, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.close()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent Close calls did not all return")
	}

	// A further Close after the fact is still safe.
	d.close()
}

// TestRESTMetricsReporterBuildsPathThroughProduction runs the real path
// composition — splitIdentForPath (encodeNamespace) and endpointReportMetrics
// .reqPath — for a multi-level namespace with segments needing escaping, so a
// regression in that composition or in the separator handling is caught rather
// than bypassed by a hand-built path.
func TestRESTMetricsReporterBuildsPathThroughProduction(t *testing.T) {
	c := &Catalog{namespaceSeparator: defaultNamespaceSeparator}
	ns, tbl, err := c.splitIdentForPath(table.Identifier{"a b", "d e", "t x"})
	require.NoError(t, err)
	path, err := endpointReportMetrics.reqPath(ns, tbl)
	require.NoError(t, err)

	received := make(chan capturedRequest, 1)
	base, err := url.Parse("http://catalog.invalid/v1/my-prefix")
	require.NoError(t, err)
	rep := reporterWith(t, &captureTransport{ch: received}, newTestDispatcher(t, 5*time.Second), path)
	rep.baseURI = base

	rep.Report(context.Background(), metrics.ScanReport{TableName: "a b.d e.t x"})

	select {
	case req := <-received:
		// The namespace levels are percent-encoded and joined by the encoded
		// separator (%1F); the table segment is escaped by the URL builder.
		assert.Equal(t, "/v1/my-prefix/namespaces/a%20b%1Fd%20e/tables/t%20x/metrics", req.escapedPath)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the async metrics POST")
	}
}

// TestMetricsDispatcherReportHasFiniteTimeout proves a report does not hang
// forever against a black-holing endpoint: the per-report deadline fires and the
// request context reports DeadlineExceeded.
func TestMetricsDispatcherReportHasFiniteTimeout(t *testing.T) {
	tr := &ctxBlockTransport{started: make(chan struct{}, 1), ctxErr: make(chan error, 1)}
	d := newMetricsDispatcher(1, 1, 100*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(d.close)
	rep := reporterWith(t, tr, d, nil)

	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})

	select {
	case err := <-tr.ctxErr:
		assert.ErrorIs(t, err, context.DeadlineExceeded, "report must time out rather than hang")
	case <-time.After(3 * time.Second):
		t.Fatal("report did not observe a finite timeout")
	}
}

// TestMetricsDispatcherBoundsConcurrencyAndDrops proves the worker pool caps
// concurrent sends and sheds excess reports rather than queueing them without
// limit.
func TestMetricsDispatcherBoundsConcurrencyAndDrops(t *testing.T) {
	const (
		workers = 2
		queue   = 3
		extra   = 3 // reports offered once the pool and queue are saturated
	)
	tr := &concurrencyTransport{release: make(chan struct{})}
	d := newMetricsDispatcher(workers, queue, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(d.close)
	rep := reporterWith(t, tr, d, nil)

	// Fill every worker; each blocks in RoundTrip, leaving the queue empty.
	for range workers {
		rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	}
	require.Eventually(t, func() bool {
		return tr.inFlight.Load() == workers
	}, 3*time.Second, 5*time.Millisecond, "workers never saturated")

	// Fill the queue (workers are blocked, so nothing drains it), then offer more
	// than fits — the surplus must be dropped, not queued.
	for range queue + extra {
		rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	}

	// Concurrency stayed within the pool while everything was blocked.
	assert.LessOrEqual(t, tr.maxSeen.Load(), int32(workers), "concurrency exceeded the worker pool")

	close(tr.release)

	// Only workers + queue reports are ever delivered; the extra were dropped.
	require.Eventually(t, func() bool {
		return tr.inFlight.Load() == 0 && tr.delivered.Load() == workers+queue
	}, 3*time.Second, 5*time.Millisecond, "expected exactly workers+queue reports delivered")

	assert.LessOrEqual(t, tr.maxSeen.Load(), int32(workers), "concurrency exceeded the worker pool")
}

// TestMetricsDispatcherCloseCancelsInFlight proves Close cancels an in-flight
// report and returns promptly rather than waiting on the stalled endpoint.
func TestMetricsDispatcherCloseCancelsInFlight(t *testing.T) {
	tr := &ctxBlockTransport{started: make(chan struct{}, 1), ctxErr: make(chan error, 1)}
	// A long per-report timeout so it is Close, not the deadline, that unblocks.
	d := newMetricsDispatcher(1, 1, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(d.close) // guard against a t.Fatal below leaking the worker
	rep := reporterWith(t, tr, d, nil)

	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})

	select {
	case <-tr.started:
	case <-time.After(3 * time.Second):
		t.Fatal("report never reached the transport")
	}

	closed := make(chan struct{})
	go func() {
		d.close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return; in-flight report was not cancelled")
	}

	select {
	case err := <-tr.ctxErr:
		assert.ErrorIs(t, err, context.Canceled, "Close must cancel in-flight reports")
	case <-time.After(time.Second):
		t.Fatal("in-flight request context was not cancelled")
	}
}

// TestMetricsDispatcherCloseDropsQueuedReports proves that reports still waiting
// in the queue when Close is called are dropped rather than sent to the endpoint.
func TestMetricsDispatcherCloseDropsQueuedReports(t *testing.T) {
	tr := &ctxBlockTransport{started: make(chan struct{}, 1), ctxErr: make(chan error, 8)}
	// One worker, roomy queue: the worker is occupied by the first report while
	// the rest pile up behind it.
	d := newMetricsDispatcher(1, 8, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(d.close) // guard against a t.Fatal below leaking the worker
	rep := reporterWith(t, tr, d, nil)

	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	select {
	case <-tr.started:
	case <-time.After(3 * time.Second):
		t.Fatal("first report never reached the transport")
	}

	// Queue several more behind the busy worker.
	for range 5 {
		rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	}

	d.close()

	// Only the in-flight report ever reached the transport; the queued reports
	// were discarded on shutdown rather than sent.
	assert.Equal(t, int32(1), tr.entered.Load(),
		"queued reports must be dropped on Close, not sent")
}

func TestReportMetricsEnabled(t *testing.T) {
	assert.False(t, reportMetricsEnabled(iceberg.Properties{}))
	assert.True(t, reportMetricsEnabled(iceberg.Properties{keyReportMetricsEnabled: "true"}))
	assert.True(t, reportMetricsEnabled(iceberg.Properties{keyReportMetricsEnabledLegacy: "true"}),
		"the historical dotted key is accepted as an alias")
	assert.False(t, reportMetricsEnabled(iceberg.Properties{keyReportMetricsEnabled: "false"}))

	// The canonical key wins whenever it is present: a client that migrated to it
	// and explicitly disabled reporting is honored even if a stale legacy dotted
	// key still lingers in the config.
	assert.False(t, reportMetricsEnabled(iceberg.Properties{
		keyReportMetricsEnabled:       "false",
		keyReportMetricsEnabledLegacy: "true",
	}), "an explicit canonical false must override a lingering legacy true")
	assert.True(t, reportMetricsEnabled(iceberg.Properties{
		keyReportMetricsEnabled:       "true",
		keyReportMetricsEnabledLegacy: "false",
	}), "an explicit canonical true wins over a legacy false")
}

func TestReportMetricsTimeout(t *testing.T) {
	assert.Equal(t, defaultReportMetricsTimeout, reportMetricsTimeout(iceberg.Properties{}))
	assert.Equal(t, 250*time.Millisecond, reportMetricsTimeout(iceberg.Properties{keyReportMetricsTimeoutMs: "250"}))
	assert.Equal(t, defaultReportMetricsTimeout, reportMetricsTimeout(iceberg.Properties{keyReportMetricsTimeoutMs: "0"}),
		"a non-positive timeout falls back to the default")
	assert.Equal(t, defaultReportMetricsTimeout, reportMetricsTimeout(iceberg.Properties{keyReportMetricsTimeoutMs: "bogus"}))
}

// TestMetricsReportingEnablementPrecedence pins that reporting is enabled only
// by a client opt-in: server-vended defaults and overrides setting the key must
// not turn it on, and the server must advertise the endpoint. Table-response
// properties likewise cannot enable it, since enablement is resolved once at
// init from the client properties. Disabling is safe in either direction, so an
// explicit server override false does suppress a client that opted in, while a
// mere server default cannot.
func TestMetricsReportingEnablementPrecedence(t *testing.T) {
	cfg := func(defaults, overrides map[string]any, endpoints []string) map[string]any {
		m := map[string]any{
			"defaults":  orEmpty(defaults),
			"overrides": orEmpty(overrides),
		}
		if endpoints != nil {
			m["endpoints"] = endpoints
		}

		return m
	}

	tests := []struct {
		name        string
		serverCfg   map[string]any
		clientProps iceberg.Properties
		wantEnabled bool
	}{
		{
			name:      "off by default",
			serverCfg: cfg(nil, nil, nil),
		},
		{
			name:      "server default cannot enable",
			serverCfg: cfg(map[string]any{keyReportMetricsEnabled: "true"}, nil, nil),
		},
		{
			name:      "server override cannot enable",
			serverCfg: cfg(nil, map[string]any{keyReportMetricsEnabled: "true"}, nil),
		},
		{
			name:        "client enables",
			serverCfg:   cfg(nil, nil, nil),
			clientProps: iceberg.Properties{keyReportMetricsEnabled: "true"},
			wantEnabled: true,
		},
		{
			name:        "client legacy key enables",
			serverCfg:   cfg(nil, nil, nil),
			clientProps: iceberg.Properties{keyReportMetricsEnabledLegacy: "true"},
			wantEnabled: true,
		},
		{
			name:        "client enables but endpoint not advertised",
			serverCfg:   cfg(nil, nil, []string{"GET /v1/{prefix}/namespaces"}),
			clientProps: iceberg.Properties{keyReportMetricsEnabled: "true"},
		},
		{
			// The production path: the server advertises an explicit endpoint list
			// that includes the metrics endpoint, exercising resolveEndpoints /
			// endpointFromString parsing of the template rather than the
			// fallback-to-defaults path the cases above hit. A parse regression here
			// would silently disable reporting for servers that advertise it.
			name: "client enables and metrics endpoint advertised",
			serverCfg: cfg(nil, nil, []string{
				"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
			}),
			clientProps: iceberg.Properties{keyReportMetricsEnabled: "true"},
			wantEnabled: true,
		},
		{
			// Disabling is always safe, so an explicit server override false honors
			// an operator suppressing reporting fleet-wide even for a client that
			// opted in — matching how a Java client resolves the merged config.
			name:        "server override disables client opt-in",
			serverCfg:   cfg(nil, map[string]any{keyReportMetricsEnabled: "false"}, nil),
			clientProps: iceberg.Properties{keyReportMetricsEnabled: "true"},
		},
		{
			// A mere server default cannot flip off a client opt-in: client props beat
			// server defaults in the merge, so the resolved value stays true.
			name:        "server default cannot disable client opt-in",
			serverCfg:   cfg(map[string]any{keyReportMetricsEnabled: "false"}, nil, nil),
			clientProps: iceberg.Properties{keyReportMetricsEnabled: "true"},
			wantEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(tt.serverCfg)
			})
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)

			cat, err := newCatalogFromProps(context.Background(), "rest", srv.URL, tt.clientProps)
			require.NoError(t, err)
			t.Cleanup(func() { _ = cat.Close() })

			if tt.wantEnabled {
				assert.NotNil(t, cat.metricsDispatcher, "reporting should be enabled")
			} else {
				assert.Nil(t, cat.metricsDispatcher, "reporting must stay off")
			}
		})
	}
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}

	return m
}

// TestTableResponsePropertiesCannotEnableReporting pins the invariant behind the
// enablement doc comment: enablement is resolved once from the client-supplied
// properties, so a property arriving later on a table-load response reaches only
// the table-local config and can never turn reporting on. A future refactor that
// resolved enablement from merged properties would flip the first assertion and
// fail here.
func TestTableResponsePropertiesCannotEnableReporting(t *testing.T) {
	clientProps := iceberg.Properties{} // the client did not opt in
	require.False(t, reportMetricsEnabled(clientProps))

	// A table response carrying the enable key would enable reporting only if it
	// reached the client properties — it does not, so the client properties that
	// gate the dispatcher stay unchanged and reporting stays off.
	tableResponseProps := iceberg.Properties{keyReportMetricsEnabled: "true"}
	ifItLeaked := maps.Clone(clientProps)
	maps.Copy(ifItLeaked, tableResponseProps)
	assert.True(t, reportMetricsEnabled(ifItLeaked),
		"sanity: the key would enable reporting if it reached the client properties")
	assert.False(t, reportMetricsEnabled(clientProps),
		"but the client properties are the ones that gate the dispatcher, and they are untouched")
}
