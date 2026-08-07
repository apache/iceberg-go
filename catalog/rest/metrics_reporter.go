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
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/metrics"
)

const (
	// keyReportMetricsEnabled opts a REST catalog into POSTing scan/commit
	// reports to the catalog's metrics endpoint. It is disabled by default so
	// existing users see no new network traffic unless they turn it on. This is
	// the canonical, cross-implementation spelling used by Iceberg Java and the
	// Iceberg docs; keyReportMetricsEnabledLegacy is accepted as an alias.
	keyReportMetricsEnabled = "rest-metrics-reporting-enabled"
	// keyReportMetricsEnabledLegacy is the historical dotted spelling, accepted
	// as an alias so existing configs keep working.
	keyReportMetricsEnabledLegacy = "rest.metrics-reporting-enabled"
	// keyReportMetricsTimeoutMs bounds a single report's total auth + request +
	// response cycle, in milliseconds. Read from the client-supplied properties
	// only.
	keyReportMetricsTimeoutMs = "rest-metrics-reporting-timeout-ms"

	// defaultReportMetricsTimeout bounds a single report's total auth + request
	// + response cycle when keyReportMetricsTimeoutMs is unset. Telemetry is safe
	// to drop, so the bound is deliberately short.
	defaultReportMetricsTimeout = 10 * time.Second
	// metricsDispatchWorkers is the fixed number of goroutines draining the
	// dispatch queue, capping the reporter's concurrent connection use.
	metricsDispatchWorkers = 4
	// metricsDispatchQueueSize bounds how many reports may await dispatch.
	// Reports offered while the queue is full are dropped (and logged) rather
	// than queued without limit, so a stalled endpoint cannot make reporting grow
	// without bound.
	metricsDispatchQueueSize = 128
)

// reportMetricsEnabled reports whether the client opted into REST metrics
// reporting, accepting both the canonical and the legacy dotted key. It must be
// given the client-supplied properties only (never the server-merged config) so
// a server cannot flip the default and turn on outbound telemetry the client
// never asked for.
func reportMetricsEnabled(props iceberg.Properties) bool {
	return props.GetBool(keyReportMetricsEnabled, false) ||
		props.GetBool(keyReportMetricsEnabledLegacy, false)
}

// reportMetricsTimeout resolves the per-report deadline from the client-supplied
// properties, falling back to defaultReportMetricsTimeout for a missing or
// non-positive value.
func reportMetricsTimeout(props iceberg.Properties) time.Duration {
	ms := props.GetInt(keyReportMetricsTimeoutMs, int(defaultReportMetricsTimeout/time.Millisecond))
	if ms <= 0 {
		return defaultReportMetricsTimeout
	}

	return time.Duration(ms) * time.Millisecond
}

// metricsJob is a single report awaiting dispatch to a table's metrics endpoint.
type metricsJob struct {
	baseURI *url.URL
	cl      *http.Client
	path    []string
	req     metrics.ReportMetricsRequest
}

// metricsDispatcher POSTs metrics reports to REST metrics endpoints on a fixed
// pool of workers draining a bounded queue. It is owned by the catalog and
// shared across that catalog's table reporters, so concurrent report volume
// stays bounded no matter how many tables are loaded or how often they are
// scanned. A stalled endpoint sheds load — reports are dropped and logged —
// rather than accumulating goroutines and connections. Close cancels in-flight
// reports and drains the workers.
type metricsDispatcher struct {
	jobs    chan metricsJob
	timeout time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	logger  *slog.Logger // nil means resolve slog.Default at call time
}

func newMetricsDispatcher(workers, queueSize int, timeout time.Duration, logger *slog.Logger) *metricsDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	d := &metricsDispatcher{
		jobs:    make(chan metricsJob, queueSize),
		timeout: timeout,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
	}

	d.wg.Add(workers)
	for range workers {
		go d.worker()
	}

	return d
}

func (d *metricsDispatcher) log() *slog.Logger {
	if d.logger != nil {
		return d.logger
	}

	return slog.Default()
}

func (d *metricsDispatcher) worker() {
	defer d.wg.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case job := <-d.jobs:
			d.send(job)
		}
	}
}

func (d *metricsDispatcher) send(job metricsJob) {
	// If the dispatcher is already shutting down, drop the report before doing
	// any work: the derived context would be cancelled immediately anyway.
	if d.ctx.Err() != nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			d.log().Warn("iceberg: panic while reporting metrics to REST catalog", "recovered", r)
		}
	}()

	// Derive from the dispatcher context so Close cancels in-flight requests,
	// and add the per-report deadline so a stalled endpoint cannot pin a worker
	// (and its connection) indefinitely. The deadline covers auth plus the full
	// request and response cycle.
	ctx, cancel := context.WithTimeout(d.ctx, d.timeout)
	defer cancel()

	if _, err := doPost[metrics.ReportMetricsRequest, struct{}](
		ctx, job.baseURI, job.path, job.req, job.cl, nil, allowNoContent()); err != nil {
		// A report interrupted by Close (dispatcher context cancelled) is expected
		// shutdown behavior, not a failure worth logging. A per-report timeout
		// leaves the dispatcher context live, so genuine timeouts still surface.
		if d.ctx.Err() != nil {
			return
		}
		d.log().Warn("iceberg: failed to report metrics to REST catalog", "error", err)
	}
}

// submit offers a job to the queue without blocking. It drops the report (and
// logs the drop, so back-pressure is visible rather than silent) when the queue
// is full, and ignores reports once the dispatcher is closed.
func (d *metricsDispatcher) submit(job metricsJob) {
	select {
	case <-d.ctx.Done():
		return
	default:
	}

	select {
	case d.jobs <- job:
	case <-d.ctx.Done():
	default:
		d.log().Warn("iceberg: metrics report dropped; dispatch queue full")
	}
}

// close cancels in-flight reports and waits for the workers to return, bounded
// by the report timeout so shutdown cannot hang on a stalled endpoint.
func (d *metricsDispatcher) close() {
	d.cancel()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(d.timeout):
	}
}

// restMetricsReporter POSTs metrics reports for a single table to its REST
// metrics endpoint (POST .../tables/{table}/metrics) via the catalog-owned
// dispatcher. It satisfies metrics.Reporter.
type restMetricsReporter struct {
	baseURI    *url.URL
	cl         *http.Client
	path       []string // table metrics path, relative to baseURI
	dispatcher *metricsDispatcher
}

var _ metrics.Reporter = (*restMetricsReporter)(nil)

// Report wraps the report in a ReportMetricsRequest and hands it to the
// catalog's dispatcher, returning immediately. Per the Reporter contract it
// never blocks or fails the observed scan/commit: dispatch is asynchronous and
// bounded, the send is detached from the caller's cancellation (the scan/commit
// is already done), and any error is logged and swallowed by the dispatcher.
func (rep *restMetricsReporter) Report(_ context.Context, report metrics.MetricsReport) {
	if report == nil {
		return
	}

	rep.dispatcher.submit(metricsJob{
		baseURI: rep.baseURI,
		cl:      rep.cl,
		path:    rep.path,
		req:     metrics.NewReportMetricsRequest(report),
	})
}

// Close satisfies [metrics.Reporter]. A per-table reporter holds no resources of
// its own: the goroutines that dispatch its reports live in the catalog-owned
// dispatcher (closed by Catalog.Close), and it only borrows the catalog's shared
// HTTP client. So there is nothing to release here.
func (rep *restMetricsReporter) Close() error { return nil }
