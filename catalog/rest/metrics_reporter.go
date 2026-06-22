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

	"github.com/apache/iceberg-go/metrics"
)

// keyReportMetricsEnabled opts a REST catalog into POSTing scan/commit reports
// to the catalog's metrics endpoint. It is disabled by default so existing
// users see no new network traffic unless they turn it on.
const keyReportMetricsEnabled = "rest.metrics-reporting-enabled"

// restMetricsReporter POSTs metrics reports to a table's REST metrics endpoint
// (POST .../tables/{table}/metrics). It is bound to a single table's path and
// satisfies metrics.Reporter.
type restMetricsReporter struct {
	baseURI *url.URL
	cl      *http.Client
	path    []string // table metrics path, relative to baseURI
}

var _ metrics.Reporter = (*restMetricsReporter)(nil)

// Report wraps the report in a ReportMetricsRequest and POSTs it on a
// background goroutine. Per the Reporter contract it never blocks or fails the
// observed scan/commit: the send is detached from the caller's cancellation,
// and any error is logged and swallowed.
func (rep *restMetricsReporter) Report(ctx context.Context, report metrics.MetricsReport) {
	if report == nil {
		return
	}

	req := metrics.NewReportMetricsRequest(report)
	// Detach from the caller's cancellation (the scan/commit is already done)
	// while preserving any context values used by the HTTP client.
	sendCtx := context.WithoutCancel(ctx)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Default().Warn("iceberg: panic while reporting metrics to REST catalog", "recovered", r)
			}
		}()

		if _, err := doPost[metrics.ReportMetricsRequest, struct{}](
			sendCtx, rep.baseURI, rep.path, req, rep.cl, nil, allowNoContent()); err != nil {
			slog.Default().Warn("iceberg: failed to report metrics to REST catalog", "error", err)
		}
	}()
}

// Close satisfies [metrics.Reporter]. The reporter is stateless — it borrows
// the catalog's shared HTTP client rather than owning one — so there is nothing
// to release.
func (rep *restMetricsReporter) Close() error { return nil }
