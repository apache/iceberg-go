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

package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// NopReporter is a [Reporter] that discards every report. It is the default
// when no reporter is configured, so that instrumentation is free unless a user
// opts in. The zero value is ready to use.
type NopReporter struct{}

var _ Reporter = NopReporter{}

// Report implements [Reporter] and does nothing.
func (NopReporter) Report(context.Context, MetricsReport) {}

// LoggingReporter is a [Reporter] that logs each report via an [slog.Logger]. It
// is a convenient default for development and debugging.
type LoggingReporter struct {
	logger *slog.Logger // nil means resolve slog.Default at call time
}

var _ Reporter = (*LoggingReporter)(nil)

// NewLoggingReporter returns a [LoggingReporter] that logs to logger. If logger
// is nil, [slog.Default] is resolved at each Report call, so a later
// [slog.SetDefault] is honored rather than snapshotted at construction.
func NewLoggingReporter(logger *slog.Logger) *LoggingReporter {
	return &LoggingReporter{logger: logger}
}

// Report logs report at info level.
func (r *LoggingReporter) Report(ctx context.Context, report MetricsReport) {
	if report == nil {
		return
	}
	logger := r.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.InfoContext(ctx, "iceberg metrics report", "report", report)
}

// InMemoryReporter is a [Reporter] that retains every report it receives. It is
// primarily intended for tests and inspection. It is safe for concurrent use.
type InMemoryReporter struct {
	mu      sync.Mutex
	reports []MetricsReport
}

var _ Reporter = (*InMemoryReporter)(nil)

// Report appends report to the retained set.
func (r *InMemoryReporter) Report(_ context.Context, report MetricsReport) {
	if report == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, report)
}

// Reports returns a copy of the reports received so far, in arrival order.
func (r *InMemoryReporter) Reports() []MetricsReport {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]MetricsReport(nil), r.reports...)
}

// Reset discards all retained reports.
func (r *InMemoryReporter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = nil
}

// Combine returns a [Reporter] that forwards each report to all of the given
// reporters in order. nil reporters are skipped. A panic in one reporter must
// not prevent the others from receiving the report, so each call is isolated;
// in keeping with the [Reporter] contract a misbehaving reporter never affects
// the observed operation. A recovered panic is logged (with the reporter type)
// at warn level via [slog.Default] so a broken reporter is not silently
// swallowed.
//
// As a convenience, Combine with no non-nil reporters returns [NopReporter].
// Otherwise every reporter — even a lone one — is wrapped so it receives the
// per-reporter panic isolation the contract advertises. Wrapping a single
// reporter also closes a typed-nil hole: a concrete nil pointer (e.g.
// (*LoggingReporter)(nil)) is a non-nil interface, so it passes the nil filter;
// returning it unwrapped would let its eventual Report nil-deref escape with no
// recover to catch it.
func Combine(reporters ...Reporter) Reporter {
	nonNil := make([]Reporter, 0, len(reporters))
	for _, r := range reporters {
		if r != nil {
			nonNil = append(nonNil, r)
		}
	}

	if len(nonNil) == 0 {
		return NopReporter{}
	}

	return &compositeReporter{reporters: nonNil}
}

// compositeReporter fans a report out to several reporters, isolating each from
// the others' panics.
type compositeReporter struct {
	reporters []Reporter
	logger    *slog.Logger // nil means resolve slog.Default at call time
}

var _ Reporter = (*compositeReporter)(nil)

func (c *compositeReporter) Report(ctx context.Context, report MetricsReport) {
	for _, r := range c.reporters {
		func() {
			defer func() {
				if v := recover(); v != nil {
					// Swallow per the Reporter contract, but surface the
					// failure (with the offending reporter type) so a broken
					// reporter is traceable rather than showing up only as
					// mysteriously-missing metrics.
					logger := c.logger
					if logger == nil {
						logger = slog.Default()
					}
					logger.WarnContext(ctx, "iceberg metrics reporter panicked; recovered",
						"reporter", fmt.Sprintf("%T", r), "panic", v)
				}
			}()
			r.Report(ctx, report)
		}()
	}
}
