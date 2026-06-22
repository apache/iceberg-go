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
	"log/slog"
	"sync"
)

// Nop is a [Reporter] that discards every report. It is the default when no
// reporter is configured, so that instrumentation is free unless a user opts
// in. The zero value is ready to use.
type Nop struct{}

var _ Reporter = Nop{}

// Report implements [Reporter] and does nothing.
func (Nop) Report(context.Context, Report) {}

// LoggingReporter is a [Reporter] that logs each report via an [slog.Logger]. It
// is a convenient default for development and debugging.
type LoggingReporter struct {
	logger *slog.Logger
}

var _ Reporter = (*LoggingReporter)(nil)

// NewLoggingReporter returns a [LoggingReporter] that logs to logger. If logger
// is nil, [slog.Default] is used.
func NewLoggingReporter(logger *slog.Logger) *LoggingReporter {
	if logger == nil {
		logger = slog.Default()
	}

	return &LoggingReporter{logger: logger}
}

// Report logs report at info level.
func (r *LoggingReporter) Report(ctx context.Context, report Report) {
	if report == nil {
		return
	}
	r.logger.InfoContext(ctx, "iceberg metrics report", "report", report)
}

// InMemoryReporter is a [Reporter] that retains every report it receives. It is
// primarily intended for tests and inspection. It is safe for concurrent use.
type InMemoryReporter struct {
	mu      sync.Mutex
	reports []Report
}

var _ Reporter = (*InMemoryReporter)(nil)

// Report appends report to the retained set.
func (r *InMemoryReporter) Report(_ context.Context, report Report) {
	if report == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, report)
}

// Reports returns a copy of the reports received so far, in arrival order.
func (r *InMemoryReporter) Reports() []Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Report(nil), r.reports...)
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
// the observed operation.
//
// As a convenience, Combine with no reporters returns [Nop], and with a single
// non-nil reporter returns that reporter directly.
func Combine(reporters ...Reporter) Reporter {
	nonNil := make([]Reporter, 0, len(reporters))
	for _, r := range reporters {
		if r != nil {
			nonNil = append(nonNil, r)
		}
	}

	switch len(nonNil) {
	case 0:
		return Nop{}
	case 1:
		return nonNil[0]
	default:
		return compositeReporter(nonNil)
	}
}

type compositeReporter []Reporter

func (c compositeReporter) Report(ctx context.Context, report Report) {
	for _, r := range c {
		func() {
			defer func() { _ = recover() }()
			r.Report(ctx, report)
		}()
	}
}
