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
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testReport is a minimal MetricsReport used to exercise the reporters until
// the concrete report types (ScanReport, CommitReport) land. MetricsReport is
// an open marker interface, so any type satisfies it.
type testReport struct{ name string }

// countingReporter records how many reports it received.
type countingReporter struct {
	mu    sync.Mutex
	count int
}

func (c *countingReporter) Report(context.Context, MetricsReport) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
}

func (c *countingReporter) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.count
}

// panickingReporter always panics, to verify Combine isolates failures.
type panickingReporter struct{}

func (panickingReporter) Report(context.Context, MetricsReport) { panic("boom") }

func TestNopReporter(t *testing.T) {
	// NopReporter must accept anything, including nil, without panicking.
	assert.NotPanics(t, func() {
		NopReporter{}.Report(context.Background(), nil)
		NopReporter{}.Report(context.Background(), testReport{})
	})
}

func TestInMemoryReporter(t *testing.T) {
	var r InMemoryReporter
	require.Empty(t, r.Reports())

	r.Report(context.Background(), nil) // ignored
	require.Empty(t, r.Reports())

	r.Report(context.Background(), testReport{name: "a"})
	r.Report(context.Background(), testReport{name: "b"})

	got := r.Reports()
	require.Len(t, got, 2)
	assert.Equal(t, testReport{name: "a"}, got[0])
	assert.Equal(t, testReport{name: "b"}, got[1])

	// Reports returns a copy: mutating it must not affect the reporter.
	got[0] = nil
	require.Len(t, r.Reports(), 2)

	r.Reset()
	require.Empty(t, r.Reports())
}

// TestReportersIgnoreTypedNil guards the isNilReport helper: once the concrete
// report types land as pointers, a typed nil (e.g. (*testReport)(nil)) is a
// non-nil interface that a plain report == nil check would miss. Both reporters
// must still treat it as "no report".
func TestReportersIgnoreTypedNil(t *testing.T) {
	// A typed nil pointer is a non-nil interface at the language level (report
	// == nil is false), so it slips past a plain nil check; isNilReport must
	// still classify it as "no report".
	var typedNil MetricsReport = (*testReport)(nil)
	t.Run("InMemoryReporter drops it", func(t *testing.T) {
		var r InMemoryReporter
		assert.NotPanics(t, func() {
			r.Report(context.Background(), typedNil)
		})
		assert.Empty(t, r.Reports(), "a typed-nil report must not be retained")
	})

	t.Run("LoggingReporter drops it", func(t *testing.T) {
		var buf bytes.Buffer
		r := NewLoggingReporter(slog.New(slog.NewTextHandler(&buf, nil)))
		assert.NotPanics(t, func() {
			r.Report(context.Background(), typedNil)
		})
		assert.Empty(t, buf.String(), "a typed-nil report must produce no output")
	})
}

func TestLoggingReporterNilLoggerAndReport(t *testing.T) {
	t.Run("nil logger falls back to default", func(t *testing.T) {
		r := NewLoggingReporter(nil) // must fall back to slog.Default
		require.NotNil(t, r)
		assert.NotPanics(t, func() {
			r.Report(context.Background(), nil)
			r.Report(context.Background(), testReport{name: "x"})
		})
	})

	t.Run("logs the report at info level", func(t *testing.T) {
		var buf bytes.Buffer
		r := NewLoggingReporter(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))

		// A nil report is ignored and must produce no output.
		r.Report(context.Background(), nil)
		require.Empty(t, buf.String())

		r.Report(context.Background(), testReport{name: "scan-42"})
		out := buf.String()
		assert.Contains(t, out, "level=INFO")
		assert.Contains(t, out, "iceberg metrics report")
		assert.Contains(t, out, "scan-42", "the report value must be logged")
	})
}

func TestCombine(t *testing.T) {
	t.Run("no reporters returns Nop", func(t *testing.T) {
		assert.IsType(t, NopReporter{}, Combine())
		assert.IsType(t, NopReporter{}, Combine(nil, nil))
	})

	t.Run("single reporter is wrapped for isolation", func(t *testing.T) {
		// Even one reporter is wrapped, so a typed-nil concrete pointer (a
		// non-nil interface that passes the nil filter) still gets the panic
		// isolation the contract promises instead of escaping unwrapped.
		var lr *LoggingReporter // (*LoggingReporter)(nil) as a Reporter is non-nil
		r := Combine(lr)
		assert.IsType(t, &compositeReporter{}, r)
		assert.NotPanics(t, func() {
			r.Report(context.Background(), testReport{name: "x"})
		})
	})

	t.Run("fans out to all", func(t *testing.T) {
		a, b := &countingReporter{}, &countingReporter{}
		Combine(a, b).Report(context.Background(), testReport{})
		assert.Equal(t, 1, a.calls())
		assert.Equal(t, 1, b.calls())
	})

	t.Run("isolates a panicking reporter", func(t *testing.T) {
		after := &countingReporter{}
		r := Combine(panickingReporter{}, after)
		assert.NotPanics(t, func() {
			r.Report(context.Background(), testReport{})
		})
		assert.Equal(t, 1, after.calls(), "reporters after a panic still run")
	})

	t.Run("logs a recovered panic", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
		after := &countingReporter{}
		r := CombineWithLogger(logger, panickingReporter{}, after)

		assert.NotPanics(t, func() {
			r.Report(context.Background(), testReport{})
		})
		assert.Equal(t, 1, after.calls(), "reporters after a panic still run")

		out := buf.String()
		assert.Contains(t, out, "level=WARN")
		assert.Contains(t, out, "panicked", "the warn path must fire")
		assert.Contains(t, out, "panickingReporter", "the offending reporter type is logged")
		assert.Contains(t, out, "boom", "the panic value is logged")
		assert.Contains(t, out, "stack=", "the stack at the panic is logged for locatability")
	})
}

func TestInMemoryReporterConcurrent(t *testing.T) {
	var r InMemoryReporter
	const writers = 100

	var wg sync.WaitGroup
	wg.Add(writers)
	for range writers {
		go func() {
			defer wg.Done()
			r.Report(context.Background(), testReport{})
		}()
	}

	// Exercise the read-under-write path: loop on Reports() in the foreground
	// while the writers run, so -race actually has a concurrent reader to flag
	// rather than just confirming the goroutines finish.
	done := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = r.Reports()
			}
		}
	}()

	wg.Wait()
	close(done)
	reader.Wait()

	assert.Len(t, r.Reports(), writers)
}
