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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testReport is a minimal Report. The marker is unexported (sealed to this
// package), so report values can only be created from within the package — this
// white-box test is how we exercise the reporters until the concrete report
// types land.
type testReport struct{ name string }

func (testReport) isMetricsReport() {}

// countingReporter records how many reports it received.
type countingReporter struct {
	mu    sync.Mutex
	count int
}

func (c *countingReporter) Report(context.Context, Report) {
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

func (panickingReporter) Report(context.Context, Report) { panic("boom") }

func TestNopReporter(t *testing.T) {
	// Nop must accept anything, including nil, without panicking.
	assert.NotPanics(t, func() {
		Nop{}.Report(context.Background(), nil)
		Nop{}.Report(context.Background(), testReport{})
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

func TestLoggingReporterNilLoggerAndReport(t *testing.T) {
	r := NewLoggingReporter(nil) // must fall back to slog.Default
	require.NotNil(t, r)
	assert.NotPanics(t, func() {
		r.Report(context.Background(), nil)
		r.Report(context.Background(), testReport{name: "x"})
	})
}

func TestCombine(t *testing.T) {
	t.Run("no reporters returns Nop", func(t *testing.T) {
		assert.IsType(t, Nop{}, Combine())
		assert.IsType(t, Nop{}, Combine(nil, nil))
	})

	t.Run("single reporter returned directly", func(t *testing.T) {
		c := &countingReporter{}
		assert.Same(t, c, Combine(c))
		assert.Same(t, c, Combine(nil, c, nil))
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
}

func TestInMemoryReporterConcurrent(t *testing.T) {
	var r InMemoryReporter
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			r.Report(context.Background(), testReport{})
		}()
	}
	wg.Wait()
	assert.Len(t, r.Reports(), n)
}
