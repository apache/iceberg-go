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
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/apache/iceberg-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturedRequest struct {
	method string
	path   string
	body   []byte
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
		c.ch <- capturedRequest{method: r.Method, path: r.URL.Path, body: body}
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

func newTestReporter(t *testing.T, tr *captureTransport) *restMetricsReporter {
	t.Helper()
	base, err := url.Parse("http://catalog.invalid")
	require.NoError(t, err)

	return &restMetricsReporter{
		baseURI: base,
		cl:      &http.Client{Transport: tr},
		path:    []string{"namespaces", "db", "tables", "t", "metrics"},
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
