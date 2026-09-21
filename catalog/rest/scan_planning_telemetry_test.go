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
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Keep this test serial: it temporarily replaces the global OTel providers.
func TestScanPlanningTelemetryWiring(t *testing.T) {
	// Deterministic sampling also makes accidental t.Parallel calls fail before
	// the test can replace process-wide providers.
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader),
		sdkmetric.WithExemplarFilter(func(ctx context.Context) bool {
			span := trace.SpanFromContext(ctx)
			assert.True(t, span.IsRecording(), "metrics must be recorded before the operation span ends")

			return span.SpanContext().IsSampled()
		}))
	recorder := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	oldMeter, oldTracer := otel.GetMeterProvider(), otel.GetTracerProvider()
	otel.SetMeterProvider(meter)
	otel.SetTracerProvider(tracer)
	t.Cleanup(func() {
		otel.SetMeterProvider(oldMeter)
		otel.SetTracerProvider(oldTracer)
		require.NoError(t, meter.Shutdown(context.Background()))
		require.NoError(t, tracer.Shutdown(context.Background()))
	})

	var plans, polls, tasks atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan, endpointFetchPlanResult, endpointFetchScanTasks, endpointCancelPlanning}, func(mux *http.ServeMux) {
		mux.HandleFunc("POST /v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, _ *http.Request) {
			if plans.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)

				return
			}
			_, _ = w.Write([]byte(`{"status":"submitted","plan-id":"sensitive-plan"}`))
		})
		mux.HandleFunc("GET /v1/namespaces/db/tables/tbl/plan/sensitive-plan", func(w http.ResponseWriter, _ *http.Request) {
			switch polls.Add(1) {
			case 1:
				w.WriteHeader(http.StatusServiceUnavailable)
			case 2:
				_, _ = w.Write([]byte(`{"status":"submitted"}`))
			default:
				_, _ = w.Write([]byte(`{"status":"completed"}`))
			}
		})
		mux.HandleFunc("GET /v1/namespaces/db/tables/tbl/plan/expired", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"NoSuchPlanIdException","message":"sensitive-plan","code":404}}`))
		})
		mux.HandleFunc("POST /v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, _ *http.Request) {
			if tasks.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)

				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"type":"NoSuchPlanTaskException","message":"sensitive-task","code":404}}`))
		})
		mux.HandleFunc("DELETE /v1/namespaces/db/tables/tbl/plan/sensitive-plan", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
	})
	ctx, parent := tracer.Tracer("test").Start(t.Context(), "caller")
	defer parent.End()
	ident := table.Identifier{"db", "tbl"}
	_, err := cat.PlanTableScan(ctx, ident, PlanTableScanRequest{})
	require.NoError(t, err)
	_, err = cat.WaitForPlan(ctx, ident, "sensitive-plan", fastWaitOpts)
	require.NoError(t, err)
	_, err = cat.FetchPlanningResult(ctx, ident, "expired", FetchPlanningResultOptions{})
	require.ErrorIs(t, err, ErrPlanExpired)
	_, err = cat.FetchScanTasks(ctx, ident, FetchScanTasksRequest{PlanTask: "sensitive-task"})
	require.ErrorIs(t, err, ErrNoSuchPlanTask)
	require.NoError(t, cat.CancelPlanning(ctx, ident, "sensitive-plan"))

	// Exercise both scanner fallback paths and confirm explicit local scans
	// don't count as fallbacks. The full-capability catalog must not be called.
	meta, fs := newPlanningParityTable(t)
	local := table.New(ident, meta, "metadata.json", func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
	capable := table.New(ident, meta, "metadata.json", func(context.Context) (iceio.IO, error) { return fs, nil }, cat)
	for _, scan := range []*table.Scan{
		local.Scan(),
		local.Scan(table.WithScanPlanningMode(table.ScanPlanningAuto)),
		capable.Scan(table.WithScanPlanningMode(table.ScanPlanningAuto), table.WithSelectedFields(iceberg.LastUpdatedSequenceNumberColumnName)),
	} {
		_, err := scan.PlanFiles(ctx)
		require.NoError(t, err)
		require.NoError(t, scan.Close())
	}
	assert.Equal(t, int32(2), plans.Load())
	assert.Equal(t, int32(3), polls.Load())
	assert.Equal(t, int32(2), tasks.Load())

	spans := recorder.Ended()
	require.Len(t, spans, 7, "POST retries belong to one logical span; each poll has its own")
	outcomes := make(map[string]int)
	for _, span := range spans {
		assert.Equal(t, parent.SpanContext().SpanID(), span.Parent().SpanID())
		assert.Equal(t, trace.SpanKindClient, span.SpanKind())
		assert.Equal(t, "github.com/apache/iceberg-go/scan-planning", span.InstrumentationScope().Name)
		require.Len(t, span.Attributes(), 1)
		assert.Equal(t, attribute.Key("iceberg.scan.planning.operation"), span.Attributes()[0].Key)
		assert.Empty(t, span.Events())
		outcome := "success"
		if span.Status().Code == codes.Error {
			outcome = "error"
			assert.Equal(t, "scan planning failed", span.Status().Description)
		}
		outcomes[span.Attributes()[0].Value.AsString()+"/"+outcome]++
	}
	assert.Equal(t, map[string]int{"plan/success": 1, "fetch-result/success": 2, "fetch-result/error": 2, "fetch-tasks/error": 1, "cancel/success": 1}, outcomes)

	var resource metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &resource))
	counts := make(map[string]map[string]int64)
	var durations uint64
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				counts[m.Name] = make(map[string]int64)
				for _, point := range data.DataPoints {
					op, _ := point.Attributes.Value(attribute.Key("iceberg.scan.planning.operation"))
					outcome, _ := point.Attributes.Value(attribute.Key("iceberg.scan.planning.outcome"))
					reason, _ := point.Attributes.Value(attribute.Key("iceberg.scan.planning.reason"))
					key := op.AsString()
					if m.Name == "iceberg.scan.planning.requests" {
						assert.Equal(t, 2, point.Attributes.Len())
						key += "/" + outcome.AsString()
					} else {
						assert.Equal(t, 1, point.Attributes.Len())
					}
					if m.Name == "iceberg.scan.planning.fallbacks" {
						key = reason.AsString()
					}
					if m.Name == "iceberg.scan.planning.expirations" {
						require.Len(t, point.Exemplars, 1)
						exemplar := point.Exemplars[0]
						var matched bool
						for _, span := range spans {
							if span.Name() == "iceberg.scan.planning."+op.AsString() && span.Status().Code == codes.Error {
								spanID, traceID := span.SpanContext().SpanID(), span.SpanContext().TraceID()
								if string(exemplar.SpanID) == string(spanID[:]) && string(exemplar.TraceID) == string(traceID[:]) {
									matched = true
								}
							}
						}
						assert.True(t, matched, "expiration exemplar must identify its failed operation span")
					}
					counts[m.Name][key] += point.Value
				}
			case metricdata.Histogram[float64]:
				assert.Equal(t, "ms", m.Unit)
				for _, point := range data.DataPoints {
					durations += point.Count
				}
			}
		}
	}
	assert.Equal(t, map[string]map[string]int64{
		"iceberg.scan.planning.requests":    {"plan/success": 1, "fetch-result/success": 2, "fetch-result/error": 2, "fetch-tasks/error": 1, "cancel/success": 1},
		"iceberg.scan.planning.retries":     {"plan": 1, "fetch-result": 1, "fetch-tasks": 1},
		"iceberg.scan.planning.expirations": {"fetch-result": 1, "fetch-tasks": 1},
		"iceberg.scan.planning.fallbacks":   {"capability": 1, "row-lineage": 1},
	}, counts)
	assert.Equal(t, uint64(7), durations)
}
