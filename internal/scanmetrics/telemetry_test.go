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

package scanmetrics

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestPlanningTelemetry(t *testing.T) {
	// Provider replacement is process-wide; Setenv also prevents t.Parallel.
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
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

	ctx, finish := Start(t.Context(), "plan")
	Retry(ctx, "plan")
	finish(nil)
	ctx, finish = Start(t.Context(), "fetch-result")
	Expired(ctx, "fetch-result")
	finish(errors.New("secret token and sensitive filter value"))
	Fallback(t.Context(), "capability")

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, "iceberg.scan.planning.plan", spans[0].Name())
	assert.Equal(t, codes.Unset, spans[0].Status().Code)
	assert.Equal(t, codes.Error, spans[1].Status().Code)
	assert.Equal(t, "scan planning failed", spans[1].Status().Description)
	assert.Empty(t, spans[1].Events(), "raw errors must not be exported")
	require.Len(t, spans[1].Attributes(), 1)
	assert.Equal(t, "fetch-result", spans[1].Attributes()[0].Value.AsString())

	var resource metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &resource))
	counts := make(map[string]int64)
	durationCount := uint64(0)
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					counts[m.Name] += point.Value
				}
			case metricdata.Histogram[float64]:
				assert.Equal(t, "ms", m.Unit)
				for _, point := range data.DataPoints {
					durationCount += point.Count
				}
			}
		}
	}
	assert.Equal(t, map[string]int64{
		"iceberg.scan.planning.requests":    2,
		"iceberg.scan.planning.retries":     1,
		"iceberg.scan.planning.expirations": 1,
		"iceberg.scan.planning.fallbacks":   1,
	}, counts)
	assert.Equal(t, uint64(2), durationCount)
}
