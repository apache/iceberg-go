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

package otel

import (
	"context"
	"testing"

	"github.com/DataDog/iceberg-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestReporter(t *testing.T, opts ...Option) (*Reporter, func() metricdata.ResourceMetrics) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	rep, err := NewReporter(append([]Option{WithMeter(mp.Meter("test"))}, opts...)...)
	require.NoError(t, err)

	return rep, func() metricdata.ResourceMetrics {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))

		return rm
	}
}

func findMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not emitted", name)

	return metricdata.Metrics{}
}

func metricNames(rm metricdata.ResourceMetrics) map[string]bool {
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}

	return names
}

func sumValue(t *testing.T, m metricdata.Metrics) (int64, attribute.Set) {
	t.Helper()
	s, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %q is not an int64 sum", m.Name)
	require.Len(t, s.DataPoints, 1)

	return s.DataPoints[0].Value, s.DataPoints[0].Attributes
}

func histogramSum(t *testing.T, m metricdata.Metrics) float64 {
	t.Helper()
	h, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "metric %q is not a float64 histogram", m.Name)
	require.Len(t, h.DataPoints, 1)

	return h.DataPoints[0].Sum
}

func attrStr(t *testing.T, set attribute.Set, key string) string {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q missing", key)

	return v.AsString()
}

func attrInt(t *testing.T, set attribute.Set, key string) int64 {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q missing", key)

	return v.AsInt64()
}

// attributeKeys returns the union of attribute keys across every data point in
// the collection, mirroring Java's per-test attribute-set assertions.
func attributeKeys(rm metricdata.ResourceMetrics) map[string]bool {
	keys := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if s, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					for _, kv := range dp.Attributes.ToSlice() {
						keys[string(kv.Key)] = true
					}
				}
			}
		}
	}

	return keys
}

// fullScanMetrics is a ScanMetricsResult with every field populated, used to
// assert full instrument coverage.
func fullScanMetrics() metrics.ScanMetricsResult {
	c := func(v int64) *metrics.CounterResult { return metrics.NewCounterResult(metrics.UnitCount, v) }

	return metrics.ScanMetricsResult{
		TotalPlanningDuration:      metrics.NewNanosTimerResult(1, 150_000_000), // 150ms
		ResultDataFiles:            c(10),
		ResultDeleteFiles:          c(2),
		TotalDataManifests:         c(8),
		TotalDeleteManifests:       c(4),
		ScannedDataManifests:       c(5),
		SkippedDataManifests:       c(3),
		TotalFileSizeInBytes:       metrics.NewCounterResult(metrics.UnitBytes, 1024000),
		TotalDeleteFileSizeInBytes: metrics.NewCounterResult(metrics.UnitBytes, 2048),
		SkippedDataFiles:           c(7),
		SkippedDeleteFiles:         c(1),
		ScannedDeleteManifests:     c(3),
		SkippedDeleteManifests:     c(1),
		IndexedDeleteFiles:         c(6),
		EqualityDeleteFiles:        c(2),
		PositionalDeleteFiles:      c(4),
		DVs:                        c(9),
	}
}

// fullCommitMetrics is a CommitMetricsResult with every field populated.
func fullCommitMetrics() metrics.CommitMetricsResult {
	c := func(v int64) *metrics.CounterResult { return metrics.NewCounterResult(metrics.UnitCount, v) }

	return metrics.CommitMetricsResult{
		TotalDuration:                metrics.NewNanosTimerResult(1, 200_000_000), // 200ms
		Attempts:                     c(1),
		AddedDataFiles:               c(5),
		RemovedDataFiles:             c(2),
		TotalDataFiles:               c(15),
		AddedDeleteFiles:             c(3),
		AddedEqualityDeleteFiles:     c(1),
		AddedPositionalDeleteFiles:   c(2),
		AddedDVs:                     c(4),
		RemovedDeleteFiles:           c(1),
		RemovedEqualityDeleteFiles:   c(0),
		RemovedPositionalDeleteFiles: c(1),
		RemovedDVs:                   c(0),
		TotalDeleteFiles:             c(8),
		AddedRecords:                 c(1000),
		RemovedRecords:               c(50),
		TotalRecords:                 c(5000),
		AddedFilesSizeBytes:          metrics.NewCounterResult(metrics.UnitBytes, 512000),
		RemovedFilesSizeBytes:        metrics.NewCounterResult(metrics.UnitBytes, 10000),
		TotalFilesSizeBytes:          metrics.NewCounterResult(metrics.UnitBytes, 2000000),
		AddedPositionalDeletes:       c(100),
		RemovedPositionalDeletes:     c(20),
		TotalPositionalDeletes:       c(300),
		AddedEqualityDeletes:         c(50),
		RemovedEqualityDeletes:       c(10),
		TotalEqualityDeletes:         c(150),
		ManifestsCreated:             c(3),
		ManifestsReplaced:            c(2),
		ManifestsKept:                c(7),
		ManifestEntriesProcessed:     c(500),
	}
}

func TestReporterScanFullCoverage(t *testing.T) {
	rep, collect := newTestReporter(t)
	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t", SchemaID: 3, Metrics: fullScanMetrics()})

	rm := collect()
	names := metricNames(rm)

	// The planning-duration histogram plus one counter per scan spec must all exist.
	assert.True(t, names["iceberg.scan.planning.duration"], "planning duration missing")
	for _, s := range scanCounterSpecs {
		assert.True(t, names[s.name], "scan metric %q not emitted", s.name)
	}

	// Spot-check values and units across the data / delete / dv paths.
	files, _ := sumValue(t, findMetric(t, rm, "iceberg.scan.result.data_files"))
	assert.Equal(t, int64(10), files)
	skippedDelManifests, _ := sumValue(t, findMetric(t, rm, "iceberg.scan.delete_manifests.skipped"))
	assert.Equal(t, int64(1), skippedDelManifests)
	dvs, _ := sumValue(t, findMetric(t, rm, "iceberg.scan.dvs"))
	assert.Equal(t, int64(9), dvs)

	bytesMetric := findMetric(t, rm, "iceberg.scan.file_size.bytes")
	assert.Equal(t, "By", bytesMetric.Unit)
	countMetric := findMetric(t, rm, "iceberg.scan.result.data_files")
	assert.Empty(t, countMetric.Unit, "count instruments carry no unit")

	assert.InDelta(t, 150.0, histogramSum(t, findMetric(t, rm, "iceberg.scan.planning.duration")), 0.0001)
}

func TestReporterCommitFullCoverage(t *testing.T) {
	rep, collect := newTestReporter(t)
	rep.Report(context.Background(), metrics.CommitReport{TableName: "db.t", Operation: "append", Metrics: fullCommitMetrics()})

	rm := collect()
	names := metricNames(rm)

	assert.True(t, names["iceberg.commit.duration"], "commit duration missing")
	for _, s := range commitCounterSpecs {
		assert.True(t, names[s.name], "commit metric %q not emitted", s.name)
	}

	recs, _ := sumValue(t, findMetric(t, rm, "iceberg.commit.records.added"))
	assert.Equal(t, int64(1000), recs)
	eqAdded, _ := sumValue(t, findMetric(t, rm, "iceberg.commit.delete_files.equality.added"))
	assert.Equal(t, int64(1), eqAdded)
	manifestsKept, _ := sumValue(t, findMetric(t, rm, "iceberg.commit.manifests.kept"))
	assert.Equal(t, int64(7), manifestsKept)

	bytesMetric := findMetric(t, rm, "iceberg.commit.file_size.added_bytes")
	assert.Equal(t, "By", bytesMetric.Unit)

	assert.InDelta(t, 200.0, histogramSum(t, findMetric(t, rm, "iceberg.commit.duration")), 0.0001)
}

// TestReporterNilMetricsEmitNothing mirrors Java's testNullMetricsAreHandled: an
// empty metrics result produces no time series.
func TestReporterNilMetricsEmitNothing(t *testing.T) {
	rep, collect := newTestReporter(t)
	rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})

	assert.Empty(t, metricNames(collect()), "empty ScanMetricsResult must emit no metrics")
}

// TestReporterMultipleReports verifies counters accumulate across reports.
func TestReporterMultipleReports(t *testing.T) {
	rep, collect := newTestReporter(t)
	for i := 0; i < 3; i++ {
		rep.Report(context.Background(), metrics.CommitReport{
			TableName: "db.t",
			Operation: "append",
			Metrics: metrics.CommitMetricsResult{
				Attempts:       metrics.NewCounterResult(metrics.UnitCount, 1),
				AddedDataFiles: metrics.NewCounterResult(metrics.UnitCount, 10),
			},
		})
	}

	rm := collect()
	files, _ := sumValue(t, findMetric(t, rm, "iceberg.commit.data_files.added"))
	assert.Equal(t, int64(30), files)
	attempts, _ := sumValue(t, findMetric(t, rm, "iceberg.commit.attempts"))
	assert.Equal(t, int64(3), attempts)
}

// TestReporterDefaultAttributeSet asserts the default attribute set matches Java:
// table-name + operation, with schema-id and snapshot-id excluded.
func TestReporterDefaultAttributeSet(t *testing.T) {
	rep, collect := newTestReporter(t)
	rep.Report(context.Background(), metrics.ScanReport{
		TableName: "db.t", SchemaID: 3,
		Metrics: metrics.ScanMetricsResult{ResultDataFiles: metrics.NewCounterResult(metrics.UnitCount, 1)},
	})
	rep.Report(context.Background(), metrics.CommitReport{
		TableName: "db.t", Operation: "append",
		Metrics: metrics.CommitMetricsResult{Attempts: metrics.NewCounterResult(metrics.UnitCount, 1)},
	})

	keys := attributeKeys(collect())
	assert.Contains(t, keys, keyTableName)
	assert.Contains(t, keys, keyOperation)
	assert.NotContains(t, keys, keySchemaID, "schema-id is opt-in, not in the default set")
	assert.NotContains(t, keys, "iceberg.snapshot.id", "snapshot-id is never an attribute")
}

func TestReporterScanDefaultAttributes(t *testing.T) {
	rep, collect := newTestReporter(t)
	rep.Report(context.Background(), metrics.ScanReport{
		TableName: "db.t", SchemaID: 3,
		Metrics: metrics.ScanMetricsResult{ResultDataFiles: metrics.NewCounterResult(metrics.UnitCount, 5)},
	})

	_, attrs := sumValue(t, findMetric(t, collect(), "iceberg.scan.result.data_files"))
	assert.Equal(t, "db.t", attrStr(t, attrs, keyTableName))
	_, ok := attrs.Value(attribute.Key(keySchemaID))
	assert.False(t, ok, "schema-id must be omitted by default")
}

func TestReporterAttributeAllowlistOptInSchemaID(t *testing.T) {
	rep, collect := newTestReporter(t, WithAttributes(AttrTableName, AttrSchemaID, AttrOperation))
	rep.Report(context.Background(), metrics.ScanReport{
		TableName: "db.t", SchemaID: 7,
		Metrics: metrics.ScanMetricsResult{ResultDataFiles: metrics.NewCounterResult(metrics.UnitCount, 1)},
	})

	_, attrs := sumValue(t, findMetric(t, collect(), "iceberg.scan.result.data_files"))
	assert.Equal(t, int64(7), attrInt(t, attrs, keySchemaID))
	assert.Equal(t, "db.t", attrStr(t, attrs, keyTableName))
}

func TestReporterAttributeAllowlistExcludesTableName(t *testing.T) {
	rep, collect := newTestReporter(t, WithAttributes(AttrOperation))
	rep.Report(context.Background(), metrics.CommitReport{
		TableName: "db.t", Operation: "append",
		Metrics: metrics.CommitMetricsResult{Attempts: metrics.NewCounterResult(metrics.UnitCount, 1)},
	})

	_, attrs := sumValue(t, findMetric(t, collect(), "iceberg.commit.attempts"))
	assert.Equal(t, "append", attrStr(t, attrs, keyOperation))
	_, ok := attrs.Value(attribute.Key(keyTableName))
	assert.False(t, ok, "table-name must be omitted when not in the allowlist")
}

func TestReporterAttributeAllowlistNoneEmitsNoAttributes(t *testing.T) {
	rep, collect := newTestReporter(t, WithAttributes())
	rep.Report(context.Background(), metrics.CommitReport{
		TableName: "db.t", Operation: "append",
		Metrics: metrics.CommitMetricsResult{Attempts: metrics.NewCounterResult(metrics.UnitCount, 1)},
	})

	assert.Empty(t, attributeKeys(collect()), "empty allowlist must emit no attributes")
}

func TestReporterPointerReports(t *testing.T) {
	rep, collect := newTestReporter(t)
	rep.Report(context.Background(), &metrics.ScanReport{
		TableName: "db.t",
		Metrics:   metrics.ScanMetricsResult{ResultDataFiles: metrics.NewCounterResult(metrics.UnitCount, 7)},
	})
	rep.Report(context.Background(), &metrics.CommitReport{
		TableName: "db.t",
		Metrics:   metrics.CommitMetricsResult{AddedRecords: metrics.NewCounterResult(metrics.UnitCount, 9)},
	})

	rm := collect()
	files, _ := sumValue(t, findMetric(t, rm, "iceberg.scan.result.data_files"))
	assert.Equal(t, int64(7), files)
	recs, _ := sumValue(t, findMetric(t, rm, "iceberg.commit.records.added"))
	assert.Equal(t, int64(9), recs)
}

func TestReporterIgnoresNilReport(t *testing.T) {
	rep, _ := newTestReporter(t)
	assert.NotPanics(t, func() {
		rep.Report(context.Background(), nil)
		// Typed-nil pointers are non-nil interface values; they must be ignored,
		// not dereferenced (mirrors metrics.isNilReport).
		rep.Report(context.Background(), (*metrics.ScanReport)(nil))
		rep.Report(context.Background(), (*metrics.CommitReport)(nil))
	})
}

func TestReporterClose(t *testing.T) {
	rep, _ := newTestReporter(t)
	assert.NoError(t, rep.Close())
}

// TestReporterDefaultGlobalMeter exercises the WithMeter-unset path: with no SDK
// registered the global provider yields no-op instruments, so construction
// succeeds and Report is a silent no-op rather than a panic.
func TestReporterDefaultGlobalMeter(t *testing.T) {
	rep, err := NewReporter()
	require.NoError(t, err)
	assert.NotPanics(t, func() {
		rep.Report(context.Background(), metrics.ScanReport{TableName: "db.t"})
	})
}
