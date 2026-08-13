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

// Package otel provides an OpenTelemetry-backed [metrics.Reporter] for
// iceberg-go. It maps every field of a ScanReport's ScanMetricsResult and a
// CommitReport's CommitMetricsResult to an OpenTelemetry instrument, so
// scan/commit metrics can flow to any OTLP backend (Prometheus, Datadog,
// CloudWatch, ...). Durations are histograms (ms); every other field is a
// monotonic counter (bytes carry the "By" unit, counts are unit-less). Fields
// that are unset in a given report are skipped, so no empty time series is
// created.
//
// The host application owns the OpenTelemetry SDK: the reporter obtains a Meter
// from the global MeterProvider (or one supplied via [WithMeter]). If no SDK is
// registered, the global provider returns no-op instruments and metric calls
// are silently dropped — the standard OpenTelemetry contract.
//
// Cardinality: by default scan metrics carry iceberg.table.name and commit
// metrics carry iceberg.table.name and iceberg.operation. iceberg.schema.id is
// opt-in via [WithAttributes]. The snapshot id is never emitted as an attribute
// — it is unique per commit, so attaching it would create a new time series for
// every commit. Because iceberg.table.name is on by default, the number of time
// series grows with the number of tables; the OpenTelemetry metrics SDK caps a
// stream at 2000 attribute combinations by default and folds the overflow into a
// single otel.metric.overflow=true point, so drop iceberg.table.name (via
// [WithAttributes]) in deployments with a very large number of tables.
//
// The names, units, and attribute set mirror Java's OtelMetricsReporter
// (apache/iceberg#16250) so metrics are consistent across implementations.
// Treat them as provisional until that PR merges.
package otel

import (
	"context"

	"github.com/DataDog/iceberg-go/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName scopes the emitted instruments, mirroring Java's
// "org.apache.iceberg" meter name.
const instrumentationName = "github.com/apache/iceberg-go"

// unitBytes is the UCUM unit emitted for byte-valued counters, matching Java.
const unitBytes = "By"

// Attribute short names accepted by [WithAttributes].
const (
	AttrTableName = "table-name"
	AttrSchemaID  = "schema-id"
	AttrOperation = "operation"
)

// Emitted OTel attribute keys. Snapshot ID is deliberately never an attribute
// (unbounded cardinality).
const (
	keyTableName = "iceberg.table.name"
	keySchemaID  = "iceberg.schema.id"
	keyOperation = "iceberg.operation"
)

// scanCounterSpec describes one scan counter instrument and how to read its
// value out of a ScanMetricsResult.
type scanCounterSpec struct {
	name, desc, unit string
	get              func(metrics.ScanMetricsResult) *metrics.CounterResult
}

// commitCounterSpec describes one commit counter instrument and how to read its
// value out of a CommitMetricsResult.
type commitCounterSpec struct {
	name, desc, unit string
	get              func(metrics.CommitMetricsResult) *metrics.CounterResult
}

// scanCounterSpecs enumerates every counter derived from ScanMetricsResult. The
// order, names, units, and descriptions mirror Java's ScanInstruments.
var scanCounterSpecs = []scanCounterSpec{
	{"iceberg.scan.result.data_files", "Number of data files included in scan result", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.ResultDataFiles }},
	{"iceberg.scan.result.delete_files", "Number of delete files included in scan result", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.ResultDeleteFiles }},
	{"iceberg.scan.data_manifests.total", "Total number of data manifests", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.TotalDataManifests }},
	{"iceberg.scan.delete_manifests.total", "Total number of delete manifests", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.TotalDeleteManifests }},
	{"iceberg.scan.data_manifests.scanned", "Number of data manifests scanned", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.ScannedDataManifests }},
	{"iceberg.scan.data_manifests.skipped", "Number of data manifests skipped", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.SkippedDataManifests }},
	{"iceberg.scan.file_size.bytes", "Total file size of data files in scan result", unitBytes, func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.TotalFileSizeInBytes }},
	{"iceberg.scan.delete_file_size.bytes", "Total file size of delete files in scan result", unitBytes, func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.TotalDeleteFileSizeInBytes }},
	{"iceberg.scan.data_files.skipped", "Number of data files skipped during scan", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.SkippedDataFiles }},
	{"iceberg.scan.delete_files.skipped", "Number of delete files skipped during scan", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.SkippedDeleteFiles }},
	{"iceberg.scan.delete_manifests.scanned", "Number of delete manifests scanned", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.ScannedDeleteManifests }},
	{"iceberg.scan.delete_manifests.skipped", "Number of delete manifests skipped", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.SkippedDeleteManifests }},
	{"iceberg.scan.delete_files.indexed", "Number of indexed delete files", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.IndexedDeleteFiles }},
	{"iceberg.scan.delete_files.equality", "Number of equality delete files", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.EqualityDeleteFiles }},
	{"iceberg.scan.delete_files.positional", "Number of positional delete files", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.PositionalDeleteFiles }},
	{"iceberg.scan.dvs", "Number of deletion vectors in scan result", "", func(m metrics.ScanMetricsResult) *metrics.CounterResult { return m.DVs }},
}

// commitCounterSpecs enumerates every counter derived from CommitMetricsResult.
// The order, names, units, and descriptions mirror Java's CommitInstruments.
var commitCounterSpecs = []commitCounterSpec{
	{"iceberg.commit.attempts", "Number of commit attempts", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.Attempts }},
	{"iceberg.commit.data_files.added", "Number of data files added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedDataFiles }},
	{"iceberg.commit.data_files.removed", "Number of data files removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedDataFiles }},
	{"iceberg.commit.data_files.total", "Total number of data files after commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.TotalDataFiles }},
	{"iceberg.commit.delete_files.added", "Number of delete files added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedDeleteFiles }},
	{"iceberg.commit.delete_files.equality.added", "Number of equality delete files added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedEqualityDeleteFiles }},
	{"iceberg.commit.delete_files.positional.added", "Number of positional delete files added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedPositionalDeleteFiles }},
	{"iceberg.commit.dvs.added", "Number of deletion vectors added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedDVs }},
	{"iceberg.commit.delete_files.removed", "Number of delete files removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedDeleteFiles }},
	{"iceberg.commit.delete_files.equality.removed", "Number of equality delete files removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedEqualityDeleteFiles }},
	{"iceberg.commit.delete_files.positional.removed", "Number of positional delete files removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedPositionalDeleteFiles }},
	{"iceberg.commit.dvs.removed", "Number of deletion vectors removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedDVs }},
	{"iceberg.commit.delete_files.total", "Total number of delete files after commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.TotalDeleteFiles }},
	{"iceberg.commit.records.added", "Number of records added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedRecords }},
	{"iceberg.commit.records.removed", "Number of records removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedRecords }},
	{"iceberg.commit.records.total", "Total number of records after commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.TotalRecords }},
	{"iceberg.commit.file_size.added_bytes", "Total size of data files added by commit", unitBytes, func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedFilesSizeBytes }},
	{"iceberg.commit.file_size.removed_bytes", "Total size of data files removed by commit", unitBytes, func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedFilesSizeBytes }},
	{"iceberg.commit.file_size.total_bytes", "Total size of all data files after commit", unitBytes, func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.TotalFilesSizeBytes }},
	{"iceberg.commit.positional_deletes.added", "Number of positional deletes added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedPositionalDeletes }},
	{"iceberg.commit.positional_deletes.removed", "Number of positional deletes removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedPositionalDeletes }},
	{"iceberg.commit.positional_deletes.total", "Total number of positional deletes after commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.TotalPositionalDeletes }},
	{"iceberg.commit.equality_deletes.added", "Number of equality deletes added by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.AddedEqualityDeletes }},
	{"iceberg.commit.equality_deletes.removed", "Number of equality deletes removed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.RemovedEqualityDeletes }},
	{"iceberg.commit.equality_deletes.total", "Total number of equality deletes after commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.TotalEqualityDeletes }},
	{"iceberg.commit.manifests.created", "Number of manifests created by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.ManifestsCreated }},
	{"iceberg.commit.manifests.replaced", "Number of manifests replaced by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.ManifestsReplaced }},
	{"iceberg.commit.manifests.kept", "Number of manifests kept by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.ManifestsKept }},
	{"iceberg.commit.manifest_entries.processed", "Number of manifest entries processed by commit", "", func(m metrics.CommitMetricsResult) *metrics.CounterResult { return m.ManifestEntriesProcessed }},
}

// Reporter records iceberg metrics reports to OpenTelemetry instruments.
type Reporter struct {
	allow map[string]bool

	scanPlanningDuration metric.Float64Histogram
	scanCounters         []metric.Int64Counter // parallel to scanCounterSpecs

	commitDuration metric.Float64Histogram
	commitCounters []metric.Int64Counter // parallel to commitCounterSpecs
}

var _ metrics.Reporter = (*Reporter)(nil)

type config struct {
	meter metric.Meter
	allow map[string]bool
}

// Option configures a [Reporter].
type Option func(*config)

// WithMeter sets the Meter used to create instruments. By default the global
// MeterProvider's Meter is used.
func WithMeter(m metric.Meter) Option {
	return func(c *config) { c.meter = m }
}

// WithAttributes sets the allowlist of attribute short names (AttrTableName,
// AttrSchemaID, AttrOperation) attached to emitted metrics. Names not listed are
// omitted, which is how cardinality is bounded. The default is AttrTableName and
// AttrOperation; AttrSchemaID is opt-in. Passing no names emits metrics with no
// attributes at all.
func WithAttributes(short ...string) Option {
	return func(c *config) {
		c.allow = make(map[string]bool, len(short))
		for _, s := range short {
			c.allow[s] = true
		}
	}
}

// NewReporter creates an OpenTelemetry reporter, creating its instruments on the
// configured (or global) Meter.
func NewReporter(opts ...Option) (*Reporter, error) {
	cfg := config{allow: map[string]bool{AttrTableName: true, AttrOperation: true}}
	for _, o := range opts {
		o(&cfg)
	}
	meter := cfg.meter
	if meter == nil {
		meter = otel.Meter(instrumentationName)
	}

	r := &Reporter{allow: cfg.allow}
	var err error
	mkHist := func(h *metric.Float64Histogram, name, desc string) {
		if err != nil {
			return
		}
		*h, err = meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("ms"))
	}

	mkHist(&r.scanPlanningDuration, "iceberg.scan.planning.duration", "Time spent planning a table scan")
	r.scanCounters = make([]metric.Int64Counter, len(scanCounterSpecs))
	for i, s := range scanCounterSpecs {
		if err != nil {
			break
		}
		r.scanCounters[i], err = newCounter(meter, s.name, s.desc, s.unit)
	}

	mkHist(&r.commitDuration, "iceberg.commit.duration", "Time spent on commit operation")
	r.commitCounters = make([]metric.Int64Counter, len(commitCounterSpecs))
	for i, s := range commitCounterSpecs {
		if err != nil {
			break
		}
		r.commitCounters[i], err = newCounter(meter, s.name, s.desc, s.unit)
	}

	if err != nil {
		return nil, err
	}

	return r, nil
}

// newCounter builds an Int64Counter, attaching a unit only when one is set
// (counts are unit-less; byte counters carry "By").
func newCounter(meter metric.Meter, name, desc, unit string) (metric.Int64Counter, error) {
	opts := []metric.Int64CounterOption{metric.WithDescription(desc)}
	if unit != "" {
		opts = append(opts, metric.WithUnit(unit))
	}

	return meter.Int64Counter(name, opts...)
}

// Report implements [metrics.Reporter].
func (r *Reporter) Report(ctx context.Context, report metrics.MetricsReport) {
	switch rep := report.(type) {
	case metrics.ScanReport:
		r.reportScan(ctx, rep)
	case *metrics.ScanReport:
		if rep != nil {
			r.reportScan(ctx, *rep)
		}
	case metrics.CommitReport:
		r.reportCommit(ctx, rep)
	case *metrics.CommitReport:
		if rep != nil {
			r.reportCommit(ctx, *rep)
		}
	}
}

// Close implements [io.Closer]. The reporter is stateless — it does not own the
// OpenTelemetry SDK or MeterProvider (the host application does), so there is
// nothing to release and Close always returns nil.
func (r *Reporter) Close() error { return nil }

func (r *Reporter) reportScan(ctx context.Context, sr metrics.ScanReport) {
	var attrs []attribute.KeyValue
	if r.allow[AttrTableName] {
		attrs = append(attrs, attribute.String(keyTableName, sr.TableName))
	}
	if r.allow[AttrSchemaID] {
		attrs = append(attrs, attribute.Int(keySchemaID, sr.SchemaID))
	}
	opt := metric.WithAttributes(attrs...)

	m := sr.Metrics
	recordDuration(ctx, r.scanPlanningDuration, m.TotalPlanningDuration, opt)
	for i, s := range scanCounterSpecs {
		addCounter(ctx, r.scanCounters[i], s.get(m), opt)
	}
}

func (r *Reporter) reportCommit(ctx context.Context, cr metrics.CommitReport) {
	var attrs []attribute.KeyValue
	if r.allow[AttrTableName] {
		attrs = append(attrs, attribute.String(keyTableName, cr.TableName))
	}
	if r.allow[AttrOperation] {
		attrs = append(attrs, attribute.String(keyOperation, cr.Operation))
	}
	opt := metric.WithAttributes(attrs...)

	m := cr.Metrics
	recordDuration(ctx, r.commitDuration, m.TotalDuration, opt)
	for i, s := range commitCounterSpecs {
		addCounter(ctx, r.commitCounters[i], s.get(m), opt)
	}
}

func addCounter(ctx context.Context, c metric.Int64Counter, cr *metrics.CounterResult, opt metric.AddOption) {
	if cr != nil {
		c.Add(ctx, cr.Value, opt)
	}
}

// recordDuration records a timer's total duration in milliseconds. Durations are
// emitted in nanoseconds (see metrics.NewNanosTimerResult), so convert to ms.
func recordDuration(ctx context.Context, h metric.Float64Histogram, tr *metrics.TimerResult, opt metric.RecordOption) {
	if tr != nil {
		h.Record(ctx, float64(tr.TotalDuration)/1e6, opt)
	}
}
