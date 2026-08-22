<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Metrics Reporting

Iceberg Go implements Iceberg's Metrics Reporting API. After scan planning it can
emit a **`ScanReport`** (files and manifests considered, scanned, and skipped;
bytes read; planning duration), and after a commit a **`CommitReport`** (commit
attempts, duration, and the added/removed/total file, record, and delete counts).
These metrics are otherwise invisible from outside the client; a reporter gives
operators a standard way to collect them.

A pluggable [`metrics.Reporter`](https://pkg.go.dev/github.com/apache/iceberg-go/metrics#Reporter)
is the sink that receives those reports.

## Opt-in by default

Reporting is **strictly opt-in**. With no reporter configured the instrumented
code paths do no work, and the client emits no logs and no network traffic.

This is a deliberate divergence from Iceberg Java, which defaults to the logging
reporter. A library must not start emitting output the caller never asked for, so
Iceberg Go defaults to a no-op reporter until you select one. Callers migrating
from Java that want reports by default must select the `logging` reporter
explicitly.

## Selecting a reporter

### Catalog-wide, via a property

Set `metrics-reporter-impl` to a registered reporter name. Every table loaded
from the catalog inherits it. This works for REST-catalog config too, since it is
an ordinary catalog property.

```go
cat, err := catalog.Load(ctx, "prod", iceberg.Properties{
    "metrics-reporter-impl": "logging", // built-in: "nop" (default) or "logging"
})
```

An unrecognized name is an error, so a typo surfaces rather than silently
disabling metrics.

### Per-scan override

Override the reporter for a single scan with `table.WithReporter`; it takes
precedence over the reporter inherited from the table.

```go
inmem := &metrics.InMemoryReporter{}

scan := tbl.Scan(table.WithReporter(inmem))
// ... run the scan ...

for _, report := range inmem.Reports() {
    if sr, ok := report.(metrics.ScanReport); ok {
        fmt.Println(sr.TableName, sr.Metrics.ResultDataFiles)
    }
}
```

## Built-in reporters

| Reporter | Purpose |
|---|---|
| `metrics.NopReporter{}` | Discards every report. The default when nothing is configured. |
| `metrics.NewLoggingReporter(logger)` | Logs each report via an `slog.Logger` (`slog.Default()` when `nil`). Registered as `"logging"`. |
| `&metrics.InMemoryReporter{}` | Retains every report for inspection via `Reports()`; `Reset()` clears them. Intended for tests. |
| `metrics.Combine(reporters...)` | Fans each report out to several reporters. A panic in one is isolated from the rest. |

## Writing a custom reporter

Implement the two-method [`Reporter`](https://pkg.go.dev/github.com/apache/iceberg-go/metrics#Reporter)
interface and type-switch on the concrete report:

```go
type myReporter struct{}

func (myReporter) Report(ctx context.Context, report metrics.MetricsReport) {
    switch r := report.(type) {
    case metrics.ScanReport:
        // record r.Metrics ...
    case metrics.CommitReport:
        // record r.Metrics ...
    }
}

func (myReporter) Close() error { return nil } // release any held resources
```

Two contract rules matter:

- **Never block or fail the operation.** `Report` is called inline at the
  scan/commit completion point. A network-backed reporter must dispatch the send
  on a background worker, and any error must be handled internally (logged and
  swallowed), never returned to the caller.
- **Be safe for concurrent use.** `Report` may be called from multiple
  goroutines.

To make a custom reporter selectable by name from `metrics-reporter-impl`,
register a factory for it (typically from `init`):

```go
func init() {
    metrics.Register("my-reporter", func(props map[string]string) (metrics.Reporter, error) {
        return myReporter{}, nil
    })
}
```

## Reporting to a REST catalog

A REST catalog can also POST each report to the catalog's
`.../tables/{table}/metrics` endpoint. This is a separate opt-in from the
in-process reporter above and is **off by default**; the POST is dispatched on a
background worker so it never stalls a scan or commit. See the
[REST metrics-reporting properties](./configuration.md#metrics-reporting) for the
enablement flag and timeout.

## OpenTelemetry

> **Experimental.** The OpenTelemetry metric and attribute names track the
> still-unmerged Iceberg Java reporter
> ([apache/iceberg#16250](https://github.com/apache/iceberg/pull/16250)) and may
> change to stay aligned with it.

The `metrics/otel` package provides an OpenTelemetry-backed reporter. The host
owns the OpenTelemetry SDK; the reporter only looks up a meter (the global
`MeterProvider` by default, or one you pass with `WithMeter`).

```go
import "github.com/apache/iceberg-go/metrics/otel"

rep, err := otel.NewReporter(
    otel.WithMeter(meter),
    otel.WithAttributes(otel.AttrTableName, otel.AttrOperation),
)
```

It exports a curated subset of the report fields under `iceberg.scan.*` /
`iceberg.commit.*` instrument names (e.g. `iceberg.scan.planning.duration`,
`iceberg.scan.result.data_files`, `iceberg.commit.duration`,
`iceberg.commit.data_files.added`).

`WithAttributes` bounds label cardinality by allowlist. The default set is table
name + operation; schema id (`otel.AttrSchemaID`) is opt-in. The snapshot id is
**never** attached as a metric attribute because it is unbounded — per-snapshot
detail stays available through the full reports and the table's snapshot history.

The OpenTelemetry reporter is not registered under a `metrics-reporter-impl` name
automatically. To select it by property, register a factory for it as shown in
[Writing a custom reporter](#writing-a-custom-reporter); otherwise pass the
instance to `table.WithReporter` or `table.WithMetricsReporter`.
