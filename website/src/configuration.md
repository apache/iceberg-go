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

# Configuration

This page documents how to configure Apache Iceberg Go: the CLI's YAML config file, per-catalog `Option` surfaces, file-system credentials, table write properties, concurrency, and how to plug in custom catalogs and IO backends.

Only properties and options that the iceberg-go code actually reads are listed. Properties defined in the Apache Iceberg spec but not yet wired into iceberg-go are intentionally omitted - check `pkg.go.dev/github.com/apache/iceberg-go` for the latest read sites.

## CLI configuration file

The `iceberg` CLI loads catalog defaults from `~/.iceberg-go.yaml` (override the directory with `GOICEBERG_HOME`). The schema, defined in [`config/config.go`](https://github.com/apache/iceberg-go/blob/main/config/config.go), is:

```yaml
default-catalog: default
max-workers: 5
catalog:
  default:
    type: rest
    uri: https://example.com/iceberg
    warehouse: s3://my-bucket/warehouse
    credential: <client-id>:<client-secret>
    output: text
    aws-profile: ""
    rest:
      sigv4-enabled: false
      signing-name: ""
      signing-region: ""
```

| Key | Purpose |
|---|---|
| `default-catalog` | Name used when `--catalog-name` is not passed on the CLI. |
| `max-workers` | Worker pool size for concurrent operations. Default `5`. |
| `catalog.<name>.type` | One of `rest`, `hive`, `glue`, `sql`, `hadoop`. `dynamodb` is recognized by the CLI but not implemented yet. |
| `catalog.<name>.uri` | Catalog endpoint or DSN. |
| `catalog.<name>.warehouse` | Warehouse identifier (REST/Glue) or location (Hive/SQL). |
| `catalog.<name>.credential` | Credential string passed through to the catalog's auth handler. |
| `catalog.<name>.output` | CLI output format (e.g. `text`, `json`). |
| `catalog.<name>.aws-profile` | AWS named profile for the Glue catalog. When unset, the AWS SDK default credential chain is used. |
| `catalog.<name>.sql-driver` | `database/sql` driver name for the SQL catalog. Maps to the `sql.driver` property. The default CLI binary only compiles in `sqliteshim`; other drivers require a custom build. |
| `catalog.<name>.sql-dialect` | SQL dialect for the SQL catalog (`postgres`, `mysql`, `sqlite`, `mssql`, `oracle`). Maps to the `sql.dialect` property. The default CLI binary only ships `sqlite` via `sqliteshim`; other dialects need a custom build with their drivers. |
| `catalog.<name>.rest.sigv4-enabled` | Enable AWS SigV4 signing for REST. |
| `catalog.<name>.rest.signing-name` | SigV4 service name. |
| `catalog.<name>.rest.signing-region` | SigV4 region. |

## Catalog options

Each catalog package exposes its own functional `Option` set. The lists below reflect the public option surface; `pkg.go.dev` is authoritative for the current set.

### REST (`catalog/rest`)

The most option-rich surface. Source: [`catalog/rest/options.go`](https://github.com/apache/iceberg-go/blob/main/catalog/rest/options.go).

| Group | Options |
|---|---|
| Authentication | `WithCredential`, `WithOAuthToken`, `WithAuthManager`, `WithAuthURI`, `WithScope`, `WithAudience`, `WithResource` |
| AWS SigV4 | `WithSigV4`, `WithSigV4RegionSvc`, `WithAwsConfig` |
| HTTP | `WithHeaders`, `WithTLSConfig`, `WithOAuthTLSConfig`, `WithCustomTransport` |
| Catalog routing | `WithPrefix`, `WithWarehouseLocation`, `WithMetadataLocation` |
| Pass-through | `WithAdditionalProps` |

#### Metrics reporting

The REST catalog can POST scan and commit metrics to the catalog's
`.../tables/{table}/metrics` endpoint. These properties are passed as
client-supplied catalog properties.

| Property | Description |
|---|---|
| `rest-metrics-reporting-enabled` | Opt into POSTing metrics reports. **Off by default.** |
| `rest.metrics-reporting-enabled` | Legacy dotted alias; consulted only when the canonical key above is absent. |
| `rest-metrics-reporting-timeout-ms` | Per-report request/response deadline in milliseconds. Default `10000`. |

> **Note on cross-client parity.** Iceberg Java defaults metrics reporting to
> *on* (`METRICS_REPORTING_ENABLED_DEFAULT=true`); iceberg-go deliberately
> defaults it *off*, so no client emits new outbound telemetry unless it
> explicitly opts in. A Java client that never set the flag will therefore stop
> reporting when ported here until `rest-metrics-reporting-enabled=true` is set.
> Enablement is read from client properties only — a server cannot turn on
> reporting the client never asked for — but an explicit server override setting
> the flag to `false` *is* honored (disabling is always safe), so an operator can
> suppress reporting fleet-wide. `rest-metrics-reporting-timeout-ms` is a
> Go-specific extension with no Java, PyIceberg, or Rust equivalent; do not
> assume parity on it in multi-client deployments.

### Hive (`catalog/hive`)

Source: [`catalog/hive/options.go`](https://github.com/apache/iceberg-go/blob/main/catalog/hive/options.go).

- `WithURI(uri string)` - Thrift URI for the Hive Metastore (e.g. `thrift://127.0.0.1:9083`).
- `WithWarehouse(warehouse string)`
- `WithProperties(props iceberg.Properties)`

Lock-check catalog properties (passed via `WithProperties` / `catalog.Load` props):

| Property | Default | Description |
|---|---|---|
| `iceberg.hive.lock-check-min-wait-ms` | `50` | Minimum wait between lock-status checks, in milliseconds. Matches the Java Hive catalog key. |
| `iceberg.hive.lock-check-max-wait-ms` | `5000` | Maximum wait between lock-status checks, in milliseconds. Matches the Java Hive catalog key. |
| `lock-check-min-wait-time` / `lock-check-max-wait-time` | same defaults | Legacy Go aliases that accept Go duration strings (e.g. `100ms`, `5s`). Ignored when the corresponding Java key is set. |
| `lock-check-retries` | `4` | Go-specific upper bound on check attempts. Java instead uses `iceberg.hive.lock-timeout-ms` (not wired in Go yet). |

Invalid values (non-positive waits/retries, or min ≥ max) are ignored and the previous/default settings are kept.

### Glue (`catalog/glue`)

Source: [`catalog/glue/options.go`](https://github.com/apache/iceberg-go/blob/main/catalog/glue/options.go).

- `WithAwsConfig(cfg aws.Config)` - AWS SDK v2 config; respects the AWS default credential chain.
- `WithAwsProperties(props AwsProperties)` - explicit overrides for region/endpoint/access keys.

### SQL (`catalog/sql`)

The SQL catalog has no functional-option surface. Construct it with `NewCatalog`:

```go
db, _ := sql.Open(sqliteshim.ShimName, "file:catalog.db")
cat, err := sqlcat.NewCatalog("default", db, sqlcat.SQLite, iceberg.Properties{
    "warehouse": "file:///tmp/warehouse",
})
```

Supported dialects: `sqlcat.Postgres`, `sqlcat.MySQL`, `sqlcat.SQLite`, `sqlcat.MSSQL`, `sqlcat.Oracle` (`catalog/sql/sql.go:50`).

### Shared options on the base `catalog` package

Operations that create or update tables/views accept these (`catalog/catalog.go`):

- `WithLocation`, `WithPartitionSpec`, `WithSortOrder`, `WithProperties`, `WithStagedUpdates`
- View-specific: `WithViewLocation`, `WithViewProperties`

## File-system credentials

iceberg-go registers the local file system (`file://`) automatically. Cloud schemes are *not* registered until you add a blank import:

```go
import _ "github.com/apache/iceberg-go/io/gocloud/s3"    // s3, s3a, s3n, oss
import _ "github.com/apache/iceberg-go/io/gocloud/gcs"   // gs
import _ "github.com/apache/iceberg-go/io/gocloud/azure" // abfs, abfss, wasb, wasbs
```

Each backend package links its own cloud SDK. Importing `io/gocloud` registers all three at once and links all cloud SDKs.

Without a matching blank import, these schemes return `ErrIOSchemeNotFound` with a hint naming the package to import.

All credential and tuning property keys are constants in [`io/config.go`](https://github.com/apache/iceberg-go/blob/main/io/config.go). They can be supplied through table properties, catalog properties, or per-call `iceberg.Properties` arguments depending on context.

### S3

Authentication is resolved in this order (`io/gocloud/s3/s3.go`):

1. Static credentials in properties: `s3.access-key-id` + `s3.secret-access-key` (+ optional `s3.session-token`).
2. The standard AWS SDK v2 default credential chain - environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), `~/.aws/credentials`, container/IAM role.

Tuning properties:

| Key (constant) | Purpose |
|---|---|
| `s3.region` (`io.S3Region`) / `client.region` (`io.S3ClientRegion`) | AWS region. |
| `s3.endpoint` (`io.S3EndpointURL`) | Override S3 endpoint URL (custom or compatible storage). Falls back to `AWS_S3_ENDPOINT` env. |
| `s3.access-key-id` (`io.S3AccessKeyID`) | Static access key. |
| `s3.secret-access-key` (`io.S3SecretAccessKey`) | Static secret key. |
| `s3.session-token` (`io.S3SessionToken`) | Static session token. |
| `s3.proxy-uri` (`io.S3ProxyURI`) | HTTP proxy URL. |
| `s3.connect-timeout` (`io.S3ConnectTimeout`) | Either a number of seconds (`"60"`, `"60.0"`) or a Go duration (`"5s"`). |
| `s3.force-virtual-addressing` (`io.S3ForceVirtualAddressing`) | Force virtual-host-style addressing. |
| `s3.compat-mode` (`io.S3CompatMode`) | Enable for non-AWS S3 endpoints. Defaults to off. |
| `s3.signer.uri` (`io.S3SignerURI`) | Reserved for remote-signing endpoint (not yet implemented). |

### Google Cloud Storage

Authentication resolution (`io/gocloud/gcs/gcs.go`):

1. Explicit JSON key bytes via `gcs.jsonkey` or path via `gcs.keypath`.
2. Optional `gcs.credtype` selecting one of `service_account`, `authorized_user`, `impersonated_service_account`, `external_account`.
3. The GCP default credentials chain (`gcp.DefaultCredentials`) - falls back to anonymous if no creds are found.

Tuning properties:

| Key (constant) | Purpose |
|---|---|
| `gcs.endpoint` (`io.GCSEndpoint`) | Custom GCS endpoint URL. |
| `gcs.keypath` (`io.GCSKeyPath`) | Path to a JSON service-account key file. |
| `gcs.jsonkey` (`io.GCSJSONKey`) | JSON key as a string. |
| `gcs.credtype` (`io.GCSCredType`) | Credential type override. |
| `gcs.usejsonapi` (`io.GCSUseJSONAPI`) | Set to `"true"` to enable the GCS JSON API for reads. |

### Azure Data Lake Storage / Blob

Authentication is selected based on the property keys present (`io/gocloud/azure/azure.go`):

1. Shared key: both `adls.auth.shared-key.account.name` and `adls.auth.shared-key.account.key` set.
2. Per-host SAS token: `adls.sas-token.<hostname>` (prefix-matched against the storage account host).
3. Per-host connection string: `adls.connection-string.<hostname>`.
4. Managed identity: `adls.auth.managed-identity.enabled` set to a truthy value.

Tuning properties:

| Key (constant) | Purpose |
|---|---|
| `adls.auth.shared-key.account.name` (`io.ADLSSharedKeyAccountName`) | Account name. |
| `adls.auth.shared-key.account.key` (`io.ADLSSharedKeyAccountKey`) | Account key. |
| `adls.sas-token.<host>` (prefix `io.ADLSSasTokenPrefix`) | Per-host SAS token. |
| `adls.connection-string.<host>` (prefix `io.ADLSConnectionStringPrefix`) | Per-host connection string. |
| `adls.client-id` (`io.ADLSClientID`) | Client/application ID for AAD auth. |
| `adls.endpoint` (`io.ADLSEndpoint`) | Storage domain (e.g. `blob.core.windows.net`). |
| `adls.protocol` (`io.ADLSProtocol`) | `http` or `https`. |
| `adls.auth.managed-identity.enabled` (`io.ADLSManagedIdentityEnabled`) | Enable Azure Managed Identity auth. |

## Environment variables

iceberg-go reads only a small set of environment variables directly. AWS / GCP / Azure credentials flow through the respective SDKs, not through iceberg-go-defined env vars.

| Variable | Purpose | Read at |
|---|---|---|
| `GOICEBERG_HOME` | Directory containing `.iceberg-go.yaml`. Defaults to the user's home directory. | `config/config.go:87` |
| `ICEBERG_SQL_DEBUG` | SQL catalog query logging - `1` (failed queries), `2` (all queries). | `catalog/sql/sql.go:206` |
| `AWS_S3_ENDPOINT` | Fallback S3 endpoint when `s3.endpoint` is unset. | `io/gocloud/s3/s3.go` |

There is no `PYICEBERG_*`-style env var convention. Use the YAML config file or pass `iceberg.Properties` to overrides programmatically.

## Concurrency

| Setting | Source | Effect |
|---|---|---|
| `max-workers` in `~/.iceberg-go.yaml` (`config.EnvConfig.MaxWorkers`) | YAML config | Worker pool size used by parallel column writes, snapshot producers, scan plan, equality-delete writers. Default `5`. |
| `WithMaxConcurrency(n int) ScanOption` | Code (`table.WithMaxConcurrency`) | Per-scan override. `WitMaxConcurrency` (missing `h`) remains as a deprecated alias. |
| `WithMaxWriteWorkers(n int)` | Code (per-write API on `WriteRecords`) | Per-write override of the worker count. |
| `WithClusteredWrite()` | Code (per-write API on `WriteRecords`) | Forces single-threaded writes. Mutually exclusive with `WithMaxWriteWorkers`. |

## Pluggability

Two registries are user-extensible. The third (LocationProvider) is currently informational.

### IO scheme registry

Register a custom URL scheme with [`io.Register`](https://github.com/apache/iceberg-go/blob/main/io/registry.go):

```go
import (
    "context"
    "net/url"

    "github.com/apache/iceberg-go/io"
)

func init() {
    io.Register("myfs", func(ctx context.Context, parsed *url.URL, props map[string]string) (io.IO, error) {
        return newMyFS(parsed, props)
    })
}
```

`io.Register` panics on `nil` factory or duplicate scheme. Built-in schemes: `file`, `""` (the empty scheme).
Cloud schemes (`s3`, `gs`, `abfs`, etc.) are registered only when the matching backend package under `io/gocloud` is blank-imported.

`io.GetRegisteredSchemes()` returns the current scheme list; `io.Unregister(scheme)` removes one.

### Catalog type registry

Register a custom catalog type with [`catalog.Register`](https://github.com/apache/iceberg-go/blob/main/catalog/registry.go):

```go
import (
    "context"

    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog"
)

func init() {
    catalog.Register("mycatalog", catalog.RegistrarFunc(
        func(ctx context.Context, name string, props iceberg.Properties) (catalog.Catalog, error) {
            return newMyCatalog(name, props)
        },
    ))
}
```

After registration, `catalog.Load(ctx, "default", iceberg.Properties{"type": "mycatalog", ...})` will route to the factory. Built-in types: `rest`, `hive`, `glue`, `sql`, `hadoop`.

`catalog.GetRegisteredCatalogs()` returns the current list; `catalog.Unregister(catalogType)` removes one.

### LocationProvider

[`table/locations.go`](https://github.com/apache/iceberg-go/blob/main/table/locations.go) defines the `LocationProvider` interface and ships two implementations: `simpleLocationProvider` (default) and `objectStoreLocationProvider` (selected by `write.object-storage.enabled = true`). The provider is chosen by table properties and is not user-pluggable today.

## Table write properties

Property key constants are in [`table/properties.go`](https://github.com/apache/iceberg-go/blob/main/table/properties.go) and Parquet keys in [`table/internal/parquet_files.go`](https://github.com/apache/iceberg-go/blob/main/table/internal/parquet_files.go). The keys below have verified read sites in non-test code.

### Format and file sizing

| Key | Default | Description |
|---|---|---|
| `write.format.default` | `parquet` | File format used when writing data files. Read in `table/writer.go` and `table/rolling_data_writer.go`. |
| `write.target-file-size-bytes` | (set by writer) | Target size for newly written data files. Read in `table/arrow_utils.go` and `table/equality_delete_writer.go`. |

### Scan reads

| Key | Default | Description |
|---|---|---|
| `read.split.target-size` | `134217728` | Target size for splitting large local Parquet files at their recorded row-group offsets. |

### Metrics and metadata lifecycle

| Key | Description |
|---|---|
| `write.metadata.metrics.default` | Default per-column metrics mode. |
| `write.metadata.metrics.column.<name>` | Per-column override prefix. |
| `write.metadata.delete-after-commit.enabled` | When true, expire old metadata files after a successful commit. |
| `write.metadata.previous-versions-max` | Cap on retained metadata files. Default `100`. |
| `write.metadata.compression-codec` | Compression for metadata JSON. |

### Manifest and commit

| Key | Description |
|---|---|
| `commit.manifest-merge.enabled` | Merge small manifests during commit. |
| `commit.manifest.target-size-bytes` | Target size for merged manifests. |
| `commit.manifest.min-count-to-merge` | Minimum manifest count that triggers a merge. |
| `commit.retry.num-retries` | Retries for `ErrCommitFailed`. |
| `commit.retry.min-wait-ms` / `commit.retry.max-wait-ms` / `commit.retry.total-timeout-ms` | Backoff bounds. |

### Snapshot retention

| Key | Description |
|---|---|
| `history.expire.min-snapshots-to-keep` | Minimum snapshots to retain when expiring. Default `1`. |
| `history.expire.max-snapshot-age-ms` | Maximum age of retained snapshots. Default `432000000` (5 days). |
| `history.expire.max-ref-age-ms` | Maximum age of branch/tag refs that are not the main branch. Default `Long.MAX_VALUE` (forever). |
| `gc.enabled` | Controls physical garbage collection. Snapshot expiration is rejected when `false`, and data-file cleanup skips deletion. |

The unprefixed retention property names are accepted as compatibility fallbacks. The `history.expire.*` properties take precedence when both forms are set.

### Delete mode

| Key | Description |
|---|---|
| `write.delete.mode` | Delete strategy used by row-level delete writers. |

### Object-store data layout

| Key | Description |
|---|---|
| `write.data.path` | Override data file directory. |
| `write.metadata.path` | Override metadata file directory. |
| `write.object-storage.enabled` | Switch the location provider to the hashed object-storage layout. |
| `write.object-storage.partitioned-paths` | Whether partition values are included in object-storage paths. |

### Parquet writer

All defined in `table/internal/parquet_files.go` and read by the Parquet writer:

| Key | Default |
|---|---|
| `write.parquet.row-group-size-bytes` | 128 MB |
| `write.parquet.row-group-limit` | 1,048,576 rows |
| `write.parquet.page-size-bytes` | 1 MB |
| `write.parquet.page-row-limit` | 20,000 rows |
| `write.parquet.dict-size-bytes` | 2 MB |
| `write.parquet.page-version` | `2` |
| `write.parquet.compression-codec` | `zstd` |
| `write.parquet.compression-level` | `-1` (codec default) |
| `write.parquet.bloom-filter-max-bytes` | 1 MB |
| `write.parquet.bloom-filter-enabled.column.<name>` | (per-column toggle, prefix-matched) |
| `parquet.enable.dictionary` | `true` (global dictionary encoding switch; same key as Iceberg Java) |
| `write.parquet.dict-encoding-enabled.column.<name>` | (per-column toggle, prefix-matched; overrides the global switch) |
| `write.parquet.shred-variants` | `false` (opt-in shredding of top-level variant columns) |
| `write.parquet.variant-inference-buffer-size` | 100 rows (buffered per file to infer the shredding schema) |

The bloom filter and dictionary toggles are parsed the way Iceberg Java parses them: only `true` (case-insensitive) enables, so any other value — including `1` — reads as `false`.

### Parquet reader

| Key | Description |
|---|---|
| `read.parquet.batch-size` | Arrow record-batch size used by the Parquet reader. |
