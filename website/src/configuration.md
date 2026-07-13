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
| `catalog.<name>.type` | One of `rest`, `hive`, `glue`, `sql`, `hadoop`. |
| `catalog.<name>.uri` | Catalog endpoint or DSN. |
| `catalog.<name>.warehouse` | Warehouse identifier (REST/Glue) or location (Hive/SQL). |
| `catalog.<name>.credential` | Credential string passed through to the catalog's auth handler. |
| `catalog.<name>.output` | CLI output format (e.g. `text`, `json`). |
| `catalog.<name>.aws-profile` | AWS named profile for the Glue catalog. When unset, the AWS SDK default credential chain is used. |
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

### Hive (`catalog/hive`)

Source: [`catalog/hive/options.go`](https://github.com/apache/iceberg-go/blob/main/catalog/hive/options.go).

- `WithURI(uri string)` - Thrift URI for the Hive Metastore (e.g. `thrift://127.0.0.1:9083`).
- `WithWarehouse(warehouse string)`
- `WithProperties(props iceberg.Properties)`

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
import _ "github.com/apache/iceberg-go/io/gocloud"
```

The `init()` function in [`io/gocloud/register.go`](https://github.com/apache/iceberg-go/blob/main/io/gocloud/register.go) registers `s3`, `s3a`, `s3n`, `oss`, `gs`, `abfs`, `abfss`, `wasb`, and `wasbs`. Without the blank import, these schemes return `ErrIOSchemeNotFound` with a hint to add the import.

All credential and tuning property keys are constants in [`io/config.go`](https://github.com/apache/iceberg-go/blob/main/io/config.go). They can be supplied through table properties, catalog properties, or per-call `iceberg.Properties` arguments depending on context.

### S3

Authentication is resolved in this order (`io/gocloud/s3.go`):

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
| `s3.signer.uri` (`io.S3SignerURI`) | Reserved for remote-signing endpoint (not yet implemented). |

### Google Cloud Storage

Authentication resolution (`io/gocloud/gcs.go`):

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

Authentication is selected based on the property keys present (`io/gocloud/azure.go`):

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
| `AWS_S3_ENDPOINT` | Fallback S3 endpoint when `s3.endpoint` is unset. | `io/gocloud/s3.go:193` |

There is no `PYICEBERG_*`-style env var convention. Use the YAML config file or pass `iceberg.Properties` to overrides programmatically.

## Concurrency

| Setting | Source | Effect |
|---|---|---|
| `max-workers` in `~/.iceberg-go.yaml` (`config.EnvConfig.MaxWorkers`) | YAML config | Worker pool size used by parallel column writes, snapshot producers, scan plan, equality-delete writers. Default `5`. |
| `WithScanMaxConcurrency(n int) ScanOption` | Code (`table.WithScanMaxConcurrency`) | Per-scan override. `WitMaxConcurrency` (missing `h`) remains as a deprecated alias. |
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

`io.Register` panics on `nil` factory or duplicate scheme. Built-in schemes: `file`, `""` (the empty scheme). Cloud schemes (`s3`, `gs`, `abfs`, etc.) are registered by `io/gocloud` only when its package is blank-imported.

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
| `min-snapshots-to-keep` | Minimum snapshots to retain when expiring. |
| `max-snapshot-age-ms` | Maximum age of retained snapshots. |
| `max-ref-age-ms` | Maximum age of branch/tag refs that are not the main branch. |
| `gc.enabled` | Gate for orphan-file cleanup. |

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
| `write.parquet.shred-variants` | `false` (opt-in shredding of top-level variant columns) |
| `write.parquet.variant-inference-buffer-size` | 100 rows (buffered per file to infer the shredding schema) |

### Parquet reader

| Key | Description |
|---|---|
| `read.parquet.batch-size` | Arrow record-batch size used by the Parquet reader. |
