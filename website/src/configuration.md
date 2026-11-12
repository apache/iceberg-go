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
| `catalog.<name>.type` | One of `rest`, `hive`, `glue`, `sql`, `hadoop`, `dynamodb`. |
| `catalog.<name>.uri` | Catalog endpoint or DSN. |
| `catalog.<name>.warehouse` | Warehouse identifier (REST/Glue) or location (Hive/SQL). |
| `catalog.<name>.credential` | Credential string passed through to the catalog's auth handler. |
| `catalog.<name>.output` | CLI output format (e.g. `text`, `json`). |
| `catalog.<name>.aws-profile` | AWS named profile for Glue and DynamoDB catalogs. When unset, the AWS SDK default credential chain is used. |
| `catalog.<name>.sql-driver` | `database/sql` driver name for the SQL catalog. Maps to the `sql.driver` property. The default CLI binary only compiles in `sqliteshim`; other drivers require a custom build. |
| `catalog.<name>.sql-dialect` | SQL dialect for the SQL catalog (`postgres`, `mysql`, `sqlite`, `mssql`, `oracle`). Maps to the `sql.dialect` property. The default CLI binary only ships `sqlite` via `sqliteshim`; other dialects need a custom build with their drivers. |
| `catalog.<name>.rest.sigv4-enabled` | Enable AWS SigV4 signing for REST. When enabled, requests are signed with the `s3.*` credential properties if set (`s3.access-key-id` / `s3.secret-access-key` / `s3.session-token`), otherwise with the AWS default credential chain. The Java-client names (`rest.access-key-id` / `rest.secret-access-key` / `rest.session-token`) are accepted as aliases, resolved per field with the `s3.*` keys taking precedence. |
| `catalog.<name>.rest.signing-name` | SigV4 service name. |
| `catalog.<name>.rest.signing-region` | SigV4 region. |

## Catalog options

Each catalog package exposes its own functional `Option` set. The lists below reflect the public option surface; `pkg.go.dev` is authoritative for the current set.

### REST (`catalog/rest`)

The most option-rich surface. Source: [`catalog/rest/options.go`](https://github.com/apache/iceberg-go/blob/main/catalog/rest/options.go).

When `GET /v1/config` returns a non-null `idempotency-key-lifetime`, the client
sends fresh UUIDv7 `Idempotency-Key` headers on namespace, table, view and
transaction mutations, including create view, and on scan cancellation. After
negotiation, a configured default `Idempotency-Key` is dropped from all requests
when the server advertises a lifetime; the initial `GET /v1/config` may still
carry it because advertisement is not yet known. Scan plan and task requests
send keys independently of this advertisement and have a separate retry policy.
Go's standard HTTP transport may replay certain failed requests on reused
connections with the same key. The advertised lifetime is not currently enforced,
including zero or malformed values.

| Group | Options |
|---|---|
| Authentication | `WithCredential`, `WithOAuthToken`, `WithAuthManager`, `NewOauth2AuthManager`, `WithAuthURI`, `WithScope`, `WithAudience`, `WithResource` |
| AWS SigV4 | `WithSigV4`, `WithSigV4RegionSvc`, `WithSigner` |
| HTTP | `WithHeaders`, `WithTLSConfig`, `WithOAuthTLSConfig`, `WithCustomTransport` |
| Catalog routing | `WithPrefix`, `WithWarehouseLocation`, `WithMetadataLocation` |
| Pass-through | `WithAdditionalProps` |

> **AWS SigV4 is an optional backend.** `catalog/rest` no longer links the AWS
> SDK. To sign REST requests with SigV4, pull in the
> [`catalog/rest/sigv4`](https://github.com/apache/iceberg-go/blob/main/catalog/rest/sigv4/sigv4.go)
> sub-package one of two ways:
>
> - Property or ambient credentials: add a blank import
>   `_ "github.com/apache/iceberg-go/catalog/rest/sigv4"` and enable signing with
>   `WithSigV4` / `WithSigV4RegionSvc` or the `rest.sigv4-enabled` property. The
>   backend signs with the `s3.*` credential properties when set (falling back to
>   the `rest.*` aliases), otherwise with the AWS default credential chain.
> - Explicit config: pass `sigv4.WithAWSConfig(cfg)` to `NewCatalog` alongside
>   `WithSigV4` / `WithSigV4RegionSvc` (no blank import needed). It supplies the
>   `aws.Config`; the signing service comes from those options (or a server
>   `/v1/config` override), and so does the region when one is set. Otherwise
>   the region falls back to `cfg.Region`.
>
> The former `rest.WithAwsConfig(aws.Config)` has been removed in favor of these
> paths. Because signing is enabled by `WithSigV4` / `WithSigV4RegionSvc` (or the
> `rest.sigv4-enabled` property) rather than by supplying a config, a program
> that loads a REST catalog with `rest.sigv4-enabled=true` now fails at startup
> until the `catalog/rest/sigv4` backend is blank-imported; the error names the
> import to add.

#### Authentication type

The `rest.auth.type` property selects how the client authenticates, as in Iceberg
Java. It is read only from the properties the client supplies; a server's
`/v1/config` defaults and overrides can't change it. An `AuthManager` passed with
`WithAuthManager` takes precedence over it.

| Value | Behavior |
|---|---|
| unset or `oauth2` | Use `token`, or the client-credentials flow with `credential`. With neither, send no authorization header. |
| `none` | Send no authorization header, even if `token` or `credential` is set. |
| `basic` | HTTP Basic authentication with `rest.auth.basic.username` and `rest.auth.basic.password`, both required. Like `rest.auth.type`, they are read only from the client's properties. |
| `sigv4` | Sign every request with AWS SigV4 and authenticate with the type in `rest.auth.sigv4.delegate-auth-type` (default `oauth2`; `sigv4` is rejected). Requires a blank import of `catalog/rest/sigv4`. Unlike `rest.sigv4-enabled`, a server override can't turn this signing off. |
| `google` | Google credentials, for catalogs such as BigLake. Requires a blank import of `catalog/rest/googleauth`. |

Other values select an auth manager registered with `rest.RegisterAuthManager`.
An unregistered value fails `NewCatalog` / `catalog.Load`.

When SigV4 signs a request that already carries an `Authorization` header, such
as the `oauth2` or `basic` header of the delegate auth type, the header moves to
`Original-Authorization` and is signed with the request, as in Java. This also
applies to `rest.sigv4-enabled`.

#### Google BigLake

With `rest.auth.type=google`, the
[`catalog/rest/googleauth`](https://github.com/apache/iceberg-go/blob/main/catalog/rest/googleauth/googleauth.go)
backend authenticates with Google credentials, using the same properties as
Java's `GoogleAuthManager`:

| Property | Description |
|---|---|
| `gcp.auth.credentials-path` | Path to a credentials JSON file, such as a service account key. |
| `gcp.auth.credentials-json` | The same JSON, inline. Setting both is an error. |
| `gcp.auth.scopes` | Comma-separated OAuth scopes. Default `https://www.googleapis.com/auth/cloud-platform`. |

Without either credentials property, Application Default Credentials are used.
Like `rest.auth.type`, these are read only from the client's properties.

Token requests don't go through the catalog's HTTP client, so a transport, TLS
config or proxy set with `WithCustomTransport`, `WithTLSConfig` or
`WithTransportFactory` doesn't apply to them, as in Java. They use a client with
the default transport and a 60-second timeout. To use a specific client, put it
in the context's `oauth2.HTTPClient` value when loading the catalog, or build
the token source yourself (for example with `googleauth.TokenSource`) and pass
`NewOauth2AuthManager(ts)` to `WithAuthManager`. One exception: on Google Cloud
compute, Application Default Credentials come from the metadata server, which
the Google library always reaches through its own client.

```go
import (
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/rest/googleauth"
)

cat, err := catalog.Load(ctx, "biglake", iceberg.Properties{
	"type":                       "rest",
	"uri":                        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
	"warehouse":                  "gs://lakehouse-public-data",
	"rest.auth.type":             "google",
	"header.x-goog-user-project": "my-project",
})
```

#### Custom token sources

`WithCredential` uses the OAuth2 client-credentials flow against the catalog's
token endpoint. For other identity providers, wrap any `oauth2.TokenSource` with
`NewOauth2AuthManager` and pass it to `WithAuthManager`. The Google setup above
can also be written this way:

```go
ts, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
if err != nil {
	return err
}
cat, err := rest.NewCatalog(ctx, "biglake", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
	rest.WithAuthManager(rest.NewOauth2AuthManager(ts)),
	rest.WithWarehouseLocation("gs://lakehouse-public-data"),
	rest.WithHeaders(map[string]string{"x-goog-user-project": "my-project"}))
```

The manager reuses a token until it expires, then asks the source for a new one.
That refresh runs on the HTTP client the source was built with. For
`google.DefaultTokenSource` that is the client in the context's
`oauth2.HTTPClient` value, or `http.DefaultClient`, which has no timeout. With
`rest.auth.type=google`, the backend sets a client with a 60-second timeout
unless the context already carries one.

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

- `WithAWSConfig(cfg aws.Config)` - AWS SDK v2 config; respects the AWS default credential chain.
- `WithAWSProperties(props AWSProperties)` - catalog properties, such as `glue.id` and the `s3.*` FileIO keys. `NewCatalog` ignores the `glue.region`, endpoint, credential, and retry keys; only `catalog.Load` reads them.

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

When remote signing is disabled, authentication is resolved in this order (`io/gocloud/s3/s3.go`):

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
| `s3.remote-signing-enabled` (`io.S3RemoteSigningEnabled`) | Set to `true` to have a REST service sign S3 requests. Defaults to `false`. |
| `s3.signer.uri` (`io.S3SignerURI`) | Legacy signing service base URI. Requests use `<base>/v1/aws/s3/sign`. |

With remote signing enabled, S3 uses anonymous SDK credentials and obtains each
request's signature from the signing service. No local AWS credentials are needed.
REST catalogs use the table's `/sign` endpoint by default and pass through the
response's `remote-signing-config` headers and properties. When that config is
present, legacy `signer.endpoint`, `signer.uri`, and `s3.signer.uri` settings
are ignored. `rest.remote-signing.endpoint` can explicitly override the default.
It may be an absolute URL or a path relative to the catalog URI.
Legacy settings are supported when the response has no remote signing config.

A table-specific token takes precedence over the catalog token. The catalog's
refreshing authentication is reused only when the signer shares the catalog's
origin. For a separate signing service, use the headers supplied in
`remote-signing-config` or a table-specific token. For a standalone S3 FileIO,
set `s3.signer.uri`, `s3.region`, and an optional `token` for the signing service.
When also supplying `rest.remote-signing.config`, set
`rest.remote-signing.endpoint` to the complete signing endpoint instead.

Object uploads use unsigned payloads. Only bulk-delete XML bodies are sent to the
signer. Streaming signatures (`aws-chunked`) are unsupported. Caller-supplied
checksum values are preserved; an upload that sets `ChecksumAlgorithm` must also
supply its checksum value. Signing requests use the caller's context and a
30-second timeout unless the HTTP client has a configured timeout.
Remote signing accepts a standard `*http.Client` or the AWS SDK's buildable client
for S3 requests so redirects can be disabled. Other custom HTTP clients are rejected.
S3 requests must resolve to SigV4; SigV4a and S3 Express signing are not supported.

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

Authentication is selected in the following order (`io/gocloud/azure/azure.go`); the first applicable option wins, and if none of 1-5 apply the default credential chain (6) is used:

1. Shared key: a nonempty `adls.auth.shared-key.account.name` selects this path and requires a nonempty `adls.auth.shared-key.account.key`.
2. Per-host SAS token: `adls.sas-token.<hostname>`, where `<hostname>` exactly matches the storage account hostname (for example, `myaccount.dfs.core.windows.net`).
3. Per-account connection string: `adls.connection-string.<account-name>` (for example, `adls.connection-string.myaccount`).
4. Static access token: a nonempty `adls.token`, supplied without the `Bearer ` prefix.
5. Managed identity: `adls.auth.managed-identity.enabled` set to exactly `"true"` selects this path and uses `ManagedIdentityCredential` directly. Set `adls.client-id` to select a user-assigned managed identity; otherwise, the system-assigned managed identity is used.
6. Default credential chain: `DefaultAzureCredential`, which includes managed identity among its credential sources.

`adls.token` accepts an OAuth2 access token for Azure Storage, including a token supplied by a catalog. An empty value is ignored. A rejected or expired token does not fall back to managed identity or the default credential chain.

The token is static and is not refreshed. As in [Java](https://github.com/apache/iceberg/blob/main/azure/src/main/java/org/apache/iceberg/azure/AzureProperties.java) and [PyIceberg](https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/fsspec.py), the credential reports a one-hour cache window to the Azure SDK each time the SDK requests the token. This does not extend the token's actual validity: Azure enforces its expiration. Callers must obtain a new token and recreate the FileIO when it expires; changing the original properties map does not update an existing FileIO.

A REST catalog can also vend `adls.token` as a storage credential. The token carries no expiry of its own, so a vended token's expiry comes from `expiration-time` in the same credential when the catalog supplies one. Without it, a table FileIO that can fetch new credentials assumes a 60-minute lifetime and renews them shortly before it ends, and one that cannot, such as a FileIO for a scan plan's credentials, treats them as never expiring. A token whose actual lifetime is shorter than that fails with authorization errors until it is renewed, so catalogs that vend `adls.token` should send `expiration-time` with it. For the storage account it is vended for, a vended token replaces the SAS token and connection string configured for that account and any shared key, which would otherwise take precedence over it; a shared key is not scoped to an account, so one configured for another account would be used for this account too. The SAS tokens and connection strings of other accounts are kept. Expiries configured for other credentials, such as a SAS expiry or an `expiration-time` in the catalog or table configuration, do not affect when the token is renewed.

> **Note on cross-client parity.** The two properties in step 5 diverge from the
> other clients. `adls.auth.managed-identity.enabled` is Go-specific: neither
> Iceberg Java nor PyIceberg defines a property that forces
> `ManagedIdentityCredential`, so catalog properties that set it do not carry
> over. `adls.client-id` collides in name only — PyIceberg reads it as a service
> principal client ID used together with `adls.client-secret` and
> `adls.tenant-id`, neither of which iceberg-go defines (see the
> [PyIceberg ADLS configuration](https://py.iceberg.apache.org/configuration/#azure-data-lake)),
> while Java's `AzureProperties` defines no `adls.client-id` at all. Here it is
> only ever read as a user-assigned managed identity ID, and only when step 5 is
> selected.

Tuning properties:

| Key (constant) | Purpose |
|---|---|
| `adls.auth.shared-key.account.name` (`io.ADLSSharedKeyAccountName`) | Account name. |
| `adls.auth.shared-key.account.key` (`io.ADLSSharedKeyAccountKey`) | Account key. |
| `adls.sas-token.<host>` (prefix `io.ADLSSasTokenPrefix`) | Per-host SAS token. |
| `adls.connection-string.<account-name>` (prefix `io.ADLSConnectionStringPrefix`) | Per-account connection string. |
| `adls.token` (`io.ADLSToken`) | Static Azure Storage access token, without the `Bearer ` prefix. Subject to the precedence above; not refreshed. |
| `adls.client-id` (`io.ADLSClientID`) | Client ID of a user-assigned managed identity. Used only when the explicit managed-identity authentication path is selected. |
| `adls.endpoint` (`io.ADLSEndpoint`) | Storage domain (e.g. `blob.core.windows.net`). |
| `adls.protocol` (`io.ADLSProtocol`) | `http` or `https`. |
| `adls.auth.managed-identity.enabled` (`io.ADLSManagedIdentityEnabled`) | Set to exactly `"true"` to use `ManagedIdentityCredential` directly, subject to the precedence above. |

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
| `WithMaxConcurrency(n int) ScanOption` | Code (`table.WithMaxConcurrency`) | Per-scan override. |
| `WithMaxWriteWorkers(n int)` | Code (per-write API on `WriteRecords`) | Per-write override of the worker count. |
| `WithClusteredWrite()` | Code (per-write API on `WriteRecords`) | Forces single-threaded writes. Mutually exclusive with `WithMaxWriteWorkers`. |

## Pluggability

Two registries are user-extensible. The third (LocationProvider) is currently informational.

### IO scheme registry

Register a custom URL scheme from an `init` function with [`io.MustRegister`](https://github.com/apache/iceberg-go/blob/main/io/registry.go):

```go
import (
    "context"
    "net/url"

    "github.com/apache/iceberg-go/io"
)

func init() {
    io.MustRegister("myfs", func(ctx context.Context, parsed *url.URL, props map[string]string) (io.IO, error) {
        return newMyFS(parsed, props)
    })
}
```

`io.Register` and `io.MustRegister` both panic for invalid or duplicate global schemes, preserving the existing registration contract. `registry.Register` on an independent registry returns `ErrInvalidSchemeFactory` or `ErrSchemeAlreadyRegistered` instead. Built-in schemes: `file`, `""` (the empty scheme).
Cloud schemes (`s3`, `gs`, `abfs`, etc.) are registered only when the matching backend package under `io/gocloud` is blank-imported.

`io.GetRegisteredSchemes()` returns the current scheme list; `io.Unregister(scheme)` removes one.

To register schemes at runtime without changing other users of the same process, create an independent registry (catalogs and `view.CreateView` still resolve through the default registry):

```go
registry := io.NewRegistry()
if err := registry.Register("myfs", myFactory); err != nil {
    return err
}
fs, err := registry.LoadFS(ctx, props, "myfs://bucket/key")
```

`io.NewRegistry()` copies schemes already registered globally (including blank-imported cloud backends) but keeps later registrations independent. `registry.Clone()` makes an isolated copy of another registry. `registry.Names()` lists the schemes registered in that instance. `registry.LoadFSFunc()` is the lazy factory equivalent. Global `io.LoadFS()` and `io.LoadFSFunc()` continue using the default registry.

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

After registration, `catalog.Load(ctx, "default", iceberg.Properties{"type": "mycatalog", ...})` will route to the factory. Custom catalogs must implement `Close() error` to satisfy `catalog.Catalog`, and callers must close them when finished. Built-in types: `rest`, `hive`, `glue`, `sql`, `hadoop`.

`catalog.GetRegisteredCatalogs()` returns the current list; `catalog.Unregister(catalogType)` removes one.

### LocationProvider

[`table/locations.go`](https://github.com/apache/iceberg-go/blob/main/table/locations.go) defines the `LocationProvider` interface and ships two implementations: `simpleLocationProvider` (default) and `objectStoreLocationProvider` (selected by `write.object-storage.enabled = true`). The provider is chosen by table properties and is not user-pluggable today.

## Table write properties

Property key constants are in [`table/properties.go`](https://github.com/apache/iceberg-go/blob/main/table/properties.go) and Parquet keys in [`table/internal/parquet_files.go`](https://github.com/apache/iceberg-go/blob/main/table/internal/parquet_files.go). The keys below have verified read sites in non-test code.

### Format and file sizing

| Key | Default | Description |
|---|---|---|
| `write.format.default` | `parquet` | File format used when writing data files. Read in `table/writer.go` and `table/rolling_data_writer.go`. |
| `write.target-file-size-bytes` | `536870912` (512 MB) | Target size for newly written data files. Read in `table/arrow_utils.go`. |
| `write.delete.target-file-size-bytes` | `67108864` (64 MB) | Target size for equality-delete and v2 position-delete files where file rolling applies. Read in `table/equality_delete_writer.go` and `table/arrow_utils.go`. |

### Scan reads

| Key | Default | Description |
|---|---|---|
| `read.split.target-size` | `134217728` | Target size for coalescing safe row-group ranges when splitting large local Parquet files. A range may exceed the target when no supplied offset can divide it safely. |
| `read.manifest-list-cache.enabled` | `true` | Reuse decoded snapshot manifest lists across scans of a table handle. Set to `false` to disable retention and shared in-flight reads. Applied when loading or refreshing a table, and when creating a transaction scan. |
| `io.manifest.cache-enabled` | `false` | Reuse raw manifest file content across local scans. Read from catalog FileIO properties in REST, Glue, SQL, Hive and Hadoop; REST table metadata and load-response config may override the catalog value. |
| `io.manifest.cache.expiration-interval-ms` | `60000` | Idle expiration in milliseconds. Zero disables time-based expiration; negative or unparsable values warn and disable the cache. Expired entries are reclaimed on cache access/insertion, without a background cleanup task. |
| `io.manifest.cache.max-total-bytes` | `104857600` | Retained raw bytes per independently loaded table. Handles returned by commit share their source table's cache and budget. Concurrent population buffers and bytes held by active readers are not counted. Non-positive or unparsable values warn and disable caching. |
| `io.manifest.cache.max-content-length` | `8388608` | Maximum size of one manifest eligible for content caching. Non-positive or unparsable values warn and disable caching. |

The catalog's FileIO properties configure manifest caching for all built-in
catalogs. With direct `table.New` calls, use `WithScanPlanningIOProperties`
or `WithSavedConfig`; saved config wins when the same key is present in both.
Matching expiration interval, total byte limit and per-manifest byte limit
preserve warm cache entries across refresh and commit. Changing a limit replaces
the cache.

Unlike Java, which rejects invalid manifest-cache limits, Go warns and disables
the content cache for negative or unparsable expiration or
non-positive/unparsable size limits.

The manifest-list cache retains at most 64 snapshots and 32,768 manifest
descriptors in total per table handle. These are count limits, not byte limits:
memory use also depends on manifest paths, partition summaries, and key metadata.
Refreshing the table replaces its manifest-list cache.

The optional manifest-content cache is separate. It stores immutable raw manifest
bytes and still decodes, projects, and filters entries independently for each
scan. Cache misses for the same manifest share one in-flight read. If population
fails, its producer and each existing waiter independently fall back to one
ordinary FileIO open; if the producer was canceled, live waiters can start
another shared load. Failed content is not
retained, so a later scan can retry. Manifests larger than the configured
per-file limit bypass the cache. The current integration covers local scan
planning; manifest lists continue to use the decoded manifest-list cache.

The Java-compatible `read.split.planning-lookback` and `read.split.open-file-cost`
names and defaults are also exported by the Go API for shared configuration,
but are reserved for future task-group planning and are not currently read.

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
| `commit.retry.num-retries` | Retries for `ErrCommitFailed`. Default `4`; `0` fails on the first conflict. |
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
| `write.parquet.compression-codec.column.<name>` | Global codec |
| `write.parquet.compression-level.column.<name>` | Global level, or the column codec's default if the codecs differ |
| `write.parquet.bloom-filter-max-bytes` | 1 MB |
| `write.parquet.bloom-filter-enabled.column.<name>` | (per-column toggle, prefix-matched) |
| `write.parquet.bloom-filter-fpp.column.<name>` | 0.01 (per-column false-positive probability, strictly between 0 and 1) |
| `write.parquet.bloom-filter-ndv.column.<name>` | unset (expected distinct values; positive integer up to 4,294,967,295) |
| `write.parquet.bloom-filter-adaptive-enabled` | `false` (size enabled Bloom filters to observed NDV unless a per-column NDV is specified) |
| `write.parquet.stats-enabled.column.<name>` | `true` (per-column Parquet statistics toggle) |
| `parquet.enable.dictionary` | `true` (global dictionary encoding switch; same key as Iceberg Java) |
| `write.parquet.dict-encoding-enabled.column.<name>` | (per-column toggle, prefix-matched; overrides the global switch) |
| `write.parquet.shred-variants` | `false` (opt-in shredding of top-level variant columns) |
| `write.parquet.variant-inference-buffer-size` | 100 rows (buffered per file to infer the shredding schema) |

The bloom filter, statistics, and dictionary toggles follow Iceberg Java parsing: only `true`
(case-insensitive) enables a toggle; other values, including `1`, are false. Bloom filters remain
disabled for a column unless its enabled toggle is set. When adaptive sizing is enabled, an explicit
column NDV takes precedence. Adaptive sizing rejects FPP values extremely close to 1 that could make
the writer hang.

Statistics settings apply to data files and use Iceberg field names, including nested paths such as
`items.element`. Disabling statistics can reduce pruning and omit some Iceberg file metrics; other
metadata may still be recorded.

Column compression keys use case-sensitive Iceberg leaf paths, such as
`items.element` or `attributes.value.name`. They apply to data files and use
the global settings for columns without overrides. An explicit column level
must be an integer valid for its effective codec. Missing fields are ignored.
Parquet variant leaves do not have Iceberg field IDs and cannot be selected by
these keys. Arrow does not register legacy `lz4` or `lzo` codecs by default;
`lz4_raw` is supported.
Bloom column settings use Iceberg names, including `items.element` and
`lookup.value`. Writers resolve these to the physical Parquet paths. Existing
physical-path settings remain supported; logical names take precedence when
both are present. Logical settings follow column renames and are removed when
the column is deleted. Data-column Bloom settings do not apply to delete files.

Equality-delete and v2 position-delete Parquet writers also support these
overrides, defined in `table/properties.go`. Unset overrides inherit the
corresponding `write.parquet.*` setting and its default. If the delete codec
differs from the data codec and no delete level is set, the delete writer uses
the codec's default level instead of inheriting the data codec's level.

| Key | Overrides |
|---|---|
| `write.delete.parquet.row-group-size-bytes` | Row-group size target |
| `write.delete.parquet.page-size-bytes` | Data page size |
| `write.delete.parquet.page-row-limit` | Page write batch size |
| `write.delete.parquet.dict-size-bytes` | Dictionary page size |
| `write.delete.parquet.page-version` | Data page version: `v1` or `v2` (`1` and `2` also accepted) |
| `write.delete.parquet.compression-codec` | Compression codec |
| `write.delete.parquet.compression-level` | Compression level |

As in Java, data-column Bloom filter and dictionary overrides do not apply to
delete files. The global `parquet.enable.dictionary` switch still applies.
Metrics configuration is independent: equality deletes use the table metrics
settings, while position deletes always retain full `file_path` and `pos` bounds.

### Parquet reader

| Key | Description |
|---|---|
| `read.parquet.batch-size` | Arrow record-batch size used by the Parquet reader. |
| `read.parquet.max-compressed-page-size-bytes` | Maximum compressed page body. Default: 64 MiB. |
| `read.parquet.max-uncompressed-page-size-bytes` | Maximum uncompressed page body. Default: 256 MiB. |

The page limits are Go-specific reader settings; Iceberg Java ignores them. Explicit values must be integers from `1` to `2147483647`, the largest size representable in a Parquet page header. Table properties apply to scans, deletes, delete-file inspection and maintenance. A scan can override them through `table.WithOptions` without changing saved metadata. Compressed and uncompressed limits are independent; raising one does not raise the other.
