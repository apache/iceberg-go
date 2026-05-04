<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# gocloud blob I/O (S3, GCS, Azure)

This package connects [gocloud.dev/blob](https://gocloud.dev/blob) drivers to
`iceberg-go`’s filesystem abstraction. Register it with a blank import:

```go
import _ "github.com/apache/iceberg-go/io/gocloud"
```

Full API docs: [pkg.go.dev/github.com/apache/iceberg-go/io/gocloud](https://pkg.go.dev/github.com/apache/iceberg-go/io/gocloud).

## S3: HTTP client and connection reuse

For Amazon S3, if `aws.Config.HTTPClient` is **nil**, the backend configures an
AWS SDK HTTP client with **larger per-host connection pools** than Go’s
`http.DefaultTransport` defaults. That improves workloads with many small object
reads (same S3 hostname), such as Parquet footer reads, without changing proxy
or TLS behavior (the SDK “buildable” client keeps those aligned with normal AWS
clients).

If `HTTPClient` is already set—by you, by `s3.proxy-uri` in table/storage
properties, or by any other means—it is **not** replaced.

## Providing your own `aws.Config` (`utils.WithAwsConfig`)

Some catalogs (for example **AWS Glue**) put the catalog’s
[`aws.Config`](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws#Config) on
[`context.Context`](https://pkg.go.dev/context) before opening manifests or
data files. You can do the same anywhere you control the context, or override
behavior for tests and custom tooling:

```go
import (
    "context"

    "github.com/apache/iceberg-go/utils"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
if err != nil {
    return err
}

// Example: fully custom HTTP client (optional).
cfg.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
    t.MaxIdleConnsPerHost = 64
    // ... any other *http.Transport fields
})

ctx = utils.WithAwsConfig(ctx, &cfg)
// Use ctx for iceberg table / IO operations that resolve S3 through gocloud.
```

[`utils.GetAwsConfig`](https://pkg.go.dev/github.com/apache/iceberg-go/utils#GetAwsConfig)
returns that pointer when the S3 backend builds the bucket; if it returns `nil`,
configuration comes from Iceberg/S3 properties and the shared config loader
instead.
