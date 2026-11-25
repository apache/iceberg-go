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

# Install

## Go library

Use Go 1.26.3 or later to build from `main`. For a tagged release, see the
Go requirement in that tag's `go.mod`.

From your Go module, install the latest release:

```sh
go get github.com/apache/iceberg-go@latest
```

Start with the [Getting Started](./getting-started.md) walkthrough or see
[Releases](./releases.md#using-a-release) for version selection.

## CLI

```sh
go install github.com/apache/iceberg-go/cmd/iceberg@latest
```

See the [CLI guide](./cli.md) for command examples and source builds.
