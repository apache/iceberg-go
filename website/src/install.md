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

In this quickstart, we’ll glean insights from code segments and learn how to:

## Requirements

<div class="warning">
Go 1.23 or later is required to build.
</div>

## Installation

To install `iceberg-go` package, you need to install Go and set your Go workspace first.
If you don't have a go.mod file, create it with `go mod init gin`.

1. Download and install it:

```sh
go get -u github.com/apache/iceberg-go
```

2. Import it in your code:

```go
import "github.com/apache/iceberg-go"
```
