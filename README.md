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

# Iceberg Golang

[![Go Reference](https://pkg.go.dev/badge/github.com/apache/iceberg-go.svg)](https://pkg.go.dev/github.com/apache/iceberg-go)

`iceberg` is a Golang implementation of the [Iceberg table spec](https://iceberg.apache.org/spec/).

## Build From Source

### Prerequisites

* Go 1.23 or later

### Build

```shell
$ git clone https://github.com/apache/iceberg-go.git
$ cd iceberg-go/cmd/iceberg && go build .
```

## Feature Support / Roadmap

### FileSystem Support

| Filesystem Type      | Supported |
| :------------------: | :-------: |
| S3                   |    X      |
| Google Cloud Storage |           |
| Azure Blob Storage   |           |
| Local Filesystem     |    X      |

### Metadata

| Operation                | Supported |
| :----------------------- | :-------: |
| Get Schema               |     X     |
| Get Snapshots            |     X     |
| Get Sort Orders          |     X     |
| Get Partition Specs      |     X     |
| Get Manifests            |     X     |
| Create New Manifests     |     X     |
| Plan Scan                |     x     |
| Plan Scan for Snapshot   |     x     |

### Catalog Support

| Operation                | REST | Hive | DynamoDB | Glue |
|:-------------------------|:----:| :--: | :------: |:----:|
| Load Table               |  X   |      |          |  X   |
| List Tables              |  X   |      |          |  X   |
| Create Table             |  X   |      |          |  X   |
| Update Current Snapshot  |      |      |          |      |
| Create New Snapshot      |      |      |          |      |
| Rename Table             |  X   |      |          |  X   |
| Drop Table               |  X   |      |          |  X   |
| Alter Table              |      |      |          |      |
| Check Table Exists       |  X   |      |          |      |
| Set Table Properties     |  X   |      |          |  X   |
| Create Namespace         |  X   |      |          |  X   |
| Check Namespace Exists   |  X   |      |          |      |
| Drop Namespace           |  X   |      |          |  X   |
| Set Namespace Properties |  X   |      |          |  X   |

### Read/Write Data Support

* No intrinsic support for writing data yet.
* Plan to add [Apache Arrow](https://pkg.go.dev/github.com/apache/arrow-go/) support eventually.
* Data can currently be read as an Arrow Table or as a stream of Arrow record batches.

# Get in Touch

- [Iceberg community](https://iceberg.apache.org/community/)
