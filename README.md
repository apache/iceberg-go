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
| Plan Scan                |           |
| Plan Scan for Snapshot   |           |

### Catalog Support

| Operation                | REST | Hive | DynamoDB | Glue |
| :----------------------- | :--: | :--: | :------: | :--: |
| Create Table             |      |      |          |      |
| Update Current Snapshot  |      |      |          |      |
| Create New Snapshot      |      |      |          |      |
| Rename Table             |      |      |          |      |
| Drop Table               |      |      |          |      |
| Alter Table              |      |      |          |      |
| Set Table Properties     |      |      |          |      |
| Create Namespace         |      |      |          |      |
| Drop Namespace           |      |      |          |      |
| Set Namespace Properties |      |      |          |      |

### Read/Write Data Support

* No intrinsic support for reading/writing data yet
  * Data can be manually read currently by retrieving data files via Manifests.
  * Plan to add [Apache Arrow](https://pkg.go.dev/github.com/apache/arrow/go/v14@v14.0.0) support eventually.

# Get in Touch

- [Iceberg community](https://iceberg.apache.org/community/)