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

# Iceberg Go

Iceberg Go is a go-native implementation for accessing iceberg tables.

[![Go Reference](https://pkg.go.dev/badge/github.com/apache/iceberg-go.svg)](https://pkg.go.dev/github.com/apache/iceberg-go)

`iceberg` is a Golang implementation of the [Iceberg table spec](https://iceberg.apache.org/spec/).


## Feature Support / Roadmap

### FileSystem Support

| Filesystem Type      | Supported |
| :------------------: | :-------: |
| S3                   |    X      |
| Google Cloud Storage |    X      |
| Azure Blob Storage   |    X      |
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
| Plan Scan                |     X     |
| Plan Scan for Snapshot   |     X     |

### Catalog Support

| Operation                   | REST | Hive |  Glue  | SQL  |
|:----------------------------|:----:| :--: |:------:|:----:|
| Load Table                  |  X   |      |   X    |  X   |
| List Tables                 |  X   |      |   X    |  X   |
| Create Table                |  X   |      |   X    |  X   |
| Register Table              |  X   |      |   X    |      |
| Update Current Snapshot     |  X   |      |   X    |  X   |
| Create New Snapshot         |  X   |      |   X    |  X   |
| Rename Table                |  X   |      |   X    |  X   |
| Drop Table                  |  X   |      |   X    |  X   |
| Alter Table                 |  X   |      |   X    |  X   |
| Check Table Exists          |  X   |      |   X    |  X   |
| Set Table Properties        |  X   |      |   X    |  X   |
| List Namespaces             |  X   |      |   X    |  X   |
| Create Namespace            |  X   |      |   X    |  X   |
| Check Namespace Exists      |  X   |      |   X    |  X   |
| Drop Namespace              |  X   |      |   X    |  X   |
| Update Namespace Properties |  X   |      |   X    |  X   |
| Create View                 |  X   |      |        |  X   |
| Load View                   |      |      |        |  X   |
| List View                   |  X   |      |        |  X   |
| Drop View                   |  X   |      |        | X    |
| Check View Exists           |  X   |      |        |  X   |

### Read/Write Data Support

* Data can currently be read as an Arrow Table or as a stream of Arrow record batches.

#### Supported Write Operations

As long as the FileSystem is supported and the Catalog supports altering
the table, the following tracks the current write support:

| Operation         |Supported|
|:-----------------:|:-------:|
| Append Stream     |   X     |
| Append Data Files |   X     |
| Rewrite Files     |         |
| Rewrite manifests |         |
| Overwrite Files   |         |
| Write Pos Delete  |         |
| Write Eq Delete   |         |
| Row Delta         |         |


## Get in Touch

- [Iceberg community](https://iceberg.apache.org/community/)
- [Iceberg-Go Slack](https://apache-iceberg.slack.com/archives/C05J3MJ42BD)