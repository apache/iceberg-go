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
| Plan Scan                |     x     |
| Plan Scan for Snapshot   |     x     |

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


### CLI Usage
Run `go build ./cmd/iceberg` from the root of this repository to build the CLI executable, alternately you can run `go install github.com/apache/iceberg-go/cmd/iceberg@latest` to install it to the `bin` directory of your `GOPATH`.

The `iceberg` CLI usage is very similar to [pyiceberg CLI](https://py.iceberg.apache.org/cli/) \
You can pass the catalog URI with `--uri` argument.

Example:
You can start the Iceberg REST API docker image which runs on default in port `8181`
```
docker pull apache/iceberg-rest-fixture:latest
docker run -p 8181:8181 apache/iceberg-rest-fixture:latest
```
and run the `iceberg` CLI pointing to the REST API server.

```
 ./iceberg --uri http://0.0.0.0:8181 list
┌─────┐
| IDs |
| --- |
└─────┘
```
**Create Namespace**
```
./iceberg --uri http://0.0.0.0:8181 create namespace taxitrips
```

**List Namespace**
```
 ./iceberg --uri http://0.0.0.0:8181 list
┌───────────┐
| IDs       |
| --------- |
| taxitrips |
└───────────┘


```
# Get in Touch

- [Iceberg community](https://iceberg.apache.org/community/)
