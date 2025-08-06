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
| HDFS                 |    X      |
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
## Hive Metastore Catalog

The project provides an experimental catalog backed by the Hive Metastore using
[gohive](https://github.com/beltran/gohive).

### Configuration

The catalog can be configured programmatically with `hive.Config` or via the
generic catalog loader. Examples below show Plain and Kerberos authentication
modes.

#### Plain (NONE)

```go
cfg := hive.Config{
    Host:     "hm.example.com",
    Port:     9083,
    Auth:     "NONE",
    Username: "myuser",
    Password: "mypassword",
}
cat, err := hive.NewHiveCatalog(cfg)
if err != nil {
    panic(err)
}
```

#### Kerberos (KERBEROS)

Kerberos requires building gohive with the `kerberos` build tag and a valid
ticket (for example via `kinit`).

```go
cfg := hive.Config{
    Host: "hm.example.com",
    Port: 9083,
    Auth: "KERBEROS",
}
cat, err := hive.NewHiveCatalog(cfg)
if err != nil {
    panic(err)
}
```

Connection properties can also be expressed using the generic catalog factory:

```go
cat, err := catalog.Load(ctx, "hive", iceberg.Properties{
    "uri": "hive://hm.example.com:9083?auth=KERBEROS",
})
if err != nil {
    panic(err)
}
```

### Usage

```go
ctx := context.Background()
cat, err := hive.NewHiveCatalog(hive.Config{Host: "hm.example.com", Port: 9083, Auth: "NONE"})
if err != nil {
    panic(err)
}

schema := iceberg.NewSchema(iceberg.NestedFieldMap{
    1: iceberg.PrimitiveField(1, "id", iceberg.IntType{}, false),
})

tbl, err := cat.CreateTable(ctx, table.Identifier{"db", "tbl"}, schema)
if err != nil {
    panic(err)
}

if _, err := cat.LoadTable(ctx, table.Identifier{"db", "tbl"}, nil); err != nil {
    panic(err)
}
```

### Limitations and Known Issues

- Only a subset of catalog operations is implemented; write commits are not
  supported.
- Connections use the binary Thrift protocol; HTTP transport and TLS are not
  supported.
- Kerberos authentication relies on external tickets and has limited test
  coverage.

### CLI Usage
Run `go build ./cmd/iceberg` from the root of this repository to build the CLI executable, alternately you can run `go install github.com/apache/iceberg-go/cmd/iceberg` to install it to the `bin` directory of your `GOPATH`.

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
