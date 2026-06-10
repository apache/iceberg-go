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

# Feature Status

This page tracks what Apache Iceberg Go currently supports. The matrix is kept in sync with the project [`README.md`](https://github.com/apache/iceberg-go/blob/main/README.md); if you spot a discrepancy, file an issue.

## Spec format version coverage

Apache Iceberg Go reads and writes table format versions 1, 2, and 3. The maximum supported version is enforced at [`table/metadata.go`](https://github.com/apache/iceberg-go/blob/main/table/metadata.go) (`supportedTableFormatVersion = 3`). For active tracking, see issues [#589 (V3)](https://github.com/apache/iceberg-go/issues/589) and [#829 (V2 completion)](https://github.com/apache/iceberg-go/issues/829).

### V1

All V1 features are supported. V1 is the format-version baseline.

### V2

| Feature | Status |
|---|---|
| Sequence numbers | Supported |
| Manifest entry status (added / existing / deleted) | Supported |
| Positional deletes | Supported (read + write) |
| Equality deletes | Read supported. Write supported via `Transaction.WriteEqualityDeletes`; full row-delta / merge-on-read flow tracked under [#829](https://github.com/apache/iceberg-go/issues/829) |
| Partition spec evolution | Supported |
| Sort order enforcement on write | Supported (PR [#1157](https://github.com/apache/iceberg-go/pull/1157), closes [#833](https://github.com/apache/iceberg-go/issues/833)) |
| `ReplaceDataFiles` using `OpReplace` | Pending ([#841](https://github.com/apache/iceberg-go/issues/841)) |

### V3

| Feature | Status |
|---|---|
| Nanosecond timestamps (`timestamp_ns`, `timestamptz_ns`) | Supported |
| Default values (`initial-default`, `write-default`) | Supported |
| Row lineage (`_row_id`, `_last_updated_sequence_number`) | Supported |
| Encryption keys in metadata | Supported |
| Variant type, non-shredded | Supported (PR [#932](https://github.com/apache/iceberg-go/pull/932); umbrella [#929](https://github.com/apache/iceberg-go/issues/929)) |
| Variant type, shredded reader / writer | In progress ([#986](https://github.com/apache/iceberg-go/issues/986), [#987](https://github.com/apache/iceberg-go/issues/987)) |
| Deletion vectors, read | Supported |
| Deletion vectors, write (unpartitioned) | Supported |
| Deletion vectors, write (partitioned) | In progress ([#1135](https://github.com/apache/iceberg-go/issues/1135), PR [#1151](https://github.com/apache/iceberg-go/pull/1151)) |
| Geometry / Geography types (schema) | Supported |
| Geometry / Geography (transforms, statistics, pruning) | In progress (umbrella [#989](https://github.com/apache/iceberg-go/issues/989)) |
| Multi-argument transforms | Infrastructure present; no concrete implementations exercised yet |

## FileSystem support

| Filesystem Type      | Supported |
| :------------------: | :-------: |
| S3                   |    X      |
| Google Cloud Storage |    X      |
| Azure Blob Storage   |    X      |
| Local Filesystem     |    X      |

S3, GCS, and Azure require a blank import: `_ "github.com/apache/iceberg-go/io/gocloud"`. See [Configuration](./configuration.md).

## Metadata operations

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

## Catalog support

| Operation                   | REST | Hive |  Glue  | SQL  | Hadoop |
|:----------------------------|:----:|:----:|:------:|:----:|:------:|
| Load Table                  |  X   |  X   |   X    |  X   |   X    |
| List Tables                 |  X   |  X   |   X    |  X   |   X    |
| Create Table                |  X   |  X   |   X    |  X   |   X    |
| Register Table              |  X   |  X   |   X    |      |        |
| Update Current Snapshot     |  X   |  X   |   X    |  X   |   X    |
| Create New Snapshot         |  X   |  X   |   X    |  X   |   X    |
| Rename Table                |  X   |  X   |   X    |  X   |        |
| Drop Table                  |  X   |  X   |   X    |  X   |   X    |
| Alter Table                 |  X   |  X   |   X    |  X   |   X    |
| Check Table Exists          |  X   |  X   |   X    |  X   |   X    |
| Set Table Properties        |  X   |  X   |   X    |  X   |   X    |
| List Namespaces             |  X   |  X   |   X    |  X   |   X    |
| Create Namespace            |  X   |  X   |   X    |  X   |   X    |
| Check Namespace Exists      |  X   |  X   |   X    |  X   |   X    |
| Drop Namespace              |  X   |  X   |   X    |  X   |   X    |
| Update Namespace Properties |  X   |  X   |   X    |  X   |        |
| Create View                 |  X   |  X   |        |  X   |        |
| Load View                   |      |  X   |        |  X   |        |
| List View                   |  X   |  X   |        |  X   |        |
| Drop View                   |  X   |  X   |        |  X   |        |
| Check View Exists           |  X   |  X   |        |  X   |        |

A Hadoop catalog is also available - see [`catalog/hadoop`](https://github.com/apache/iceberg-go/tree/main/catalog/hadoop).

## Read / write data

Data can be read as an Arrow `Table` or as a stream of Arrow record batches via `iter.Seq2`. See [API Reference](./api.md).

### Supported write operations

As long as the FileSystem is supported and the Catalog supports altering the table:

| Operation            | Supported |
|:---------------------|:---------:|
| Append Stream        |     X     |
| Append Data Files    |     X     |
| Rewrite Files        |           |
| Rewrite manifests    |           |
| Overwrite Files      |     X     |
| Copy-On-Write Delete |     X     |
| Write Pos Delete     |     X     |
| Write Eq Delete      |           |
| Row Delta            |           |
