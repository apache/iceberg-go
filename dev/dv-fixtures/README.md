<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# DV Puffin fixture generator

`GenerateDVFixtures.java` is a JUnit class that produces Java-authored Puffin
deletion-vector fixtures consumed by iceberg-go's cross-client tests in
`table/dv/dv_cross_client_test.go`.

The Java upstream (apache/iceberg, apache/iceberg-python, apache/iceberg-rust)
does not commit any `.puffin` DV fixtures — every Java DV-Puffin test builds
the file in-memory. To get Java-authored bytes, this generator must be run
against an `apache/iceberg` checkout.

## Regenerate

1. Drop the file into your `apache/iceberg` checkout at
   `core/src/test/java/org/apache/iceberg/deletes/GenerateDVFixtures.java`.
   It must live in the `org.apache.iceberg.deletes` package so it can construct
   the package-private `BitmapPositionDeleteIndex` directly.

2. From the apache/iceberg repo root:

   ```bash
   ./gradlew :iceberg-core:test --tests org.apache.iceberg.deletes.GenerateDVFixtures
   ```

3. Copy the produced bytes into iceberg-go:

   ```bash
   cp core/build/dv-fixtures/single-blob-dv.puffin <iceberg-go>/table/dv/testdata/deletes/
   cp core/build/dv-fixtures/multi-blob-dv.puffin  <iceberg-go>/table/dv/testdata/deletes/
   ```

## Fixture contents

| Fixture                | Blobs | Positions per blob                                                               |
| ---------------------- | ----- | -------------------------------------------------------------------------------- |
| `single-blob-dv.puffin`| 1     | `[1, 3, 5, 7, 9]` referencing `s3://warehouse/db/table/data/00000-0-abc.parquet` |
| `multi-blob-dv.puffin` | 2     | `[0, 100, 200]` → `file-001.parquet`; `[50, 150]` → `file-002.parquet`          |

Compression is forced to `null` (uncompressed) so the bytes are reproducible
across JVM and zstd library versions. Snapshot ID and sequence number are
both `-1`, matching the spec for inheriting blobs.
