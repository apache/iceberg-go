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

# Other Iceberg implementations

Apache Iceberg Go is one of several official Iceberg implementations. Pick the one that matches your runtime; the table format and catalog protocols are the same across all of them.

| Project | Language | Repository | Documentation |
|---|---|---|---|
| Apache Iceberg | Java (reference) | [apache/iceberg](https://github.com/apache/iceberg) | [iceberg.apache.org](https://iceberg.apache.org/) |
| PyIceberg | Python | [apache/iceberg-python](https://github.com/apache/iceberg-python) | [py.iceberg.apache.org](https://py.iceberg.apache.org/) |
| iceberg-rust | Rust | [apache/iceberg-rust](https://github.com/apache/iceberg-rust) | [rust.iceberg.apache.org](https://rust.iceberg.apache.org/) |
| iceberg-cpp | C++ | [apache/iceberg-cpp](https://github.com/apache/iceberg-cpp) | [cpp.iceberg.apache.org](https://cpp.iceberg.apache.org/) (early stage) |

## When to use which

- **Java** is the reference implementation and is what every query engine integrates against (Spark, Flink, Trino, Hive, Presto, Dremio, etc.). If you are running a JVM workload, this is the canonical choice.
- **PyIceberg** is for Python and the dataframe ecosystem (PyArrow, Pandas, Polars, DuckDB, Daft, Ray). Most data-science and ML workflows live here.
- **iceberg-rust** is the Rust implementation, used by `pyiceberg-core`, DataFusion-based engines, and other Rust-native systems.
- **iceberg-cpp** is early-stage (0.2.0 released 2026-01-26). Track the project for native C++ integration once it stabilizes.
- **iceberg-go** (this project) is for Go services and tooling. Tight Apache Arrow Go integration makes it a good fit for streaming Arrow record batches into and out of Iceberg tables.

## Specifications and shared concepts

The Iceberg spec, terminology, partitioning semantics, evolution semantics, REST Catalog OpenAPI, and multi-engine support policy live with the main project at [iceberg.apache.org](https://iceberg.apache.org/). All the implementations above target the same spec.

For Apache Iceberg Go-specific guidance, continue with the [API Reference](./api.md), [CLI](./cli.md), or [Configuration](./configuration.md).
