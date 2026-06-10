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

Apache Iceberg Go is a Go-native implementation of [Apache Iceberg](https://iceberg.apache.org/), the open table format for analytic datasets. Read and write Iceberg tables from Go services and tooling without a JVM.

[![Go Reference](https://pkg.go.dev/badge/github.com/apache/iceberg-go.svg)](https://pkg.go.dev/github.com/apache/iceberg-go)

## Where to start

- **[Install](./install.md)** - get the library and CLI on your machine.
- **[CLI](./cli.md)** - inspect tables, run maintenance, manage refs.
- **[API](./api.md)** - construct catalogs, scan tables, write Arrow data, evolve schemas.
- **[Configuration](./configuration.md)** - YAML config file, catalog options, FileIO and credentials, table properties.

## What works today

A capability matrix - filesystem, metadata operations, catalog support, and write operations - lives on the dedicated **[Feature Status](./feature-status.md)** page.

## Beyond Go

Apache Iceberg is multi-language. PyIceberg, iceberg-rust, iceberg-cpp, and the Java reference implementation all target the same spec - see **[Other Iceberg implementations](./ecosystem.md)** for cross-links.

## Help and contribution

- Questions and discussion: **[Community](./community.md)**
- Contributing code or docs: **[Contributing](./contributing.md)**
- Cutting or verifying a release: **[Releases](./releases.md)**

The canonical Iceberg specification, terminology, and multi-engine policy live with the main project at [iceberg.apache.org](https://iceberg.apache.org/). This site covers what is specific to the Go implementation.
