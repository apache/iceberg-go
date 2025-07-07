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

# Glossary

This glossary defines important terms used throughout the Iceberg ecosystem, organized in tables for easy reference.

## Core Concepts

| Term | Definition |
|------|------------|
| **Catalog** | A centralized service that manages table metadata and provides a unified interface for accessing Iceberg tables. Catalogs can be implemented as Hive metastore, AWS Glue, REST API, or SQL-based solutions. |
| **Table** | A collection of data files organized by a schema, with metadata tracking changes over time through snapshots. Tables support ACID transactions and schema evolution. |
| **Schema** | The structure definition of a table, specifying field names, types, and whether fields are required or optional. Schemas are versioned and can evolve over time. |
| **Snapshot** | A point-in-time view of a table's data, representing the state after a specific operation (append, overwrite, delete, etc.). Each snapshot contains metadata about the operation and references to data files. |
| **Manifest** | A metadata file that lists data files and their metadata (location, partition information, record counts, etc.). Manifests are organized into manifest lists for efficient access. |
| **Manifest List** | A file that contains references to manifest files for a specific snapshot, enabling efficient discovery of data files without reading all manifests. |

## Data Types

### Primitive Types

| Type | Description |
|------|-------------|
| `boolean` | True/false values |
| `int` (32-bit) | Integer values |
| `long` (64-bit) | Long integer values |
| `float` (32-bit) | Single precision floating point |
| `double` (64-bit) | Double precision floating point |
| `date` | Date values (days since epoch) |
| `time` | Time values (microseconds since midnight) |
| `timestamp` | Timestamp values (microseconds since epoch) |
| `timestamptz` | Timestamp with timezone |
| `string` | UTF-8 encoded strings |
| `uuid` | UUID values |
| `binary` | Variable length binary data |
| `fixed[n]` | Fixed length binary data of n bytes |
| `decimal(p,s)` | Decimal values with precision p and scale s |

### Nested Types

| Type | Description |
|------|-------------|
| `struct` | Collection of named fields |
| `list` | Ordered collection of elements |
| `map` | Key-value pairs |

## Operations

| Operation | Description |
|-----------|-------------|
| **Append** | An operation that adds new data files to a table without removing existing data. Creates a new snapshot with the additional files. |
| **Overwrite** | An operation that replaces existing data files with new ones, typically based on a partition predicate. Creates a new snapshot with the replacement files. |
| **Delete** | An operation that removes data files from a table, either by marking them as deleted or by removing references to them. |
| **Replace** | An operation that completely replaces all data in a table with new data, typically used for full table refreshes. |

## Partitioning

| Term | Definition |
|------|------------|
| **Partition** | A logical division of table data based on column values, used to improve query performance by allowing selective reading of relevant data files. |
| **Partition Spec** | Defines how table data is partitioned by specifying source columns and transformations (identity, bucket, truncate, year, month, day, hour). |
| **Partition Field** | A field in the partition spec that defines how a source column is transformed for partitioning. |
| **Partition Path** | The file system path structure created by partition values, typically in the format `partition_name=value/`. |

### Partition Transforms

| Transform | Description |
|-----------|-------------|
| `identity` | Use the column value directly |
| `bucket[n]` | Hash the value into n buckets |
| `truncate[n]` | Truncate strings to n characters |
| `year` | Extract year from date/timestamp |
| `month` | Extract month from date/timestamp |
| `day` | Extract day from date/timestamp |
| `hour` | Extract hour from timestamp |
| `void` | Always returns null (used for unpartitioned tables) |

## Expressions and Predicates

| Term | Definition |
|------|------------|
| **Expression** | A computation or comparison that can be evaluated against table data, used for filtering and transformations. |
| **Predicate** | A boolean expression used to filter data, such as column comparisons, null checks, or set membership tests. |
| **Bound Predicate** | A predicate that has been resolved against a specific schema, with field references bound to actual columns. |
| **Unbound Predicate** | A predicate that contains unresolved field references, typically in string form before binding to a schema. |
| **Literal** | A constant value used in expressions and predicates, such as numbers, strings, dates, etc. |

## File Formats

| Format | Usage | Description |
|--------|-------|-------------|
| **Parquet** | Data files | The primary data file format used by Iceberg, providing columnar storage with compression and encoding optimizations. |
| **Avro** | Metadata files | Used for manifests and manifest lists due to its schema evolution capabilities and compact binary format. |
| **ORC** | Data files | An alternative columnar format supported by some Iceberg implementations. |

## Metadata

| Term | Definition |
|------|------------|
| **Metadata File** | A JSON file containing table metadata including schema, partition spec, properties, and snapshot information. |
| **Metadata Location** | The URI pointing to the current metadata file for a table, stored in the catalog. |
| **Properties** | Key-value pairs that configure table behavior, such as compression settings, write options, and custom metadata. |
| **Statistics** | Metadata about data files including record counts, file sizes, and value ranges for optimization. |

## Transactions

| Term | Definition |
|------|------------|
| **Transaction** | A sequence of operations that are committed atomically, ensuring data consistency and ACID properties. |
| **Commit** | The process of finalizing a transaction by creating a new snapshot and updating the metadata file. |
| **Rollback** | The process of undoing changes in a transaction, typically by reverting to a previous snapshot. |

## References

| Term | Definition |
|------|------------|
| **Branch** | A named reference to a specific snapshot, allowing multiple concurrent views of table data. |
| **Tag** | An immutable reference to a specific snapshot, typically used for versioning and releases. |

## Storage

| Term | Definition |
|------|------------|
| **Warehouse** | The root directory or bucket where table data and metadata are stored. |
| **Location Provider** | A component that generates file paths for table data and metadata based on table location and naming conventions. |
| **FileIO** | An abstraction layer for reading and writing files across different storage systems (local filesystem, S3, GCS, Azure Blob, etc.). |

## Query Optimization

| Technique | Description |
|-----------|-------------|
| **Column Pruning** | A technique that reads only the columns needed for a query, reducing I/O and improving performance. |
| **Partition Pruning** | A technique that skips reading data files from irrelevant partitions based on query predicates. |
| **Predicate Pushdown** | A technique that applies filtering predicates at the storage layer, reducing data transfer and processing. |
| **Statistics-based Optimization** | Using table and file statistics to optimize query execution plans and file selection. |

## Schema Evolution

| Term | Definition |
|------|------------|
| **Schema Evolution** | The process of modifying a table's schema over time while maintaining backward compatibility. |
| **Column Addition** | Adding new columns to a table schema, which are typically optional to maintain compatibility. |
| **Column Deletion** | Removing columns from a table schema, which may be logical (marking as deleted) or physical. |
| **Column Renaming** | Changing column names while preserving data and type information. |
| **Type Evolution** | Changing column types in ways that maintain data compatibility (e.g., int32 to int64). |

## Time Travel

| Term | Definition |
|------|------------|
| **Time Travel** | The ability to query a table as it existed at a specific point in time using snapshot timestamps. |
| **Snapshot Isolation** | A property that ensures queries see a consistent view of data as it existed at a specific snapshot. |

## ACID Properties

| Property | Description |
|----------|-------------|
| **Atomicity** | Ensures that all operations in a transaction either succeed completely or fail completely. |
| **Consistency** | Ensures that the table remains in a valid state after each transaction. |
| **Isolation** | Ensures that concurrent transactions do not interfere with each other. |
| **Durability** | Ensures that committed changes are permanently stored and survive system failures. |

