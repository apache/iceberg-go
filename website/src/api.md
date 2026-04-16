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

# Catalog

`Catalog` is the entry point for accessing iceberg tables. You can use a catalog to:

* Create and list namespaces.
* Create, load, and drop tables

Multiple catalog implementations are available: REST, Hive, Glue, and SQL. Here is an
example of how to create a `RestCatalog`:

```go
import (
    "context"
    "github.com/apache/iceberg-go/catalog"
    "github.com/apache/iceberg-go/catalog/rest"
)

// Create a REST catalog
cat, err := rest.NewCatalog(context.Background(), "rest", "http://localhost:8181", 
    rest.WithOAuthToken("your-token"))
if err != nil {
    log.Fatal(err)
}
```

You can run the following code to list all root namespaces:

```go
// List all root namespaces
namespaces, err := cat.ListNamespaces(context.Background(), nil)
if err != nil {
    log.Fatal(err)
}

for _, ns := range namespaces {
    fmt.Printf("Namespace: %v\n", ns)
}
```

Then you can run the following code to create namespace:
```go
// Create a namespace
namespace := catalog.ToIdentifier("my_namespace")
err = cat.CreateNamespace(context.Background(), namespace, nil)
if err != nil {
    log.Fatal(err)
}
```

## Other Catalog Types

### SQL Catalog

You can also use SQL-based catalogs:

```go
import (
    "github.com/apache/iceberg-go/catalog"
    "github.com/apache/iceberg-go/io"
)

// Create a SQLite catalog
cat, err := catalog.Load(context.Background(), "local", iceberg.Properties{
    "type":               "sql",
    "uri":                "file:iceberg-catalog.db",
    "sql.dialect":        "sqlite",
    "sql.driver":         "sqlite",
    io.S3Region:          "us-east-1",
    io.S3AccessKeyID:     "admin",
    io.S3SecretAccessKey: "password",
    "warehouse":          "file:///tmp/warehouse",
})
if err != nil {
    log.Fatal(err)
}
```

### Glue Catalog

For AWS Glue integration:

```go
import (
    "github.com/apache/iceberg-go/catalog/glue"
    "github.com/aws/aws-sdk-go-v2/config"
)

// Create AWS config
awsCfg, err := config.LoadDefaultConfig(context.TODO())
if err != nil {
    log.Fatal(err)
}

// Create Glue catalog
cat := glue.NewCatalog(glue.WithAwsConfig(awsCfg))

// Create a table in Glue
tableIdent := catalog.ToIdentifier("my_database", "my_table")
tbl, err := cat.CreateTable(
    context.Background(),
    tableIdent,
    schema,
    catalog.WithLocation("s3://my-bucket/tables/my_table"),
)
if err != nil {
    log.Fatal(err)
}
```

# Table

After creating `Catalog`, we can manipulate tables through `Catalog`.

You can use the following code to create a table:

```go
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog"
    "github.com/apache/iceberg-go/table"
)

// Create a simple schema
schema := iceberg.NewSchemaWithIdentifiers(1, []int{2},
    iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false},
    iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
    iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
)

// Create a table identifier
tableIdent := catalog.ToIdentifier("my_namespace", "my_table")

// Create a table with optional properties
tbl, err := cat.CreateTable(
    context.Background(),
    tableIdent,
    schema,
    catalog.WithProperties(map[string]string{"owner": "me"}),
    catalog.WithLocation("s3://my-bucket/tables/my_table"),
)
if err != nil {
    log.Fatal(err)
}
```

Also, you can load a table directly:

```go
// Load an existing table
tbl, err := cat.LoadTable(context.Background(), tableIdent, nil)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Table: %s\n", tbl.Identifier())
fmt.Printf("Location: %s\n", tbl.MetadataLocation())
```

## Schema Creation

Here are some examples of creating different types of schemas:

```go
// Simple schema with primitive types
simpleSchema := iceberg.NewSchemaWithIdentifiers(1, []int{2},
    iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
    iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
    iceberg.NestedField{ID: 3, Name: "active", Type: iceberg.PrimitiveTypes.Bool, Required: false},
)

// Schema with nested struct
nestedSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
    iceberg.NestedField{ID: 1, Name: "person", Type: &iceberg.StructType{
        FieldList: []iceberg.NestedField{
            {ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
            {ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
        },
    }, Required: false},
)

// Schema with list and map types
complexSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
    iceberg.NestedField{ID: 1, Name: "tags", Type: &iceberg.ListType{
        ElementID: 2, Element: iceberg.PrimitiveTypes.String, ElementRequired: true,
    }, Required: false},
    iceberg.NestedField{ID: 3, Name: "metadata", Type: &iceberg.MapType{
        KeyID: 4, KeyType: iceberg.PrimitiveTypes.String,
        ValueID: 5, ValueType: iceberg.PrimitiveTypes.String, ValueRequired: true,
    }, Required: false},
)
```

## Table Operations

Here are some common table operations:

```go
// List tables in a namespace
tables := cat.ListTables(context.Background(), catalog.ToIdentifier("my_namespace"))
for tableIdent, err := range tables {
    if err != nil {
        log.Printf("Error listing table: %v", err)
        continue
    }
    fmt.Printf("Table: %v\n", tableIdent)
}

// Check if table exists
exists, err := cat.CheckTableExists(context.Background(), tableIdent)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Table exists: %t\n", exists)

// Drop a table
err = cat.DropTable(context.Background(), tableIdent)
if err != nil {
    log.Fatal(err)
}

// Rename a table
fromIdent := catalog.ToIdentifier("my_namespace", "old_table")
toIdent := catalog.ToIdentifier("my_namespace", "new_table")
renamedTable, err := cat.RenameTable(context.Background(), fromIdent, toIdent)
if err != nil {
    log.Fatal(err)
}
```

## Working with Table Metadata

Once you have a table, you can access its metadata and properties:

```go
// Access table metadata
metadata := tbl.Metadata()
fmt.Printf("Table UUID: %s\n", metadata.TableUUID())
fmt.Printf("Format version: %d\n", metadata.Version())
fmt.Printf("Last updated: %d\n", metadata.LastUpdatedMillis())

// Access table schema
schema := tbl.Schema()
fmt.Printf("Schema ID: %d\n", schema.ID)
fmt.Printf("Number of fields: %d\n", schema.NumFields())

// Access table properties
props := tbl.Properties()
fmt.Printf("Owner: %s\n", props["owner"])

// Access the current snapshot
if snapshot := tbl.CurrentSnapshot(); snapshot != nil {
    fmt.Printf("Current snapshot ID: %d\n", snapshot.SnapshotID)
    fmt.Printf("Snapshot timestamp: %d\n", snapshot.TimestampMs)
}

// List all snapshots
for _, snapshot := range tbl.Snapshots() {
    fmt.Printf("Snapshot %d: %s\n", snapshot.SnapshotID, snapshot.Summary.Operation)
}
```

## Creating Tables with Partitioning

You can create tables with partitioning:

```go
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog"
)

// Create schema
schema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
    iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
    iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
    iceberg.NestedField{ID: 3, Name: "date", Type: iceberg.PrimitiveTypes.Date, Required: false},
)

// Create a partition spec
partitionSpec := iceberg.NewPartitionSpec(
    iceberg.PartitionField{SourceID: 3, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "date"},
)

// Create a table with partitioning
tbl, err := cat.CreateTable(
    context.Background(),
    tableIdent,
    schema,
    catalog.WithPartitionSpec(&partitionSpec),
    catalog.WithLocation("s3://my-bucket/tables/partitioned_table"),
)
if err != nil {
    log.Fatal(err)
}
```

# Scanning (Reading Data)

The `Scan` API reads data from Iceberg tables as Apache Arrow record batches. It supports
column projection, row filtering, snapshot selection, and time travel.

## Basic Scan

```go
import (
    "context"
    "fmt"

    "github.com/apache/iceberg-go/table"
)

// Scan all data from a table
scan := tbl.Scan()
arrowSchema, records, err := scan.ToArrowRecords(context.Background())
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Schema: %s\n", arrowSchema)
for batch, err := range records {
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Batch with %d rows\n", batch.NumRows())
    batch.Release()
}
```

## Column Selection and Row Filtering

```go
import "github.com/apache/iceberg-go"

// Select specific columns and filter rows
scan := tbl.Scan(
    table.WithSelectedFields("id", "name", "date"),
    table.WithRowFilter(
        iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(100)),
    ),
    table.WithCaseSensitive(true),
    table.WithLimit(1000),
)

arrowSchema, records, err := scan.ToArrowRecords(context.Background())
if err != nil {
    log.Fatal(err)
}
```

## Time Travel

```go
// Read a specific snapshot by ID
scan := tbl.Scan(
    table.WithSnapshotID(snapshotID),
)

// Read data as of a specific timestamp (milliseconds since epoch)
scan = tbl.Scan(
    table.WithSnapshotAsOf(timestampMs),
)

// Read from a named branch or tag
scan = tbl.Scan()
scan, err := scan.UseRef("audit-branch")
if err != nil {
    log.Fatal(err)
}
```

## Reading as an Arrow Table

```go
// Load all results into a single Arrow Table (in-memory)
arrowTable, err := tbl.Scan().ToArrowTable(context.Background())
if err != nil {
    log.Fatal(err)
}
defer arrowTable.Release()

fmt.Printf("Total rows: %d\n", arrowTable.NumRows())
fmt.Printf("Total columns: %d\n", arrowTable.NumCols())
```

# Writing Data

The write API uses Apache Arrow as the input format. Data can be written using
`arrow.Table` or `array.RecordReader` (streaming).

## Append Data

```go
import (
    "context"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

// Append using a RecordReader (streaming)
// The RecordReader's schema must be compatible with the table schema.
newTable, err := tbl.Append(context.Background(), recordReader, nil)
if err != nil {
    log.Fatal(err)
}

// Append using an Arrow Table with a batch size
newTable, err = tbl.AppendTable(context.Background(), arrowTable, 1024, nil)
if err != nil {
    log.Fatal(err)
}
```

## Overwrite Data

Overwrite replaces existing data. An optional filter controls which rows are replaced.

```go
import "github.com/apache/iceberg-go"

// Overwrite all existing data with new data
newTable, err := tbl.Overwrite(context.Background(), recordReader, nil)
if err != nil {
    log.Fatal(err)
}

// Overwrite only rows matching a filter
newTable, err = tbl.Overwrite(context.Background(), recordReader, nil,
    table.WithOverwriteFilter(
        iceberg.EqualTo(iceberg.Reference("date"), "2024-01-01"),
    ),
)
if err != nil {
    log.Fatal(err)
}
```

## Delete Data

Delete removes rows matching a filter expression.

```go
import "github.com/apache/iceberg-go"

// Delete rows matching a filter
newTable, err := tbl.Delete(context.Background(),
    iceberg.LessThan(iceberg.Reference("id"), int32(100)),
    nil, // snapshot properties
)
if err != nil {
    log.Fatal(err)
}
```

# Transactions

Transactions group multiple operations into a single atomic commit. Use transactions
when you need to perform multiple writes or metadata changes together.

## Basic Transaction

```go
import (
    "context"

    "github.com/apache/iceberg-go"
)

txn := tbl.NewTransaction()

// Append new data
err := txn.Append(context.Background(), recordReader, nil)
if err != nil {
    log.Fatal(err)
}

// Set table properties
err = txn.SetProperties(iceberg.Properties{
    "commit.user": "data-pipeline",
})
if err != nil {
    log.Fatal(err)
}

// Commit all changes atomically
newTable, err := txn.Commit(context.Background())
if err != nil {
    log.Fatal(err)
}
```

## Multi-Operation Transaction

```go
txn := tbl.NewTransaction()

// Delete old data
err := txn.Delete(context.Background(),
    iceberg.LessThan(iceberg.Reference("date"), "2023-01-01"),
    nil,
)
if err != nil {
    log.Fatal(err)
}

// Append replacement data
err = txn.Append(context.Background(), recordReader, nil)
if err != nil {
    log.Fatal(err)
}

// Commit both operations as one atomic snapshot
newTable, err := txn.Commit(context.Background())
if err != nil {
    log.Fatal(err)
}
```

## Branch Transactions

```go
// Create a transaction that commits to a specific branch
txn := tbl.NewTransactionOnBranch("staging")

err := txn.Append(context.Background(), recordReader, nil)
if err != nil {
    log.Fatal(err)
}

newTable, err := txn.Commit(context.Background())
if err != nil {
    log.Fatal(err)
}
```
