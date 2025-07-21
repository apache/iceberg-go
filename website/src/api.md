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

Currently, only the REST catalog has been implemented, and other catalogs are under active development. Here is an 
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
