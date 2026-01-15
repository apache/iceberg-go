# Hive Metastore Catalog

This package provides a Hive Metastore catalog implementation for Apache Iceberg in Go.

## Features

- Full `catalog.Catalog` interface implementation
- Namespace (database) management: create, drop, list, describe
- Table management: create, drop, rename, list, load
- Table metadata commits with optimistic locking
- Support for NOSASL and Kerberos authentication
- Uses the [gohive](https://github.com/beltran/gohive) library for Thrift communication

## Prerequisites

### Running Hive Metastore Locally

Start the Hive Metastore using Docker Compose:

```bash
cd internal/recipe
docker-compose up -d hive-metastore hiveserver2
```

This starts:
- **hive-metastore** on port `9083` (Thrift RPC)
- **hiveserver2** on port `10002` (Web UI) and `10003` (JDBC)

Verify the services are running:

```bash
docker ps | grep hive
```

Access the HiveServer2 Web UI at: http://localhost:10002

## CLI Usage

The `iceberg` CLI supports the Hive catalog. Use `--catalog hive` and `--uri thrift://localhost:9083`.

### List Namespaces

```bash
go run ./cmd/iceberg list --catalog hive --uri thrift://localhost:9083
```

### Create a Namespace

```bash
# Use /tmp/iceberg-warehouse which is mounted in Docker containers
go run ./cmd/iceberg create namespace --catalog hive --uri thrift://localhost:9083 \
  --description "Test namespace for Iceberg tables" \
  --location-uri /tmp/iceberg-warehouse/test_ns \
  test_ns
```

### Describe a Namespace

```bash
go run ./cmd/iceberg describe namespace --catalog hive --uri thrift://localhost:9083 test_ns
```

### Create a Table

```bash
go run ./cmd/iceberg create table --catalog hive --uri thrift://localhost:9083 \
  --schema '[{"name":"id","type":"long","required":true},{"name":"name","type":"string"},{"name":"created_at","type":"timestamp"}]' \
  --location-uri /tmp/iceberg-warehouse/test_ns/users \
  test_ns.users
```

### List Tables in a Namespace

```bash
go run ./cmd/iceberg list --catalog hive --uri thrift://localhost:9083 test_ns
```

### Describe a Table

```bash
go run ./cmd/iceberg describe table --catalog hive --uri thrift://localhost:9083 test_ns.users
```

### Get Table Schema

```bash
go run ./cmd/iceberg schema --catalog hive --uri thrift://localhost:9083 test_ns.users
```

### Get Table Location

```bash
go run ./cmd/iceberg location --catalog hive --uri thrift://localhost:9083 test_ns.users
```

### Set Table Properties

```bash
go run ./cmd/iceberg properties set table --catalog hive --uri thrift://localhost:9083 \
  test_ns.users write.format.default parquet
```

### Get Table Properties

```bash
go run ./cmd/iceberg properties get table --catalog hive --uri thrift://localhost:9083 test_ns.users
```

### Drop a Table

```bash
go run ./cmd/iceberg drop table --catalog hive --uri thrift://localhost:9083 test_ns.users
```

### Drop a Namespace

```bash
go run ./cmd/iceberg drop namespace --catalog hive --uri thrift://localhost:9083 test_ns
```

## Programmatic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog/hive"
)

func main() {
    ctx := context.Background()

    // Create catalog
    props := iceberg.Properties{
        hive.URI:       "thrift://localhost:9083",
        hive.Warehouse: "/tmp/iceberg-warehouse",
    }

    cat, err := hive.NewCatalog(props)
    if err != nil {
        log.Fatal(err)
    }
    defer cat.Close()

    // List namespaces
    namespaces, err := cat.ListNamespaces(ctx, nil)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Namespaces:")
    for _, ns := range namespaces {
        fmt.Printf("  - %v\n", ns)
    }

    // Create a namespace
    err = cat.CreateNamespace(ctx, hive.DatabaseIdentifier("my_db"), iceberg.Properties{
        "location": "/tmp/iceberg-warehouse/my_db",
        "comment":  "My test database",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Create a table
    schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
        iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
        iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
    )

    tbl, err := cat.CreateTable(ctx, hive.TableIdentifier("my_db", "my_table"), schema)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Created table: %s\n", tbl.Identifier())
}
```

## Configuration Properties

| Property | Description | Default |
|----------|-------------|---------|
| `uri` | Thrift URI for Hive Metastore (e.g., `thrift://localhost:9083`) | Required |
| `warehouse` | Default warehouse location for tables | - |
| `hive.kerberos-authentication` | Enable Kerberos authentication | `false` |
