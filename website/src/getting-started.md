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

# Getting Started

This walkthrough takes you from `go get` to a fully written-and-read Iceberg table in a few minutes, using a local SQLite-backed catalog so you do not need any external services. By the end you will have:

- An Iceberg catalog stored in a SQLite file
- A table with an Arrow schema
- Data written from Apache Arrow Go and read back
- A schema evolution committed
- A branch created on top of the table

If you would rather see all the operations as a reference, jump straight to the [API](./api.md).

## 1. Install

```sh
go mod init iceberg-go-tutorial
go get github.com/apache/iceberg-go@latest
go get github.com/apache/arrow-go/v18@latest
go get github.com/uptrace/bun/driver/sqliteshim@latest
```

`iceberg-go` itself only registers the local file system. For S3, GCS, or Azure Blob you would also blank-import `github.com/apache/iceberg-go/io/gocloud`. We are staying on local disk for this tutorial.

## 2. Open a local catalog

The SQL catalog stores its metadata in a database, and the actual data files live under a "warehouse" directory. We will use SQLite for both the catalog DB and a local folder for the warehouse.

```go
package main

import (
    "context"
    "database/sql"
    "log"

    "github.com/apache/iceberg-go"
    sqlcat "github.com/apache/iceberg-go/catalog/sql"
    "github.com/uptrace/bun/driver/sqliteshim"
)

func openCatalog(ctx context.Context) (*sqlcat.Catalog, error) {
    db, err := sql.Open(sqliteshim.ShimName, "file:./tutorial-catalog.db?cache=shared")
    if err != nil {
        return nil, err
    }
    return sqlcat.NewCatalog("default", db, sqlcat.SQLite, iceberg.Properties{
        "warehouse":            "file:///tmp/iceberg-tutorial",
        "init_catalog_tables":  "true",
    })
}
```

`init_catalog_tables: "true"` lets the catalog create its bookkeeping tables (`iceberg_tables`, `iceberg_namespace_properties`) on first use. Make sure `/tmp/iceberg-tutorial` is writable.

## 3. Create a namespace and a table

Iceberg organizes tables under namespaces (the analog of databases). We will create one and put a table inside it.

```go
import (
    "github.com/apache/iceberg-go/catalog"
    "github.com/apache/iceberg-go/table"
)

func createTrips(ctx context.Context, cat *sqlcat.Catalog) (*table.Table, error) {
    ns := table.Identifier{"taxi"}
    if err := cat.CreateNamespace(ctx, ns, nil); err != nil {
        return nil, err
    }

    schema := iceberg.NewSchema(1,
        iceberg.NestedField{ID: 1, Name: "trip_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
        iceberg.NestedField{ID: 2, Name: "fare", Type: iceberg.PrimitiveTypes.Float64, Required: false},
        iceberg.NestedField{ID: 3, Name: "borough", Type: iceberg.PrimitiveTypes.String, Required: false},
    )

    ident := catalog.ToIdentifier("taxi", "trips")
    return cat.CreateTable(ctx, ident, schema)
}
```

If the namespace already exists, `CreateNamespace` returns an error - the example skips error handling for brevity; production code should check for `iceberg.ErrAlreadyExists` and recover.

## 4. Write some Arrow data

The write path takes either a streaming [`array.RecordReader`](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow/array#RecordReader) or a fully materialized [`arrow.Table`](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow#Table). We will build a small Arrow table inline and append it.

```go
import (
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func writeSomeTrips(ctx context.Context, tbl *table.Table) (*table.Table, error) {
    mem := memory.NewGoAllocator()

    arrowSchema := arrow.NewSchema([]arrow.Field{
        {Name: "trip_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "fare", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
        {Name: "borough", Type: arrow.BinaryTypes.String, Nullable: true},
    }, nil)

    b := array.NewRecordBuilder(mem, arrowSchema)
    defer b.Release()

    b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
    b.Field(1).(*array.Float64Builder).AppendValues([]float64{12.50, 8.75, 22.10}, nil)
    b.Field(2).(*array.StringBuilder).AppendValues([]string{"Manhattan", "Brooklyn", "Queens"}, nil)

    rec := b.NewRecord()
    defer rec.Release()

    arrowTbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
    defer arrowTbl.Release()

    return tbl.AppendTable(ctx, arrowTbl, 1024 /* batchSize */, nil)
}
```

`AppendTable` returns a refreshed `*table.Table` reflecting the new snapshot.

## 5. Read the data back

```go
func readAll(ctx context.Context, tbl *table.Table) error {
    arrowTbl, err := tbl.Scan().ToArrowTable(ctx)
    if err != nil {
        return err
    }
    defer arrowTbl.Release()

    log.Printf("read %d rows in %d columns", arrowTbl.NumRows(), arrowTbl.NumCols())
    return nil
}
```

For larger tables, use streaming so only one batch is in memory at a time:

```go
arrowSchema, batches, err := tbl.Scan().ToArrowRecords(ctx)
if err != nil {
    return err
}
log.Printf("scan schema: %s", arrowSchema)

for batch, err := range batches {
    if err != nil {
        return err
    }
    log.Printf("batch with %d rows", batch.NumRows())
    batch.Release()
}
```

## 6. Filter and project

Use the [predicate DSL](./row-filter-syntax.md) to push filters down into the scan, and `WithSelectedFields` to project columns:

```go
filter := iceberg.GreaterThan(iceberg.Reference("fare"), float64(10.0))

arrowTbl, err := tbl.Scan(
    table.WithSelectedFields("trip_id", "fare"),
    table.WithRowFilter(filter),
).ToArrowTable(ctx)
```

## 7. Evolve the schema

Most schema changes go through a transaction. Add a `tip` column:

```go
import "github.com/apache/iceberg-go/table"

func addTipColumn(ctx context.Context, tbl *table.Table) (*table.Table, error) {
    txn := tbl.NewTransaction()

    err := table.NewUpdateSchema(txn, true /* caseSensitive */, false /* allowIncompatible */).
        AddColumn([]string{"tip"}, iceberg.PrimitiveTypes.Float64, "Tip in dollars", false, nil).
        Commit()
    if err != nil {
        return nil, err
    }

    return txn.Commit(ctx)
}
```

The returned table has the new schema and a fresh metadata file.

## 8. Branch the table

A branch is a named ref that points at a snapshot. Creating one requires a metadata commit that adds the ref - `NewTransactionOnBranch` only writes to a branch that already exists. Two steps:

```go
import "github.com/apache/iceberg-go/table"

// Step 1: create the branch via Catalog.CommitTable.
func createExperimentBranch(ctx context.Context, cat *sqlcat.Catalog, tbl *table.Table) (*table.Table, error) {
    snap := tbl.CurrentSnapshot()
    update := table.NewSetSnapshotRefUpdate(
        "experiment", snap.SnapshotID, table.BranchRef,
        0 /* maxRefAgeMs */, 0 /* maxSnapshotAgeMs */, 0 /* minSnapshotsToKeep */,
    )
    reqs := []table.Requirement{
        table.AssertTableUUID(tbl.Metadata().TableUUID()),
        table.AssertRefSnapshotID("experiment", nil), // branch must not yet exist
    }
    if _, _, err := cat.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{update}); err != nil {
        return nil, err
    }
    // Reload so subsequent transactions see the new ref.
    return cat.LoadTable(ctx, tbl.Identifier())
}

// Step 2: open a transaction on the branch and write.
func writeOnBranch(ctx context.Context, tbl *table.Table, arrowTbl arrow.Table) (*table.Table, error) {
    txn := tbl.NewTransactionOnBranch("experiment")
    if err := txn.AppendTable(ctx, arrowTbl, 1024, nil); err != nil {
        return nil, err
    }
    return txn.Commit(ctx)
}
```

Reads can target the branch with `tbl.Scan().UseRef("experiment")`.

## Where to go next

- [API](./api.md) - the full Go surface for catalogs, tables, scans, writes, transactions, schema and partition evolution, snapshot management, maintenance, and views.
- [Configuration](./configuration.md) - cloud credentials, table properties, concurrency, custom catalog and IO registration.
- [CLI](./cli.md) - inspect, expire, compact, branch, and tag from the command line.
- [Row Filter Syntax](./row-filter-syntax.md) and [Expression DSL](./expression-dsl.md) - the predicate DSL in detail.

The full Iceberg specification, terminology, and engine integration policy live with the main project at [iceberg.apache.org](https://iceberg.apache.org/).
