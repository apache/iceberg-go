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

# API

The Go API surface for Apache Iceberg Go. New to the project? Walk through the [Getting Started](./getting-started.md) tutorial first - the recipes here assume you already have a catalog and a table.

For configuration knobs (catalog options, FileIO credentials, table properties), see [Configuration](./configuration.md). For predicate construction details, see [Row Filter Syntax](./row-filter-syntax.md) and [Expression DSL](./expression-dsl.md).

## Catalog

`catalog.Catalog` is the entry point for everything: namespaces, tables, views.

### Constructing a catalog

#### REST

```go
import (
    "context"

    "github.com/apache/iceberg-go/catalog/rest"
)

cat, err := rest.NewCatalog(context.Background(), "rest", "http://localhost:8181",
    rest.WithOAuthToken("your-token"))
```

#### SQL (SQLite, Postgres, MySQL, Oracle, MSSQL)

```go
import (
    "database/sql"

    "github.com/apache/iceberg-go"
    sqlcat "github.com/apache/iceberg-go/catalog/sql"
    "github.com/uptrace/bun/driver/sqliteshim"
)

db, err := sql.Open(sqliteshim.ShimName, "file:catalog.db")
// handle err
cat, err := sqlcat.NewCatalog("default", db, sqlcat.SQLite, iceberg.Properties{
    "warehouse": "file:///tmp/warehouse",
})
```

#### Glue

```go
import (
    "github.com/apache/iceberg-go/catalog/glue"
    "github.com/aws/aws-sdk-go-v2/config"
)

awsCfg, err := config.LoadDefaultConfig(context.TODO())
// handle err
cat := glue.NewCatalog(glue.WithAwsConfig(awsCfg))
```

#### Hive

```go
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog/hive"
)

cat, err := hive.NewCatalog(iceberg.Properties{},
    hive.WithURI("thrift://localhost:9083"),
    hive.WithWarehouse("s3://my-bucket/warehouse"))
```

#### Hadoop

```go
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog/hadoop"
)

cat, err := hadoop.NewCatalog("default", "file:///tmp/warehouse", iceberg.Properties{})
```

#### Via the registry

`catalog.Load` looks up the right backend via the `type` property (or `uri` scheme as a fallback) plus your `~/.iceberg-go.yaml`. Useful when you want runtime selection:

```go
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog"
)

cat, err := catalog.Load(ctx, "default", iceberg.Properties{
    "type":      "rest",
    "uri":       "http://localhost:8181",
    "warehouse": "s3://my-bucket/warehouse",
})
```

### Namespaces

```go
ns := table.Identifier{"sales"}

err := cat.CreateNamespace(ctx, ns, iceberg.Properties{"owner": "data-team"})

namespaces, err := cat.ListNamespaces(ctx, nil) // []table.Identifier
exists, err := cat.CheckNamespaceExists(ctx, ns)
props, err := cat.LoadNamespaceProperties(ctx, ns)
summary, err := cat.UpdateNamespaceProperties(ctx, ns,
    []string{"deprecated"}, // removals
    iceberg.Properties{"owner": "platform-team"}, // updates
)
err = cat.DropNamespace(ctx, ns)
```

`table.Identifier` is `[]string`; use `catalog.ToIdentifier("sales", "orders")` (or `catalog.ToIdentifier("sales.orders")`) to build one from string parts.

### Tables

#### Defining a schema

```go
import "github.com/apache/iceberg-go"

schema := iceberg.NewSchema(1,
    iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
    iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
    iceberg.NestedField{ID: 3, Name: "active", Type: iceberg.PrimitiveTypes.Bool, Required: false},
)
```

For nested types use `&iceberg.StructType{...}`, `&iceberg.ListType{...}`, or `&iceberg.MapType{...}`. Use `NewSchemaWithIdentifiers(id, identifierIDs, fields...)` to mark identifier columns.

#### Create

```go
import "github.com/apache/iceberg-go/catalog"

ident := catalog.ToIdentifier("sales", "orders")

tbl, err := cat.CreateTable(ctx, ident, schema,
    catalog.WithLocation("s3://my-bucket/sales/orders"),
    catalog.WithProperties(iceberg.Properties{"owner": "data-team"}),
)
```

Optional `catalog.WithPartitionSpec`, `catalog.WithSortOrder`, and `catalog.WithStagedUpdates` are also available.

#### Load, exists, list, drop, rename

```go
tbl, err := cat.LoadTable(ctx, ident)

exists, err := cat.CheckTableExists(ctx, ident)

for ident, err := range cat.ListTables(ctx, table.Identifier{"sales"}) {
    if err != nil { /* ... */ }
    fmt.Println(ident)
}

err = cat.DropTable(ctx, ident)

renamed, err := cat.RenameTable(ctx,
    catalog.ToIdentifier("sales", "orders"),
    catalog.ToIdentifier("sales", "orders_v2"))
```

`ListTables` returns an `iter.Seq2[table.Identifier, error]` that streams results.

#### Inspecting metadata

```go
tbl.Identifier()         // table.Identifier
tbl.Location()           // string
tbl.MetadataLocation()   // string (path of current metadata.json)
tbl.Metadata()           // table.Metadata
tbl.Schema()             // *iceberg.Schema (current)
tbl.Schemas()            // map[int]*iceberg.Schema
tbl.Spec()               // iceberg.PartitionSpec
tbl.SortOrder()          // table.SortOrder
tbl.Properties()         // iceberg.Properties

if snap := tbl.CurrentSnapshot(); snap != nil {
    fmt.Println(snap.SnapshotID, snap.TimestampMs, snap.Summary)
}

// All snapshots
for _, snap := range tbl.Metadata().Snapshots() {
    fmt.Println(snap.SnapshotID)
}

// Stream all manifest files across all snapshots
for mf, err := range tbl.AllManifests(ctx) {
    if err != nil { /* ... */ }
    fmt.Println(mf.FilePath())
}
```

## Reading data

`(t Table) Scan(opts ...ScanOption) *Scan` returns a scan that you can resolve into Arrow data.

### Streaming record batches

```go
import "github.com/apache/iceberg-go/table"

scan := tbl.Scan()
arrowSchema, batches, err := scan.ToArrowRecords(ctx)
if err != nil { /* ... */ }

fmt.Println(arrowSchema)
for batch, err := range batches {
    if err != nil { /* ... */ }
    fmt.Printf("batch with %d rows\n", batch.NumRows())
    batch.Release()
}
```

`ToArrowRecords` returns `iter.Seq2[arrow.RecordBatch, error]` so only one batch is in memory at a time. Always call `batch.Release()` to free Arrow buffers.

### Materializing as an Arrow Table

```go
arrowTbl, err := tbl.Scan().ToArrowTable(ctx)
if err != nil { /* ... */ }
defer arrowTbl.Release()
fmt.Printf("%d rows in %d cols\n", arrowTbl.NumRows(), arrowTbl.NumCols())
```

### Projection and filters

```go
import "github.com/apache/iceberg-go"

scan := tbl.Scan(
    table.WithSelectedFields("id", "name"),
    table.WithRowFilter(
        iceberg.NewAnd(
            iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(100)),
            iceberg.IsIn(iceberg.Reference("region"), "us-east", "us-west"),
        ),
    ),
    table.WithLimit(1000),
    table.WithCaseSensitive(true),
)
```

For the predicate vocabulary, see [Row Filter Syntax](./row-filter-syntax.md).

### Time travel

```go
// By snapshot ID
scan := tbl.Scan(table.WithSnapshotID(snap.SnapshotID))

// As of a timestamp (milliseconds since epoch)
scan = tbl.Scan(table.WithSnapshotAsOf(time.Now().Add(-24*time.Hour).UnixMilli()))
```

### Reading from a branch or tag

```go
scan, err := tbl.Scan().UseRef("audit-branch")
if err != nil { /* ... */ }

arrowTbl, err := scan.ToArrowTable(ctx)
```

### Iterating tasks for custom processing

If you need finer control (custom file readers, distributed scan planning):

```go
scan := tbl.Scan(table.WithRowFilter(myFilter))
tasks, err := scan.PlanFiles(ctx)
if err != nil { /* ... */ }

arrowSchema, batches, err := scan.ReadTasks(ctx, tasks)
```

## Writing data

The shortcut methods on `Table` open a transaction, perform the write, and commit. Use `NewTransaction` directly when you need to combine multiple operations.

### Append

```go
import (
    "github.com/apache/arrow-go/v18/arrow/array"
)

// From a streaming RecordReader
newTbl, err := tbl.Append(ctx, recordReader, nil /* snapshot props */)

// From an in-memory Arrow Table; batchSize controls the rolling writer
newTbl, err = tbl.AppendTable(ctx, arrowTbl, 1024, nil)
```

### Overwrite

```go
import "github.com/apache/iceberg-go/table"

// Replace all data
newTbl, err := tbl.Overwrite(ctx, recordReader, nil)

// Replace only rows matching a filter
newTbl, err = tbl.Overwrite(ctx, recordReader, nil,
    table.WithOverwriteFilter(
        iceberg.EqualTo(iceberg.Reference("date"), "2026-01-01"),
    ),
)
```

`OverwriteTable` is the `arrow.Table` variant.

### Delete

```go
newTbl, err := tbl.Delete(ctx,
    iceberg.LessThan(iceberg.Reference("id"), int64(100)),
    nil, /* snapshot props */
)
```

### Add existing files

When you already have data files (e.g. produced by another writer), register them without rewriting:

```go
txn := tbl.NewTransaction()
err := txn.AddFiles(ctx, []string{
    "s3://my-bucket/sales/orders/data/file-1.parquet",
    "s3://my-bucket/sales/orders/data/file-2.parquet",
}, nil /* snapshot props */, false /* ignoreDuplicates */)
if err != nil { /* ... */ }

newTbl, err := txn.Commit(ctx)
```

`ReplaceDataFiles(ctx, filesToDelete, filesToAdd, snapshotProps)` and `ReplaceDataFilesWithDataFiles(ctx, filesToDelete, dataFilesToAdd, snapshotProps, opts...)` are also available on `*Transaction` for swapping files atomically.

### Transactions

Group writes and metadata changes into one atomic snapshot:

```go
txn := tbl.NewTransaction()

if err := txn.Delete(ctx,
    iceberg.LessThan(iceberg.Reference("date"), "2026-01-01"), nil); err != nil {
    /* ... */
}
if err := txn.Append(ctx, recordReader, nil); err != nil {
    /* ... */
}
if err := txn.SetProperties(iceberg.Properties{"commit.user": "data-pipeline"}); err != nil {
    /* ... */
}

newTbl, err := txn.Commit(ctx)
```

To target a specific branch:

```go
txn := tbl.NewTransactionOnBranch("staging")
```

`Commit` retries automatically on conflict (`ErrCommitFailed`) - tune via the `commit.retry.*` table properties.

## Schema and partition evolution

### Schema evolution

```go
import "github.com/apache/iceberg-go/table"

txn := tbl.NewTransaction()

err := table.NewUpdateSchema(txn, true /* caseSensitive */, false /* allowIncompatibleChanges */).
    AddColumn([]string{"tip"}, iceberg.PrimitiveTypes.Float64, "Tip in dollars", false, nil).
    RenameColumn([]string{"name"}, "full_name").
    DeleteColumn([]string{"deprecated_field"}).
    Commit()
if err != nil { /* ... */ }

newTbl, err := txn.Commit(ctx)
```

Reorder fields with `MoveFirst`, `MoveBefore`, or `MoveAfter`. Set `allowIncompatibleChanges` to `true` to permit type narrowing or making optional columns required.

### Partition evolution

```go
us := table.NewUpdateSpec(txn, true /* caseSensitive */)
us.AddField("event_time", iceberg.DayTransform{}, "event_day") // sourceColName, transform, partitionFieldName
us.RemoveField("legacy_partition")
if err := us.Commit(); err != nil { /* ... */ }
```

`AddField` chains; `AddIdentity(sourceCol)` is a shortcut for an identity transform; `RenameField(name, newName)` renames an existing partition field.

Available transforms (root `iceberg` package): `IdentityTransform{}`, `YearTransform{}`, `MonthTransform{}`, `DayTransform{}`, `HourTransform{}`, `BucketTransform{NumBuckets: N}`, `TruncateTransform{Width: W}`.

## Snapshots and refs

### Inspecting

```go
if snap := tbl.CurrentSnapshot(); snap != nil {
    fmt.Println(snap.SnapshotID, snap.TimestampMs, snap.Summary)
}
snap := tbl.SnapshotByID(snapshotID)
named := tbl.SnapshotByName("audit")

for _, s := range tbl.Metadata().Snapshots() {
    fmt.Println(s.SnapshotID, s.Summary)
}
```

### Branches and tags

The CLI's `branch create` and `tag create` commands ([CLI](./cli.md)) are the most ergonomic surface today. Programmatically, ref creation goes through `Catalog.CommitTable` with a `SetSnapshotRef` update:

```go
import "github.com/apache/iceberg-go/table"

snap := tbl.CurrentSnapshot()
update := table.NewSetSnapshotRefUpdate(
    "audit",                  // ref name
    snap.SnapshotID,
    table.BranchRef,          // or table.TagRef
    0,                        // maxRefAgeMs (0 = unset)
    0,                        // maxSnapshotAgeMs (0 = unset)
    0,                        // minSnapshotsToKeep (0 = unset)
)
reqs := []table.Requirement{
    table.AssertTableUUID(tbl.Metadata().TableUUID()),
    table.AssertRefSnapshotID("audit", nil), // ref must not already exist
}

_, _, err := cat.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{update})
```

Constants `table.MainBranch`, `table.BranchRef`, `table.TagRef` live in [`table/refs.go`](https://github.com/apache/iceberg-go/blob/main/table/refs.go). A higher-level builder is on the roadmap.

### Expiration and rollback

```go
// Expire snapshots older than the table's retention properties
err := txn.ExpireSnapshots(/* options */)

// Roll back to a previous snapshot
err = txn.RollbackToSnapshot(targetSnapshotID)
```

Tune retention with the `min-snapshots-to-keep`, `max-snapshot-age-ms`, and `max-ref-age-ms` table properties (see [Configuration](./configuration.md)).

## Maintenance

### Orphan file cleanup

```go
import (
    "time"

    "github.com/apache/iceberg-go/table"
)

result, err := tbl.DeleteOrphanFiles(ctx,
    table.WithFilesOlderThan(72*time.Hour),
    table.WithDryRun(false),
    table.WithMaxConcurrency(8),
)
if err != nil { /* ... */ }
fmt.Printf("removed %d files\n", len(result.DeletedFiles))
```

Also see `table.WithLocation`, `table.WithDeleteFunc`, `table.WithPrefixMismatchMode`, `table.WithEqualSchemes`, and `table.WithEqualAuthorities` in `table/orphan_cleanup.go`.

### Compaction (rewrite data files)

```go
import "github.com/apache/iceberg-go/table"

txn := tbl.NewTransaction()
result, err := txn.RewriteDataFiles(ctx, groups /* []table.CompactionTaskGroup */, table.RewriteDataFilesOptions{})
if err != nil { /* ... */ }

newTbl, err := txn.Commit(ctx)
fmt.Printf("rewrote %d files into %d (%d -> %d bytes)\n",
    result.RemovedDataFiles, result.AddedDataFiles, result.BytesBefore, result.BytesAfter)
```

The `table/compaction` subpackage provides bin-packing planning. The `iceberg compact analyze` and `compact run` CLI commands wrap the same machinery - see [CLI](./cli.md).

### Expiring snapshots

```go
import (
    "time"

    "github.com/apache/iceberg-go/table"
)

txn := tbl.NewTransaction()
err := txn.ExpireSnapshots(
    table.WithOlderThan(7*24*time.Hour),
    table.WithRetainLast(10),
)
if err != nil { /* ... */ }

newTbl, err := txn.Commit(ctx)
```

Pass `table.WithPostCommit(true)` to delete the unreferenced data and metadata files after the commit lands. The `iceberg expire-snapshots` CLI command wraps the same operation - see [CLI](./cli.md).

## Views

Views are created and loaded through catalogs that support them (REST, Hive, SQL):

```go
import "github.com/apache/iceberg-go/view"

// Create
v, err := view.CreateView(
    ctx,
    "my-catalog",
    table.Identifier{"analytics", "monthly_orders"},
    schema,
    "SELECT month, sum(amount) FROM orders GROUP BY month",
    table.Identifier{"sales"},                      // default namespace for unqualified names
    "s3://my-bucket/views/monthly_orders",
    iceberg.Properties{},
)

// Inspect
v.CurrentVersion()      // *view.Version
v.CurrentSchema()       // *iceberg.Schema
v.Versions()            // []*view.Version
v.Schemas()             // map[int]*iceberg.Schema
v.Properties()          // iceberg.Properties
```

`view.New(ident, meta, metadataLocation)` constructs a view from already-loaded metadata; `view.NewFromLocation(ctx, ident, metadataLocation, fsysFactory)` loads metadata from disk or object storage.

## Iceberg ↔ Arrow types

When iceberg-go converts an Iceberg schema to Arrow (e.g. for the scanner output) or vice versa, the type mapping is:

| Iceberg type | Arrow type |
|---|---|
| `boolean` | `arrow.FixedWidthTypes.Boolean` |
| `int` | `arrow.PrimitiveTypes.Int32` |
| `long` | `arrow.PrimitiveTypes.Int64` |
| `float` | `arrow.PrimitiveTypes.Float32` |
| `double` | `arrow.PrimitiveTypes.Float64` |
| `decimal(p, s)` | `arrow.Decimal128Type{Precision: p, Scale: s}` |
| `date` | `arrow.FixedWidthTypes.Date32` |
| `time` | `arrow.FixedWidthTypes.Time64us` |
| `timestamp` | `&arrow.TimestampType{Unit: arrow.Microsecond}` (no zone) |
| `timestamptz` | `arrow.FixedWidthTypes.Timestamp_us` (`UTC` zone) |
| `timestamp_ns` | `&arrow.TimestampType{Unit: arrow.Nanosecond}` (no zone) |
| `timestamptz_ns` | `arrow.FixedWidthTypes.Timestamp_ns` (`UTC` zone) |
| `string` | `arrow.BinaryTypes.String` |
| `binary` | `arrow.BinaryTypes.Binary` |
| `fixed[L]` | `&arrow.FixedSizeBinaryType{ByteWidth: L}` |
| `uuid` | `arrow.FixedWidthTypes.UUID` (extension type) |
| `struct<...>` | `arrow.StructOf(...)` |
| `list<E>` | `arrow.ListOf(E)` (or `LargeListOf` if `useLargeTypes`) |
| `map<K, V>` | `arrow.MapOf(K, V)` |
| `variant` | `arrow.ExtensionType` for Variant |

Helpers in [`table/arrow_utils.go`](https://github.com/apache/iceberg-go/blob/main/table/arrow_utils.go):

- `SchemaToArrowSchema(sc *iceberg.Schema, nameMapping NameMapping, useLargeTypes, includeRowLineage bool) (*arrow.Schema, error)`
- `VisitArrowSchema[T](sc *arrow.Schema, visitor ArrowSchemaVisitor[T]) (T, error)`

For a writer-side schema (Arrow → Iceberg), the scanner and writers handle conversion automatically as long as your Arrow schema is compatible with the table schema.
