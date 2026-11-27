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

This reference follows the development branch of Apache Iceberg Go. New to the project? Walk through the [Getting Started](./getting-started.md) tutorial first - the recipes here assume you already have a catalog and a table.

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
if err != nil { panic(err) }
defer cat.Close()
```

#### SQL (SQLite, Postgres, MySQL, Oracle, MSSQL)

```go
import (
    "context"
    "database/sql"

    "github.com/apache/iceberg-go"
    sqlcat "github.com/apache/iceberg-go/catalog/sql"
    "github.com/uptrace/bun/driver/sqliteshim"
)

db, err := sql.Open(sqliteshim.ShimName, "file:catalog.db")
if err != nil { panic(err) }
defer db.Close()
cat, err := sqlcat.NewCatalog(context.Background(), "default", db, sqlcat.SQLite, iceberg.Properties{
    "warehouse": "file:///tmp/warehouse",
})
if err != nil { panic(err) }
defer cat.Close()
```

#### Glue

```go
import (
    "context"

    "github.com/apache/iceberg-go/catalog/glue"
    "github.com/aws/aws-sdk-go-v2/config"
)

awsCfg, err := config.LoadDefaultConfig(context.TODO())
if err != nil { panic(err) }
cat, err := glue.NewCatalog(glue.WithAWSConfig(awsCfg))
if err != nil { panic(err) }
defer cat.Close()
```

#### Hive

```go
import (
    "context"

    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog/hive"
)

cat, err := hive.NewCatalog(context.Background(), iceberg.Properties{},
    hive.WithURI("thrift://localhost:9083"),
    hive.WithWarehouse("s3://my-bucket/warehouse"))
if err != nil { panic(err) }
defer cat.Close()
```

#### Hadoop

```go
import (
    "context"

    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog/hadoop"
)

cat, err := hadoop.NewCatalog(context.Background(), "default", "file:///tmp/warehouse", iceberg.Properties{})
if err != nil { panic(err) }
defer cat.Close()
```

#### Via the registry

`catalog.Load` looks up the right backend via the `type` property (or `uri` scheme as a fallback) plus your `~/.iceberg-go.yaml`. Useful when you want runtime selection:

```go
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog"
    _ "github.com/apache/iceberg-go/catalog/rest"
)

cat, err := catalog.Load(ctx, "default", iceberg.Properties{
    "type":      "rest",
    "uri":       "http://localhost:8181",
    "warehouse": "s3://my-bucket/warehouse",
})
if err != nil { panic(err) }
defer cat.Close()
```

Every catalog implements `Close() error`. Close it when done to release catalog-owned
resources. A caller-supplied database connection remains caller-owned.

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

Use `errors.Is(err, catalog.ErrNamespaceAlreadyExists)` to recognize an
existing namespace when retrying creation.

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

For nested types use `iceberg.NewStructType(fields...)`, `iceberg.NewListType(elementID, element, required)`, or `iceberg.NewMapType(keyID, key, valueID, value, valueRequired)`. Their fields can't be assigned directly: read them through accessors, and to change one, build a new value. Use `NewSchemaWithIdentifiers(id, identifierIDs, fields...)` to mark identifier columns.

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

Use `catalog.As[catalog.RegisterableCatalog](cat)` to discover table
registration support. `RegisterTable(ctx, ident, metadataLocation)`
registers a table from an existing metadata file and returns the table.

For a REST table with catalog-vended storage credentials, discover
`catalog.RefreshableCredentialCatalog` with `catalog.As`. If the server
supports the credentials endpoint, `RefreshTableCredentials(ctx, tbl)`
returns the table to use for subsequent operations. When building a table
yourself with `table.New`, pass `table.WithSavedConfig` so its file I/O
properties are available to the refresh. REST-loaded tables and views also
expose catalog-provided labels through `Labels()`.

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

Parquet and Avro data files are readable. Avro scan tasks must cover the
whole file. The reader decodes the file before projecting columns, and
supports nullable unions with `null` and one value type.

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

### Dictionary-encoded columns

```go
scan := tbl.Scan(table.WithDictionaryColumns("region", "event.name"))
```

`WithDictionaryColumns` returns the named string and binary columns as Arrow dictionary arrays. Requested columns always have type `dictionary<int32, utf8>` or `dictionary<int32, binary>`, even with `arrow.use_large_types`. The dictionary can change from one batch to the next, so don't rely on dictionary identity across batches. Filtering on a requested column decodes the dictionary for each batch to evaluate the predicate, so a filtered scan may not be faster.

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

### Incremental changelog scans

`NewIncrementalChangelogScan` returns rows added or deleted between snapshots. `PlanFiles` returns the tasks. `Read` reads a planned task list, and `ToArrowRecords` plans and reads. Remote planning is not supported.

```go
schema, batches, err := tbl.NewIncrementalChangelogScan(
    table.WithSelectedFields("id", "name"),
).FromSnapshotExclusive(startSnapshotID).ToArrowRecords(ctx)
```

`FromSnapshotInclusive`, `FromSnapshotExclusive`, and `ToSnapshot` bound the snapshot range. Projection, row filters, and limits apply while the rows are read. If the start snapshot has been expired, changes in expired snapshots before the oldest retained one are not reported. Each snapshot that adds deletes is planned against every live data file, narrowed only by the partition filter, so the cost follows the table size rather than the size of the snapshot range.

Each batch is the scan projection followed by three columns:

- `_change_type` — `INSERT` or `DELETE`
- `_change_ordinal` — int32 ordinal of the snapshot that produced the change
- `_commit_snapshot_id` — snapshot ID that committed the change

Added data files are inserts. Removed data files are deletes of the rows in that file. Delete files added in the range emit the rows they newly remove. A rewritten file emits a DELETE and an INSERT for every row in that file, including rows that did not change. `UPDATE` rows are not produced. Delete-file handling is iceberg-go behavior.

Use `ToNormalizedArrowRecords` to remove matching DELETE/INSERT occurrences of unchanged rows within each snapshot. Set `ComputeUpdates` to label remaining rows with the same identifier as `UPDATE_BEFORE` and `UPDATE_AFTER`:

```go
schema, batches, err := tbl.NewIncrementalChangelogScan(
    table.WithSelectedFields("id", "name"),
).FromSnapshotExclusive(startSnapshotID).ToNormalizedArrowRecords(ctx,
    table.ChangelogNormalizationOptions{
        ComputeUpdates:   true,
        IdentifierFields: []string{"id"},
    })
```

The zero options value only removes carryovers. Update images use `IdentifierFields`, or the effective schema's identifier fields when the list is empty. Explicit identifiers may be nullable primitive columns, including paths through structs, and follow the scan's case sensitivity. After removing carryovers, an identifier must have at most one DELETE and one INSERT per snapshot. Duplicate identifiers return an error instead of selecting an ambiguous update image.

Normalization compares complete rows before applying row filters, projection, and the output limit. Row-lineage columns do not affect matching. Variant values are compared by their encoded metadata/value bytes, with physical NULL distinct from an encoded Variant null. Changes from different snapshots are kept separate. Output follows snapshot and source-task order, with each UPDATE_AFTER immediately following its UPDATE_BEFORE. A batch boundary or row limit may split the pair. Callers release each returned batch.

The reader buffers one snapshot at a time. `MaxBufferedBytes` defaults to 64 MiB of retained Arrow buffers and encoded matching keys, and `MaxBufferedRows` defaults to 1,000,000 input rows per snapshot. Exceeding either limit returns `ErrChangelogBufferLimit` through the iterator before emitting that snapshot. Decoding, temporary key encoding, output batches, and per-row indexes need additional memory. Temporary keys are individually capped at `MaxBufferedBytes`. `WithArrowBatchSize` also caps output batch rows; the default output batch size is 4096.

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

### Dynamic partition overwrite

Replace only the partitions present in the incoming data. Unlike a filtered
row-level overwrite, this removes **all existing data files** in those partitions
while retaining files in other partitions.

```go
// Write Arrow record batches, replacing only partitions receiving new rows.
newTbl, err := tbl.DynamicPartitionOverwrite(ctx, recordReader, nil)

// In-memory Arrow tables work too.
newTbl, err = tbl.DynamicPartitionOverwriteTable(ctx, arrowTbl, 1024, nil)

// Or commit already-written Iceberg DataFile metadata through a transaction.
txn := tbl.NewTransaction()
err = txn.DynamicPartitionOverwriteDataFiles(ctx, newDataFiles, nil)
if err == nil {
    newTbl, err = txn.Commit(ctx)
}
```

This supports Iceberg format v1-v3. Incoming files must use the current
partition spec. On partitioned tables, files under older specs are preserved;
only matching current-spec partitions are replaced. If the current spec is
unpartitioned, non-empty input replaces all live files, including files under
older specs. For v3, replacement also removes deletion vectors tied to
replaced data files. Empty input leaves the table untouched.

A concurrent add or removal in a replaced partition fails validation, while
independent changes to other partitions can be retried after a catalog refresh.
A new target branch also pins the source branch to prevent missing concurrent
writes. Default partition-spec and sort-order changes fail validation. Written
data files are not automatically cleaned up if staging or commit fails, so
callers must handle those files before retrying with a new transaction.

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
err := txn.AddDataFilesByPath(ctx, []string{
    "s3://my-bucket/sales/orders/data/file-1.parquet",
    "s3://my-bucket/sales/orders/data/file-2.parquet",
}, nil /* snapshot props */, false /* ignoreDuplicates */)
if err != nil { /* ... */ }

newTbl, err := txn.Commit(ctx)
```

For bucket-partitioned Parquet files, provide each file's partition tuple
with `WithAddDataFilesByPathPartitionValues`. Key the values by file path
and partition field ID. Supply every non-void partition field, and make
sure the values describe every row in the file.

`ReplaceDataFilesByPath(ctx, filesToDelete, filesToAdd, snapshotProps)` performs an
**overwrite** of existing files. It is suitable when the logical table data
changes. `ReplaceDataFiles(ctx, filesToDelete, dataFilesToAdd, snapshotProps, opts...)`
is the corresponding data-file metadata API.

### Stage externally rewritten Parquet files

When another engine has **already written** replacement Parquet files with the
same live rows and physical record count, use
`StageRewrittenDataFilesByPath`. This stages an Iceberg `replace` snapshot
instead of an `overwrite`, but it does not read or rewrite the row data:

```go
txn := tbl.NewTransaction()
err := txn.StageRewrittenDataFilesByPath(ctx, oldFilePaths, newFilePaths, nil)
if err != nil { /* ... */ }
newTbl, err := txn.Commit(ctx)
```

This path-only helper supports unpartitioned format v1 and v2 tables. It rejects
a source file with any applicable position or equality deletes, as well as
deletion vectors. It requires the same total physical record count and checks
column value, null, and NaN counts when available for every file on both sides.
Missing optional statistics are unknown, not zero. A source column with known
value counts cannot disappear when replacement metrics collection is enabled.
Footer metadata cannot prove row-level equivalence, so callers must preserve
every live row. Format v3 and partitioned tables are not supported, including
older partitioned specs that are no longer current.

For compaction that must **apply deletes**, remove delete files, or rewrite
partitioned data, use `RewriteDataFiles`. An external writer that needs to
supply data-file metadata, a sequence number, or related delete-file changes
can use `NewRewrite` with `FileToDataFile` instead.

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
txn, err := tbl.NewTransactionOnBranch("staging")
if err != nil { /* ... */ }
```

Cherry-pick an existing snapshot onto the transaction's target branch:

```go
err := txn.CherryPickSnapshot(ctx, sourceSnapshotID, nil)
if err != nil { /* ... */ }
newTbl, err := txn.Commit(ctx)
```

A direct-child source fast-forwards without copying data and ignores
`snapshotProps`. A divergent v1/v2 append is replayed with fresh snapshot
metadata; user-supplied WAP publication markers are not trusted. Divergent v3
appends and divergent replace-partitions overwrites are tracked in
[issue #2415](https://github.com/apache/iceberg-go/issues/2415). Cherry-picks
pin the refs they rely on. Divergent replays onto new branches also pin main;
fast-forwards onto new branches do not. A moved pinned ref fails the commit;
other conflicts can be retried under the normal commit retry policy.

`Commit` retries a conflict (`ErrCommitFailed`) up to `commit.retry.num-retries` times, 4 by default. See [Automatic retry](./concurrent-writes.md#automatic-retry) for the `commit.retry.*` table properties.

`txn.SetLocation(location)` stages a new table base location in the same
storage scheme and authority, including the bucket or container where
applicable. Existing files keep their paths. New writes use the staged base
and honor explicit write-path properties. Commit the transaction to publish
the change.

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

### Sort order evolution

Use the transaction builder to replace the full default sort order. It binds column names against the transaction's staged schema. An empty builder resets the order to unsorted:

~~~go
txn := tbl.NewTransaction()
builder := txn.NewReplaceSortOrder(true).
    Asc("event_time", table.NullsLast).
    AddField("user_id", iceberg.BucketTransform{NumBuckets: 16}, table.SortASC, table.NullsFirst)
if err := builder.Commit(); err != nil { /* ... */ }

newTbl, err := txn.Commit(ctx)
~~~

Call `Transaction.ReplaceSortOrder` directly when you already have a `table.SortOrder`, such as one built from field IDs:

~~~go
order, err := table.NewSortOrder(1, []table.SortField{{
    SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
    Direction: table.SortASC, NullOrder: table.NullsFirst,
}})
if err != nil { /* ... */ }
if err := txn.ReplaceSortOrder(order); err != nil { /* ... */ }
~~~

Replacing the default sort order affects future writes; it does not rewrite existing data files. To physically reorder existing data, run a separate sort compaction.

Go writers sort each batch they write by the default sort order. Batches are sorted independently, so written files are not sorted as a whole and record no `sort_order_id`; run a sort compaction to sort and tag them. Sort columns may be nested in structs, where a null struct sorts as a null value. Floats sort as the spec orders them (`-NaN < -Inf < -0 < 0 < Inf < NaN`).

Use `SortOrder.Satisfies(required)` to compare sort metadata using Iceberg's
transform ordering rules.

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

### Comparing physical files between snapshots (v1-v3)

Use `tbl.DiffSnapshotFiles` to compare the live content files
referenced by two snapshot IDs, including snapshots on different branches:

```go
diff, err := tbl.DiffSnapshotFiles(ctx, fromSnapshotID, toSnapshotID)
if err != nil {
    return err
}
for _, file := range diff.AddedDataFiles {
    fmt.Println("added:", file.FilePath())
}
```

The result also includes removed data files and added/removed delete files,
including positional deletes, equality deletes, and v3 deletion vectors.
Shared immutable manifests are skipped. Entries are read with a minimal
projection, so returned files do not contain complete column statistics and
must **not** be passed to a `ManifestWriter`. This compares **physical files**,
not logical row changes.

See the [Iceberg snapshot specification](https://iceberg.apache.org/spec/#snapshots)
and [manifest specification](https://iceberg.apache.org/spec/#manifests).

### Branches and tags

To create a branch or tag at an existing snapshot, use `branch create` or
`tag create` in the [CLI](./cli.md). In Go, call `Catalog.CommitTable` with
`table.NewSetSnapshotRefUpdate` to choose the starting snapshot and any retention
settings. This example uses the table's current snapshot:

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

To create a branch with its first append, target it with
`tbl.NewTransactionOnBranch`. The append starts from `main` if the branch is
missing and creates the branch on commit. An append to an existing branch
builds on that branch's head:

```go
txn, err := tbl.NewTransactionOnBranch("audit")
if err != nil { /* ... */ }
if err := txn.AppendTable(ctx, arrowTbl, 1024, nil); err != nil { /* ... */ }
updated, err := txn.Commit(ctx)
```

See [reading from a branch or tag](#reading-from-a-branch-or-tag) to scan
the updated ref.

### Branch promotion and replacement

Use a transaction to advance a branch to another branch or tag, or to move it
to an existing snapshot. Fast-forward checks that the target's current snapshot
is an ancestor of the source. It creates the target when missing, while
`ReplaceBranch` requires an existing target branch.

```go
txn, err := tbl.NewTransactionOnBranch("main")
if err != nil { /* ... */ }
if err := txn.FastForwardBranch("main", "audit"); err != nil { /* ... */ }
updated, err := txn.Commit(ctx)

// Without ancestry checks (can move a branch backwards or across histories):
txn, err = updated.NewTransactionOnBranch("main")
if err != nil { /* ... */ }
err = txn.ReplaceBranch("main", snapshotID)
// Or use ReplaceBranchFromRef("main", "audit"), which can create a missing target.
```

For a non-main target, call `tbl.NewTransactionOnBranch(target)` rather than
`tbl.NewTransaction()`. The latter uses `main` as its commit branch, so a
concurrent append to `main` can reject an otherwise independent promotion.

Existing branch retention settings are preserved. Both the target and source
snapshot IDs are pinned when promoting from a ref, and the target is pinned
when replacing by ID. If the source snapshot was staged in the same transaction,
the commit branch is pinned too, preventing retry from changing its ancestry.
A pinned ref that moves prevents the normal retry of a co-staged append. A retention-only change with the same snapshot ID cannot
be detected by snapshot-ID requirements in any catalog. REST catalogs also
cannot detect ref type changes with the same ID.

Fast-forward needs the complete snapshot ancestry. REST catalogs using
`snapshot-loading-mode=refs` can omit intermediate ancestors, causing valid
fast-forwards to fail. Load with `snapshot-loading-mode=all` instead.
Replacing by snapshot ID can also fail if the requested historical snapshot
was omitted. See [Concurrent Writes](./concurrent-writes.md).

`tbl.RenameBranch(ctx, from, to)` renames a non-main branch to an unused
name and preserves its snapshot and retention settings. Use
`txn.RenameBranch(from, to)` to stage the rename alongside other updates.

### Expiration and rollback

```go
// Expire snapshots older than the table's retention properties
err := txn.ExpireSnapshots(/* options */)

// Roll back to a previous snapshot
err = txn.RollbackToSnapshot(targetSnapshotID)
```

`txn.RollbackToTime(timestampMs)` selects the latest reachable ancestor
strictly before the given Unix timestamp in milliseconds.

Tune retention with the `history.expire.min-snapshots-to-keep`, `history.expire.max-snapshot-age-ms`, and `history.expire.max-ref-age-ms` table properties (see [Configuration](./configuration.md)).

### Snapshot integrity audit (v1 and later)

Use `ValidateSnapshot` to inspect a snapshot's manifest list and entries without
changing table data. This is especially useful for checking imported v1 tables
before allowing them into production.

```go
report, err := tbl.ValidateSnapshot(ctx, snapshotID)
if err != nil {
    return err // I/O failed or the scan was canceled; report may be incomplete.
}
if !report.Valid() {
    for _, issue := range report.Issues {
        fmt.Printf("%s: %s (%s)\n", issue.Code, issue.FilePath, issue.ManifestPath)
    }
}
```

The report checks manifest versions, partition spec references, content types,
manifest entry counts, duplicate manifest paths, and duplicate live content files
across **all** manifests in a snapshot. It also checks that live deletion vectors
have a target path, that their blob ranges fit within the recorded file size,
and that no two distinct vectors target the same data file.
Deleted entries do not count as live.
A report is valid only when `Complete` is true and `Issues` is empty.
This is a metadata-only audit: it does not check data-file contents or whether
data-file objects still exist. The audit works with both v1 manifest lists and
older v1 snapshots containing embedded `manifests` paths. For embedded
manifests, the partition spec ID is checked against table metadata when the
optional header provides it, or inferred when the partition fields match exactly
one historical spec. Missing, ambiguous, or mismatched partition specs are
reported as issues rather than silently accepted.

The file uniqueness and manifest requirements come from the
[Iceberg spec](https://iceberg.apache.org/spec/#content-file-uniqueness)
and [manifest list spec](https://iceberg.apache.org/spec/#manifest-lists).

## Maintenance

### Partition statistics (v1-v3)

`ComputePartitionStatistics` writes a Parquet statistics file for the transaction's
current branch head and stages its registration. Use a fresh transaction and
commit it to publish the reference:

```go
txn := tbl.NewTransaction()
stats, err := txn.ComputePartitionStatistics(ctx)
if err != nil { /* ... */ }
newTbl, err := txn.Commit(ctx)
```

The operation recomputes statistics from live manifest entries without reading
data files, delete-file contents, or Puffin blobs. It groups rows by partition
spec ID and partition tuple and sorts by the unified tuple with NULL first.
V2 includes position and equality delete metrics. V3 also counts deletion vectors
in `dv_count`; their records contribute to `position_delete_record_count`, while
`position_delete_file_count` counts classic files only, including V2 files
retained after an upgrade.

`total_record_count` is exact for tables without deletes and for DV-only tables
whose live targets, partitions, sequence numbers, and record counts can be
validated from metadata. A live classic position or equality delete makes this
field NULL for every partition, since deletes may apply across specs. Unknown DV
sequence numbers also leave the affected total NULL. Optional last-updated
fields are left NULL. `PartitionStatisticsSchema` exposes the versioned schema;
`PartitionStatisticsSchemaV1` remains available.

The table must have a snapshot and at least one partition field in its spec
history. Dropped partition source fields and changed transform result types are
not supported. A concurrent change to the target branch, current schema, or
partition layout rejects the commit. Statistics do not create a new snapshot.

### Puffin statistics

`puffin.Writer.AddBlob` accepts `BlobMetadataInput.CompressionCodec` with
`CompressionCodecLZ4` or `CompressionCodecZstd`. `puffin.Reader.ReadBlob`
decodes the stored codec. Write deletion-vector blobs with an empty codec.

`tbl.ComputeNDVs(ctx, snapshotID, columns...)` scans the selected snapshot and
returns estimated distinct counts and serialized Theta sketches keyed by
Iceberg field ID. Pass `0` for the current snapshot. Omit the column names to
analyze all supported top-level primitive columns in the snapshot schema.
Geometry, geography, and unknown types are skipped by default. Explicit column
names must refer to supported top-level primitive columns. The estimates
exclude null values and empty strings and binary values.

Use `table/stats.NewSketch` to build a sketch directly from serialized Iceberg
single values, and `NewUnion` to merge compatible sketches. The serialized
sketch is the payload for an `apache-datasketches-theta-v1` Puffin blob.

After writing a Puffin statistics file, pass its snapshot ID, path, sizes,
and blob metadata to `txn.SetStatistics`. Commit to publish the reference.
`txn.RemoveStatistics(snapshotID)` stages removal of the reference.

### Delete-file maintenance

- `RewritePositionDeleteFiles` compacts compatible v2 Parquet position-delete
  files from the current partition spec that have no deleted-row payload.
- `ConvertEqualityDeletesToDeletionVectors` rewrites live equality deletes
  on a v3 table as file-specific deletion vectors.
- `PlanDanglingDeleteFiles` identifies whole delete files that apply to no
  data file in the target snapshot. `RemoveDanglingDeleteFiles` removes their
  references on v2 and v3. The physical files remain available to older
  snapshots that reference them.

The table-level methods operate on `main`. Use the transaction variants to
target another branch or combine changes, then commit the transaction.

### Orphan file cleanup

```go
import (
    "time"

    "github.com/apache/iceberg-go/table/maintenance"
)

result, err := maintenance.DeleteOrphanFiles(ctx, tbl,
    maintenance.WithCleanupFilesOlderThan(72*time.Hour),
    maintenance.WithCleanupDryRun(false),
    maintenance.WithCleanupMaxConcurrency(8),
)
if err != nil { /* ... */ }
fmt.Printf("removed %d files\n", len(result.DeletedFiles))
```

Also see `maintenance.WithCleanupLocation`, `maintenance.WithCleanupDeleteFunc`, `maintenance.WithPrefixMismatchMode`, `maintenance.WithEqualSchemes`, and `maintenance.WithEqualAuthorities` in `table/maintenance/orphan_cleanup.go`.

Use `maintenance.PlanOrphanFiles` to inspect candidates before passing the
plan to `maintenance.ExecuteOrphanCleanup`. Execution deletes exactly the
planned files without rescanning. `maintenance.DeleteOrphanFiles` combines
planning and deletion and also supports dry runs. The default minimum file
age is 72 hours.

**Safety:** All three orphan-cleanup entry points, including dry runs, reject
`gc.enabled=false` and tables loaded by REST with
`snapshot-loading-mode=refs`. Refs-only metadata can omit snapshots whose
files are still live. Both checks run before filesystem access. A refs-only
error matches `table.ErrInvalidOperation` through `errors.Is`.

Create the REST catalog with `snapshot-loading-mode=all` (or omit the
property), for example using `rest.WithAdditionalProps`, then reload the
table before cleanup. A one-time `LoadTableWithFullSnapshotHistory` load is
enough for planning and dry runs. Deletion refreshes the table using the
catalog's configured mode, so its regular loads must also return full
history. A server override that forces `refs` prevents deletion.
`maintenance.PurgeFiles`, used when dropping a table, is not subject to
this restriction; best-effort purge may miss files outside the table
location that were omitted from refs-only metadata.

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

Compaction tasks represent complete data files, not independent scan splits. The planner collapses split tasks for the same file into one whole-file task, rejecting inconsistent delete or lineage metadata and dropping residual filters because they select files but do not filter rows during compaction. Direct calls to `ExecuteCompactionGroup` reject partial ranges and residual row filters. Attached delete files are still applied when rewriting each source file.

To sort instead of bin-pack, pass `table.WithCompactionSortOrder` as a group option. Each group is sorted as a whole, so every output file holds a sorted, non-overlapping range of it. Output files record the order's `sort_order_id` when the order is one of the table's sort orders, every field has one source column, every field but the last is identity and the last preserves order (see `table.WithCompactionSortOrder`). Files of different groups still overlap, and by default a group is packed to one target file, so raise `compaction.Config.MaxGroupSizeBytes` until a group covers a partition. Set `compaction.Config.RewriteAll` to also rewrite files that are already well-sized:

```go
cfg := compaction.DefaultConfig()
cfg.RewriteAll = true
cfg.MaxGroupSizeBytes = 8 << 30 // 8 GiB per group
plan, err := compaction.Analyze(ctx, tbl, cfg)
if err != nil { /* ... */ }

result, err := txn.RewriteDataFiles(ctx, groups /* from plan.Groups */, table.RewriteDataFilesOptions{
    GroupOptions: []table.CompactionGroupOption{table.WithCompactionSortOrder(tbl.SortOrder())},
})
```

When every file of a group already records the order's `sort_order_id` (for example, files written by an earlier sort rewrite), every sort field is identity, and no partition of the group has more files than the scan concurrency (`table.WithCompactionScanConcurrency`, `GOMAXPROCS` by default), the files are streamed and merged, holding up to about five batches per file instead of the whole group. Otherwise a sort rewrite holds each group decoded in memory, and with the batches queued for the writer that can reach about twice the group's decoded size, so keep `compaction.Config.MaxGroupSizeBytes` within what one rewrite can hold. The first sort rewrite of a table always takes this in-memory path, since its files do not record the order yet. The CLI equivalent is `iceberg compact run --sort [--rewrite-all] [--max-group-size BYTES]`, which uses the table's default sort order.

To cluster spatially instead, pass `table.WithCompactionGeometryHilbertOrder` with the name of a top-level geometry or geography column. Each partition of a group is ordered along a Hilbert curve over the centre of every geometry's XY bounding box, snapped to a grid of `2^bits` cells per axis (`table.WithHilbertBits`, 16 by default) over the extent of the data (`table.WithHilbertBounds` fixes the extent). Rows close in space tend to share the same file and row group, so per-file geometry bounds overlap little and spatial filters prune files. Rows with no location (a null or empty geometry) go last. Geography is accepted but ordered on planar x/y, with no antimeridian wrapping. This is not the multi-column `hilbert(c1, c2)` sort order of Java's rewrite procedure; it orders one geometry column by location. A Hilbert rewrite always holds the whole group decoded in memory, so keep `compaction.Config.MaxGroupSizeBytes` within what one rewrite can hold. The option cannot be combined with `table.WithCompactionSortOrder`, and because a Hilbert order is not an Iceberg sort order, output files record no `sort_order_id`:

```go
result, err := txn.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{
    GroupOptions: []table.CompactionGroupOption{
        table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(12)),
    },
})
```

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

Snapshot expiration needs complete ancestry to honor branch retention and snapshot
references. When a REST table was loaded with `snapshot-loading-mode=refs`,
expiration requests `snapshots=all` for the transaction's metadata revision
and keeps any updates already staged in the transaction. Use
`Transaction.ExpireSnapshotsWithContext(ctx, ...)` to control that request.

If the catalog cannot supply complete history or the metadata revision has
changed, staging returns an error matching `table.ErrInvalidOperation`.
Reload the table and start a new transaction if the revision changed.
Commit retries and resumed commits also request full history before replanning
expiration. If the catalog still returns refs-only history, the commit stops
before resubmitting and returns an error matching `table.ErrTransactionUnusable`.

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
    view.Representation{SQL: "SELECT month, sum(amount) FROM orders GROUP BY month", Dialect: "spark"},
    table.Identifier{"sales"},                      // default namespace for unqualified names
    "s3://my-bucket/views/monthly_orders",
    iceberg.Properties{},
)

// Inspect
v.CurrentVersion()        // *view.Version
v.CurrentSchema()         // *iceberg.Schema
v.Versions()              // []*view.Version
v.VersionByID(1)          // *view.Version, or nil
v.VersionAsOf(timestampMs) // *view.Version, or nil (Unix milliseconds)
v.History()               // []view.VersionLogEntry
v.Schemas()               // map[int]*iceberg.Schema
v.Properties()            // iceberg.Properties
```

`view.New(ident, meta, metadataLocation)` constructs a view from already-loaded metadata; `view.NewFromLocation(ctx, ident, metadataLocation, fsysFactory)` loads metadata from disk or object storage.

Pass `view.WithIORegistry(registry)` to `view.CreateView`,
`view.CreateViewWithIOProperties`, `view.CreateViewFromVersion`, or
`view.WriteMetadata` to use isolated FileIO. For `view.NewFromLocation`,
pass a filesystem factory from the registry. The IO-properties helpers
keep filesystem configuration (`ioProps`) separate from persisted view
properties (`metadataProps`). See
[FileIO configuration](./configuration.md#file-system-credentials).

Discover view updates with `catalog.As[catalog.UpdatableViewCatalog](cat)`.
`catalog.RollbackView(ctx, cat, ident, versionID)` selects a retained version
on a catalog that supports both loading and updating views.

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
| `variant` | `arrow.ExtensionType` for Variant (see [Variant Type](./variant.md)) |

Helpers in [`table/arrow_utils.go`](https://github.com/apache/iceberg-go/blob/main/table/arrow_utils.go):

- `SchemaToArrowSchema(sc *iceberg.Schema, nameMapping NameMapping, useLargeTypes, includeRowLineage bool) (*arrow.Schema, error)`
- `VisitArrowSchema[T](sc *arrow.Schema, visitor ArrowSchemaVisitor[T]) (T, error)`

For a writer-side schema (Arrow → Iceberg), the scanner and writers handle conversion automatically as long as your Arrow schema is compatible with the table schema.
