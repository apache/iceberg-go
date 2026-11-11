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
cat := glue.NewCatalog(glue.WithAWSConfig(awsCfg))
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
    "context"
)

cat, err := hadoop.NewCatalog(context.Background(), "default", "file:///tmp/warehouse", iceberg.Properties{})
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

The Go writers currently support only top-level sort columns. A nested-field order can be registered, as in Java, but a later append fails until the default order is replaced.

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

Use `tbl.Inspect().DiffSnapshotFiles` to compare the live content files
referenced by two snapshot IDs, including snapshots on different branches:

```go
diff, err := tbl.Inspect().DiffSnapshotFiles(ctx, fromSnapshotID, toSnapshotID)
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

### Expiration and rollback

```go
// Expire snapshots older than the table's retention properties
err := txn.ExpireSnapshots(/* options */)

// Roll back to a previous snapshot
err = txn.RollbackToSnapshot(targetSnapshotID)
```

Tune retention with the `history.expire.min-snapshots-to-keep`, `history.expire.max-snapshot-age-ms`, and `history.expire.max-ref-age-ms` table properties (see [Configuration](./configuration.md)).

### Snapshot integrity audit (v1 and later)

Use `ValidateSnapshot` to inspect a snapshot's manifest list and entries without
changing table data. This is especially useful for checking imported v1 tables
before allowing them into production.

```go
report, err := tbl.Inspect().ValidateSnapshot(ctx, snapshotID)
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
