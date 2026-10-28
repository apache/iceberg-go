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

# Releases

Apache Iceberg Go follows the standard Apache release process. Releases are cut from `main`, voted on by the Apache Iceberg PMC, and published as signed source tarballs to `https://downloads.apache.org/iceberg/` and as Go module versions tagged `vX.Y.Z`.

## Latest release

- [Latest release on GitHub](https://github.com/apache/iceberg-go/releases/latest)
- All releases: [github.com/apache/iceberg-go/releases](https://github.com/apache/iceberg-go/releases)

## Using a release

Pin a tagged version directly with Go modules:

```sh
go get github.com/apache/iceberg-go@vX.Y.Z
```

To track `main`, use `@main` instead of a version tag.

## Release notes

Per-release notes (highlights, breaking changes, contributors) are published on the [GitHub Releases page](https://github.com/apache/iceberg-go/releases). Iceberg Go does not maintain a curated `CHANGELOG.md` in the repository; the GitHub Releases page is the canonical source.

### Unreleased compatibility notes

- **Catalog capabilities split into narrower interfaces.** `catalog.Catalog` now embeds `TableCatalog` and `NamespaceCatalog` (the existing table and namespace method signatures are unchanged). All `Catalog` implementations must add `Name() string` to report their configured name; this is a breaking change for third-party catalogs. `ViewCatalog` is optional and currently implemented by REST and Hive. Future catalog capabilities should use optional interfaces rather than extending `Catalog`.

- **Path-based data-file addition renamed.** Replace `Transaction.AddFiles` with `AddDataFilesByPath`, `AddFilesOption` with `AddDataFilesByPathOption`, and `WithAddFilesConcurrency` with `WithAddDataFilesByPathConcurrency`. Behavior is unchanged.
- **Manifest builder constructor renamed.** Replace `iceberg.NewManifestFile(...)` with `iceberg.NewManifestBuilder(...)`. It still returns a `ManifestBuilder`.
- **Manifest sequence APIs use explicit names.** `ManifestBuilder.SequenceNum` and `ManifestFile.SequenceNum` become `SequenceNumber`, and `ManifestFile.MinSequenceNum` becomes `MinSequenceNumber`. `ManifestEntryBuilder.SequenceNum` and `ManifestEntry.SequenceNum` become `DataSequenceNumber`. `FileSequenceNum` becomes `FileSequenceNumber` for entries and their builder. Avro and JSON field names are unchanged.
- **View and UDF metadata builder constructors are infallible.** `view.NewMetadataBuilder` and `udf.NewMetadataBuilder` now return only a builder instead of `(builder, error)`; remove error handling at call sites.
- **Table metadata getter renamed.** Replace `table.Metadata.Version()` with `table.Metadata.FormatVersion()`, consistent with Iceberg Java and Go view/UDF metadata. Metadata JSON is unchanged.
- **Snapshot ref type field renamed.** Replace `table.SnapshotRef.SnapshotRefType` with `table.SnapshotRef.Type`. The JSON field is still `"type"`.
- **As-of snapshot lookups now use inclusive timestamps.** Replace `tbl.SnapshotAsOf(timestampMs, true)` with `tbl.SnapshotAsOf(timestampMs)`. The method always selects the latest snapshot at or before the timestamp, matching Java Iceberg's `SnapshotUtil.nullableSnapshotIdAsOfTime`. For strict-before lookup, use `SnapshotLogEntryAsOf(tbl.Metadata().SnapshotLogs(), timestampMs, false)`.
- **Snapshot timestamp property key corrected.** `PropertyCurrentSnapshotTimestamp` now uses `current-snapshot-timestamp-ms`. The old `current-snapshot-timestamp` spelling is no longer reserved and remains usable as a custom property.
- **FileIO open and remove take a context.** `io.IO.Open` and `io.IO.Remove` now take a `context.Context` first argument, so custom `io.IO` implementations must add it. `maintenance.WithCleanupDeleteFunc` now takes `func(context.Context, string) error`.
- **Hadoop catalog construction takes a context.** `hadoop.NewCatalog(name, warehouse, props)` becomes `hadoop.NewCatalog(ctx, name, warehouse, props)`. The context bounds filesystem setup; the catalog's filesystem keeps its values but not its cancellation.
- **Config loading reports errors.** `config.LoadConfig` now returns `([]byte, error)`, and `config.LoadConfigFile` is removed; call `LoadConfig` instead. A missing default `~/.iceberg-go.yaml` still returns no data and no error.
- **Branch transactions return initialization errors.** `Table.NewTransactionOnBranch(branch)` now returns `(*Transaction, error)`, and `Table.NewTransactionOnBranchWithError` is removed. Use `NewTransactionOnBranch` and handle its error.
- **Schema JSON constructor uses the Go initialism.** Replace `iceberg.NewSchemaFromJsonFields` with `iceberg.NewSchemaFromJSONFields`.
- **AWS identifiers use the Go initialism.** `glue.AwsProperties`, `glue.WithAwsConfig` and `glue.WithAwsProperties` become `glue.AWSProperties`, `glue.WithAWSConfig` and `glue.WithAWSProperties`; `sigv4.WithAwsConfig` becomes `sigv4.WithAWSConfig`; `utils.WithAwsConfig` and `utils.GetAwsConfig` become `utils.WithAWSConfig` and `utils.GetAWSConfig`; and `config.CatalogConfig.AwsProfile` becomes `AWSProfile`. The `aws-profile` YAML key is unchanged.
- **KMS factory registration now takes a context.** Change `encryption.KMSFactory` implementations to accept `(context.Context, map[string]string)` instead of only properties, and call `encryption.LoadKeyManagementClient(ctx, props)` instead of `LoadKeyManagementClient(props)`. The existing global `RegisterKMS`, `UnregisterKMS`, and `GetRegisteredKMSNames` functions still use the default registry; `RegisterKMS` still panics for invalid registrations. Use `encryption.MustRegisterKMS` for provider initialization. For isolated registrations, create `registry := encryption.NewKMSRegistry()` after importing any KMS providers and call `registry.Register`, `registry.Load(ctx, props)`, `registry.Names`, or `registry.Unregister`. New registries snapshot the global registrations; subsequent changes do not propagate.
- **Name-mapping child lookup renamed.** Replace `MappedField.GetField(name)` with `MappedField.FindFieldByName(name)`, which returns `(*MappedField, bool)`. The new method returns a pointer to the actual child field rather than a copy, and reports a missing name with `false`.
- **Data file block size setter removed.** `iceberg.DataFileBuilder.BlockSizeInBytes` is removed. New v1 manifests use the Avro schema's 64 MiB default when the field is unset. V2/v3 omit this deprecated field, and legacy v1 values remain readable.
- **View creation APIs take a named SQL representation.** `view.NewVersionFromSQL`, `view.CreateView`, `view.CreateViewWithIOProperties`, and `sql.Catalog.CreateView` now accept a `view.Representation{SQL: sql, Dialect: dialect}` instead of positional SQL/dialect strings. The dialect is required; Hive preserves the selected representation's dialect.
- **View SQL representation construction uses named fields.** Replace `view.NewRepresentation(sql, dialect)` with `view.NewRepresentation(view.Representation{SQL: sql, Dialect: dialect})`. Serialized JSON is unchanged.
- **Reserved table properties now use a read-only API.** Replace references to `table.ReservedProperties` with `table.ReservedProperties()`. The function returns a fresh slice, so callers cannot change metadata validation by modifying it.
- **Fixed and decimal constructors return errors.** `iceberg.FixedTypeOf` and `iceberg.DecimalTypeOf` now return `(type, error)` instead of panicking for invalid inputs. Use `MustFixedTypeOf` or `MustDecimalTypeOf` only for known-valid values.
- **SQL UDF representations now use struct literals.** The `udf.NewSQLRepresentation(dialect, sql)` function, available since v0.7.0, is removed. Use `udf.SQLRepresentation{Dialect: "trino", SQL: "x + 1"}` instead. Empty fields and duplicate dialects are checked during metadata validation; serialized JSON is unchanged.
- **Filesystem adapter construction consolidated.** Replace `io.FSPreProcName(fsys, fn)` with `io.FS(fsys, io.WithNamePreprocessor(fn))`. Plain `io.FS(fsys)` remains supported.
- **Table purge is required on catalogs.** The `catalog.Catalog` interface now embeds `catalog.PurgeableCatalog`; every custom catalog implementation must provide `PurgeTable(ctx, identifier) error`. The narrow `catalog.PurgeableTable` interface is renamed to `catalog.PurgeableCatalog`. Built-in purge behavior is unchanged.
- **Unify statistics blob types with Puffin.** Replace `table.BlobType` with `puffin.BlobType` when constructing `table.BlobMetadata`. Replace `table.BlobTypeApacheDatasketchesThetaV1` with `puffin.BlobTypeDataSketchesTheta` and `table.BlobTypeDeletionVectorV1` with `puffin.BlobTypeDeletionVector`. Use `puffin.BlobType.IsKnown()` to identify the types defined by the Puffin specification; the redundant `table.BlobType.IsValid()` method is removed. Unknown non-empty blob type names still round-trip through table metadata.
- **View representation field uses the Go SQL initialism.** Replace `view.Representation.Sql` with `view.Representation.SQL`. Serialized view metadata still uses the `sql` JSON field.
- **Orphan cleanup results now expose only `OrphanFiles`.** Replace `result.OrphanFileLocations` with `result.OrphanFiles` (each entry has `Path` and `SizeBytes`). `OrphanCleanupPlan.Files()` still provides path-only lists during planning.
- **Catalog identifier helper renamed.** `catalog.TableNameFromIdent` is removed. Use `catalog.ObjectNameFromIdent` for tables, views, and functions.
- **Substrait expression conversion no longer takes case sensitivity.** `substrait.ConvertExpr(schema, expression, caseSensitive)` becomes `substrait.ConvertExpr(schema, expression)`. Case sensitivity remains part of expression binding.

- **Transform formatting requires the result type and uses the Java-aligned method name.** `Transform.ToHumanStr(value)` and `Transform.ToHumanStrType(resultType, value)` are removed. Use `Transform.ToHumanString(resultType, value)` to format partition values, including timestamps with time zones.
- **Catalogs now require cleanup.** The `catalog.Catalog` interface includes `Close() error`. Custom catalog implementations must provide `Close`, and callers should close catalog instances after use. `catalog.Closer` is removed. Replace `cat.(catalog.Closer)` checks with `defer cat.Close()`; `catalog.Catalog` already implements `io.Closer`, so no type assertion is needed.

- **Puffin writer property setter renamed.** `puffin.Writer.AddProperties` is now `puffin.Writer.SetProperties`. Repeated calls still merge keys; use `ClearProperties` to clear custom properties before finishing.

- **Orphan file cleanup moved to `table/maintenance`.** The orphan-cleanup and file-purge API is no longer on `table.Table`; it now lives in the `table/maintenance` package as free functions that take the table as their first argument (matching `table/compaction`). For example, `tbl.DeleteOrphanFiles(ctx, ...)` becomes `maintenance.DeleteOrphanFiles(ctx, tbl, ...)`. Because `table/maintenance` imports `table` (and not the reverse), this cannot be offered as a deprecation shim on `table.Table` without an import cycle, so callers must switch to the new package. The following moved from `table` to `table/maintenance`:
    - Functions (now free functions taking `*table.Table`): `DeleteOrphanFiles`, `PlanOrphanFiles`, `ExecuteOrphanCleanup`, `PurgeFiles`.
    - Types: `OrphanCleanupOption`, `OrphanCleanupResult`, `OrphanFile`, `OrphanCleanupPlan`, `PrefixMismatchMode`.
    - Constants: `PrefixMismatchError`, `PrefixMismatchIgnore`, `PrefixMismatchDelete`.
    - Option constructors: `WithCleanupLocation`, `WithCleanupFilesOlderThan`, `WithCleanupDryRun`, `WithCleanupDeleteFunc`, `WithCleanupMaxConcurrency`, `WithPrefixMismatchMode`, `WithEqualSchemes`, `WithEqualAuthorities`. The four generic names (`WithLocation`, `WithFilesOlderThan`, `WithDryRun`, `WithDeleteFunc`) gained the `WithCleanup` prefix for consistency.

    Relatedly, the previously unexported `isGCEnabled` is now `table.IsGCEnabled`, so the new package can gate destructive operations on the `gc.enabled` table property.
- **Data-file replacement methods renamed.** Use `ReplaceDataFilesByPath` for string paths. Use `ReplaceDataFiles` for prebuilt `DataFile` values instead of `ReplaceDataFilesWithDataFiles`.

- `maintenance.PurgeFiles` (previously `Table.PurgeFiles`) now calls `IO.Remove` concurrently, with up to 32 calls in flight, when the filesystem does not implement `BulkRemovableIO`. Custom IO implementations, including those registered with `io.Register`, must make `Remove` safe for concurrent use and synchronize any shared mutable state. The bulk deletion path is unchanged.

- **Partition and sort source accessors renamed.** Replace `PartitionField.SourceID()` and `SortField.SourceID()` with `FirstSourceID()`. Their behavior and serialized fields are unchanged.

- **Row-lineage column predicate renamed.** Use `iceberg.IsRowLineageColumn` instead of `iceberg.IsMetadataColumn` to identify `_row_id` and `_last_updated_sequence_number`. Use `iceberg.IsReservedFieldID` to check the full reserved metadata field ID range.
- **File writes and directory walks now take an operation context.** Update `WriteFileIO` and `ListableIO` implementations and calls to `Create`, `WriteFile`, and `WalkDir` to pass `context.Context`; cloud writes and listings use it for cancellation.

## Verifying and producing releases

If you are validating an RC or cutting a new release, see:

- [Verify a release](./verify-release.md) - what to do when a `[VOTE]` thread is posted on `dev@iceberg.apache.org`.
- [How to release](./how-to-release.md) - PMC/committer process for cutting an RC and publishing.
