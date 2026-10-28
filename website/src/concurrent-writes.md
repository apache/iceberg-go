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

# Concurrent Writes

When multiple writers commit to the same table, iceberg-go uses optimistic
concurrency control: every commit is validated against the table state it was
built on. If another writer committed first, the commit is rejected rather than
silently clobbering their snapshot.

## Conflict detection

Each producer (`Append`, `Overwrite`, `Delete`, `RowDelta`, `RewriteDataFiles`)
runs a set of validators before the commit lands. They check that the files the
operation assumed (added, deleted, or filtered) still match the current table
state. A failed validation surfaces as one of:

- `table.ErrCommitFailed` — the base snapshot moved on; the commit can be
  retried after refreshing (see below).
- `table.ErrCommitDiverged` — terminal. The base snapshot is no longer on the
  branch at all, so a retry cannot reconcile the change.
- `table.ErrTransactionUnusable` — this transaction can't be committed again.
  If the error also matches `ErrCommitFailed`, retry in a new transaction from a
  reloaded table.

```go
import (
    "context"
    "errors"

    "github.com/apache/iceberg-go/table"
)

_, err := txn.Commit(context.Background())
switch {
case errors.Is(err, table.ErrTransactionUnusable) && errors.Is(err, table.ErrCommitFailed):
    // a concurrent change: retry in a new transaction from a reloaded table
case errors.Is(err, table.ErrCommitDiverged), errors.Is(err, table.ErrTransactionUnusable):
    // unrecoverable: rebuild the operation from the latest table
case errors.Is(err, table.ErrCommitFailed):
    // retriable: refresh and try again (the retry loop does this for you)
}
```

## Isolation levels

Delete and update operations are validated under an isolation level, set per
operation through table properties:

| Property                        | Default        | Values                      |
|:--------------------------------|:---------------|:----------------------------|
| `write.delete.isolation-level`  | `serializable` | `serializable`, `snapshot`  |
| `write.update.isolation-level`  | `serializable` | `serializable`, `snapshot`  |

`serializable` rejects the commit if *any* concurrent snapshot added data
matching the operation's filter. `snapshot` is more permissive — it only
rejects when concurrent deletes touch the same files.

## Automatic retry

iceberg-go can refresh the table and replay the operation against the latest
snapshot between attempts. On each retry the table metadata is reloaded, the
producer's validators re-run against the fresh snapshot, and the commit is
re-submitted only if it is still valid. `RollbackToSnapshot` and
`AssertRefSnapshotID` pin their refs, so retries do not rebase those assertions.
If a pinned ref moved, the commit fails with `ErrCommitFailed` instead of
retrying. `ExpireSnapshots` does not pin refs. Retries replan it as described in
[Snapshot expiration](#snapshot-expiration).

Retry is **off by default** (`commit.retry.num-retries` is `0`). Opt in by
setting the retry properties — see [Manifest and commit](./configuration.md#manifest-and-commit)
in the configuration reference for the full list:

```go
// Opt in to retries (e.g. 4 attempts after the first) for a contended table.
_, err := cat.CreateTable(ctx, ident, schema,
    catalog.WithProperties(iceberg.Properties{
        "commit.retry.num-retries": "4",
    }),
)
```

> **Catalog support:** the REST, SQL, Glue, Hive, and Hadoop catalogs report
> commit conflicts, including requirements that fail because of a concurrent
> change, as `ErrCommitFailed`, so the retry loop engages on all of them. A
> commit on another branch that takes the staged snapshot's sequence number or
> first-row-id is a conflict too, and the retry renumbers the snapshot.

Only the implicit assertion on the commit branch's head is rebased after a
refresh. Other requirements, including the schema, spec, sort order, and pinned
refs, must still hold. Otherwise, the commit fails without another attempt and
the transaction is unusable.

After a conflict, calling `Commit` again on the same transaction refreshes the
table and replays the staged changes like the next retry attempt, so it can
rebase onto a peer's commit even with retries disabled. Each call gets the
configured retry budget. A commit carrying delete-file removals that cannot be
replayed resubmits only if its branch has not moved. The rejections after a
refresh that make the transaction unusable are listed on
`ErrTransactionUnusable`.

A failed commit keeps the files it staged until a later commit lands. A
transaction you abandon leaves them for orphan file removal, as one with
retries disabled always has. An abandoned manifest rewrite also leaves the
manifests that later attempts merged. `TableCommit` refuses a transaction whose
commit failed, since the multi-table path does not refresh. Call `Commit` to
resume it, or build a new transaction.

### Snapshot expiration

A retry replays staged changes in order against the refreshed table. It replans
each `ExpireSnapshots` call at its original position, with the original options
and cutoff time. The plan retains snapshots that a concurrent commit made
reachable, such as rollback targets and new branch heads. It can expire
snapshots that a concurrent append moved outside the retention window. A
transaction that appends and expires snapshots rebases onto a concurrent append
like any other append.

Each attempt requires the refs that the plans read to be unchanged since
loading or refreshing the table. If a concurrent commit moves one of those
refs before the attempt succeeds, it causes another conflict. The next attempt
replans the expiry. If a concurrent commit creates a ref that the plan did not
read, that ref does not cause a conflict. Expiring its snapshot also removes
the ref.

- Retries replan an expiry even if staging or an earlier attempt produced no
  expiry updates.
- If no updates remain, for example because a concurrent commit already expired
  the same snapshots, `Commit` returns the refreshed table without committing.
- A transaction containing only expiries with no updates makes no commit attempt
  and never refreshes or replans.
- Post-commit cleanup deletes files for snapshots expired by the successful
  attempt, if cleanup is enabled.

A retry fails with `ErrTransactionUnusable` in these cases:

- Replanning changes whether a non-main commit branch is removed. Later staged
  changes depend on the original outcome. The error also matches
  `ErrCommitFailed`, because a new transaction from the refreshed table can plan
  consistently.
- GC is disabled after refresh. The error does not match `ErrCommitFailed`.
  A new transaction fails when staging the expiry too.
- A staged change cannot apply after refresh, such as a ref update to a snapshot
  that a concurrent commit expired. The error does not match `ErrCommitFailed`.
  The same operations fail again without new input.
