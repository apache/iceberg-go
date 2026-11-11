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
- `table.ErrDataFilesMissing` — terminal. Data files that the operation
  references, such as the target of a position delete or deletion vector, are
  not live, so retrying the same delete cannot succeed. Rebuild the delete
  against the current table. When `Transaction.Commit` returns it, the error
  also matches `ErrTransactionUnusable`.
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
case errors.Is(err, table.ErrCommitRejected):
    // the catalog wrote nothing: fix the cause, then call Commit again
}
```

A commit can also fail with `table.ErrCommitRejected`, which means the catalog
rejected it before writing anything, for example an update the catalog can't
apply or a REST 400, 401, 403 or 404. The commit is not retried, and the
transaction can still be committed.

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

On each retry, iceberg-go reloads the table metadata, applies staged changes
in order and runs the producer's validators against the fresh snapshot. It
submits the commit only if it is still valid. An append staged after a rollback
is rebuilt on the rollback target.

`RollbackToSnapshot`, `AssertRefSnapshotID`, and `CherryPickSnapshot` pin the
refs they guard. If a pinned ref moves, the commit fails instead of rewriting
that assertion to the new head. Cherry-picks can retry table-level conflicts
when their pinned refs stay unchanged. A divergent replay onto a new branch
also pins main; a fast-forward onto a missing branch points directly to the
source snapshot and does not depend on main.

Each replayed snapshot from a staged write gets the current timestamp, or one
millisecond after its parent if that is later. Its timestamp reflects the
attempt that commits it, not staging time.

`RollbackToSnapshot`, `AssertRefSnapshotID`, `FastForwardBranch`,
`ReplaceBranchFromRef`, and `ReplaceBranch` pin the refs they guard.
Promoting from a ref pins both the source and target. If the source points to
a snapshot staged in the same transaction, promotion also pins the commit
branch, preventing retry from rebuilding that source on a different history.
If a pinned ref moves, the commit fails instead of rebasing a co-staged append.
`ExpireSnapshots` does not pin refs. Retries replan it as described in
[Snapshot expiration](#snapshot-expiration).

`Commit` retries a conflict up to 4 times by default (`commit.retry.num-retries`
is `4`, as in Java and PyIceberg). Set it to `0` to fail on the first conflict;
see [Manifest and commit](./configuration.md#manifest-and-commit) in the
configuration reference for the other retry properties:

```go
// Fail on the first conflict instead of retrying.
_, err := cat.CreateTable(ctx, ident, schema,
    catalog.WithProperties(iceberg.Properties{
        "commit.retry.num-retries": "0",
    }),
)
```

> **Catalog support:** the REST, SQL, Glue, Hive, and Hadoop catalogs report
> commit conflicts, including requirements that fail because of a concurrent
> change, as `ErrCommitFailed`, so the retry loop engages on all of them. A
> commit on another branch that takes the staged snapshot's sequence number or
> first-row-id is a conflict too, and the retry renumbers the snapshot.

Snapshot-ID assertions cannot detect concurrent retention-only edits.
REST catalogs also cannot detect a ref type change that leaves its snapshot ID
unchanged. Avoid editing ref retention concurrently with branch promotion.

Only the implicit assertion on the commit branch's head is rebased after a
refresh. Other requirements, including the schema, spec, sort order, and pinned
refs, must still hold. Otherwise, the commit fails without another attempt and
the transaction is unusable.

After a conflict, calling `Commit` again refreshes the table and replays staged
changes even with retries disabled. Each call gets the configured retry
budget. A commit that removes delete files and cannot be replayed resubmits
only if its branch has not moved.

A staged change that cannot apply to the refreshed table makes the transaction
unusable. For example, a staged format upgrade cannot apply if a peer has
already committed a higher format version. The error keeps its cause, so it
also matches `ErrCommitFailed` when the cause is a conflict. See
`ErrTransactionUnusable` for all conditions that end the transaction after
refresh.

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

Each attempt requires the refs read by expiries that remove snapshots or refs
to be unchanged since loading or refreshing the table. An expiry that removes
nothing adds no such requirement. If a concurrent commit moves one of those
refs before the attempt succeeds, it causes another conflict. The next attempt
replans the expiry. If a concurrent commit creates a ref that the plan did not
read, that ref does not cause a conflict. Expiring its snapshot also removes
the ref.

- Retries replan an expiry even if staging or an earlier attempt produced no
  expiry updates.
- A retry stamps snapshots staged earlier in the transaction after the expiry's
  cutoff, so the retry keeps them even if the staged plan expired them.
- If no updates remain, for example because a concurrent commit already expired
  the same snapshots, `Commit` returns the refreshed table without committing.
- A transaction containing only expiries with no updates makes no commit attempt
  and never refreshes or replans.
- Post-commit cleanup deletes files for snapshots expired by the successful
  attempt, if cleanup is enabled.

A retry fails with `ErrTransactionUnusable` if GC is disabled after refresh, a
staged change no longer applies, or replanning changes whether a non-main
commit branch is removed. `ErrTransactionUnusable` and `ErrCommitFailed` list
these cases and which of them a new transaction can retry.
