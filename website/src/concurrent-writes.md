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

```go
import (
    "context"
    "errors"

    "github.com/apache/iceberg-go/table"
)

_, err := txn.Commit(context.Background())
switch {
case errors.Is(err, table.ErrCommitDiverged):
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
re-submitted only if it is still valid.

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

> **Catalog support:** retry currently engages on the REST catalog, which wraps
> commit conflicts as `ErrCommitFailed`. The Glue, SQL, and Hive catalogs do
> not yet wrap their conflict errors, so the retry loop will not fire on them.
