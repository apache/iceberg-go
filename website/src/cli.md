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

# CLI

Run `go build ./cmd/iceberg` from the root of this repository to build the CLI executable, alternately you can run `go install github.com/apache/iceberg-go/cmd/iceberg` to install it to the `bin` directory of your `GOPATH`.

The `iceberg` CLI usage is very similar to [pyiceberg CLI](https://py.iceberg.apache.org/cli/). \
You can pass the catalog URI with the `--uri` argument.

## Connecting to a catalog

Start a local REST catalog (the default `--catalog` type):
```
docker pull apache/iceberg-rest-fixture:latest
docker run -p 8181:8181 apache/iceberg-rest-fixture:latest
```
and run the `iceberg` CLI pointing to the REST API server:
```
 ./iceberg --uri http://0.0.0.0:8181 list
┌─────┐
| IDs |
| --- |
└─────┘
```

Catalog connection flags are global and apply to every subcommand:

| Flag | Description |
| ---- | ----------- |
| `--catalog` | Catalog type: `rest` (default), `glue`, `hive`, `hadoop` |
| `--uri` | Catalog URI (REST/Hive) |
| `--warehouse` | Warehouse location |
| `--credential` | Credentials for the catalog |
| `--token` | OAuth token (skips OAuth flow) |
| `--scope` | OAuth scope (default `catalog`) |
| `--catalog-name` | Catalog name to load from config file (default `default`) |
| `--config` | Path to a config file |
| `--aws-profile` | AWS named profile to use (Glue catalog); overrides `aws-profile` in the config file |

To avoid passing flags every time, define a config file at `~/.iceberg-go.yaml`:
```yaml
default-catalog: default
catalog:
  default:
    type: rest
    uri: http://localhost:8181
    warehouse: s3://my-warehouse
```
Flags on the command line override values from the config file.

## Output format

All commands accept `--output text` (default, human-readable) or `--output json` (machine-readable, suitable for piping into `jq` or scripts).

## Catalog and namespace commands

**Create namespace**
```
./iceberg --uri http://0.0.0.0:8181 create namespace taxitrips
```

**List namespaces**
```
 ./iceberg --uri http://0.0.0.0:8181 list
┌───────────┐
| IDs       |
| --------- |
| taxitrips |
└───────────┘
```

**Create table**

Note: only the identity transform is supported for `--partition-spec` at this moment.
```shell
# Create a simple table with REST catalog and Minio
./iceberg create table default.table-1 \
        --properties write.format.default=parquet \
        --partition-spec foo \
        --sort-order foo:desc:nulls-last \
        --schema '[{"id":1,"name":"foo","type":"string","required":false},{"id":2,"name":"bar","type":"int","required":true}]' \
        --catalog rest \
        --uri http://localhost:8181
Table default.table-1 created successfully

# Describe the newly created table
./iceberg describe --catalog rest --uri http://localhost:8181 default.table-1
Table format version | 2
Metadata location    | s3://warehouse/default/table-1/metadata/00000-f0ccaadd-d988-482e-99da-3a37870288fe.metadata.json
Table UUID           | 33fa3fac-e638-4335-a085-343c6d9e7de5
Last updated         | 1753133512562
Sort Order           | 1: [
                     | 1 desc nulls-last
                     | ]
Partition Spec       | [
                     |  1000: foo: identity(1)
                     | ]

Current Schema, id=0
├──1: foo: optional string
└──2: bar: required int

Current Snapshot |

Snapshots

Properties
key                             | value
-----------------------------------------
write.format.default            | parquet
write.parquet.compression-codec | zstd
```

## Inspecting tables

### `info` — single-screen summary

```shell
iceberg info my_db.events
```
Reports format version, location, current snapshot, schema, partition spec, sort order, snapshot count, ref count, and property count for a table.

### `snapshots` — snapshot history

```shell
iceberg snapshots my_db.events
```
Lists all snapshots with timestamp, parent snapshot, operation (`append`, `overwrite`, `delete`, `replace`), and added/deleted data file counts.

### `refs` — branches and tags

```shell
iceberg refs my_db.events
iceberg refs --type branch my_db.events
iceberg refs --type tag    my_db.events
```
Lists snapshot refs along with their retention settings (`max-ref-age`, `max-snapshot-age`, `min-snapshots-to-keep`).

### `partition-stats` — partition statistics files

```shell
iceberg partition-stats my_db.events                          # current snapshot
iceberg partition-stats --snapshot-id 7234981023498 my_db.events
iceberg partition-stats --all my_db.events                    # all snapshots
```

### `schema --show-defaults`

```shell
iceberg schema --show-defaults my_db.events
```
Prints the schema and surfaces each field's `initial-default` and `write-default` — useful when debugging schema-evolution behavior.

## Snapshot maintenance

### `expire-snapshots`

Drop old snapshots so their unreferenced data files become eligible for cleanup.

| Flag | Description |
| ---- | ----------- |
| `--older-than DURATION` | Expire snapshots older than the given duration (`7d`, `168h`) |
| `--retain-last N` | Always keep at least N snapshots, regardless of age |
| `--dry-run` | List what would be expired without committing |
| `--yes` | Skip the confirmation prompt |

```shell
# Preview
iceberg expire-snapshots --older-than 7d --dry-run my_db.events

# Commit, retaining at least 5 snapshots
iceberg expire-snapshots --older-than 7d --retain-last 5 my_db.events
```

### `clean-orphan-files`

Remove data files in the table location that are not referenced by any snapshot's manifests (e.g. left behind by failed writes).

| Flag | Description |
| ---- | ----------- |
| `--older-than DURATION` | Only consider files older than this (default `72h`, gives in-flight writes time to finish) |
| `--location PATH` | Scan a different directory (e.g. an old warehouse path after migration) |
| `--dry-run` | List orphan files without deleting |
| `--yes` | Skip the confirmation prompt |

```shell
iceberg clean-orphan-files --dry-run my_db.events
iceberg clean-orphan-files --older-than 5d my_db.events
iceberg clean-orphan-files --location s3://old-warehouse/my_db/events my_db.events
```

## Rollback

Reset the current snapshot pointer to a previous snapshot. The target must be an ancestor of the current snapshot.

| Flag | Description |
| ---- | ----------- |
| `--snapshot-id ID` | Snapshot to roll back to (required) |
| `--yes` | Skip the confirmation prompt |

```shell
iceberg rollback --snapshot-id 6891234567890 my_db.events
```

## Format upgrade

Upgrade the table format version (metadata-only operation; no data files are rewritten). Refuses downgrades and same-version "upgrades".

| Flag | Description |
| ---- | ----------- |
| `--dry-run` | Show what would change without committing |
| `--yes` | Skip the confirmation prompt |

```shell
iceberg upgrade --dry-run my_db.events 2
iceberg upgrade my_db.events 2
```

## Branches and tags

### `branch create`

```shell
iceberg branch create my_db.events ml-experiment-v3
```

| Flag | Description |
| ---- | ----------- |
| `--snapshot-id ID` | Snapshot the branch points at (default: current snapshot) |
| `--max-ref-age DURATION` | Branch itself expires after this age |
| `--max-snapshot-age DURATION` | Snapshots on the branch older than this can be expired |
| `--min-snapshots-to-keep N` | Always retain at least N snapshots on the branch |
| `--yes` | Skip the confirmation prompt |

```shell
iceberg branch create \
  --snapshot-id 7234981023498 \
  --max-ref-age 30d \
  --max-snapshot-age 7d \
  --min-snapshots-to-keep 10 \
  my_db.events audit-2026-q2
```

### `tag create`

```shell
iceberg tag create my_db.events pre-migration-v4
```

| Flag | Description |
| ---- | ----------- |
| `--snapshot-id ID` | Snapshot the tag points at (default: current snapshot) |
| `--max-ref-age DURATION` | Tag is auto-cleaned after this age |
| `--yes` | Skip the confirmation prompt |

```shell
iceberg tag create \
  --snapshot-id 7234981023498 \
  --max-ref-age 90d \
  my_db.events monthly-backup-may
```

## Automation

Two flags make these commands safe to run from cron jobs or CI:

- `--yes` skips the interactive prompt. Without it in a non-interactive environment, the CLI exits with `stdin is not a terminal: use --yes to confirm in non-interactive mode` rather than hanging.
- `--output json` emits structured output that can be consumed by `jq` and downstream tooling.

Daily maintenance over every table in a namespace:
```bash
#!/bin/bash
TABLES=$(iceberg list my_db --output json | jq -r '.identifiers[].name')
for table in $TABLES; do
  iceberg expire-snapshots --older-than 7d --retain-last 3 --yes \
    --output json "my_db.$table"
  iceberg clean-orphan-files --older-than 3d --yes \
    --output json "my_db.$table"
done
```

Tag tables before a deploy:
```bash
iceberg tag create --yes my_db.events "pre-deploy-$VERSION"
iceberg tag create --yes my_db.users  "pre-deploy-$VERSION"
```

Audit-only report (no commits):
```bash
iceberg expire-snapshots --older-than 7d --dry-run --output json my_db.events \
  | jq '{table, would_expire: .expired_snapshot_count}'
```

## Safety features

Every write command has multiple layers of protection:

- **`--dry-run`** — shows the would-be effect without committing. Look for `[DRY RUN]` in text output or `"dry_run": true` in JSON.
- **`--yes`** — required to skip the prompt; without it, non-interactive shells get an explicit error rather than hanging.
- **TTY detection** — interactive prompts are only shown when stdout is a terminal.
- **Ancestor validation** — `rollback` rejects target snapshots that are not in the current branch's history.
- **Version check** — `upgrade` refuses same-version or downgrade requests.