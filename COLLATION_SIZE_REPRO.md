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

# Collation bounds: manifest size reproduction

Measures how much manifest space multi-version collation-aware bounds cost under
each candidate on-disk layout, so the layout choice in the Iceberg collation spec
can be made on measured numbers.

## Branches

| branch | layout |
|---|---|
| `prototype/collation-support` | provisional standalone `data_file.collation_stats` map (keeps the prototype working; not a proposal) |
| `collation-size/inline` | inline in the column's stats: `min_code_point`, `max_code_point`, `collation_bounds: [{version, lower_bound, upper_bound}]` (spec PR apache/iceberg#16972) |
| `collation-size/derived` | derived `COLLATE(field, collation, version)` fields with synthetic field IDs, bounds stored in the existing `lower_bounds` / `upper_bounds` maps |

The layout is defined only in `collation_layout.go` and
`internal/collation_layout_schema.go`. Everything else, including the harness in
`cmd/collationsize`, is identical on all branches:

```sh
git diff collation-size/inline collation-size/derived --stat
# only collation_layout.go and internal/collation_layout_schema.go differ
```

## Run

```sh
git checkout collation-size/inline  && go run ./cmd/collationsize -csv > inline.csv
git checkout collation-size/derived && go run ./cmd/collationsize -csv > derived.csv
go run ./cmd/collationsize -compare inline.csv derived.csv
```

`go run ./cmd/collationsize` without `-csv` prints a table. `-out DIR` keeps every
manifest so it can be inspected with any Avro tool. `-seed`, `-versions`,
`-bound-lens`, `-entries`, `-wide-cols`, and `-wide-versions` change the sweep. Output is
deterministic for a given seed.

## What it does

For each scenario the harness:

1. Builds a table of `max(10, cols)` columns. The first `cols` columns are
   strings collated `en_US-ci`; the rest alternate between `long` and plain
   `string`. Tables wider than 10 columns are fully collated.
2. Builds `entries` data files with `iceberg.NewDataFileBuilder`: column sizes,
   value counts, null counts, byte-order lower/upper bounds for every column
   (strings truncated to 16 bytes), and, for each collated column, collation
   bounds for `versions` collation versions computed with
   `iceberg.ComputeCollatedBounds` from 8 sampled values of `B` bytes. With
   `cp=true` it also supplies the min/max code point.
3. Writes them as a v3 manifest with `iceberg.WriteManifest`, the production
   writer: real Avro encoding, real OCF container, deflate codec.
4. Reads the manifest back with `iceberg.ReadManifest` and fails if any
   collated column lost a usable bound for any version, so a layout cannot win
   by not storing the bounds.
5. Reports the OCF size, the header size (Avro schema and table schema, paid
   once per manifest), per-entry compressed and uncompressed body size, and the
   size of any per-table metadata the layout needs outside the manifest (derived
   field definitions).

## Output shape

```
layout: <layout name>   seed: 42   go1.x
cols  ver    B     cp  entries  ocf bytes  header  deflate/entry  raw/entry  table md
   1    1   16   true        1       ....    ....          ....       ....         0
...
```

`-compare A.csv B.csv` prints, per scenario, the total and per-entry A-B
differences in percent.

## Caveats

- Bounds for every version are computed with the one collator Go ships (one CLDR
  version per process); only the version tag differs between them. Real version
  bumps rarely change the min/max, so this is the common case, and it is the same
  for both layouts.
- The inline layout is written in v3 encoding as a map keyed by field ID. A v4
  `content_stats` struct is positional and would not store the key, so the inline
  numbers are slightly pessimistic.
- The derived layout does not store code points (the derived-field proposal does
  not define them). The `cp=false` rows supply no code points to either layout and
  compare equal information.
- The derived layout keeps its synthetic field ID registry in process memory; a
  real implementation stores the definitions in the table schema. Their size is
  reported in the `table md` column.
