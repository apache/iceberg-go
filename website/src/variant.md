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

# Variant Type

`variant` is the Iceberg v3 semi-structured type: a single self-describing column
that holds arbitrary JSON-like values (objects, arrays, and scalars) without a
fixed schema. iceberg-go supports both non-shredded and shredded variants, and can
push predicates down into individual variant fields.

## Reading and writing

A variant column is declared with `iceberg.VariantType{}` in the table schema and
maps to the Arrow `variant` extension type (see [API](./api.md)). Non-shredded
variant values need no extra configuration: write an Arrow record whose variant
column uses the extension type, and read it back the same way.

Build the column with a variant builder and write it like any other Arrow column
(the table must be format version 3):

```go
import (
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/extensions"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/apache/arrow-go/v18/parquet/variant"
)

// tbl was created with table.PropertyFormatVersion = "3" and this schema.
schema := iceberg.NewSchema(0,
    iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.VariantType{}},
)
arrowSchema, _ := table.SchemaToArrowSchema(schema, nil, true, false)

bldr := extensions.NewVariantBuilder(memory.DefaultAllocator, extensions.NewDefaultVariantType())
defer bldr.Release()

var vb variant.Builder
_ = vb.Append(map[string]any{"x": int64(320), "target": "button-submit"})
val, _ := vb.Build()
bldr.Append(val)

col := bldr.NewArray()
defer col.Release()
rec := array.NewRecord(arrowSchema, []arrow.Array{col}, 1)
defer rec.Release()

tx := tbl.NewTransaction()
_ = tx.AppendTable(ctx, array.NewTableFromRecords(arrowSchema, []arrow.Record{rec}), 1024, nil)
tbl, _ = tx.Commit(ctx)
```

A normal scan reads it back; the variant column comes back as an `*extensions.VariantArray`:

```go
result, _ := tbl.Scan().ToArrowTable(ctx)
defer result.Release()

variants := result.Column(0).Data().Chunk(0).(*extensions.VariantArray)
v, _ := variants.Value(0)
fmt.Println(v.Value()) // variant.ObjectValue for the object above
```

## Shredding

Shredding stores the fields of a variant as typed Parquet sub-columns instead of a
single opaque blob. This lets scans read and filter a field as a native column
rather than decoding every value. Shredding is opt-in and configured with two write
properties (see [Configuration](./configuration.md#parquet-writer)):

- `write.parquet.shred-variants` - enable shredding of top-level variant columns
  (default `false`).
- `write.parquet.variant-inference-buffer-size` - rows buffered per file to infer
  the shredding schema (default `100`).

The reader accepts both shredded and non-shredded data; no read-side
configuration is required.

## Filtering on variant fields

`iceberg.Extract(ref, path, typ)` builds a term that navigates a dotted JSONPath
into a variant column and casts the leaf to `typ`. It plugs into the same predicate
builders as `iceberg.Reference`:

```go
filter := iceberg.EqualTo(
    iceberg.Extract(iceberg.Reference("payload"), "$.user.age", iceberg.PrimitiveTypes.Int64),
    int64(30),
)
// filter is a BooleanExpression; pass it to Scan().WithRowFilter(filter).
```

See [Row Filter Syntax](./row-filter-syntax.md#variant-extraction) for the supported
target types and caveats.

## Constraints

- A variant column cannot be a partition source or an identity-transform source.
- Extract predicates are evaluated as per-row residual filters. Row-group stats and
  bloom pruning skip them; comparison pruning against shredded variant bounds is
  best-effort.
- Extract terms are DSL-only: there is no string row-filter form, and they are not
  REST-serializable, so they are dropped from any server-side pushed filter and
  evaluated locally.
