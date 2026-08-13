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

# Row Filter Syntax

Row filters drive predicate pushdown during scans, partition pruning, and row-level deletes. The DSL lives in the root `iceberg` package - all you need is the import:

```go
import "github.com/apache/iceberg-go"
```

A column is referenced with `iceberg.Reference("column_name")`. Predicate constructors return a `BooleanExpression` (or an `UnboundPredicate`, which satisfies `BooleanExpression`) that can be combined with the boolean combinators in the [Expression DSL](./expression-dsl.md) and passed to APIs like `table.WithRowFilter(...)`.

## Equality

```go
iceberg.EqualTo(iceberg.Reference("status"), "active")
iceberg.NotEqualTo(iceberg.Reference("retries"), int32(0))
```

`EqualTo[T]` and `NotEqualTo[T]` are generic over `LiteralType` (`bool`, `int32`, `int64`, `float32`, `float64`, `string`, `[]byte`, plus a few iceberg-specific types). The value's type must be in that set - bare `int` literals are not, so use `int32(0)` / `int64(0)` explicitly. They wrap `LiteralPredicate(OpEQ, ...)` and `LiteralPredicate(OpNEQ, ...)` (`predicates.go:83-91`).

## Comparison

```go
iceberg.LessThan(iceberg.Reference("amount"), 100.0)
iceberg.LessThanEqual(iceberg.Reference("amount"), 100.0)
iceberg.GreaterThan(iceberg.Reference("created_at"), int64(1700000000))
iceberg.GreaterThanEqual(iceberg.Reference("score"), int32(50))
```

Operators: `OpLT`, `OpLTEQ`, `OpGT`, `OpGTEQ` (`exprs.go:47-50`). Constructors at `predicates.go:98-124`.

## Set membership

```go
iceberg.IsIn(iceberg.Reference("region"), "us-east", "us-west", "eu-west")
iceberg.NotIn(iceberg.Reference("status"), "deleted", "archived")
```

`IsIn` and `NotIn` are variadic. They return a `BooleanExpression` (not `UnboundPredicate`) because the result can simplify automatically:

- Zero values - reduces to `AlwaysFalse{}` (for `IsIn`) or `AlwaysTrue{}` (for `NotIn`).
- One value - reduces to `EqualTo` / `NotEqualTo`.

See `predicates.go:55-78`.

## Null checks

```go
iceberg.IsNull(iceberg.Reference("deleted_at"))
iceberg.NotNull(iceberg.Reference("user_id"))
```

These wrap `UnaryPredicate(OpIsNull, ...)` and `UnaryPredicate(OpNotNull, ...)`. Both panic if the term is `nil` (`predicates.go:23-32`).

## NaN checks (float / double columns only)

```go
iceberg.IsNaN(iceberg.Reference("ratio"))
iceberg.NotNaN(iceberg.Reference("ratio"))
```

Operators `OpIsNan` and `OpNotNan`. Use these instead of `EqualTo(..., math.NaN())` - NaN is never equal to itself.

## String prefix

```go
iceberg.StartsWith(iceberg.Reference("path"), "/var/log/")
iceberg.NotStartsWith(iceberg.Reference("name"), "tmp_")
```

Operators `OpStartsWith` and `OpNotStartsWith` (`exprs.go:53-54`). The value must be a `string`.

## Constants

```go
iceberg.AlwaysTrue{}
iceberg.AlwaysFalse{}
```

These satisfy `BooleanExpression` and short-circuit during expression simplification. Useful as a base case when filters are built dynamically:

```go
filter := iceberg.BooleanExpression(iceberg.AlwaysTrue{})
for _, clause := range userClauses {
    filter = iceberg.NewAnd(filter, clause)
}
```

## Operator reference

The full operator set is the `Operation` enum at `exprs.go:34-62`:

| Operator | Constant | Convenience builder |
|---|---|---|
| `<` | `OpLT` | `LessThan` |
| `<=` | `OpLTEQ` | `LessThanEqual` |
| `>` | `OpGT` | `GreaterThan` |
| `>=` | `OpGTEQ` | `GreaterThanEqual` |
| `==` | `OpEQ` | `EqualTo` |
| `!=` | `OpNEQ` | `NotEqualTo` |
| `IS NULL` | `OpIsNull` | `IsNull` |
| `IS NOT NULL` | `OpNotNull` | `NotNull` |
| `IS NaN` | `OpIsNan` | `IsNaN` |
| `IS NOT NaN` | `OpNotNan` | `NotNaN` |
| `IN` | `OpIn` | `IsIn` |
| `NOT IN` | `OpNotIn` | `NotIn` |
| `STARTS WITH` | `OpStartsWith` | `StartsWith` |
| `NOT STARTS WITH` | `OpNotStartsWith` | `NotStartsWith` |
| `AND` / `OR` / `NOT` | `OpAnd` / `OpOr` / `OpNot` | `NewAnd` / `NewOr` / `NewNot` (see [Expression DSL](./expression-dsl.md)) |

## Putting it together

A typical filter passed to a scan:

```go
filter := iceberg.NewAnd(
    iceberg.GreaterThanEqual(iceberg.Reference("event_time"), int64(1700000000)),
    iceberg.IsIn(iceberg.Reference("region"), "us-east", "us-west"),
    iceberg.NotNull(iceberg.Reference("user_id")),
)

scan := tbl.Scan(table.WithRowFilter(filter))
```

For boolean combination, term details, and the lower-level escape hatches (`UnaryPredicate`, `LiteralPredicate`, `SetPredicate`), see [Expression DSL](./expression-dsl.md).
