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

# Expression DSL

This page covers the building blocks behind the row-filter shortcuts in [Row Filter Syntax](./row-filter-syntax.md): boolean combinators, terms, and the lower-level predicate constructors.

The DSL lives entirely in the root `iceberg` package (see `exprs.go` and `predicates.go`).

## Boolean combinators

```go
iceberg.NewAnd(a, b)              // a AND b
iceberg.NewAnd(a, b, c, d)        // a AND b AND c AND d (variadic)
iceberg.NewOr(a, b)               // a OR b
iceberg.NewOr(a, b, c, d)         // a OR b OR c OR d
iceberg.NewNot(a)                 // NOT a
```

`NewAnd` and `NewOr` accept two required arguments plus a variadic tail (`exprs.go:226`, `exprs.go:287`). They simplify automatically:

- `NewAnd(x, AlwaysTrue{})` reduces to `x`.
- `NewAnd(x, AlwaysFalse{})` reduces to `AlwaysFalse{}`.
- `NewOr(x, AlwaysFalse{})` reduces to `x`.
- `NewOr(x, AlwaysTrue{})` reduces to `AlwaysTrue{}`.
- `NewNot(NewNot(x))` reduces to `x`.

## Constants

```go
iceberg.AlwaysTrue{}
iceberg.AlwaysFalse{}
```

Both satisfy `BooleanExpression`. Use them as the identity element when composing filters dynamically.

## Terms

A *term* is the left-hand side of a predicate. Iceberg-go has two flavors:

- `Reference("column_name")` - an unbound term that names a column (`exprs.go:373`). Typing happens at bind time, when the expression is matched against a schema. This is what you almost always want.
- `BoundReference` - the resolved form, produced by `Reference.Bind(schema, caseSensitive)` (`exprs.go:389`). You only encounter these when writing custom expression visitors.

The interfaces are:

```go
type Term interface { ... }                        // shared marker
type UnboundTerm interface { Term; ... }           // pre-bind
type BoundTerm interface { Term; Ref() BoundReference; ... }
```

(`exprs.go:317-348`)

## Predicates

A *predicate* applies an `Operation` to one or more terms. `BooleanExpression` is the shared interface (`exprs.go:123`).

### Convenience builders (recommended)

For all the common shapes, the constructors in `predicates.go` are the right tool. See [Row Filter Syntax](./row-filter-syntax.md) for the full list (`EqualTo`, `LessThan`, `IsIn`, `IsNull`, `StartsWith`, etc.).

### Lower-level escape hatches

When the convenience builders are not enough (custom operators, dynamic operation selection, working with already-typed `Literal` values), use the predicate constructors directly:

```go
// Unary predicates: IS NULL / NOT NULL / IS NaN / NOT NaN
pred := iceberg.UnaryPredicate(iceberg.OpIsNull, iceberg.Reference("col"))

// Literal predicates: <, <=, >, >=, ==, !=, STARTS WITH, NOT STARTS WITH
lit := iceberg.NewLiteral(int64(42))
pred := iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference("col"), lit)

// Set predicates: IN / NOT IN
lits := []iceberg.Literal{iceberg.NewLiteral("a"), iceberg.NewLiteral("b")}
pred := iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("col"), lits)
```

`UnaryPredicate` lives at `exprs.go:534`. `LiteralPredicate` and `SetPredicate` live in the same file.

## Negation

Every `BooleanExpression` and every `Operation` knows how to negate itself.

```go
op := iceberg.OpEQ
op.Negate()  // -> OpNEQ
```

(`exprs.go:65-98`. `OpNot`, `OpAnd`, and `OpOr` panic on direct negation - negate the wrapping expression instead.)

```go
expr := iceberg.EqualTo(iceberg.Reference("status"), "active")
inverted := expr.Negate()  // equivalent to NotEqualTo(...)
```

## Binding and evaluation

Most user code stops at constructing the unbound expression - the scan pipeline handles binding, projection, and evaluation internally. If you are writing a custom visitor:

- `(Reference).Bind(schema, caseSensitive)` returns a `BoundTerm` (`exprs.go:389`).
- `BoundExpression`s expose `Ref()`, `Type()`, and (for terms) the underlying `accessor` for evaluating against a `StructLike` row.

For projection, evaluation, and visitor patterns, see `visitors.go` and the scan internals in `table/scanner.go`.

## When to reach for what

| Goal | Use |
|---|---|
| Filter a scan or row-level delete | Convenience builders + `NewAnd`/`NewOr`/`NewNot` |
| Combine many clauses dynamically | Start from `AlwaysTrue{}` (for AND) or `AlwaysFalse{}` (for OR), fold with `NewAnd`/`NewOr` |
| Construct a predicate whose operator is chosen at runtime | `UnaryPredicate(op, term)`, `LiteralPredicate(op, term, lit)`, `SetPredicate(op, term, lits)` |
| Walk an expression tree | A custom `BooleanExprVisitor` from `visitors.go` |

For the per-operator cookbook, return to [Row Filter Syntax](./row-filter-syntax.md).
