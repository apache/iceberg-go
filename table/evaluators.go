// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

const (
	rowsMightMatch, rowsMustMatch      = true, true
	rowsCannotMatch, rowsMightNotMatch = false, false
	inPredicateLimit                   = 200
)

// newManifestEvaluator returns a function that can be used to evaluate whether a particular
// manifest file has rows that might or might not match a given partition filter by using
// the stats provided in the partitions (UpperBound/LowerBound/ContainsNull/ContainsNaN).
func newManifestEvaluator(spec iceberg.PartitionSpec, schema *iceberg.Schema, partitionFilter iceberg.BooleanExpression, caseSensitive bool) (func(iceberg.ManifestFile) (bool, error), error) {
	partType := spec.PartitionType(schema)
	partSchema := iceberg.NewSchema(0, partType.FieldList...)
	filter, err := iceberg.RewriteNotExpr(partitionFilter)
	if err != nil {
		return nil, err
	}

	boundFilter, err := iceberg.BindExpr(partSchema, filter, caseSensitive)
	if err != nil {
		return nil, err
	}

	return (&manifestEvalVisitor{partitionFilter: boundFilter}).Eval, nil
}

type manifestEvalVisitor struct {
	partitionFields []iceberg.FieldSummary
	partitionFilter iceberg.BooleanExpression
}

func (m *manifestEvalVisitor) Eval(manifest iceberg.ManifestFile) (bool, error) {
	if parts := manifest.Partitions(); len(parts) > 0 {
		m.partitionFields = parts
		return iceberg.VisitExpr(m.partitionFilter, m)
	}

	return rowsMightMatch, nil
}

func allBoundCmp[T iceberg.LiteralType](bound iceberg.Literal, set iceberg.Set[iceberg.Literal], want int) bool {
	val := bound.(iceberg.TypedLiteral[T])
	cmp := val.Comparator()

	return set.All(func(e iceberg.Literal) bool {
		return cmp(val.Value(), e.(iceberg.TypedLiteral[T]).Value()) == want
	})
}

func allBoundCheck(bound iceberg.Literal, set iceberg.Set[iceberg.Literal], want int) bool {
	switch bound.Type().(type) {
	case iceberg.BooleanType:
		return allBoundCmp[bool](bound, set, want)
	case iceberg.Int32Type:
		return allBoundCmp[int32](bound, set, want)
	case iceberg.Int64Type:
		return allBoundCmp[int64](bound, set, want)
	case iceberg.Float32Type:
		return allBoundCmp[float32](bound, set, want)
	case iceberg.Float64Type:
		return allBoundCmp[float64](bound, set, want)
	case iceberg.DateType:
		return allBoundCmp[iceberg.Date](bound, set, want)
	case iceberg.TimeType:
		return allBoundCmp[iceberg.Time](bound, set, want)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return allBoundCmp[iceberg.Timestamp](bound, set, want)
	case iceberg.BinaryType, iceberg.FixedType:
		return allBoundCmp[[]byte](bound, set, want)
	case iceberg.StringType:
		return allBoundCmp[string](bound, set, want)
	case iceberg.UUIDType:
		return allBoundCmp[uuid.UUID](bound, set, want)
	case iceberg.DecimalType:
		return allBoundCmp[iceberg.Decimal](bound, set, want)
	}
	panic(iceberg.ErrType)
}

func (m *manifestEvalVisitor) VisitIn(term iceberg.BoundTerm, literals iceberg.Set[iceberg.Literal]) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	if literals.Len() > inPredicateLimit {
		return rowsMightMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if allBoundCheck(lower, literals, 1) {
		return rowsCannotMatch
	}

	if field.UpperBound != nil {
		upper, err := iceberg.LiteralFromBytes(term.Type(), *field.UpperBound)
		if err != nil {
			panic(err)
		}

		if allBoundCheck(upper, literals, -1) {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotIn(term iceberg.BoundTerm, literals iceberg.Set[iceberg.Literal]) bool {
	// because the bounds are not necessarily a min or max value, this cannot be answered using them
	// notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitIsNan(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNaN != nil && !*field.ContainsNaN {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotNan(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNaN != nil && *field.ContainsNaN && !field.ContainsNull && field.LowerBound == nil {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitIsNull(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if !field.ContainsNull {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotNull(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	// ContainsNull encodes whether at least one partition value is null
	// lowerBound is null if all partition values are null
	allNull := field.ContainsNull && field.LowerBound == nil
	if allNull && (term.Ref().Type().Equals(iceberg.PrimitiveTypes.Float32) || term.Ref().Type().Equals(iceberg.PrimitiveTypes.Float64)) {
		// floating point types may include NaN values, which we check separately
		// in case bounds don't include NaN values, ContainsNaN needsz to be checked
		allNull = field.ContainsNaN != nil && !*field.ContainsNaN
	}

	if allNull {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func getCmp[T iceberg.LiteralType](b iceberg.TypedLiteral[T]) func(iceberg.Literal, iceberg.Literal) int {
	cmp := b.Comparator()
	return func(l1, l2 iceberg.Literal) int {
		return cmp(l1.(iceberg.TypedLiteral[T]).Value(), l2.(iceberg.TypedLiteral[T]).Value())
	}
}

func getCmpLiteral(boundary iceberg.Literal) func(iceberg.Literal, iceberg.Literal) int {
	switch l := boundary.(type) {
	case iceberg.TypedLiteral[bool]:
		return getCmp(l)
	case iceberg.TypedLiteral[int32]:
		return getCmp(l)
	case iceberg.TypedLiteral[int64]:
		return getCmp(l)
	case iceberg.TypedLiteral[float32]:
		return getCmp(l)
	case iceberg.TypedLiteral[float64]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Date]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Time]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Timestamp]:
		return getCmp(l)
	case iceberg.TypedLiteral[[]byte]:
		return getCmp(l)
	case iceberg.TypedLiteral[string]:
		return getCmp(l)
	case iceberg.TypedLiteral[uuid.UUID]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Decimal]:
		return getCmp(l)
	}
	panic(iceberg.ErrType)
}

func (m *manifestEvalVisitor) VisitEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil || field.UpperBound == nil {
		// values are all null and literal cannot contain null
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	cmp := getCmpLiteral(lower)
	if cmp(lower, lit) == 1 {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if cmp(lit, upper) == 1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	// because bounds are not necessarily a min or max, this cannot be answered
	// using them. notEq(col, X) with (X, Y) doesn't guarantee X is a value in col
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitGreaterEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(upper)(lit, upper) == 1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitGreater(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(upper)(lit, upper) >= 0 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitLessEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(lower)(lit, lower) == -1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitLess(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(lower)(lit, lower) <= 0 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitStartsWith(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	var prefix string
	if val, ok := lit.(iceberg.TypedLiteral[string]); ok {
		prefix = val.Value()
	} else {
		prefix = string(lit.(iceberg.TypedLiteral[[]byte]).Value())
	}

	lenPrefix := len(prefix)

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	// truncate lower bound so that it's length is not greater than the length of prefix
	var v string
	switch l := lower.(type) {
	case iceberg.TypedLiteral[string]:
		v = l.Value()
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	case iceberg.TypedLiteral[[]byte]:
		v = string(l.Value())
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	}

	if v > prefix {
		return rowsCannotMatch
	}

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	switch u := upper.(type) {
	case iceberg.TypedLiteral[string]:
		v = u.Value()
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	case iceberg.TypedLiteral[[]byte]:
		v = string(u.Value())
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	}

	if v < prefix {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotStartsWith(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNull || field.LowerBound == nil || field.UpperBound == nil {
		return rowsMightMatch
	}

	// NotStartsWith will match unless ALL values must start with the prefix.
	// this happens when the lower and upper bounds BOTH start with the prefix
	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	var (
		prefix, lowerBound, upperBound string
	)
	if val, ok := lit.(iceberg.TypedLiteral[string]); ok {
		prefix = val.Value()
		lowerBound, upperBound = lower.(iceberg.TypedLiteral[string]).Value(), upper.(iceberg.TypedLiteral[string]).Value()
	} else {
		prefix = string(lit.(iceberg.TypedLiteral[[]byte]).Value())
		lowerBound = string(lower.(iceberg.TypedLiteral[[]byte]).Value())
		upperBound = string(upper.(iceberg.TypedLiteral[[]byte]).Value())
	}

	lenPrefix := len(prefix)
	if len(lowerBound) < lenPrefix {
		return rowsMightMatch
	}

	if lowerBound[:lenPrefix] == prefix {
		// if upper is shorter then upper can't start with the prefix
		if len(upperBound) < lenPrefix {
			return rowsMightMatch
		}

		if upperBound[:lenPrefix] == prefix {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitTrue() bool {
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitFalse() bool {
	return rowsCannotMatch
}

func (m *manifestEvalVisitor) VisitUnbound(iceberg.UnboundPredicate) bool {
	panic("need bound predicate")
}

func (m *manifestEvalVisitor) VisitBound(pred iceberg.BoundPredicate) bool {
	return iceberg.VisitBoundPredicate(pred, m)
}

func (m *manifestEvalVisitor) VisitNot(child bool) bool       { return !child }
func (m *manifestEvalVisitor) VisitAnd(left, right bool) bool { return left && right }
func (m *manifestEvalVisitor) VisitOr(left, right bool) bool  { return left || right }
