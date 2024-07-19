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

package iceberg

import (
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
)

// BooleanExprVisitor is an interface for recursively visiting the nodes of a
// boolean expression
type BooleanExprVisitor[T any] interface {
	VisitTrue() T
	VisitFalse() T
	VisitNot(childREsult T) T
	VisitAnd(left, right T) T
	VisitOr(left, right T) T
	VisitUnbound(UnboundPredicate) T
	VisitBound(BoundPredicate) T
}

// BoundBooleanExprVisitor builds on BooleanExprVisitor by adding interface
// methods for visiting bound expressions, because we do casting of literals
// during binding you can assume that the BoundTerm and the Literal passed
// to a method have the same type.
type BoundBooleanExprVisitor[T any] interface {
	BooleanExprVisitor[T]

	VisitIn(BoundTerm, Set[Literal]) T
	VisitNotIn(BoundTerm, Set[Literal]) T
	VisitIsNan(BoundTerm) T
	VisitNotNan(BoundTerm) T
	VisitIsNull(BoundTerm) T
	VisitNotNull(BoundTerm) T
	VisitEqual(BoundTerm, Literal) T
	VisitNotEqual(BoundTerm, Literal) T
	VisitGreaterEqual(BoundTerm, Literal) T
	VisitGreater(BoundTerm, Literal) T
	VisitLessEqual(BoundTerm, Literal) T
	VisitLess(BoundTerm, Literal) T
	VisitStartsWith(BoundTerm, Literal) T
	VisitNotStartsWith(BoundTerm, Literal) T
}

// VisitExpr is a convenience function to use a given visitor to visit all parts of
// a boolean expression in-order. Values returned from the methods are passed to the
// subsequent methods, effectively "bubbling up" the results.
func VisitExpr[T any](expr BooleanExpression, visitor BooleanExprVisitor[T]) (res T, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case string:
				err = fmt.Errorf("error encountered during visitExpr: %s", e)
			case error:
				err = e
			}
		}
	}()

	return visitBoolExpr(expr, visitor), err
}

func visitBoolExpr[T any](e BooleanExpression, visitor BooleanExprVisitor[T]) T {
	switch e := e.(type) {
	case AlwaysFalse:
		return visitor.VisitFalse()
	case AlwaysTrue:
		return visitor.VisitTrue()
	case AndExpr:
		left, right := visitBoolExpr(e.left, visitor), visitBoolExpr(e.right, visitor)
		return visitor.VisitAnd(left, right)
	case OrExpr:
		left, right := visitBoolExpr(e.left, visitor), visitBoolExpr(e.right, visitor)
		return visitor.VisitOr(left, right)
	case NotExpr:
		child := visitBoolExpr(e.child, visitor)
		return visitor.VisitNot(child)
	case UnboundPredicate:
		return visitor.VisitUnbound(e)
	case BoundPredicate:
		return visitor.VisitBound(e)
	}
	panic(fmt.Errorf("%w: VisitBooleanExpression type %s", ErrNotImplemented, e))
}

// VisitBoundPredicate uses a BoundBooleanExprVisitor to call the appropriate method
// based on the type of operation in the predicate. This is a convenience function
// for implementing the VisitBound method of a BoundBooleanExprVisitor by simply calling
// iceberg.VisitBoundPredicate(pred, this).
func VisitBoundPredicate[T any](e BoundPredicate, visitor BoundBooleanExprVisitor[T]) T {
	switch e.Op() {
	case OpIn:
		return visitor.VisitIn(e.Term(), e.(BoundSetPredicate).Literals())
	case OpNotIn:
		return visitor.VisitNotIn(e.Term(), e.(BoundSetPredicate).Literals())
	case OpIsNan:
		return visitor.VisitIsNan(e.Term())
	case OpNotNan:
		return visitor.VisitNotNan(e.Term())
	case OpIsNull:
		return visitor.VisitIsNull(e.Term())
	case OpNotNull:
		return visitor.VisitNotNull(e.Term())
	case OpEQ:
		return visitor.VisitEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpNEQ:
		return visitor.VisitNotEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpGTEQ:
		return visitor.VisitGreaterEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpGT:
		return visitor.VisitGreater(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpLTEQ:
		return visitor.VisitLessEqual(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpLT:
		return visitor.VisitLess(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpStartsWith:
		return visitor.VisitStartsWith(e.Term(), e.(BoundLiteralPredicate).Literal())
	case OpNotStartsWith:
		return visitor.VisitNotStartsWith(e.Term(), e.(BoundLiteralPredicate).Literal())
	}
	panic(fmt.Errorf("%w: unhandled bound predicate type: %s", ErrNotImplemented, e))
}

// BindExpr recursively binds each portion of an expression using the provided schema.
// Because the expression can end up being simplified to just AlwaysTrue/AlwaysFalse,
// this returns a BooleanExpression.
func BindExpr(s *Schema, expr BooleanExpression, caseSensitive bool) (BooleanExpression, error) {
	return VisitExpr(expr, &bindVisitor{schema: s, caseSensitive: caseSensitive})
}

type bindVisitor struct {
	schema        *Schema
	caseSensitive bool
}

func (*bindVisitor) VisitTrue() BooleanExpression  { return AlwaysTrue{} }
func (*bindVisitor) VisitFalse() BooleanExpression { return AlwaysFalse{} }
func (*bindVisitor) VisitNot(child BooleanExpression) BooleanExpression {
	return NewNot(child)
}
func (*bindVisitor) VisitAnd(left, right BooleanExpression) BooleanExpression {
	return NewAnd(left, right)
}
func (*bindVisitor) VisitOr(left, right BooleanExpression) BooleanExpression {
	return NewOr(left, right)
}
func (b *bindVisitor) VisitUnbound(pred UnboundPredicate) BooleanExpression {
	expr, err := pred.Bind(b.schema, b.caseSensitive)
	if err != nil {
		panic(err)
	}
	return expr
}
func (*bindVisitor) VisitBound(pred BoundPredicate) BooleanExpression {
	panic(fmt.Errorf("%w: found already bound predicate: %s", ErrInvalidArgument, pred))
}

// ExpressionEvaluator returns a function which can be used to evaluate a given expression
// as long as a structlike value is passed which operates like and matches the passed in
// schema.
func ExpressionEvaluator(s *Schema, unbound BooleanExpression, caseSensitive bool) (func(structLike) (bool, error), error) {
	bound, err := BindExpr(s, unbound, caseSensitive)
	if err != nil {
		return nil, err
	}

	return (&exprEvaluator{bound: bound}).Eval, nil
}

type exprEvaluator struct {
	bound BooleanExpression
	st    structLike
}

func (e *exprEvaluator) Eval(st structLike) (bool, error) {
	e.st = st
	return VisitExpr(e.bound, e)
}

func (e *exprEvaluator) VisitUnbound(UnboundPredicate) bool {
	panic("found unbound predicate when evaluating expression")
}

func (e *exprEvaluator) VisitBound(pred BoundPredicate) bool {
	return VisitBoundPredicate(pred, e)
}

func (*exprEvaluator) VisitTrue() bool                { return true }
func (*exprEvaluator) VisitFalse() bool               { return false }
func (*exprEvaluator) VisitNot(child bool) bool       { return !child }
func (*exprEvaluator) VisitAnd(left, right bool) bool { return left && right }
func (*exprEvaluator) VisitOr(left, right bool) bool  { return left || right }

func (e *exprEvaluator) VisitIn(term BoundTerm, literals Set[Literal]) bool {
	v := term.evalToLiteral(e.st)
	if !v.Valid {
		return false
	}

	return literals.Contains(v.Val)
}

func (e *exprEvaluator) VisitNotIn(term BoundTerm, literals Set[Literal]) bool {
	return !e.VisitIn(term, literals)
}

func (e *exprEvaluator) VisitIsNan(term BoundTerm) bool {
	switch term.Type().(type) {
	case Float32Type:
		v := term.(bound[float32]).eval(e.st)
		if !v.Valid {
			break
		}
		return math.IsNaN(float64(v.Val))
	case Float64Type:
		v := term.(bound[float64]).eval(e.st)
		if !v.Valid {
			break
		}
		return math.IsNaN(v.Val)
	}

	return false
}

func (e *exprEvaluator) VisitNotNan(term BoundTerm) bool {
	return !e.VisitIsNan(term)
}

func (e *exprEvaluator) VisitIsNull(term BoundTerm) bool {
	return term.evalIsNull(e.st)
}

func (e *exprEvaluator) VisitNotNull(term BoundTerm) bool {
	return !term.evalIsNull(e.st)
}

func nullsFirstCmp[T LiteralType](cmp Comparator[T], v1, v2 Optional[T]) int {
	if !v1.Valid {
		if !v2.Valid {
			// both are null
			return 0
		}
		// v1 is null, v2 is not
		return -1
	}

	if !v2.Valid {
		return 1
	}

	return cmp(v1.Val, v2.Val)
}

func typedCmp[T LiteralType](st structLike, term BoundTerm, lit Literal) int {
	v := term.(bound[T]).eval(st)
	var l Optional[T]

	rhs := lit.(TypedLiteral[T])
	if lit != nil {
		l.Valid = true
		l.Val = rhs.Value()
	}

	return nullsFirstCmp(rhs.Comparator(), v, l)
}

func doCmp(st structLike, term BoundTerm, lit Literal) int {
	// we already properly casted and converted everything during binding
	// so we can type assert based on the term type
	switch term.Type().(type) {
	case BooleanType:
		return typedCmp[bool](st, term, lit)
	case Int32Type:
		return typedCmp[int32](st, term, lit)
	case Int64Type:
		return typedCmp[int64](st, term, lit)
	case Float32Type:
		return typedCmp[float32](st, term, lit)
	case Float64Type:
		return typedCmp[float64](st, term, lit)
	case DateType:
		return typedCmp[Date](st, term, lit)
	case TimeType:
		return typedCmp[Time](st, term, lit)
	case TimestampType, TimestampTzType:
		return typedCmp[Timestamp](st, term, lit)
	case BinaryType, FixedType:
		return typedCmp[[]byte](st, term, lit)
	case StringType:
		return typedCmp[string](st, term, lit)
	case UUIDType:
		return typedCmp[uuid.UUID](st, term, lit)
	case DecimalType:
		return typedCmp[Decimal](st, term, lit)
	}
	panic(ErrType)
}

func (e *exprEvaluator) VisitEqual(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) == 0
}

func (e *exprEvaluator) VisitNotEqual(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) != 0
}

func (e *exprEvaluator) VisitGreater(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) > 0
}

func (e *exprEvaluator) VisitGreaterEqual(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) >= 0
}

func (e *exprEvaluator) VisitLess(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) < 0
}

func (e *exprEvaluator) VisitLessEqual(term BoundTerm, lit Literal) bool {
	return doCmp(e.st, term, lit) <= 0
}

func (e *exprEvaluator) VisitStartsWith(term BoundTerm, lit Literal) bool {
	var value, prefix string

	switch lit.(type) {
	case TypedLiteral[string]:
		val := term.(bound[string]).eval(e.st)
		if !val.Valid {
			return false
		}
		prefix, value = lit.(StringLiteral).Value(), val.Val
	case TypedLiteral[[]byte]:
		val := term.(bound[[]byte]).eval(e.st)
		if !val.Valid {
			return false
		}
		prefix, value = string(lit.(TypedLiteral[[]byte]).Value()), string(val.Val)
	}

	return strings.HasPrefix(value, prefix)
}

func (e *exprEvaluator) VisitNotStartsWith(term BoundTerm, lit Literal) bool {
	return !e.VisitStartsWith(term, lit)
}

// RewriteNotExpr rewrites a boolean expression to remove "Not" nodes from the expression
// tree. This is because Projections assume there are no "not" nodes.
//
// Not nodes will be replaced with simply calling `Negate` on the child in the tree.
func RewriteNotExpr(expr BooleanExpression) (BooleanExpression, error) {
	return VisitExpr(expr, rewriteNotVisitor{})
}

type rewriteNotVisitor struct{}

func (rewriteNotVisitor) VisitTrue() BooleanExpression  { return AlwaysTrue{} }
func (rewriteNotVisitor) VisitFalse() BooleanExpression { return AlwaysFalse{} }
func (rewriteNotVisitor) VisitNot(child BooleanExpression) BooleanExpression {
	return child.Negate()
}

func (rewriteNotVisitor) VisitAnd(left, right BooleanExpression) BooleanExpression {
	return NewAnd(left, right)
}

func (rewriteNotVisitor) VisitOr(left, right BooleanExpression) BooleanExpression {
	return NewOr(left, right)
}

func (rewriteNotVisitor) VisitUnbound(pred UnboundPredicate) BooleanExpression {
	return pred
}

func (rewriteNotVisitor) VisitBound(pred BoundPredicate) BooleanExpression {
	return pred
}

const (
	rowsMightMatch, rowsMustMatch      = true, true
	rowsCannotMatch, rowsMightNotMatch = false, false
	inPredicateLimit                   = 200
)

// NewManifestEvaluator returns a function that can be used to evaluate whether a particular
// manifest file has rows that might or might not match a given partition filter by using
// the stats provided in the partitions (UpperBound/LowerBound/ContainsNull/ContainsNaN).
func NewManifestEvaluator(spec PartitionSpec, schema *Schema, partitionFilter BooleanExpression, caseSensitive bool) (func(ManifestFile) (bool, error), error) {
	partType := spec.PartitionType(schema)
	partSchema := NewSchema(0, partType.FieldList...)
	filter, err := RewriteNotExpr(partitionFilter)
	if err != nil {
		return nil, err
	}

	boundFilter, err := BindExpr(partSchema, filter, caseSensitive)
	if err != nil {
		return nil, err
	}

	return (&manifestEvalVisitor{partitionFilter: boundFilter}).Eval, nil
}

type manifestEvalVisitor struct {
	partitionFields []FieldSummary
	partitionFilter BooleanExpression
}

func (m *manifestEvalVisitor) Eval(manifest ManifestFile) (bool, error) {
	if parts := manifest.Partitions(); len(parts) > 0 {
		m.partitionFields = parts
		return VisitExpr(m.partitionFilter, m)
	}

	return rowsMightMatch, nil
}

func allBoundCmp[T LiteralType](bound Literal, set Set[Literal], want int) bool {
	val := bound.(TypedLiteral[T])
	cmp := val.Comparator()

	return set.All(func(e Literal) bool {
		return cmp(val.Value(), e.(TypedLiteral[T]).Value()) == want
	})
}

func allBoundCheck(bound Literal, set Set[Literal], want int) bool {
	switch bound.Type().(type) {
	case BooleanType:
		return allBoundCmp[bool](bound, set, want)
	case Int32Type:
		return allBoundCmp[int32](bound, set, want)
	case Int64Type:
		return allBoundCmp[int64](bound, set, want)
	case Float32Type:
		return allBoundCmp[float32](bound, set, want)
	case Float64Type:
		return allBoundCmp[float64](bound, set, want)
	case DateType:
		return allBoundCmp[Date](bound, set, want)
	case TimeType:
		return allBoundCmp[Time](bound, set, want)
	case TimestampType, TimestampTzType:
		return allBoundCmp[Timestamp](bound, set, want)
	case BinaryType, FixedType:
		return allBoundCmp[[]byte](bound, set, want)
	case StringType:
		return allBoundCmp[string](bound, set, want)
	case UUIDType:
		return allBoundCmp[uuid.UUID](bound, set, want)
	case DecimalType:
		return allBoundCmp[Decimal](bound, set, want)
	}
	panic(ErrType)
}

func (m *manifestEvalVisitor) VisitIn(term BoundTerm, literals Set[Literal]) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	if literals.Len() > inPredicateLimit {
		return rowsMightMatch
	}

	lower, err := LiteralFromBytes(term.Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if allBoundCheck(lower, literals, 1) {
		return rowsCannotMatch
	}

	if field.UpperBound != nil {
		upper, err := LiteralFromBytes(term.Type(), *field.UpperBound)
		if err != nil {
			panic(err)
		}

		if allBoundCheck(upper, literals, -1) {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotIn(term BoundTerm, literals Set[Literal]) bool {
	// because the bounds are not necessarily a min or max value, this cannot be answered using them
	// notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitIsNan(term BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNaN != nil && !*field.ContainsNaN {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotNan(term BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNaN != nil && *field.ContainsNaN && !field.ContainsNull && field.LowerBound == nil {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitIsNull(term BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if !field.ContainsNull {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotNull(term BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	// ContainsNull encodes whether at least one partition value is null
	// lowerBound is null if all partition values are null
	allNull := field.ContainsNull && field.LowerBound == nil
	if allNull && (term.Ref().Type().Equals(PrimitiveTypes.Float32) || term.Ref().Type().Equals(PrimitiveTypes.Float64)) {
		// floating point types may include NaN values, which we check separately
		// in case bounds don't include NaN values, ContainsNaN needsz to be checked
		allNull = field.ContainsNaN != nil && !*field.ContainsNaN
	}

	if allNull {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func getCmp[T LiteralType](b TypedLiteral[T]) func(Literal, Literal) int {
	cmp := b.Comparator()
	return func(l1, l2 Literal) int {
		return cmp(l1.(TypedLiteral[T]).Value(), l2.(TypedLiteral[T]).Value())
	}
}

func getCmpLiteral(boundary Literal) func(Literal, Literal) int {
	switch l := boundary.(type) {
	case TypedLiteral[bool]:
		return getCmp(l)
	case TypedLiteral[int32]:
		return getCmp(l)
	case TypedLiteral[int64]:
		return getCmp(l)
	case TypedLiteral[float32]:
		return getCmp(l)
	case TypedLiteral[float64]:
		return getCmp(l)
	case TypedLiteral[Date]:
		return getCmp(l)
	case TypedLiteral[Time]:
		return getCmp(l)
	case TypedLiteral[Timestamp]:
		return getCmp(l)
	case TypedLiteral[[]byte]:
		return getCmp(l)
	case TypedLiteral[string]:
		return getCmp(l)
	case TypedLiteral[uuid.UUID]:
		return getCmp(l)
	case TypedLiteral[Decimal]:
		return getCmp(l)
	}
	panic(ErrType)
}

func (m *manifestEvalVisitor) VisitEqual(term BoundTerm, lit Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil || field.UpperBound == nil {
		// values are all null and literal cannot contain null
		return rowsCannotMatch
	}

	lower, err := LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	cmp := getCmpLiteral(lower)
	if cmp(lower, lit) == 1 {
		return rowsCannotMatch
	}

	upper, err := LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if cmp(lit, upper) == 1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotEqual(term BoundTerm, lit Literal) bool {
	// because bounds are not necessarily a min or max, this cannot be answered
	// using them. notEq(col, X) with (X, Y) doesn't guarantee X is a value in col
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitGreaterEqual(term BoundTerm, lit Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(upper)(lit, upper) == 1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitGreater(term BoundTerm, lit Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(upper)(lit, upper) >= 0 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitLessEqual(term BoundTerm, lit Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(lower)(lit, lower) == -1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitLess(term BoundTerm, lit Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(lower)(lit, lower) <= 0 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitStartsWith(term BoundTerm, lit Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	var prefix string
	if val, ok := lit.(TypedLiteral[string]); ok {
		prefix = val.Value()
	} else {
		prefix = string(lit.(TypedLiteral[[]byte]).Value())
	}

	lenPrefix := len(prefix)

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	// truncate lower bound so that it's length is not greater than the length of prefix
	var v string
	switch l := lower.(type) {
	case TypedLiteral[string]:
		v = l.Value()
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	case TypedLiteral[[]byte]:
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

	upper, err := LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	switch u := upper.(type) {
	case TypedLiteral[string]:
		v = u.Value()
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	case TypedLiteral[[]byte]:
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

func (m *manifestEvalVisitor) VisitNotStartsWith(term BoundTerm, lit Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNull || field.LowerBound == nil || field.UpperBound == nil {
		return rowsMightMatch
	}

	// NotStartsWith will match unless ALL values must start with the prefix.
	// this happens when the lower and upper bounds BOTH start with the prefix
	lower, err := LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	upper, err := LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	var (
		prefix, lowerBound, upperBound string
	)
	if val, ok := lit.(TypedLiteral[string]); ok {
		prefix = val.Value()
		lowerBound, upperBound = lower.(TypedLiteral[string]).Value(), upper.(TypedLiteral[string]).Value()
	} else {
		prefix = string(lit.(TypedLiteral[[]byte]).Value())
		lowerBound = string(lower.(TypedLiteral[[]byte]).Value())
		upperBound = string(upper.(TypedLiteral[[]byte]).Value())
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

func (m *manifestEvalVisitor) VisitUnbound(UnboundPredicate) bool {
	panic("need bound predicate")
}

func (m *manifestEvalVisitor) VisitBound(pred BoundPredicate) bool {
	return VisitBoundPredicate(pred, m)
}

func (m *manifestEvalVisitor) VisitNot(child bool) bool       { return !child }
func (m *manifestEvalVisitor) VisitAnd(left, right bool) bool { return left && right }
func (m *manifestEvalVisitor) VisitOr(left, right bool) bool  { return left || right }
