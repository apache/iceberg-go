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
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// BooleanExprVisitor is an interface for recursively visiting the nodes of a
// boolean expression
type BooleanExprVisitor[T any] interface {
	VisitTrue() T
	VisitFalse() T
	VisitNot(childResult T) T
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

// ExtractFieldIDs returns a slice containing the field IDs which are referenced
// by any terms in the given expression. This enables retrieving exactly which
// fields are needed for an expression.
func ExtractFieldIDs(expr BooleanExpression) ([]int, error) {
	res, err := VisitExpr(expr, expressionFieldIDs{})
	if err != nil {
		return nil, err
	}

	out := make([]int, 0, len(res))

	return slices.AppendSeq(out, maps.Keys(res)), nil
}

type expressionFieldIDs struct{}

func (expressionFieldIDs) VisitTrue() map[int]struct{} {
	return map[int]struct{}{}
}

func (expressionFieldIDs) VisitFalse() map[int]struct{} {
	return map[int]struct{}{}
}

func (expressionFieldIDs) VisitNot(child map[int]struct{}) map[int]struct{} {
	return child
}

func (expressionFieldIDs) VisitAnd(left, right map[int]struct{}) map[int]struct{} {
	maps.Insert(left, maps.All(right))

	return left
}

func (expressionFieldIDs) VisitOr(left, right map[int]struct{}) map[int]struct{} {
	maps.Insert(left, maps.All(right))

	return left
}

func (expressionFieldIDs) VisitUnbound(UnboundPredicate) map[int]struct{} {
	panic("expression field IDs only works for bound expressions")
}

func (expressionFieldIDs) VisitBound(pred BoundPredicate) map[int]struct{} {
	return map[int]struct{}{
		pred.Ref().Field().ID: {},
	}
}

// TranslateColumnNames converts the names of columns in an expression by looking up
// the field IDs in the file schema. If columns don't exist they are replaced with
// AlwaysFalse or AlwaysTrue depending on the operator.
func TranslateColumnNames(expr BooleanExpression, fileSchema *Schema) (BooleanExpression, error) {
	return VisitExpr(expr, columnNameTranslator{fileSchema: fileSchema})
}

type columnNameTranslator struct {
	fileSchema *Schema
}

func (columnNameTranslator) VisitTrue() BooleanExpression  { return AlwaysTrue{} }
func (columnNameTranslator) VisitFalse() BooleanExpression { return AlwaysFalse{} }
func (columnNameTranslator) VisitNot(child BooleanExpression) BooleanExpression {
	return NewNot(child)
}

func (columnNameTranslator) VisitAnd(left, right BooleanExpression) BooleanExpression {
	return NewAnd(left, right)
}

func (columnNameTranslator) VisitOr(left, right BooleanExpression) BooleanExpression {
	return NewOr(left, right)
}

func (columnNameTranslator) VisitUnbound(pred UnboundPredicate) BooleanExpression {
	panic(fmt.Errorf("%w: expected bound predicate, got: %s", ErrInvalidArgument, pred.Term()))
}

func (c columnNameTranslator) VisitBound(pred BoundPredicate) BooleanExpression {
	fileColName, found := c.fileSchema.FindColumnName(pred.Term().Ref().Field().ID)
	if !found {
		// in the case of schema evolution, the column might not be present
		// in the file schema when reading older data
		if pred.Op() == OpIsNull {
			return AlwaysTrue{}
		}

		return AlwaysFalse{}
	}

	ref := Reference(fileColName)
	switch p := pred.(type) {
	case BoundUnaryPredicate:
		return p.AsUnbound(ref)
	case BoundLiteralPredicate:
		return p.AsUnbound(ref, p.Literal())
	case BoundSetPredicate:
		return p.AsUnbound(ref, p.Literals().Members())
	default:
		panic(fmt.Errorf("%w: unsupported predicate: %s", ErrNotImplemented, pred))
	}
}
