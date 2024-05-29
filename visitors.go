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

type BooleanExprVisitor[T any] interface {
	VisitTrue() T
	VisitFalse() T
	VisitNot(childREsult T) T
	VisitAnd(left, right T) T
	VisitOr(left, right T) T
	VisitUnbound(UnboundPredicate) T
	VisitBound(BoundPredicate) T
}

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

func visitBoundPredicate[T any](e BoundPredicate, visitor BoundBooleanExprVisitor[T]) T {
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
	panic("found already bound predicate: " + pred.String())
}

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
	return visitBoundPredicate(pred, e)
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
	val := term.(bound[string]).eval(e.st)
	if !val.Valid {
		return false
	}

	return strings.HasPrefix(val.Val, lit.(StringLiteral).Value())
}

func (e *exprEvaluator) VisitNotStartsWith(term BoundTerm, lit Literal) bool {
	return !e.VisitStartsWith(term, lit)
}
