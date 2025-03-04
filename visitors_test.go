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

package iceberg_test

import (
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ExampleVisitor struct {
	visitHistory []string
}

func (e *ExampleVisitor) VisitTrue() []string {
	e.visitHistory = append(e.visitHistory, "TRUE")

	return e.visitHistory
}

func (e *ExampleVisitor) VisitFalse() []string {
	e.visitHistory = append(e.visitHistory, "FALSE")

	return e.visitHistory
}

func (e *ExampleVisitor) VisitNot([]string) []string {
	e.visitHistory = append(e.visitHistory, "NOT")

	return e.visitHistory
}

func (e *ExampleVisitor) VisitAnd(_, _ []string) []string {
	e.visitHistory = append(e.visitHistory, "AND")

	return e.visitHistory
}

func (e *ExampleVisitor) VisitOr(_, _ []string) []string {
	e.visitHistory = append(e.visitHistory, "OR")

	return e.visitHistory
}

func (e *ExampleVisitor) VisitUnbound(pred iceberg.UnboundPredicate) []string {
	e.visitHistory = append(e.visitHistory, strings.ToUpper(pred.Op().String()))

	return e.visitHistory
}

func (e *ExampleVisitor) VisitBound(pred iceberg.BoundPredicate) []string {
	e.visitHistory = append(e.visitHistory, strings.ToUpper(pred.Op().String()))

	return e.visitHistory
}

type FooBoundExprVisitor struct {
	ExampleVisitor
}

func (e *FooBoundExprVisitor) VisitBound(pred iceberg.BoundPredicate) []string {
	return iceberg.VisitBoundPredicate(pred, e)
}

func (e *FooBoundExprVisitor) VisitUnbound(pred iceberg.UnboundPredicate) []string {
	panic("found unbound predicate when evaluating")
}

func (e *FooBoundExprVisitor) VisitIn(iceberg.BoundTerm, iceberg.Set[iceberg.Literal]) []string {
	e.visitHistory = append(e.visitHistory, "IN")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitNotIn(iceberg.BoundTerm, iceberg.Set[iceberg.Literal]) []string {
	e.visitHistory = append(e.visitHistory, "NOT_IN")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitIsNan(iceberg.BoundTerm) []string {
	e.visitHistory = append(e.visitHistory, "IS_NAN")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitNotNan(iceberg.BoundTerm) []string {
	e.visitHistory = append(e.visitHistory, "NOT_NAN")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitIsNull(iceberg.BoundTerm) []string {
	e.visitHistory = append(e.visitHistory, "IS_NULL")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitNotNull(iceberg.BoundTerm) []string {
	e.visitHistory = append(e.visitHistory, "NOT_NULL")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitEqual(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "EQUAL")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitNotEqual(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "NOT_EQUAL")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitGreaterEqual(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "GREATER_THAN_OR_EQUAL")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitGreater(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "GREATER_THAN")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitLessEqual(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "LESS_THAN_OR_EQUAL")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitLess(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "LESS_THAN")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitStartsWith(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "STARTS_WITH")

	return e.visitHistory
}

func (e *FooBoundExprVisitor) VisitNotStartsWith(iceberg.BoundTerm, iceberg.Literal) []string {
	e.visitHistory = append(e.visitHistory, "NOT_STARTS_WITH")

	return e.visitHistory
}

func TestBooleanExprVisitor(t *testing.T) {
	expr := iceberg.NewAnd(
		iceberg.NewOr(
			iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("a"), int32(1))),
			iceberg.NewNot(iceberg.NotEqualTo(iceberg.Reference("b"), int32(0))),
			iceberg.EqualTo(iceberg.Reference("a"), int32(1)),
			iceberg.NotEqualTo(iceberg.Reference("b"), int32(0)),
		),
		iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("a"), int32(1))),
		iceberg.NotEqualTo(iceberg.Reference("b"), int32(0)))

	visitor := ExampleVisitor{visitHistory: make([]string, 0)}
	result, err := iceberg.VisitExpr(expr, &visitor)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"EQUAL",
		"NOT",
		"NOTEQUAL",
		"NOT",
		"OR",
		"EQUAL",
		"OR",
		"NOTEQUAL",
		"OR",
		"EQUAL",
		"NOT",
		"AND",
		"NOTEQUAL",
		"AND",
	}, result)
}

func TestBindVisitorAlready(t *testing.T) {
	bound, err := iceberg.EqualTo(iceberg.Reference("foo"), "hello").
		Bind(tableSchemaSimple, false)
	require.NoError(t, err)

	_, err = iceberg.BindExpr(tableSchemaSimple, bound, true)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.ErrorContains(t, err, "found already bound predicate: BoundEqual(term=BoundReference(field=1: foo: optional string, accessor=Accessor(position=0, inner=<nil>)), literal=hello)")
}

func TestAlwaysExprBinding(t *testing.T) {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected iceberg.BooleanExpression
	}{
		{iceberg.AlwaysTrue{}, iceberg.AlwaysTrue{}},
		{iceberg.AlwaysFalse{}, iceberg.AlwaysFalse{}},
		{iceberg.NewAnd(iceberg.AlwaysTrue{}, iceberg.AlwaysFalse{}), iceberg.AlwaysFalse{}},
		{iceberg.NewOr(iceberg.AlwaysTrue{}, iceberg.AlwaysFalse{}), iceberg.AlwaysTrue{}},
	}

	for _, tt := range tests {
		t.Run(tt.expr.String(), func(t *testing.T) {
			bound, err := iceberg.BindExpr(tableSchemaSimple, tt.expr, true)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, bound)
		})
	}
}

func TestBoundBoolExprVisitor(t *testing.T) {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected []string
	}{
		{iceberg.NewAnd(iceberg.IsIn(iceberg.Reference("foo"), "foo", "bar"),
			iceberg.IsIn(iceberg.Reference("bar"), int32(1), int32(2))), []string{"IN", "IN", "AND"}},
		{
			iceberg.NewOr(iceberg.NewNot(iceberg.IsIn(iceberg.Reference("foo"), "foo", "bar")),
				iceberg.NewNot(iceberg.IsIn(iceberg.Reference("bar"), int32(1), int32(2)))),
			[]string{"IN", "NOT", "IN", "NOT", "OR"},
		},
		{iceberg.EqualTo(iceberg.Reference("bar"), int32(1)), []string{"EQUAL"}},
		{iceberg.NotEqualTo(iceberg.Reference("foo"), "foo"), []string{"NOT_EQUAL"}},
		{iceberg.AlwaysTrue{}, []string{"TRUE"}},
		{iceberg.AlwaysFalse{}, []string{"FALSE"}},
		{iceberg.NotIn(iceberg.Reference("foo"), "bar", "foo"), []string{"NOT_IN"}},
		{iceberg.IsNull(iceberg.Reference("foo")), []string{"IS_NULL"}},
		{iceberg.NotNull(iceberg.Reference("foo")), []string{"NOT_NULL"}},
		{iceberg.GreaterThan(iceberg.Reference("foo"), "foo"), []string{"GREATER_THAN"}},
		{iceberg.GreaterThanEqual(iceberg.Reference("foo"), "foo"), []string{"GREATER_THAN_OR_EQUAL"}},
		{iceberg.LessThan(iceberg.Reference("foo"), "foo"), []string{"LESS_THAN"}},
		{iceberg.LessThanEqual(iceberg.Reference("foo"), "foo"), []string{"LESS_THAN_OR_EQUAL"}},
		{iceberg.StartsWith(iceberg.Reference("foo"), "foo"), []string{"STARTS_WITH"}},
		{iceberg.NotStartsWith(iceberg.Reference("foo"), "foo"), []string{"NOT_STARTS_WITH"}},
	}

	for _, tt := range tests {
		t.Run(tt.expr.String(), func(t *testing.T) {
			bound, err := iceberg.BindExpr(tableSchemaNested,
				tt.expr,
				true)
			require.NoError(t, err)

			visitor := FooBoundExprVisitor{ExampleVisitor: ExampleVisitor{visitHistory: []string{}}}
			result, err := iceberg.VisitExpr(bound, &visitor)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

type rowTester []any

func (r rowTester) Size() int       { return len(r) }
func (r rowTester) Get(pos int) any { return r[pos] }
func (r rowTester) Set(pos int, val any) {
	r[pos] = val
}

func rowOf(vals ...any) rowTester {
	return rowTester(vals)
}

var testSchema = iceberg.NewSchema(1,
	iceberg.NestedField{
		ID: 13, Name: "x",
		Type: iceberg.PrimitiveTypes.Int32, Required: true,
	},
	iceberg.NestedField{
		ID: 14, Name: "y",
		Type: iceberg.PrimitiveTypes.Float64, Required: true,
	},
	iceberg.NestedField{
		ID: 15, Name: "z",
		Type: iceberg.PrimitiveTypes.Int32,
	},
	iceberg.NestedField{
		ID: 16, Name: "s1",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{{
				ID: 17, Name: "s2", Required: true,
				Type: &iceberg.StructType{
					FieldList: []iceberg.NestedField{{
						ID: 18, Name: "s3", Required: true,
						Type: &iceberg.StructType{
							FieldList: []iceberg.NestedField{{
								ID: 19, Name: "s4", Required: true,
								Type: &iceberg.StructType{
									FieldList: []iceberg.NestedField{{
										ID: 20, Name: "i", Required: true,
										Type: iceberg.PrimitiveTypes.Int32,
									}},
								},
							}},
						},
					}},
				},
			}},
		},
	},
	iceberg.NestedField{ID: 21, Name: "s5", Type: &iceberg.StructType{
		FieldList: []iceberg.NestedField{{
			ID: 22, Name: "s6", Required: true, Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{{
					ID: 23, Name: "f", Required: true, Type: iceberg.PrimitiveTypes.Float32,
				}},
			},
		}},
	}},
	iceberg.NestedField{ID: 24, Name: "s", Type: iceberg.PrimitiveTypes.String})

func TestExprEvaluator(t *testing.T) {
	type testCase struct {
		str    string
		row    rowTester
		result bool
	}

	tests := []struct {
		exp   iceberg.BooleanExpression
		cases []testCase
	}{
		{iceberg.AlwaysTrue{}, []testCase{{"always true", rowOf(), true}}},
		{iceberg.AlwaysFalse{}, []testCase{{"always false", rowOf(), false}}},
		{iceberg.LessThan(iceberg.Reference("x"), int32(7)), []testCase{
			{"7 < 7 => false", rowOf(7, 8, nil, nil), false},
			{"6 < 7 => true", rowOf(6, 8, nil, nil), true},
		}},
		{iceberg.LessThan(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)), []testCase{
			{"7 < 7 => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), false},
			{"6 < 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), true},
			{"nil < 7 => true", rowOf(7, 8, nil, nil), true},
		}},
		{iceberg.LessThanEqual(iceberg.Reference("x"), int32(7)), []testCase{
			{"7 <= 7 => true", rowOf(7, 8, nil), true},
			{"6 <= 7 => true", rowOf(6, 8, nil), true},
			{"8 <= 7 => false", rowOf(8, 8, nil), false},
		}},
		{iceberg.LessThanEqual(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)), []testCase{
			{"7 <= 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), true},
			{"6 <= 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), true},
			{"8 <= 7 => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(8))))), false},
		}},
		{iceberg.GreaterThan(iceberg.Reference("x"), int32(7)), []testCase{
			{"7 > 7 => false", rowOf(7, 8, nil), false},
			{"6 > 7 => false", rowOf(6, 8, nil), false},
			{"8 > 7 => true", rowOf(8, 8, nil), true},
		}},
		{iceberg.GreaterThan(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)), []testCase{
			{"7 > 7 => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), false},
			{"6 > 7 => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), false},
			{"8 > 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(8))))), true},
		}},
		{iceberg.GreaterThanEqual(iceberg.Reference("x"), int32(7)), []testCase{
			{"7 >= 7 => true", rowOf(7, 8, nil), true},
			{"6 >= 7 => false", rowOf(6, 8, nil), false},
			{"8 >= 7 => true", rowOf(8, 8, nil), true},
		}},
		{iceberg.GreaterThanEqual(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)), []testCase{
			{"7 >= 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), true},
			{"6 >= 7 => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), false},
			{"8 >= 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(8))))), true},
		}},
		{iceberg.EqualTo(iceberg.Reference("x"), int32(7)), []testCase{
			{"7 == 7 => true", rowOf(7, 8, nil), true},
			{"6 == 7 => false", rowOf(6, 8, nil), false},
		}},
		{iceberg.EqualTo(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)), []testCase{
			{"7 == 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), true},
			{"6 == 7 => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), false},
		}},
		{iceberg.NotEqualTo(iceberg.Reference("x"), int32(7)), []testCase{
			{"7 != 7 => false", rowOf(7, 8, nil), false},
			{"6 != 7 => true", rowOf(6, 8, nil), true},
		}},
		{iceberg.NotEqualTo(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)), []testCase{
			{"7 != 7 => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), false},
			{"6 != 7 => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), true},
		}},
		{iceberg.IsNull(iceberg.Reference("z")), []testCase{
			{"nil is null", rowOf(1, 2, nil), true},
			{"3 is not null", rowOf(1, 2, 3), false},
		}},
		{iceberg.IsNull(iceberg.Reference("s1.s2.s3.s4.i")), []testCase{
			{"3 is not null", rowOf(1, 2, 3, rowOf(rowOf(rowOf(rowOf(3))))), false},
		}},
		{iceberg.NotNull(iceberg.Reference("z")), []testCase{
			{"nil is null", rowOf(1, 2, nil), false},
			{"3 is not null", rowOf(1, 2, 3), true},
		}},
		{iceberg.NotNull(iceberg.Reference("s1.s2.s3.s4.i")), []testCase{
			{"3 is not null", rowOf(1, 2, 3, rowOf(rowOf(rowOf(rowOf(3))))), true},
		}},
		{iceberg.IsNaN(iceberg.Reference("y")), []testCase{
			{"NaN is NaN", rowOf(1, math.NaN(), 3), true},
			{"2 is not NaN", rowOf(1, 2.0, 3), false},
		}},
		{iceberg.IsNaN(iceberg.Reference("s5.s6.f")), []testCase{
			{"NaN is NaN", rowOf(1, 2, 3, nil, rowOf(rowOf(math.NaN()))), true},
			{"4 is not NaN", rowOf(1, 2, 3, nil, rowOf(rowOf(4.0))), false},
			{"nil is not NaN", rowOf(1, 2, 3, nil, nil), false},
		}},
		{iceberg.NotNaN(iceberg.Reference("y")), []testCase{
			{"NaN is NaN", rowOf(1, math.NaN(), 3), false},
			{"2 is not NaN", rowOf(1, 2.0, 3), true},
		}},
		{iceberg.NotNaN(iceberg.Reference("s5.s6.f")), []testCase{
			{"NaN is NaN", rowOf(1, 2, 3, nil, rowOf(rowOf(math.NaN()))), false},
			{"4 is not NaN", rowOf(1, 2, 3, nil, rowOf(rowOf(4.0))), true},
		}},
		{iceberg.NewAnd(iceberg.EqualTo(iceberg.Reference("x"), int32(7)), iceberg.NotNull(iceberg.Reference("z"))), []testCase{
			{"7, 3 => true", rowOf(7, 0, 3), true},
			{"8, 3 => false", rowOf(8, 0, 3), false},
			{"7, null => false", rowOf(7, 0, nil), false},
			{"8, null => false", rowOf(8, 0, nil), false},
		}},
		{iceberg.NewAnd(iceberg.EqualTo(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)),
			iceberg.NotNull(iceberg.Reference("s1.s2.s3.s4.i"))), []testCase{
			{"7, 7 => true", rowOf(5, 0, 3, rowOf(rowOf(rowOf(rowOf(7))))), true},
			{"8, 8 => false", rowOf(7, 0, 3, rowOf(rowOf(rowOf(rowOf(8))))), false},
			{"7, null => false", rowOf(5, 0, 3, nil), false},
			{"8, notnull => false", rowOf(7, 0, 3, rowOf(rowOf(rowOf(rowOf(8))))), false},
		}},
		{iceberg.NewOr(iceberg.EqualTo(iceberg.Reference("x"), int32(7)), iceberg.NotNull(iceberg.Reference("z"))), []testCase{
			{"7, 3 => true", rowOf(7, 0, 3), true},
			{"8, 3 => true", rowOf(8, 0, 3), true},
			{"7, null => true", rowOf(7, 0, nil), true},
			{"8, null => false", rowOf(8, 0, nil), false},
		}},
		{iceberg.NewOr(iceberg.EqualTo(iceberg.Reference("s1.s2.s3.s4.i"), int32(7)),
			iceberg.NotNull(iceberg.Reference("s1.s2.s3.s4.i"))), []testCase{
			{"7, 7 => true", rowOf(5, 0, 3, rowOf(rowOf(rowOf(rowOf(7))))), true},
			{"8, notnull => true", rowOf(7, 0, 3, rowOf(rowOf(rowOf(rowOf(8))))), true},
			{"7, null => false", rowOf(5, 0, 3, nil), false},
			{"8, notnull => true", rowOf(7, 0, 3, rowOf(rowOf(rowOf(rowOf(8))))), true},
		}},
		{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("x"), int32(7))), []testCase{
			{"not(7 == 7) => false", rowOf(7), false},
			{"not(8 == 7) => true", rowOf(8), true},
		}},
		{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("s1.s2.s3.s4.i"), int32(7))), []testCase{
			{"not(7 == 7) => false", rowOf(7, nil, nil, rowOf(rowOf(rowOf(rowOf(7))))), false},
			{"not(8 == 7) => true", rowOf(7, nil, nil, rowOf(rowOf(rowOf(rowOf(8))))), true},
		}},
		{iceberg.IsIn(iceberg.Reference("x"), int64(7), 8, math.MaxInt64), []testCase{
			{"7 in [7, 8, Int64Max] => true", rowOf(7, 8, nil), true},
			{"9 in [7, 8, Int64Max] => false", rowOf(9, 8, nil), false},
			{"8 in [7, 8, Int64Max] => true", rowOf(8, 8, nil), true},
		}},
		{iceberg.IsIn(iceberg.Reference("x"), int64(math.MaxInt64), math.MaxInt32, math.MinInt64), []testCase{
			{"Int32Max in [Int64Max, Int32Max, Int64Min] => true", rowOf(math.MaxInt32, 7.0, nil), true},
			{"6 in [Int64Max, Int32Max, Int64Min] => false", rowOf(6, 6.9, nil), false},
		}},
		{iceberg.IsIn(iceberg.Reference("y"), float64(7), 8, 9.1), []testCase{
			{"7.0 in [7, 8, 9.1] => true", rowOf(0, 7.0, nil), true},
			{"9.1 in [7, 8, 9.1] => true", rowOf(7, 9.1, nil), true},
			{"6.8 in [7, 8, 9.1] => false", rowOf(7, 6.8, nil), false},
		}},
		{iceberg.IsIn(iceberg.Reference("s1.s2.s3.s4.i"), int32(7), 8, 9), []testCase{
			{"7 in [7, 8, 9] => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), true},
			{"6 in [7, 8, 9] => true", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), false},
			{"nil in [7, 8, 9] => false", rowOf(7, 8, nil, nil), false},
		}},
		{iceberg.NotIn(iceberg.Reference("x"), int64(7), 8, math.MaxInt64), []testCase{
			{"7 not in [7, 8, Int64Max] => false", rowOf(7, 8, nil), false},
			{"9 not in [7, 8, Int64Max] => true", rowOf(9, 8, nil), true},
			{"8 not in [7, 8, Int64Max] => false", rowOf(8, 8, nil), false},
		}},
		{iceberg.NotIn(iceberg.Reference("x"), int64(math.MaxInt64), math.MaxInt32, math.MinInt64), []testCase{
			{"Int32Max not in [Int64Max, Int32Max, Int64Min] => false", rowOf(math.MaxInt32, 7.0, nil), false},
			{"6 not in [Int64Max, Int32Max, Int64Min] => true", rowOf(6, 6.9, nil), true},
		}},
		{iceberg.NotIn(iceberg.Reference("y"), float64(7), 8, 9.1), []testCase{
			{"7.0 not in [7, 8, 9.1] => false", rowOf(0, 7.0, nil), false},
			{"9.1 not in [7, 8, 9.1] => false", rowOf(7, 9.1, nil), false},
			{"6.8 not in [7, 8, 9.1] => true", rowOf(7, 6.8, nil), true},
		}},
		{iceberg.NotIn(iceberg.Reference("s1.s2.s3.s4.i"), int32(7), 8, 9), []testCase{
			{"7 not in [7, 8, 9] => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(7))))), false},
			{"6 not in [7, 8, 9] => false", rowOf(7, 8, nil, rowOf(rowOf(rowOf(rowOf(6))))), true},
		}},
		{iceberg.EqualTo(iceberg.Reference("s"), "abc"), []testCase{
			{"abc == abc => true", rowOf(1, 2, nil, nil, nil, "abc"), true},
			{"abd == abc => false", rowOf(1, 2, nil, nil, nil, "abd"), false},
		}},
		{iceberg.StartsWith(iceberg.Reference("s"), "abc"), []testCase{
			{"abc startsWith abc => true", rowOf(1, 2, nil, nil, nil, "abc"), true},
			{"xabc startsWith abc => false", rowOf(1, 2, nil, nil, nil, "xabc"), false},
			{"Abc startsWith abc => false", rowOf(1, 2, nil, nil, nil, "Abc"), false},
			{"a startsWith abc => false", rowOf(1, 2, nil, nil, nil, "a"), false},
			{"abcd startsWith abc => true", rowOf(1, 2, nil, nil, nil, "abcd"), true},
			{"nil startsWith abc => false", rowOf(1, 2, nil, nil, nil, nil), false},
		}},
		{iceberg.NotStartsWith(iceberg.Reference("s"), "abc"), []testCase{
			{"abc not startsWith abc => false", rowOf(1, 2, nil, nil, nil, "abc"), false},
			{"xabc not startsWith abc => true", rowOf(1, 2, nil, nil, nil, "xabc"), true},
			{"Abc not startsWith abc => true", rowOf(1, 2, nil, nil, nil, "Abc"), true},
			{"a not startsWith abc => true", rowOf(1, 2, nil, nil, nil, "a"), true},
			{"abcd not startsWith abc => false", rowOf(1, 2, nil, nil, nil, "abcd"), false},
			{"nil not startsWith abc => true", rowOf(1, 2, nil, nil, nil, nil), true},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.exp.String(), func(t *testing.T) {
			ev, err := iceberg.ExpressionEvaluator(testSchema, tt.exp, true)
			require.NoError(t, err)

			for _, c := range tt.cases {
				res, err := ev(c.row)
				require.NoError(t, err)

				assert.Equal(t, c.result, res, c.str)
			}
		})
	}
}

func TestEvaluatorCmpTypes(t *testing.T) {
	sc := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "c", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 4, Name: "d", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 5, Name: "e", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 6, Name: "f", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 7, Name: "g", Type: iceberg.PrimitiveTypes.Time},
		iceberg.NestedField{ID: 8, Name: "h", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 9, Name: "i", Type: iceberg.DecimalTypeOf(9, 2)},
		iceberg.NestedField{ID: 10, Name: "j", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 11, Name: "k", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 12, Name: "l", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 13, Name: "m", Type: iceberg.FixedTypeOf(5)})

	rowData := rowOf(true,
		5, 5, float32(5.0), float64(5.0),
		29, 51661919000, 1503066061919234,
		iceberg.Decimal{Scale: 2, Val: decimal128.FromI64(3456)},
		"abcdef", []byte{0x01, 0x02, 0x03},
		uuid.New(), []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x0})

	tests := []struct {
		ref iceberg.BooleanExpression
		exp bool
	}{
		{iceberg.EqualTo(iceberg.Reference("a"), true), true},
		{iceberg.EqualTo(iceberg.Reference("a"), false), false},
		{iceberg.EqualTo(iceberg.Reference("c"), int64(5)), true},
		{iceberg.EqualTo(iceberg.Reference("c"), int64(6)), false},
		{iceberg.EqualTo(iceberg.Reference("d"), int64(5)), true},
		{iceberg.EqualTo(iceberg.Reference("d"), int64(6)), false},
		{iceberg.EqualTo(iceberg.Reference("e"), int64(5)), true},
		{iceberg.EqualTo(iceberg.Reference("e"), int64(6)), false},
		{iceberg.EqualTo(iceberg.Reference("f"), "1970-01-30"), true},
		{iceberg.EqualTo(iceberg.Reference("f"), "1970-01-31"), false},
		{iceberg.EqualTo(iceberg.Reference("g"), "14:21:01.919"), true},
		{iceberg.EqualTo(iceberg.Reference("g"), "14:21:02.919"), false},
		{iceberg.EqualTo(iceberg.Reference("h"), "2017-08-18T14:21:01.919234"), true},
		{iceberg.EqualTo(iceberg.Reference("h"), "2017-08-19T14:21:01.919234"), false},
		{iceberg.LessThan(iceberg.Reference("i"), "32.22"), false},
		{iceberg.GreaterThan(iceberg.Reference("i"), "32.22"), true},
		{iceberg.LessThanEqual(iceberg.Reference("j"), "abcd"), false},
		{iceberg.GreaterThan(iceberg.Reference("j"), "abcde"), true},
		{iceberg.GreaterThan(iceberg.Reference("k"), []byte{0x00}), true},
		{iceberg.LessThan(iceberg.Reference("k"), []byte{0x00}), false},
		{iceberg.EqualTo(iceberg.Reference("l"), uuid.New().String()), false},
		{iceberg.EqualTo(iceberg.Reference("l"), rowData[11].(uuid.UUID)), true},
		{iceberg.EqualTo(iceberg.Reference("m"), []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x1}), false},
		{iceberg.EqualTo(iceberg.Reference("m"), []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x0}), true},
	}

	for _, tt := range tests {
		t.Run(tt.ref.String(), func(t *testing.T) {
			ev, err := iceberg.ExpressionEvaluator(sc, tt.ref, true)
			require.NoError(t, err)

			res, err := ev(rowData)
			require.NoError(t, err)
			assert.Equal(t, tt.exp, res)
		})
	}
}

func TestRewriteNot(t *testing.T) {
	tests := []struct {
		expr, expected iceberg.BooleanExpression
	}{
		{
			iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("x"), 34.56)),
			iceberg.NotEqualTo(iceberg.Reference("x"), 34.56),
		},
		{
			iceberg.NewNot(iceberg.NotEqualTo(iceberg.Reference("x"), 34.56)),
			iceberg.EqualTo(iceberg.Reference("x"), 34.56),
		},
		{
			iceberg.NewNot(iceberg.IsIn(iceberg.Reference("x"), 34.56, 23.45)),
			iceberg.NotIn(iceberg.Reference("x"), 34.56, 23.45),
		},
		{
			iceberg.NewNot(iceberg.NewAnd(
				iceberg.EqualTo(iceberg.Reference("x"), 34.56), iceberg.EqualTo(iceberg.Reference("y"), 34.56))),
			iceberg.NewOr(
				iceberg.NotEqualTo(iceberg.Reference("x"), 34.56), iceberg.NotEqualTo(iceberg.Reference("y"), 34.56)),
		},
		{
			iceberg.NewNot(iceberg.NewOr(
				iceberg.EqualTo(iceberg.Reference("x"), 34.56), iceberg.EqualTo(iceberg.Reference("y"), 34.56))),
			iceberg.NewAnd(iceberg.NotEqualTo(iceberg.Reference("x"), 34.56), iceberg.NotEqualTo(iceberg.Reference("y"), 34.56)),
		},
		{iceberg.NewNot(iceberg.AlwaysFalse{}), iceberg.AlwaysTrue{}},
		{iceberg.NewNot(iceberg.AlwaysTrue{}), iceberg.AlwaysFalse{}},
	}

	for _, tt := range tests {
		t.Run(tt.expr.String(), func(t *testing.T) {
			out, err := iceberg.RewriteNotExpr(tt.expr)
			require.NoError(t, err)
			assert.True(t, out.Equals(tt.expected))
		})
	}
}
