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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ExprA struct{}

func (ExprA) String() string                    { return "ExprA" }
func (ExprA) Op() iceberg.Operation             { return iceberg.OpFalse }
func (ExprA) Negate() iceberg.BooleanExpression { return ExprB{} }
func (ExprA) Equals(o iceberg.BooleanExpression) bool {
	_, ok := o.(ExprA)

	return ok
}

type ExprB struct{}

func (ExprB) String() string                    { return "ExprB" }
func (ExprB) Op() iceberg.Operation             { return iceberg.OpTrue }
func (ExprB) Negate() iceberg.BooleanExpression { return ExprA{} }
func (ExprB) Equals(o iceberg.BooleanExpression) bool {
	_, ok := o.(ExprB)

	return ok
}

func TestUnaryExpr(t *testing.T) {
	assert.PanicsWithError(t, "invalid argument: invalid operation for unary predicate: LessThan", func() {
		iceberg.UnaryPredicate(iceberg.OpLT, iceberg.Reference("a"))
	})

	assert.PanicsWithError(t, "invalid argument: cannot create unary predicate with nil term", func() {
		iceberg.UnaryPredicate(iceberg.OpIsNull, nil)
	})

	t.Run("negate", func(t *testing.T) {
		n := iceberg.IsNull(iceberg.Reference("a")).Negate()
		exp := iceberg.NotNull(iceberg.Reference("a"))

		assert.Equal(t, exp, n)
		assert.True(t, exp.Equals(n))
		assert.True(t, n.Equals(exp))
	})

	sc := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Int32,
	})
	sc2 := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Float64,
	})
	sc3 := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	sc4 := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 2, Name: "a", Type: iceberg.PrimitiveTypes.Float32, Required: true,
	})

	t.Run("isnull and notnull", func(t *testing.T) {
		t.Run("bind", func(t *testing.T) {
			n, err := iceberg.IsNull(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIsNull, n.Op())
			assert.Implements(t, (*iceberg.BoundUnaryPredicate)(nil), n)
			p := n.(iceberg.BoundUnaryPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Int32, p.Term().Type())
			assert.Same(t, p.Ref(), p.Term().Ref())
			assert.Same(t, p.Ref(), p.Ref().Ref())

			f := p.Ref().Field()
			assert.True(t, f.Equals(sc.Field(0)))
		})

		t.Run("negate and bind", func(t *testing.T) {
			n1, err := iceberg.IsNull(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNull(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			assert.True(t, n1.Negate().Equals(n2))
			assert.True(t, n2.Negate().Equals(n1))
		})

		t.Run("null bind required", func(t *testing.T) {
			n1, err := iceberg.IsNull(iceberg.Reference("a")).Bind(sc3, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNull(iceberg.Reference("a")).Bind(sc3, true)
			require.NoError(t, err)

			assert.True(t, n1.Equals(iceberg.AlwaysFalse{}))
			assert.True(t, n2.Equals(iceberg.AlwaysTrue{}))
		})
	})

	t.Run("isnan notnan", func(t *testing.T) {
		t.Run("negate and bind", func(t *testing.T) {
			n1, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			assert.True(t, n1.Negate().Equals(n2))
			assert.True(t, n2.Negate().Equals(n1))
		})

		t.Run("bind float", func(t *testing.T) {
			n, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc4, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIsNan, n.Op())
			assert.Implements(t, (*iceberg.BoundUnaryPredicate)(nil), n)
			p := n.(iceberg.BoundUnaryPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float32, p.Term().Type())

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc4, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpNotNan, n2.Op())
			assert.Implements(t, (*iceberg.BoundUnaryPredicate)(nil), n2)
			p2 := n2.(iceberg.BoundUnaryPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float32, p2.Term().Type())
		})

		t.Run("bind double", func(t *testing.T) {
			n, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIsNan, n.Op())
			assert.Implements(t, (*iceberg.BoundUnaryPredicate)(nil), n)
			p := n.(iceberg.BoundUnaryPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float64, p.Term().Type())

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc2, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpNotNan, n2.Op())
			assert.Implements(t, (*iceberg.BoundUnaryPredicate)(nil), n2)
			p2 := n2.(iceberg.BoundUnaryPredicate)
			assert.IsType(t, iceberg.PrimitiveTypes.Float64, p2.Term().Type())
		})

		t.Run("bind non floating", func(t *testing.T) {
			n1, err := iceberg.IsNaN(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			n2, err := iceberg.NotNaN(iceberg.Reference("a")).Bind(sc, true)
			require.NoError(t, err)

			assert.True(t, n1.Equals(iceberg.AlwaysFalse{}))
			assert.True(t, n2.Equals(iceberg.AlwaysTrue{}))
		})
	})
}

func TestRefBindingCaseSensitive(t *testing.T) {
	ref1, ref2 := iceberg.Reference("foo"), iceberg.Reference("Foo")

	bound1, err := ref1.Bind(tableSchemaSimple, true)
	require.NoError(t, err)
	assert.True(t, bound1.Type().Equals(iceberg.PrimitiveTypes.String))

	_, err = ref2.Bind(tableSchemaSimple, true)
	assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.ErrorContains(t, err, "could not bind reference 'Foo', caseSensitive=true")

	bound2, err := ref2.Bind(tableSchemaSimple, false)
	require.NoError(t, err)
	assert.True(t, bound1.Equals(bound2))

	_, err = iceberg.Reference("foot").Bind(tableSchemaSimple, false)
	assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.ErrorContains(t, err, "could not bind reference 'foot', caseSensitive=false")
}

func TestRefTypes(t *testing.T) {
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

	t.Run("bind term", func(t *testing.T) {
		for i := 0; i < sc.NumFields(); i++ {
			fld := sc.Field(i)
			t.Run(fld.Type.String(), func(t *testing.T) {
				ref, err := iceberg.Reference(fld.Name).Bind(sc, true)
				require.NoError(t, err)

				assert.True(t, ref.Type().Equals(fld.Type))
				assert.True(t, fld.Equals(ref.Ref().Field()))
			})
		}
	})

	t.Run("bind unary", func(t *testing.T) {
		for i := 0; i < sc.NumFields(); i++ {
			fld := sc.Field(i)
			t.Run(fld.Type.String(), func(t *testing.T) {
				b, err := iceberg.IsNull(iceberg.Reference(fld.Name)).Bind(sc, true)
				require.NoError(t, err)

				assert.True(t, b.(iceberg.BoundUnaryPredicate).Ref().Type().Equals(fld.Type))

				un := b.(iceberg.BoundUnaryPredicate).AsUnbound(iceberg.Reference("foo"))
				assert.Equal(t, b.Op(), un.Op())
			})
		}
	})

	t.Run("bind literal", func(t *testing.T) {
		t.Run("bool", func(t *testing.T) {
			b1, err := iceberg.EqualTo(iceberg.Reference("a"), true).Bind(sc, true)
			require.NoError(t, err)
			assert.Equal(t, iceberg.OpEQ, b1.Op())
			assert.True(t, b1.(iceberg.BoundLiteralPredicate).Ref().Type().Equals(iceberg.PrimitiveTypes.Bool))
		})

		for i := 1; i < 9; i++ {
			fld := sc.Field(i)
			t.Run(fld.Type.String(), func(t *testing.T) {
				b, err := iceberg.EqualTo(iceberg.Reference(fld.Name), int32(5)).Bind(sc, true)
				require.NoError(t, err)

				assert.Equal(t, iceberg.OpEQ, b.Op())
				assert.True(t, b.(iceberg.BoundLiteralPredicate).Literal().Type().Equals(fld.Type))
				assert.True(t, b.(iceberg.BoundLiteralPredicate).Ref().Type().Equals(fld.Type))
			})
		}

		t.Run("string-binary", func(t *testing.T) {
			str, err := iceberg.EqualTo(iceberg.Reference("j"), "foobar").Bind(sc, true)
			require.NoError(t, err)

			bin, err := iceberg.EqualTo(iceberg.Reference("k"), []byte("foobar")).Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpEQ, str.Op())
			assert.True(t, str.(iceberg.BoundLiteralPredicate).Literal().Type().Equals(iceberg.PrimitiveTypes.String))
			assert.Equal(t, iceberg.OpEQ, bin.Op())
			assert.True(t, bin.(iceberg.BoundLiteralPredicate).Literal().Type().Equals(iceberg.PrimitiveTypes.Binary))
		})

		t.Run("fixed", func(t *testing.T) {
			fx, err := iceberg.EqualTo(iceberg.Reference("m"), []byte{0, 1, 2, 3, 4}).Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpEQ, fx.Op())
			assert.True(t, fx.(iceberg.BoundLiteralPredicate).Literal().Type().Equals(iceberg.FixedTypeOf(5)))
		})

		t.Run("uuid", func(t *testing.T) {
			uid, err := iceberg.EqualTo(iceberg.Reference("l"), uuid.New().String()).Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpEQ, uid.Op())
			assert.True(t, uid.(iceberg.BoundLiteralPredicate).Literal().Type().Equals(iceberg.PrimitiveTypes.UUID))
		})
	})

	t.Run("bind set", func(t *testing.T) {
		t.Run("bool", func(t *testing.T) {
			b, err := iceberg.IsIn(iceberg.Reference("a"), true, false).(iceberg.UnboundPredicate).Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIn, b.Op())
		})

		for i := 1; i < 9; i++ {
			fld := sc.Field(i)
			t.Run(fld.Type.String(), func(t *testing.T) {
				b, err := iceberg.IsIn(iceberg.Reference(fld.Name), int32(10), int32(5), int32(5)).(iceberg.UnboundPredicate).
					Bind(sc, true)
				require.NoError(t, err)

				assert.Equal(t, iceberg.OpIn, b.Op())
				assert.True(t, b.(iceberg.BoundSetPredicate).Ref().Type().Equals(fld.Type))
				for _, v := range b.(iceberg.BoundSetPredicate).Literals().Members() {
					assert.True(t, v.Type().Equals(fld.Type))
				}
			})
		}

		t.Run("string-binary", func(t *testing.T) {
			str, err := iceberg.IsIn(iceberg.Reference("j"), "hello", "foobar").(iceberg.UnboundPredicate).
				Bind(sc, true)
			require.NoError(t, err)

			bin, err := iceberg.IsIn(iceberg.Reference("k"), []byte("baz"), []byte("foobar")).(iceberg.UnboundPredicate).
				Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIn, str.Op())
			assert.Equal(t, iceberg.OpIn, bin.Op())

			assert.True(t, str.(iceberg.BoundSetPredicate).Ref().Type().Equals(iceberg.PrimitiveTypes.String))
			for _, v := range str.(iceberg.BoundSetPredicate).Literals().Members() {
				assert.True(t, v.Type().Equals(iceberg.PrimitiveTypes.String))
			}

			assert.True(t, bin.(iceberg.BoundSetPredicate).Ref().Type().Equals(iceberg.PrimitiveTypes.Binary))
			for _, v := range bin.(iceberg.BoundSetPredicate).Literals().Members() {
				assert.True(t, v.Type().Equals(iceberg.PrimitiveTypes.Binary))
			}
		})

		t.Run("fixed", func(t *testing.T) {
			fx, err := iceberg.IsIn(iceberg.Reference("m"), []byte{4, 5, 6, 7, 8}, []byte{0, 1, 2, 3, 4}).(iceberg.UnboundPredicate).
				Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIn, fx.Op())
			assert.True(t, fx.(iceberg.BoundSetPredicate).Ref().Type().Equals(iceberg.FixedTypeOf(5)))
			for _, v := range fx.(iceberg.BoundSetPredicate).Literals().Members() {
				assert.True(t, v.Type().Equals(iceberg.FixedTypeOf(5)))
			}
		})

		t.Run("uuid", func(t *testing.T) {
			uid, err := iceberg.IsIn(iceberg.Reference("l"), uuid.New().String(), uuid.New().String()).(iceberg.UnboundPredicate).
				Bind(sc, true)
			require.NoError(t, err)

			assert.Equal(t, iceberg.OpIn, uid.Op())
			assert.True(t, uid.(iceberg.BoundSetPredicate).Ref().Type().Equals(iceberg.PrimitiveTypes.UUID))
			for _, v := range uid.(iceberg.BoundSetPredicate).Literals().Members() {
				assert.True(t, v.Type().Equals(iceberg.PrimitiveTypes.UUID))
			}
		})
	})
}

func TestInNotInSimplifications(t *testing.T) {
	assert.PanicsWithError(t, "invalid argument: invalid operation for SetPredicate: LessThan",
		func() { iceberg.SetPredicate(iceberg.OpLT, iceberg.Reference("x"), nil) })
	assert.PanicsWithError(t, "invalid argument: cannot create set predicate with nil term",
		func() { iceberg.SetPredicate(iceberg.OpIn, nil, nil) })
	assert.NotPanics(t, func() { iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("x"), nil) })

	t.Run("in to eq", func(t *testing.T) {
		a := iceberg.IsIn(iceberg.Reference("x"), 34.56)
		b := iceberg.EqualTo(iceberg.Reference("x"), 34.56)
		assert.True(t, a.Equals(b))
	})

	t.Run("notin to notequal", func(t *testing.T) {
		a := iceberg.NotIn(iceberg.Reference("x"), 34.56)
		b := iceberg.NotEqualTo(iceberg.Reference("x"), 34.56)
		assert.True(t, a.Equals(b))
	})

	t.Run("empty", func(t *testing.T) {
		a := iceberg.IsIn[float32](iceberg.Reference("x"))
		b := iceberg.NotIn[float32](iceberg.Reference("x"))

		assert.Equal(t, iceberg.AlwaysFalse{}, a)
		assert.Equal(t, iceberg.AlwaysTrue{}, b)
	})

	t.Run("bind and negate", func(t *testing.T) {
		inexp := iceberg.IsIn(iceberg.Reference("foo"), "hello", "world")
		notin := iceberg.NotIn(iceberg.Reference("foo"), "hello", "world")
		assert.True(t, inexp.Negate().Equals(notin))
		assert.True(t, notin.Negate().Equals(inexp))
		assert.Equal(t, iceberg.OpIn, inexp.Op())
		assert.Equal(t, iceberg.OpNotIn, notin.Op())

		boundin, err := inexp.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		boundnot, err := notin.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		assert.True(t, boundin.Negate().Equals(boundnot))
		assert.True(t, boundnot.Negate().Equals(boundin))
	})

	t.Run("bind dedup", func(t *testing.T) {
		isin := iceberg.IsIn(iceberg.Reference("foo"), "hello", "world", "world")
		bound, err := isin.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		assert.Implements(t, (*iceberg.BoundSetPredicate)(nil), bound)
		bsp := bound.(iceberg.BoundSetPredicate)
		assert.Equal(t, 2, bsp.Literals().Len())
		assert.True(t, bsp.Literals().Contains(iceberg.NewLiteral("hello")))
		assert.True(t, bsp.Literals().Contains(iceberg.NewLiteral("world")))
	})

	t.Run("bind dedup to eq", func(t *testing.T) {
		isin := iceberg.IsIn(iceberg.Reference("foo"), "world", "world")
		bound, err := isin.(iceberg.UnboundPredicate).Bind(tableSchemaSimple, true)
		require.NoError(t, err)

		assert.Equal(t, iceberg.OpEQ, bound.Op())
		assert.Equal(t, iceberg.NewLiteral("world"),
			bound.(iceberg.BoundLiteralPredicate).Literal())
	})
}

func TestLiteralPredicateErrors(t *testing.T) {
	assert.PanicsWithError(t, "invalid argument: invalid operation for LiteralPredicate: In",
		func() { iceberg.LiteralPredicate(iceberg.OpIn, iceberg.Reference("foo"), iceberg.NewLiteral("hello")) })
	assert.PanicsWithError(t, "invalid argument: cannot create literal predicate with nil term",
		func() { iceberg.LiteralPredicate(iceberg.OpLT, nil, iceberg.NewLiteral("hello")) })
	assert.PanicsWithError(t, "invalid argument: cannot create literal predicate with nil literal",
		func() { iceberg.LiteralPredicate(iceberg.OpLT, iceberg.Reference("foo"), nil) })
}

func TestNegations(t *testing.T) {
	ref := iceberg.Reference("foo")

	tests := []struct {
		name     string
		ex1, ex2 iceberg.UnboundPredicate
	}{
		{"equal-not", iceberg.EqualTo(ref, "hello"), iceberg.NotEqualTo(ref, "hello")},
		{"greater-equal-less", iceberg.GreaterThanEqual(ref, "hello"), iceberg.LessThan(ref, "hello")},
		{"greater-less-equal", iceberg.GreaterThan(ref, "hello"), iceberg.LessThanEqual(ref, "hello")},
		{"starts-with", iceberg.StartsWith(ref, "hello"), iceberg.NotStartsWith(ref, "hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, tt.ex1.Equals(tt.ex2))
			assert.False(t, tt.ex2.Equals(tt.ex1))
			assert.True(t, tt.ex1.Negate().Equals(tt.ex2))
			assert.True(t, tt.ex2.Negate().Equals(tt.ex1))

			b1, err := tt.ex1.Bind(tableSchemaSimple, true)
			require.NoError(t, err)
			b2, err := tt.ex2.Bind(tableSchemaSimple, true)
			require.NoError(t, err)

			assert.False(t, b1.Equals(b2))
			assert.False(t, b2.Equals(b1))
			assert.True(t, b1.Negate().Equals(b2))
			assert.True(t, b2.Negate().Equals(b1))
		})
	}
}

func TestBoolExprEQ(t *testing.T) {
	tests := []struct {
		exp, testexpra, testexprb iceberg.BooleanExpression
	}{
		{
			iceberg.NewAnd(ExprA{}, ExprB{}),
			iceberg.NewAnd(ExprA{}, ExprB{}),
			iceberg.NewOr(ExprA{}, ExprB{}),
		},
		{
			iceberg.NewOr(ExprA{}, ExprB{}),
			iceberg.NewOr(ExprA{}, ExprB{}),
			iceberg.NewAnd(ExprA{}, ExprB{}),
		},
		{
			iceberg.NewAnd(ExprA{}, ExprB{}),
			iceberg.NewAnd(ExprB{}, ExprA{}),
			iceberg.NewOr(ExprB{}, ExprA{}),
		},
		{
			iceberg.NewOr(ExprA{}, ExprB{}),
			iceberg.NewOr(ExprB{}, ExprA{}),
			iceberg.NewAnd(ExprB{}, ExprA{}),
		},
		{iceberg.NewNot(ExprA{}), iceberg.NewNot(ExprA{}), ExprB{}},
		{ExprA{}, ExprA{}, ExprB{}},
		{ExprB{}, ExprB{}, ExprA{}},
		{
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("not_foo"), "hello", "world"),
		},
		{
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "goodbye", "world"),
		},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.True(t, tt.exp.Equals(tt.testexpra))
			assert.False(t, tt.exp.Equals(tt.testexprb))
		})
	}
}

func TestBoolExprNegate(t *testing.T) {
	tests := []struct {
		lhs, rhs iceberg.BooleanExpression
	}{
		{iceberg.NewAnd(ExprA{}, ExprB{}), iceberg.NewOr(ExprB{}, ExprA{})},
		{iceberg.NewOr(ExprB{}, ExprA{}), iceberg.NewAnd(ExprA{}, ExprB{})},
		{iceberg.NewNot(ExprA{}), ExprA{}},
		{
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.NotIn(iceberg.Reference("foo"), "hello", "world"),
		},
		{
			iceberg.NotIn(iceberg.Reference("foo"), "hello", "world"),
			iceberg.IsIn(iceberg.Reference("foo"), "hello", "world"),
		},
		{
			iceberg.GreaterThan(iceberg.Reference("foo"), int32(5)),
			iceberg.LessThanEqual(iceberg.Reference("foo"), int32(5)),
		},
		{
			iceberg.LessThan(iceberg.Reference("foo"), int32(5)),
			iceberg.GreaterThanEqual(iceberg.Reference("foo"), int32(5)),
		},
		{
			iceberg.EqualTo(iceberg.Reference("foo"), int32(5)),
			iceberg.NotEqualTo(iceberg.Reference("foo"), int32(5)),
		},
		{ExprA{}, ExprB{}},
	}

	for _, tt := range tests {
		assert.True(t, tt.lhs.Negate().Equals(tt.rhs))
	}
}

func TestBoolExprPanics(t *testing.T) {
	assert.PanicsWithError(t, "invalid argument: cannot construct AndExpr with nil arguments",
		func() { iceberg.NewAnd(nil, ExprA{}) })
	assert.PanicsWithError(t, "invalid argument: cannot construct AndExpr with nil arguments",
		func() { iceberg.NewAnd(ExprA{}, nil) })
	assert.PanicsWithError(t, "invalid argument: cannot construct AndExpr with nil arguments",
		func() { iceberg.NewAnd(ExprA{}, ExprA{}, nil) })

	assert.PanicsWithError(t, "invalid argument: cannot construct OrExpr with nil arguments",
		func() { iceberg.NewOr(nil, ExprA{}) })
	assert.PanicsWithError(t, "invalid argument: cannot construct OrExpr with nil arguments",
		func() { iceberg.NewOr(ExprA{}, nil) })
	assert.PanicsWithError(t, "invalid argument: cannot construct OrExpr with nil arguments",
		func() { iceberg.NewOr(ExprA{}, ExprA{}, nil) })

	assert.PanicsWithError(t, "invalid argument: cannot create NotExpr with nil child",
		func() { iceberg.NewNot(nil) })
}

func TestExprFolding(t *testing.T) {
	tests := []struct {
		lhs, rhs iceberg.BooleanExpression
	}{
		{
			iceberg.NewAnd(ExprA{}, ExprB{}, ExprA{}),
			iceberg.NewAnd(iceberg.NewAnd(ExprA{}, ExprB{}), ExprA{}),
		},
		{
			iceberg.NewOr(ExprA{}, ExprB{}, ExprA{}),
			iceberg.NewOr(iceberg.NewOr(ExprA{}, ExprB{}), ExprA{}),
		},
		{iceberg.NewNot(iceberg.NewNot(ExprA{})), ExprA{}},
	}

	for _, tt := range tests {
		assert.True(t, tt.lhs.Equals(tt.rhs))
	}
}

func TestBaseAlwaysTrueAlwaysFalse(t *testing.T) {
	tests := []struct {
		lhs, rhs iceberg.BooleanExpression
	}{
		{iceberg.NewAnd(iceberg.AlwaysTrue{}, ExprB{}), ExprB{}},
		{iceberg.NewAnd(iceberg.AlwaysFalse{}, ExprB{}), iceberg.AlwaysFalse{}},
		{iceberg.NewAnd(ExprB{}, iceberg.AlwaysTrue{}), ExprB{}},
		{iceberg.NewOr(iceberg.AlwaysTrue{}, ExprB{}), iceberg.AlwaysTrue{}},
		{iceberg.NewOr(iceberg.AlwaysFalse{}, ExprB{}), ExprB{}},
		{iceberg.NewOr(ExprA{}, iceberg.AlwaysFalse{}), ExprA{}},
		{iceberg.NewNot(iceberg.NewNot(ExprA{})), ExprA{}},
		{iceberg.NewNot(iceberg.AlwaysTrue{}), iceberg.AlwaysFalse{}},
		{iceberg.NewNot(iceberg.AlwaysFalse{}), iceberg.AlwaysTrue{}},
	}

	for _, tt := range tests {
		assert.True(t, tt.lhs.Equals(tt.rhs))
	}
}

func TestNegateAlways(t *testing.T) {
	assert.Equal(t, iceberg.OpTrue, iceberg.AlwaysTrue{}.Op())
	assert.Equal(t, iceberg.OpFalse, iceberg.AlwaysFalse{}.Op())

	assert.Equal(t, iceberg.AlwaysTrue{}, iceberg.AlwaysFalse{}.Negate())
	assert.Equal(t, iceberg.AlwaysFalse{}, iceberg.AlwaysTrue{}.Negate())
}

func TestBoundReferenceToString(t *testing.T) {
	ref, err := iceberg.Reference("foo").Bind(tableSchemaSimple, true)
	require.NoError(t, err)

	assert.Equal(t, "BoundReference(field=1: foo: optional string, accessor=Accessor(position=0, inner=<nil>))",
		ref.String())
}

func TestToString(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "c", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "d", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 5, Name: "e", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 6, Name: "f", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 7, Name: "g", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 8, Name: "h", Type: iceberg.DecimalTypeOf(8, 4)},
		iceberg.NestedField{ID: 9, Name: "i", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 10, Name: "j", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 11, Name: "k", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 12, Name: "l", Type: iceberg.PrimitiveTypes.Binary})

	null := iceberg.IsNull(iceberg.Reference("a"))
	nan := iceberg.IsNaN(iceberg.Reference("g"))
	boundNull, _ := null.Bind(schema, true)
	boundNan, _ := nan.Bind(schema, true)

	equal := iceberg.EqualTo(iceberg.Reference("c"), "a")
	grtequal := iceberg.GreaterThanEqual(iceberg.Reference("a"), "a")
	greater := iceberg.GreaterThan(iceberg.Reference("a"), "a")
	startsWith := iceberg.StartsWith(iceberg.Reference("b"), "foo")

	boundEqual, _ := equal.Bind(schema, true)
	boundGrtEqual, _ := grtequal.Bind(schema, true)
	boundGreater, _ := greater.Bind(schema, true)
	boundStarts, _ := startsWith.Bind(schema, true)

	tests := []struct {
		e        iceberg.BooleanExpression
		expected string
	}{
		{
			iceberg.NewAnd(null, nan),
			"And(left=IsNull(term=Reference(name='a')), right=IsNaN(term=Reference(name='g')))",
		},
		{
			iceberg.NewOr(null, nan),
			"Or(left=IsNull(term=Reference(name='a')), right=IsNaN(term=Reference(name='g')))",
		},
		{
			iceberg.NewNot(null),
			"Not(child=IsNull(term=Reference(name='a')))",
		},
		{iceberg.AlwaysTrue{}, "AlwaysTrue()"},
		{iceberg.AlwaysFalse{}, "AlwaysFalse()"},
		{
			boundNull,
			"BoundIsNull(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0, inner=<nil>)))",
		},
		{
			boundNull.Negate(),
			"BoundNotNull(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0, inner=<nil>)))",
		},
		{
			boundNan,
			"BoundIsNaN(term=BoundReference(field=7: g: optional float, accessor=Accessor(position=6, inner=<nil>)))",
		},
		{
			boundNan.Negate(),
			"BoundNotNaN(term=BoundReference(field=7: g: optional float, accessor=Accessor(position=6, inner=<nil>)))",
		},
		{
			equal,
			"Equal(term=Reference(name='c'), literal=a)",
		},
		{
			equal.Negate(),
			"NotEqual(term=Reference(name='c'), literal=a)",
		},
		{
			grtequal,
			"GreaterThanEqual(term=Reference(name='a'), literal=a)",
		},
		{
			grtequal.Negate(),
			"LessThan(term=Reference(name='a'), literal=a)",
		},
		{
			greater,
			"GreaterThan(term=Reference(name='a'), literal=a)",
		},
		{
			greater.Negate(),
			"LessThanEqual(term=Reference(name='a'), literal=a)",
		},
		{
			startsWith,
			"StartsWith(term=Reference(name='b'), literal=foo)",
		},
		{
			startsWith.Negate(),
			"NotStartsWith(term=Reference(name='b'), literal=foo)",
		},
		{
			boundEqual,
			"BoundEqual(term=BoundReference(field=3: c: optional string, accessor=Accessor(position=2, inner=<nil>)), literal=a)",
		},
		{
			boundEqual.Negate(),
			"BoundNotEqual(term=BoundReference(field=3: c: optional string, accessor=Accessor(position=2, inner=<nil>)), literal=a)",
		},
		{
			boundGreater,
			"BoundGreaterThan(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0, inner=<nil>)), literal=a)",
		},
		{
			boundGreater.Negate(),
			"BoundLessThanEqual(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0, inner=<nil>)), literal=a)",
		},
		{
			boundGrtEqual,
			"BoundGreaterThanEqual(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0, inner=<nil>)), literal=a)",
		},
		{
			boundGrtEqual.Negate(),
			"BoundLessThan(term=BoundReference(field=1: a: optional string, accessor=Accessor(position=0, inner=<nil>)), literal=a)",
		},
		{
			boundStarts,
			"BoundStartsWith(term=BoundReference(field=2: b: optional string, accessor=Accessor(position=1, inner=<nil>)), literal=foo)",
		},
		{
			boundStarts.Negate(),
			"BoundNotStartsWith(term=BoundReference(field=2: b: optional string, accessor=Accessor(position=1, inner=<nil>)), literal=foo)",
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.e.String())
	}
}

func TestBindAboveBelowIntMax(t *testing.T) {
	sc := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Float32},
	)

	ref, ref2 := iceberg.Reference("a"), iceberg.Reference("b")
	above, below := int64(math.MaxInt32)+1, int64(math.MinInt32)-1
	above2, below2 := float64(math.MaxFloat32)+1e37, float64(-math.MaxFloat32)-1e37

	tests := []struct {
		pred iceberg.UnboundPredicate
		exp  iceberg.BooleanExpression
	}{
		{iceberg.EqualTo(ref, above), iceberg.AlwaysFalse{}},
		{iceberg.EqualTo(ref, below), iceberg.AlwaysFalse{}},
		{iceberg.NotEqualTo(ref, above), iceberg.AlwaysTrue{}},
		{iceberg.NotEqualTo(ref, below), iceberg.AlwaysTrue{}},
		{iceberg.LessThan(ref, above), iceberg.AlwaysTrue{}},
		{iceberg.LessThan(ref, below), iceberg.AlwaysFalse{}},
		{iceberg.LessThanEqual(ref, above), iceberg.AlwaysTrue{}},
		{iceberg.LessThanEqual(ref, below), iceberg.AlwaysFalse{}},
		{iceberg.GreaterThan(ref, above), iceberg.AlwaysFalse{}},
		{iceberg.GreaterThan(ref, below), iceberg.AlwaysTrue{}},
		{iceberg.GreaterThanEqual(ref, above), iceberg.AlwaysFalse{}},
		{iceberg.GreaterThanEqual(ref, below), iceberg.AlwaysTrue{}},

		{iceberg.EqualTo(ref2, above2), iceberg.AlwaysFalse{}},
		{iceberg.EqualTo(ref2, below2), iceberg.AlwaysFalse{}},
		{iceberg.NotEqualTo(ref2, above2), iceberg.AlwaysTrue{}},
		{iceberg.NotEqualTo(ref2, below2), iceberg.AlwaysTrue{}},
		{iceberg.LessThan(ref2, above2), iceberg.AlwaysTrue{}},
		{iceberg.LessThan(ref2, below2), iceberg.AlwaysFalse{}},
		{iceberg.LessThanEqual(ref2, above2), iceberg.AlwaysTrue{}},
		{iceberg.LessThanEqual(ref2, below2), iceberg.AlwaysFalse{}},
		{iceberg.GreaterThan(ref2, above2), iceberg.AlwaysFalse{}},
		{iceberg.GreaterThan(ref2, below2), iceberg.AlwaysTrue{}},
		{iceberg.GreaterThanEqual(ref2, above2), iceberg.AlwaysFalse{}},
		{iceberg.GreaterThanEqual(ref2, below2), iceberg.AlwaysTrue{}},
	}

	for _, tt := range tests {
		t.Run(tt.pred.String(), func(t *testing.T) {
			b, err := tt.pred.Bind(sc, true)
			require.NoError(t, err)
			assert.Equal(t, tt.exp, b)
		})
	}
}
