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
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiteralSetExtrema(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		minLit, maxLit, ok := literalSetExtrema[int32](newLiteralSet(
			NewLiteral(int32(7)), NewLiteral(int32(-2)), NewLiteral(int32(4))).(literalSet))
		require.True(t, ok)
		assert.Equal(t, Int32Literal(-2), minLit)
		assert.Equal(t, Int32Literal(7), maxLit)
	})

	t.Run("binary", func(t *testing.T) {
		minLit, maxLit, ok := literalSetExtrema[[]byte](newLiteralSet(
			NewLiteral([]byte("z")), NewLiteral([]byte("a")), NewLiteral([]byte("m"))).(literalSet))
		require.True(t, ok)
		assert.Equal(t, BinaryLiteral("a"), minLit)
		assert.Equal(t, BinaryLiteral("z"), maxLit)
	})

	t.Run("geo has no ordering", func(t *testing.T) {
		geometry := GeometryType{}
		first, err := LiteralFromBytes(geometry, []byte("1234567890abcdef"))
		require.NoError(t, err)
		second, err := LiteralFromBytes(geometry, []byte("fedcba9876543210"))
		require.NoError(t, err)

		_, _, ok := literalSetExtrema[[]byte](newLiteralSet(first, second).(literalSet))
		assert.False(t, ok)
	})
}

type boundSetExtremaVisitVisitor struct {
	boundSetVisitVisitor
	minLit, maxLit Literal
}

func (v *boundSetExtremaVisitVisitor) VisitInWithExtrema(_ BoundTerm, _ Set[Literal], minLit, maxLit Literal) bool {
	v.minLit, v.maxLit = minLit, maxLit

	return true
}

type trustedBoundSetExtremaVisitVisitor struct {
	boundSetExtremaVisitVisitor
}

func (*trustedBoundSetExtremaVisitVisitor) BorrowBoundSets(internal.BoundPredicateRef) {}

func TestVisitBoundPredicateTrustedPassesInExtrema(t *testing.T) {
	predicate, err := IsIn(Reference("value"), "world", "hello", "ice").(UnboundPredicate).Bind(
		NewSchema(1, NestedField{ID: 1, Name: "value", Type: PrimitiveTypes.String}), true,
	)
	require.NoError(t, err)

	untrusted := boundSetExtremaVisitVisitor{boundSetVisitVisitor: boundSetVisitVisitor{needle: NewLiteral("hello")}}
	visitor := &trustedBoundSetExtremaVisitVisitor{untrusted}
	bound := predicate.(BoundPredicate)
	var found bool

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		found = VisitBoundPredicate(bound, visitor)
	}))
	assert.True(t, found)
	assert.Equal(t, StringLiteral("hello"), visitor.minLit)
	assert.Equal(t, StringLiteral("world"), visitor.maxLit)

	// Without the marker, VisitIn is called instead.
	found = VisitBoundPredicate(bound, &untrusted)
	assert.True(t, found)
	assert.Nil(t, untrusted.minLit)
	assert.Nil(t, untrusted.maxLit)
}

func TestVisitBoundPredicateTrustedGeoSkipsExtrema(t *testing.T) {
	geometry := GeometryType{}
	first, err := LiteralFromBytes(geometry, []byte("1234567890abcdef"))
	require.NoError(t, err)
	second, err := LiteralFromBytes(geometry, []byte("fedcba9876543210"))
	require.NoError(t, err)

	schema := NewSchema(1, NestedField{ID: 1, Name: "value", Type: geometry})
	bound, err := BindExpr(schema, SetPredicate(OpIn, Reference("value"), []Literal{first, second}), true)
	require.NoError(t, err)
	pred, ok := bound.(BoundPredicate)
	require.True(t, ok)

	visitor := &trustedBoundSetExtremaVisitVisitor{
		boundSetExtremaVisitVisitor{boundSetVisitVisitor: boundSetVisitVisitor{needle: first}},
	}
	assert.True(t, VisitBoundPredicate(pred, visitor))
	assert.Nil(t, visitor.minLit)
	assert.Nil(t, visitor.maxLit)
}
