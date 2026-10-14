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
	"math"
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type boundSetVisitVisitor struct {
	needle Literal
}

func (*boundSetVisitVisitor) VisitTrue() bool                    { return true }
func (*boundSetVisitVisitor) VisitFalse() bool                   { return false }
func (*boundSetVisitVisitor) VisitNot(bool) bool                 { return false }
func (*boundSetVisitVisitor) VisitAnd(bool, bool) bool           { return false }
func (*boundSetVisitVisitor) VisitOr(bool, bool) bool            { return false }
func (*boundSetVisitVisitor) VisitUnbound(UnboundPredicate) bool { return false }
func (*boundSetVisitVisitor) VisitBound(BoundPredicate) bool     { return false }
func (v *boundSetVisitVisitor) VisitIn(_ BoundTerm, lits Set[Literal]) bool {
	return lits.Contains(v.needle)
}
func (*boundSetVisitVisitor) VisitNotIn(BoundTerm, Set[Literal]) bool { return false }
func (*boundSetVisitVisitor) VisitIsNan(BoundTerm) bool               { return false }
func (*boundSetVisitVisitor) VisitNotNan(BoundTerm) bool              { return false }
func (*boundSetVisitVisitor) VisitIsNull(BoundTerm) bool              { return false }
func (*boundSetVisitVisitor) VisitNotNull(BoundTerm) bool             { return false }
func (*boundSetVisitVisitor) VisitEqual(BoundTerm, Literal) bool      { return false }
func (*boundSetVisitVisitor) VisitNotEqual(BoundTerm, Literal) bool   { return false }
func (*boundSetVisitVisitor) VisitGreaterEqual(BoundTerm, Literal) bool {
	return false
}
func (*boundSetVisitVisitor) VisitGreater(BoundTerm, Literal) bool { return false }
func (*boundSetVisitVisitor) VisitLessEqual(BoundTerm, Literal) bool {
	return false
}
func (*boundSetVisitVisitor) VisitLess(BoundTerm, Literal) bool { return false }
func (*boundSetVisitVisitor) VisitStartsWith(BoundTerm, Literal) bool {
	return false
}

func (*boundSetVisitVisitor) VisitNotStartsWith(BoundTerm, Literal) bool {
	return false
}

func TestVisitBoundPredicateRefDoesNotAllocate(t *testing.T) {
	predicate, err := IsIn(Reference("value"), "hello", "world").(UnboundPredicate).Bind(
		NewSchema(1, NestedField{ID: 1, Name: "value", Type: PrimitiveTypes.String}), true,
	)
	require.NoError(t, err)

	visitor := &boundSetVisitVisitor{needle: NewLiteral("hello")}
	bound := predicate.(BoundPredicate)
	var found bool

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		found = VisitBoundPredicateRef(bound, visitor, internal.BoundPredicateRef{})
	}))
	assert.True(t, found)
}

func TestCloneBoundLiteralSetEmptyCopiesAreWritable(t *testing.T) {
	for _, original := range []literalSet{nil, {}} {
		cloned := cloneBoundLiteralSet(original)
		assert.Zero(t, cloned.Len())
		cloned.Add(NewLiteral(int32(42)))
		assert.True(t, cloned.Contains(NewLiteral(int32(42))))
		assert.Zero(t, original.Len())
	}
}

func TestBoundSetPredicateLiteralsPreserveFloatingPointMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		typ    Type
		values []Literal
		bits   func(Literal) uint64
	}{
		{
			name: "float32",
			typ:  PrimitiveTypes.Float32,
			values: []Literal{
				NewLiteral(math.Float32frombits(0x7fc00001)),
				NewLiteral(math.Float32frombits(0x7fc00001)),
				NewLiteral(math.Float32frombits(0x7fc00002)),
				NewLiteral(float32(0)),
				NewLiteral(math.Float32frombits(0x80000000)),
				NewLiteral(float32(math.Inf(-1))),
				NewLiteral(float32(math.Inf(1))),
				NewLiteral(float32(1)),
			},
			bits: func(lit Literal) uint64 {
				return uint64(math.Float32bits(float32(lit.(Float32Literal))))
			},
		},
		{
			name: "float64",
			typ:  PrimitiveTypes.Float64,
			values: []Literal{
				NewLiteral(math.Float64frombits(0x7ff8000000000001)),
				NewLiteral(math.Float64frombits(0x7ff8000000000001)),
				NewLiteral(math.Float64frombits(0x7ff8000000000002)),
				NewLiteral(float64(0)),
				NewLiteral(math.Float64frombits(0x8000000000000000)),
				NewLiteral(math.Inf(-1)),
				NewLiteral(math.Inf(1)),
				NewLiteral(float64(1)),
			},
			bits: func(lit Literal) uint64 {
				return math.Float64bits(float64(lit.(Float64Literal)))
			},
		},
	}
	for _, tt := range tests {
		for _, op := range []Operation{OpIn, OpNotIn} {
			t.Run(tt.name+"/"+op.String(), func(t *testing.T) {
				t.Parallel()

				schema := NewSchema(1, NestedField{ID: 1, Name: "value", Type: tt.typ})
				bound, err := SetPredicate(op, Reference("value"), tt.values).(UnboundPredicate).Bind(schema, true)
				require.NoError(t, err)
				original := bound.(boundSetLiteralRef).boundSetLiteralsRef()
				cloned := bound.(BoundSetPredicate).Literals()
				require.Equal(t, 7, original.Len())
				require.Equal(t, original.Len(), cloned.Len())

				counts := func(lits Set[Literal]) map[uint64]int {
					bits := make(map[uint64]int)
					lits.All(func(lit Literal) bool {
						bits[tt.bits(lit)]++

						return true
					})

					return bits
				}
				assert.Equal(t, counts(original), counts(cloned))
			})
		}
	}
}
