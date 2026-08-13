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

	"github.com/DataDog/iceberg-go/internal"
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
