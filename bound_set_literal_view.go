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
	"slices"
)

type readOnlyLiteralSet struct {
	Set[Literal]
}

func (readOnlyLiteralSet) Add(...Literal) {
	panic(fmt.Errorf("%w: cannot add to read-only literal set", ErrInvalidArgument))
}

func (s readOnlyLiteralSet) Equals(other Set[Literal]) bool {
	if rhs, ok := other.(readOnlyLiteralSet); ok {
		other = rhs.Set
	}

	return s.Set.Equals(other)
}

func cloneBoundLiteral(lit Literal) Literal {
	switch lit := lit.(type) {
	case BinaryLiteral:
		return BinaryLiteral(slices.Clone(lit))
	case FixedLiteral:
		return FixedLiteral(slices.Clone(lit))
	case GeoLiteral:
		lit.val = slices.Clone(lit.val)

		return lit
	default:
		return lit
	}
}

func (s readOnlyLiteralSet) Members() []Literal {
	members := s.Set.Members()
	for i, literal := range members {
		members[i] = cloneBoundLiteral(literal)
	}

	return members
}

func (s readOnlyLiteralSet) All(fn func(Literal) bool) bool {
	return s.Set.All(func(literal Literal) bool {
		return fn(cloneBoundLiteral(literal))
	})
}

type boundSetLiteralRef interface {
	boundSetLiteralsRef() Set[Literal]
}

func (bsp *boundSetPredicate[T]) boundSetLiteralsRef() Set[Literal] {
	return bsp.lits
}

func boundSetLiteralsForVisit(predicate BoundPredicate) Set[Literal] {
	if ref, ok := predicate.(boundSetLiteralRef); ok {
		return ref.boundSetLiteralsRef()
	}

	setPredicate, ok := predicate.(BoundSetPredicate)
	if !ok {
		panic(fmt.Errorf("%w: %s predicate %T does not implement BoundSetPredicate",
			ErrNotImplemented, predicate.Op(), predicate))
	}

	return setPredicate.Literals()
}
