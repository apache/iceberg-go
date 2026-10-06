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
	"slices"
	"strconv"
	"testing"

	iceberg "github.com/apache/iceberg-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoundSetPredicateLiteralsCloneMutableMembers(t *testing.T) {
	t.Parallel()

	geometry, err := iceberg.GeometryTypeOf("srid:4326")
	require.NoError(t, err)
	geography, err := iceberg.GeographyTypeOf("srid:4326", "spherical")
	require.NoError(t, err)

	tests := []struct {
		name string
		typ  iceberg.Type
		op   iceberg.Operation
	}{
		{name: "binary_in", typ: iceberg.PrimitiveTypes.Binary, op: iceberg.OpIn},
		{name: "binary_not_in", typ: iceberg.PrimitiveTypes.Binary, op: iceberg.OpNotIn},
		{name: "fixed_in", typ: iceberg.FixedTypeOf(16), op: iceberg.OpIn},
		{name: "fixed_not_in", typ: iceberg.FixedTypeOf(16), op: iceberg.OpNotIn},
		{name: "geometry_in", typ: geometry, op: iceberg.OpIn},
		{name: "geometry_not_in", typ: geometry, op: iceberg.OpNotIn},
		{name: "geography_in", typ: geography, op: iceberg.OpIn},
		{name: "geography_not_in", typ: geography, op: iceberg.OpNotIn},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inputs := [][]byte{
				[]byte("0123456789abcdef"),
				[]byte("fedcba9876543210"),
			}
			predicateLiterals := make([]iceberg.Literal, len(inputs))
			expectedLiterals := make([]iceberg.Literal, len(inputs))
			for i, input := range inputs {
				expected, err := iceberg.LiteralFromBytes(tt.typ, slices.Clone(input))
				require.NoError(t, err)
				expectedLiterals[i] = expected

				if tt.typ.Equals(geometry) || tt.typ.Equals(geography) {
					predicateLiterals[i], err = iceberg.LiteralFromBytes(tt.typ, slices.Clone(input))
					require.NoError(t, err)
				} else {
					predicateLiterals[i] = iceberg.NewLiteral(slices.Clone(input))
				}
			}

			predicate := iceberg.SetPredicate(
				tt.op,
				iceberg.Reference("value"),
				predicateLiterals,
			)
			schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: tt.typ})

			bound, err := predicate.(iceberg.UnboundPredicate).Bind(schema, true)
			require.NoError(t, err)
			literals := bound.(iceberg.BoundSetPredicate).Literals()

			for _, literal := range literals.Members() {
				value, ok := literal.Any().([]byte)
				require.True(t, ok)
				value[0] ^= 0xff
			}
			for _, expected := range expectedLiterals {
				assert.True(t, bound.(iceberg.BoundSetPredicate).Literals().Contains(expected))
			}

			assert.True(t, literals.All(func(literal iceberg.Literal) bool {
				value, ok := literal.Any().([]byte)
				assert.True(t, ok)
				if !ok {
					return false
				}
				value[0] ^= 0xff

				return true
			}))
			for _, expected := range expectedLiterals {
				assert.True(t, bound.(iceberg.BoundSetPredicate).Literals().Contains(expected))
			}
		})
	}
}

func TestBoundSetPredicateLiteralsIndependentCopies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		typ     iceberg.Type
		literal func(int) iceberg.Literal
	}{
		{
			name: "int32",
			typ:  iceberg.PrimitiveTypes.Int32,
			literal: func(i int) iceberg.Literal {
				return iceberg.NewLiteral(int32(i))
			},
		},
		{
			name: "string",
			typ:  iceberg.PrimitiveTypes.String,
			literal: func(i int) iceberg.Literal {
				return iceberg.NewLiteral(strconv.Itoa(i))
			},
		},
	}

	for _, tt := range tests {
		for _, op := range []iceberg.Operation{iceberg.OpIn, iceberg.OpNotIn} {
			for _, size := range []int{2, 8, 64, 1024} {
				t.Run(tt.name+"/"+op.String()+"/"+strconv.Itoa(size), func(t *testing.T) {
					t.Parallel()

					values := make([]iceberg.Literal, size)
					for i := range values {
						values[i] = tt.literal(i)
					}
					predicate := iceberg.SetPredicate(op, iceberg.Reference("value"), append(values, values[0]))
					schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: tt.typ})
					bound, err := predicate.(iceberg.UnboundPredicate).Bind(schema, true)
					require.NoError(t, err)

					setPredicate := bound.(iceberg.BoundSetPredicate)
					first, second := setPredicate.Literals(), setPredicate.Literals()
					require.Equal(t, size, first.Len())
					require.Equal(t, size, second.Len())
					for _, literal := range values {
						assert.True(t, first.Contains(literal))
						assert.True(t, second.Contains(literal))
					}

					injected := tt.literal(size)
					first.Add(injected)
					assert.True(t, first.Contains(injected))
					assert.False(t, second.Contains(injected))
					assert.False(t, setPredicate.Literals().Contains(injected))
					assert.Equal(t, size, second.Len())
					assert.Equal(t, size, setPredicate.Literals().Len())
				})
			}
		}
	}
}
