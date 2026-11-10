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

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestManifestEvaluatorExcludedSingleValue(t *testing.T) {
	nanFalse, nanTrue := false, true
	ref := iceberg.Reference("value")
	intSeven := iceberg.NewLiteral(int32(7))
	intEight := iceberg.NewLiteral(int32(8))
	floatSeven := iceberg.NewLiteral(float32(7))
	stringABC := iceberg.NewLiteral("abc")
	binaryABC := iceberg.NewLiteral([]byte("abc"))

	tests := []struct {
		name         string
		typ          iceberg.Type
		lower        iceberg.Literal
		upper        iceberg.Literal
		containsNull bool
		containsNaN  *bool
		filter       iceberg.BooleanExpression
		want         bool
	}{
		{
			name:  "not equal excludes the only value",
			typ:   iceberg.PrimitiveTypes.Int32,
			lower: intSeven, upper: intSeven,
			filter: iceberg.NotEqualTo(ref, int32(7)),
		},
		{
			name:  "not equal keeps a different value",
			typ:   iceberg.PrimitiveTypes.Int32,
			lower: intSeven, upper: intSeven,
			filter: iceberg.NotEqualTo(ref, int32(8)), want: true,
		},
		{
			name:  "not in excludes the only value",
			typ:   iceberg.PrimitiveTypes.Int32,
			lower: intSeven, upper: intSeven,
			filter: iceberg.NotIn(ref, int32(5), int32(7)),
		},
		{
			name:  "not in keeps a value outside the set",
			typ:   iceberg.PrimitiveTypes.Int32,
			lower: intSeven, upper: intSeven,
			filter: iceberg.NotIn(ref, int32(5), int32(6)), want: true,
		},
		{
			name:  "different bounds cannot prove one value",
			typ:   iceberg.PrimitiveTypes.Int32,
			lower: intSeven, upper: intEight,
			filter: iceberg.NotEqualTo(ref, int32(7)), want: true,
		},
		{
			name:  "nulls may match not equal",
			typ:   iceberg.PrimitiveTypes.Int32,
			lower: intSeven, upper: intSeven, containsNull: true,
			filter: iceberg.NotEqualTo(ref, int32(7)), want: true,
		},
		{
			name:  "nulls may match not in",
			typ:   iceberg.PrimitiveTypes.Int32,
			lower: intSeven, upper: intSeven, containsNull: true,
			filter: iceberg.NotIn(ref, int32(5), int32(7)), want: true,
		},
		{
			name:   "missing lower bound is unknown",
			typ:    iceberg.PrimitiveTypes.Int32,
			upper:  intSeven,
			filter: iceberg.NotEqualTo(ref, int32(7)), want: true,
		},
		{
			name:   "missing upper bound is unknown",
			typ:    iceberg.PrimitiveTypes.Int32,
			lower:  intSeven,
			filter: iceberg.NotIn(ref, int32(5), int32(7)), want: true,
		},
		{
			name:   "missing both bounds is unknown",
			typ:    iceberg.PrimitiveTypes.Int32,
			filter: iceberg.NotEqualTo(ref, int32(7)), want: true,
		},
		{
			name:  "unknown float NaN presence keeps manifest",
			typ:   iceberg.PrimitiveTypes.Float32,
			lower: floatSeven, upper: floatSeven,
			filter: iceberg.NotEqualTo(ref, float32(7)), want: true,
		},
		{
			name:  "known float NaNs keep manifest",
			typ:   iceberg.PrimitiveTypes.Float32,
			lower: floatSeven, upper: floatSeven, containsNaN: &nanTrue,
			filter: iceberg.NotEqualTo(ref, float32(7)), want: true,
		},
		{
			name:  "known absent NaNs allow not equal pruning",
			typ:   iceberg.PrimitiveTypes.Float32,
			lower: floatSeven, upper: floatSeven, containsNaN: &nanFalse,
			filter: iceberg.NotEqualTo(ref, float32(7)),
		},
		{
			name:  "known absent NaNs allow not in pruning",
			typ:   iceberg.PrimitiveTypes.Float32,
			lower: floatSeven, upper: floatSeven, containsNaN: &nanFalse,
			filter: iceberg.NotIn(ref, float32(7), float32(8)),
		},
		{
			name:  "string single value prunes",
			typ:   iceberg.PrimitiveTypes.String,
			lower: stringABC, upper: stringABC,
			filter: iceberg.NotEqualTo(ref, "abc"),
		},
		{
			name:  "binary single value prunes",
			typ:   iceberg.PrimitiveTypes.Binary,
			lower: binaryABC, upper: binaryABC,
			filter: iceberg.NotIn(ref, []byte("abc"), []byte("def")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(1, iceberg.NestedField{
				ID: 1, Name: "value", Type: tt.typ, Required: false,
			})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1},
				FieldID:   1000,
				Name:      "value",
				Transform: iceberg.IdentityTransform{},
			})

			boundBytes := func(lit iceberg.Literal) *[]byte {
				if lit == nil {
					return nil
				}
				b, err := lit.MarshalBinary()
				require.NoError(t, err)
				return &b
			}
			manifest := iceberg.NewManifestBuilder(2, "manifest.avro", 1, 0, 1).
				Partitions([]iceberg.FieldSummary{{
					ContainsNull: tt.containsNull,
					ContainsNaN:  tt.containsNaN,
					LowerBound:   boundBytes(tt.lower),
					UpperBound:   boundBytes(tt.upper),
				}}).Build()

			eval, err := newManifestEvaluator(spec, schema, tt.filter, true)
			require.NoError(t, err)
			got, err := eval(manifest)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
