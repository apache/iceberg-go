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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestLiteralFromBytesPromotedNumericBounds(t *testing.T) {
	tests := []struct {
		name     string
		original iceberg.Literal
		promoted iceberg.Type
		want     iceberg.Literal
	}{
		{"int to long positive", iceberg.Int32Literal(123), iceberg.PrimitiveTypes.Int64, iceberg.Int64Literal(123)},
		{"int to long negative", iceberg.Int32Literal(-123), iceberg.PrimitiveTypes.Int64, iceberg.Int64Literal(-123)},
		{"int to long minimum", iceberg.Int32Literal(math.MinInt32), iceberg.PrimitiveTypes.Int64, iceberg.Int64Literal(math.MinInt32)},
		{"int to long maximum", iceberg.Int32Literal(math.MaxInt32), iceberg.PrimitiveTypes.Int64, iceberg.Int64Literal(math.MaxInt32)},
		{"float to double positive", iceberg.Float32Literal(1.25), iceberg.PrimitiveTypes.Float64, iceberg.Float64Literal(1.25)},
		{"float to double negative", iceberg.Float32Literal(-1.25), iceberg.PrimitiveTypes.Float64, iceberg.Float64Literal(-1.25)},
		{"float to double subnormal", iceberg.Float32Literal(math.SmallestNonzeroFloat32), iceberg.PrimitiveTypes.Float64, iceberg.Float64Literal(math.SmallestNonzeroFloat32)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.original.MarshalBinary()
			require.NoError(t, err)
			require.Len(t, raw, 4)

			got, err := iceberg.LiteralFromBytes(tt.promoted, raw)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestLiteralFromBytesPromotedFloatPreservesNegativeZero(t *testing.T) {
	raw, err := iceberg.Float32Literal(float32(math.Copysign(0, -1))).MarshalBinary()
	require.NoError(t, err)

	got, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Float64, raw)
	require.NoError(t, err)
	require.True(t, math.Signbit(float64(got.(iceberg.Float64Literal))))
}

func TestLiteralFromBytesPromotedNumericBoundsNativeAndInvalidWidths(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  iceberg.Type
		lit  iceberg.Literal
	}{
		{"long", iceberg.PrimitiveTypes.Int64, iceberg.Int64Literal(-4294967296)},
		{"double", iceberg.PrimitiveTypes.Float64, iceberg.Float64Literal(-1.125)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			native, err := tt.lit.MarshalBinary()
			require.NoError(t, err)
			require.Len(t, native, 8)

			got, err := iceberg.LiteralFromBytes(tt.typ, native)
			require.NoError(t, err)
			require.Equal(t, tt.lit, got)

			for _, length := range []int{0, 1, 2, 3, 5, 6, 7, 9} {
				_, err := iceberg.LiteralFromBytes(tt.typ, make([]byte, length))
				require.ErrorIs(t, err, iceberg.ErrInvalidBinSerialization, "length %d", length)
			}
		})
	}
}
