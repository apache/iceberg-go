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

package substrait

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v8/types"
)

// decimalLiteral16Bytes builds a DecimalLiteral whose MarshalBinary output is
// exactly 16 bytes. expr.NewLiteral (substrait-go) rejects decimal values that
// are not 16 bytes long; DecimalLiteral.MarshalBinary uses the minimum byte
// representation, so we choose an unscaled value with bit length in [121,128]
// to force a 16-byte big-endian encoding.
func decimalLiteral16Bytes(scale int) iceberg.DecimalLiteral {
	// 2^120: BigInt.BitLen() == 121, requires 16 bytes.
	val := decimal128.New(1<<56, 0)

	return iceberg.DecimalLiteral{Val: val, Scale: scale}
}

// TestToDecimalLiteralUsesBoundFieldPrecision pins the invariant that the
// emitted Substrait decimal precision comes from the bound field's
// iceberg.DecimalType, not from the literal. DecimalLiteral does not carry
// the originating column's declared precision, so v.Type() reports a
// hardcoded precision of 9; sourcing precision from v.Type() turns every
// subcase below red. See https://github.com/apache/iceberg-go/issues/1028.
func TestToDecimalLiteralUsesBoundFieldPrecision(t *testing.T) {
	cases := []struct {
		name      string
		precision int
		scale     int
	}{
		{"decimal(38,4)", 38, 4},
		{"decimal(38,10)", 38, 10},
		{"decimal(38,2)", 38, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fieldType := iceberg.DecimalTypeOf(tc.precision, tc.scale)
			lit := decimalLiteral16Bytes(tc.scale)

			got := toDecimalLiteral(fieldType, lit)
			require.NotNil(t, got, "toDecimalLiteral must not silently return nil")

			dt, ok := got.GetType().(*types.DecimalType)
			require.Truef(t, ok, "expected *types.DecimalType, got %T", got.GetType())

			assert.Equalf(t, int32(tc.precision), dt.Precision,
				"emitted precision must equal bound field precision; got decimal<%d,%d>",
				dt.Precision, dt.Scale)
			assert.Equalf(t, int32(tc.scale), dt.Scale,
				"emitted scale must equal literal scale; got decimal<%d,%d>",
				dt.Precision, dt.Scale)
		})
	}
}

// TestToDecimalLiteralFallbackWhenTypIsNotDecimal pins the defensive
// fallback: when typ is not a DecimalType, the function uses the literal's
// own (hardcoded-9) precision and must not panic.
func TestToDecimalLiteralFallbackWhenTypIsNotDecimal(t *testing.T) {
	lit := decimalLiteral16Bytes(10)

	got := toDecimalLiteral(iceberg.PrimitiveTypes.String, lit)
	require.NotNil(t, got)

	dt, ok := got.GetType().(*types.DecimalType)
	require.Truef(t, ok, "expected *types.DecimalType, got %T", got.GetType())
	assert.Equal(t, int32(9), dt.Precision)
	assert.Equal(t, int32(10), dt.Scale)
}

// TestToDecimalLiteralRealisticValues exercises values whose MarshalBinary
// output is shorter than 16 bytes. expr.NewLiteral rejects non-16-byte input
// and DecimalLiteral.MarshalBinary uses the minimum byte representation, so
// before the BE-min → LE-16 conversion these cases returned nil and panicked
// in the caller. ValueString uses substrait-go's own LE decoder; matching it
// against the expected string confirms the bytes are spec-conformant
// little-endian two's complement.
func TestToDecimalLiteralRealisticValues(t *testing.T) {
	cases := []struct {
		name     string
		val      decimal128.Num
		scale    int
		expected string
	}{
		{"positive small 1.0000", decimal128.New(0, 10000), 4, "1.0000"},
		{"positive 12345.67", decimal128.New(0, 1234567), 2, "12345.67"},
		{"zero", decimal128.New(0, 0), 4, "0.0000"},
		{"negative -1.0000", decimal128.FromI64(-10000), 4, "-1.0000"},
		{"negative -12345.67", decimal128.FromI64(-1234567), 2, "-12345.67"},
		// 16-byte negative: MarshalBinary returns exactly 16 bytes, so the
		// 0xff sign-extend fill must be fully overwritten by the copy.
		{"negative -2^120", decimal128.New(-(1 << 56), 0), 0, "-1329227995784915872903807060280344576"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fieldType := iceberg.DecimalTypeOf(18, tc.scale)
			lit := iceberg.DecimalLiteral{Val: tc.val, Scale: tc.scale}

			got := toDecimalLiteral(fieldType, lit)
			require.NotNil(t, got, "toDecimalLiteral must not return nil for sub-16-byte values")

			assert.Equal(t, tc.expected, got.ValueString())
		})
	}
}
