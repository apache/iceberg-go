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
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumericLiteralCompare(t *testing.T) {
	smallLit := iceberg.NewLiteral(int32(10)).(iceberg.Int32Literal)
	bigLit := iceberg.NewLiteral(int32(1000)).(iceberg.Int32Literal)

	assert.False(t, smallLit.Equals(bigLit))
	assert.True(t, smallLit.Equals(iceberg.NewLiteral(int32(10))))

	cmp := smallLit.Comparator()

	assert.Equal(t, -1, cmp(smallLit.Value(), bigLit.Value()))
	assert.Equal(t, 1, cmp(bigLit.Value(), smallLit.Value()))
	assert.True(t, smallLit.Type().Equals(iceberg.PrimitiveTypes.Int32))
}

func TestIntConversion(t *testing.T) {
	lit := iceberg.NewLiteral(int32(34))

	t.Run("to int64", func(t *testing.T) {
		longLit, err := lit.To(iceberg.PrimitiveTypes.Int64)
		assert.NoError(t, err)
		assert.IsType(t, iceberg.Int64Literal(0), longLit)
		assert.EqualValues(t, 34, longLit)
	})

	t.Run("to float32", func(t *testing.T) {
		floatLit, err := lit.To(iceberg.PrimitiveTypes.Float32)
		assert.NoError(t, err)
		assert.IsType(t, iceberg.Float32Literal(0), floatLit)
		assert.EqualValues(t, 34, floatLit)
	})

	t.Run("to float64", func(t *testing.T) {
		dblLit, err := lit.To(iceberg.PrimitiveTypes.Float64)
		assert.NoError(t, err)
		assert.IsType(t, iceberg.Float64Literal(0), dblLit)
		assert.EqualValues(t, 34, dblLit)
	})
}

func TestIntToDecimalConversion(t *testing.T) {
	tests := []struct {
		ty  iceberg.DecimalType
		val iceberg.Decimal
	}{
		{iceberg.DecimalTypeOf(9, 0),
			iceberg.Decimal{Val: decimal128.FromI64(34), Scale: 0}},
		{iceberg.DecimalTypeOf(9, 2),
			iceberg.Decimal{Val: decimal128.FromI64(3400), Scale: 2}},
		{iceberg.DecimalTypeOf(9, 4),
			iceberg.Decimal{Val: decimal128.FromI64(340000), Scale: 4}},
	}

	for _, tt := range tests {
		t.Run(tt.ty.String(), func(t *testing.T) {
			lit := iceberg.Int32Literal(34)

			dec, err := lit.To(tt.ty)
			require.NoError(t, err)
			assert.IsType(t, iceberg.DecimalLiteral(tt.val), dec)
			assert.EqualValues(t, tt.val, dec)
		})
	}
}

func TestIntToDateConversion(t *testing.T) {
	oneDay, _ := time.Parse("2006-01-02", "2022-03-28")
	val := int32(arrow.Date32FromTime(oneDay))
	dateLit, err := iceberg.NewLiteral(val).To(iceberg.PrimitiveTypes.Date)
	require.NoError(t, err)
	assert.True(t, dateLit.Type().Equals(iceberg.PrimitiveTypes.Date))
	assert.EqualValues(t, val, dateLit)

	lit := iceberg.Int32Literal(34)
	tm, err := lit.To(iceberg.PrimitiveTypes.Time)
	require.NoError(t, err)
	assert.EqualValues(t, lit, tm)

	tm, err = lit.To(iceberg.PrimitiveTypes.Timestamp)
	require.NoError(t, err)
	assert.EqualValues(t, lit, tm)

	tm, err = lit.To(iceberg.PrimitiveTypes.TimestampTz)
	require.NoError(t, err)
	assert.EqualValues(t, lit, tm)
}

func TestInt64Conversions(t *testing.T) {
	tests := []struct {
		from iceberg.Int64Literal
		to   iceberg.Literal
	}{
		{iceberg.Int64Literal(34), iceberg.NewLiteral(int32(34))},
		{iceberg.Int64Literal(34), iceberg.NewLiteral(float32(34))},
		{iceberg.Int64Literal(34), iceberg.NewLiteral(float64(34))},
		{iceberg.Int64Literal(19709), iceberg.NewLiteral(iceberg.Date(19709))},
		{iceberg.Int64Literal(51661919000), iceberg.NewLiteral(iceberg.Time(51661919000))},
		{iceberg.Int64Literal(1647305201), iceberg.NewLiteral(iceberg.Timestamp(1647305201))},
		{iceberg.Int64Literal(34),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(34), Scale: 0})},
		{iceberg.Int64Literal(34),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(3400), Scale: 2})},
		{iceberg.Int64Literal(34),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(340000), Scale: 4})},
	}

	for _, tt := range tests {
		t.Run(tt.to.Type().String(), func(t *testing.T) {
			got, err := tt.from.To(tt.to.Type())
			require.NoError(t, err)
			assert.True(t, tt.to.Equals(got))
		})
	}
}

func TestInt64ToInt32OutsideBound(t *testing.T) {
	bigLit := iceberg.NewLiteral(int64(math.MaxInt32 + 1))
	aboveMax, err := bigLit.To(iceberg.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Implements(t, (*iceberg.AboveMaxLiteral)(nil), aboveMax)
	assert.Equal(t, iceberg.Int32AboveMaxLiteral(), aboveMax)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, aboveMax.Type())

	smallLit := iceberg.NewLiteral(int64(math.MinInt32 - 1))
	belowMin, err := smallLit.To(iceberg.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Implements(t, (*iceberg.BelowMinLiteral)(nil), belowMin)
	assert.Equal(t, iceberg.Int32BelowMinLiteral(), belowMin)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, belowMin.Type())
}

func TestFloatConversions(t *testing.T) {
	n1, _ := decimal128.FromFloat32(34.56, 9, 1)
	n2, _ := decimal128.FromFloat32(34.56, 9, 2)
	n3, _ := decimal128.FromFloat32(34.56, 9, 4)

	tests := []struct {
		from iceberg.Float32Literal
		to   iceberg.Literal
	}{
		{iceberg.Float32Literal(34.5), iceberg.NewLiteral(float64(34.5))},
		{iceberg.Float32Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n1, Scale: 1})},
		{iceberg.Float32Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n2, Scale: 2})},
		{iceberg.Float32Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n3, Scale: 4})},
	}

	for _, tt := range tests {
		t.Run(tt.to.Type().String(), func(t *testing.T) {
			got, err := tt.from.To(tt.to.Type())
			require.NoError(t, err)
			assert.Truef(t, tt.to.Equals(got), "expected: %s, got: %s", tt.to, got)
		})
	}
}

func TestFloat64Conversions(t *testing.T) {
	n1, _ := decimal128.FromFloat64(34.56, 9, 1)
	n2, _ := decimal128.FromFloat64(34.56, 9, 2)
	n3, _ := decimal128.FromFloat64(34.56, 9, 4)

	tests := []struct {
		from iceberg.Float64Literal
		to   iceberg.Literal
	}{
		{iceberg.Float64Literal(34.5), iceberg.NewLiteral(float32(34.5))},
		{iceberg.Float64Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n1, Scale: 1})},
		{iceberg.Float64Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n2, Scale: 2})},
		{iceberg.Float64Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n3, Scale: 4})},
	}

	for _, tt := range tests {
		t.Run(tt.to.Type().String(), func(t *testing.T) {
			got, err := tt.from.To(tt.to.Type())
			require.NoError(t, err)
			assert.Truef(t, tt.to.Equals(got), "expected: %s, got: %s", tt.to, got)
		})
	}
}

func TestFloat64toFloat32OutsideBounds(t *testing.T) {
	bigLit := iceberg.NewLiteral(float64(math.MaxFloat32 + 1.0e37))
	aboveMax, err := bigLit.To(iceberg.PrimitiveTypes.Float32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Float32AboveMaxLiteral(), aboveMax)

	smallLit := iceberg.NewLiteral(float64(-math.MaxFloat32 - 1.0e37))
	belowMin, err := smallLit.To(iceberg.PrimitiveTypes.Float32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Float32BelowMinLiteral(), belowMin)
}

func TestDecimalToDecimalConversion(t *testing.T) {
	lit := iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(3411), Scale: 2})

	v, err := lit.To(iceberg.DecimalTypeOf(9, 2))
	require.NoError(t, err)
	assert.Equal(t, lit, v)

	v, err = lit.To(iceberg.DecimalTypeOf(11, 2))
	require.NoError(t, err)
	assert.Equal(t, lit, v)

	_, err = lit.To(iceberg.DecimalTypeOf(9, 0))
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, "could not convert 34.11 to decimal(9, 0)")

	_, err = lit.To(iceberg.DecimalTypeOf(9, 1))
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, "could not convert 34.11 to decimal(9, 1)")

	_, err = lit.To(iceberg.DecimalTypeOf(9, 3))
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, "could not convert 34.11 to decimal(9, 3)")
}

func TestDecimalLiteralConversions(t *testing.T) {
	n1 := iceberg.Decimal{Val: decimal128.FromI64(1234), Scale: 2}
	n2 := iceberg.Decimal{Val: decimal128.FromI64(math.MaxInt32 + 1), Scale: 0}
	n3 := iceberg.Decimal{Val: decimal128.FromI64(math.MinInt32 - 1), Scale: 10}

	tests := []struct {
		from iceberg.DecimalLiteral
		to   iceberg.Literal
	}{
		{iceberg.DecimalLiteral(n1), iceberg.NewLiteral(int32(1234))},
		{iceberg.DecimalLiteral(n1), iceberg.NewLiteral(int64(1234))},
		{iceberg.DecimalLiteral(n2), iceberg.NewLiteral(int64(math.MaxInt32 + 1))},
		{iceberg.DecimalLiteral(n1), iceberg.NewLiteral(float32(12.34))},
		{iceberg.DecimalLiteral(n1), iceberg.NewLiteral(float64(12.34))},
		{iceberg.DecimalLiteral(n3), iceberg.NewLiteral(int64(math.MinInt32 - 1))},
	}

	for _, tt := range tests {
		t.Run(tt.to.Type().String(), func(t *testing.T) {
			got, err := tt.from.To(tt.to.Type())
			require.NoError(t, err)
			assert.Truef(t, tt.to.Equals(got), "expected: %s, got: %s", tt.to, got)
		})
	}

	above, err := iceberg.DecimalLiteral(n2).To(iceberg.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Int32AboveMaxLiteral(), above)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, above.Type())

	below, err := iceberg.DecimalLiteral(n3).To(iceberg.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Int32BelowMinLiteral(), below)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, below.Type())

	n4 := iceberg.Decimal{Val: decimal128.FromU64(math.MaxInt64 + 1), Scale: 0}
	n5 := iceberg.Decimal{Val: decimal128.FromU64(math.MaxUint64).Negate(), Scale: 20}

	above, err = iceberg.DecimalLiteral(n4).To(iceberg.PrimitiveTypes.Int64)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Int64AboveMaxLiteral(), above)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, above.Type())

	below, err = iceberg.DecimalLiteral(n5).To(iceberg.PrimitiveTypes.Int64)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Int64BelowMinLiteral(), below)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, below.Type())

	v, err := decimal128.FromFloat64(math.MaxFloat32+1e37, 38, -1)
	require.NoError(t, err)
	above, err = iceberg.DecimalLiteral(iceberg.Decimal{Val: v, Scale: -1}).
		To(iceberg.PrimitiveTypes.Float32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Float32AboveMaxLiteral(), above)
	assert.Equal(t, iceberg.PrimitiveTypes.Float32, above.Type())

	below, err = iceberg.DecimalLiteral(iceberg.Decimal{Val: v.Negate(), Scale: -1}).
		To(iceberg.PrimitiveTypes.Float32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Float32BelowMinLiteral(), below)
	assert.Equal(t, iceberg.PrimitiveTypes.Float32, below.Type())
}

func TestLiteralTimestampToDate(t *testing.T) {
	v, _ := arrow.TimestampFromString("1970-01-01T00:00:00.000000+00:00", arrow.Microsecond)
	tsLit := iceberg.NewLiteral(iceberg.Timestamp(v))
	dateLit, err := tsLit.To(iceberg.PrimitiveTypes.Date)
	require.NoError(t, err)
	assert.Zero(t, dateLit)
}

func TestStringLiterals(t *testing.T) {
	sqrt2 := iceberg.NewLiteral("1.414")
	pi := iceberg.NewLiteral("3.141")
	piStr := iceberg.StringLiteral("3.141")
	piDbl := iceberg.NewLiteral(float64(3.141))

	v, err := pi.To(iceberg.PrimitiveTypes.Float64)
	require.NoError(t, err)
	assert.Equal(t, piDbl, v)

	assert.False(t, sqrt2.Equals(pi))
	assert.True(t, pi.Equals(piStr))
	assert.False(t, pi.Equals(piDbl))
	assert.Equal(t, "3.141", pi.String())

	cmp := piStr.Comparator()
	assert.Equal(t, -1, cmp(sqrt2.(iceberg.StringLiteral).Value(), piStr.Value()))
	assert.Equal(t, 1, cmp(piStr.Value(), sqrt2.(iceberg.StringLiteral).Value()))

	v, err = pi.To(iceberg.PrimitiveTypes.String)
	require.NoError(t, err)
	assert.Equal(t, pi, v)
}

func TestStringLiteralConversion(t *testing.T) {
	tm, _ := time.Parse("2006-01-02", "2017-08-18")
	expected := uuid.New()

	tests := []struct {
		from iceberg.StringLiteral
		to   iceberg.Literal
	}{
		{iceberg.StringLiteral("2017-08-18"),
			iceberg.NewLiteral(iceberg.Date(arrow.Date32FromTime(tm)))},
		{iceberg.StringLiteral("14:21:01.919"),
			iceberg.NewLiteral(iceberg.Time(51661919000))},
		{iceberg.StringLiteral("2017-08-18T14:21:01.919234+00:00"),
			iceberg.NewLiteral(iceberg.Timestamp(1503066061919234))},
		{iceberg.StringLiteral("2017-08-18T14:21:01.919234"),
			iceberg.NewLiteral(iceberg.Timestamp(1503066061919234))},
		{iceberg.StringLiteral("2017-08-18T14:21:01.919234-07:00"),
			iceberg.NewLiteral(iceberg.Timestamp(1503091261919234))},
		{iceberg.StringLiteral(expected.String()), iceberg.NewLiteral(expected)},
		{iceberg.StringLiteral("34.560"),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(34560), Scale: 3})},
		{iceberg.StringLiteral("true"), iceberg.NewLiteral(true)},
		{iceberg.StringLiteral("True"), iceberg.NewLiteral(true)},
		{iceberg.StringLiteral("false"), iceberg.NewLiteral(false)},
		{iceberg.StringLiteral("False"), iceberg.NewLiteral(false)},
		{iceberg.StringLiteral("12345"), iceberg.NewLiteral(int32(12345))},
		{iceberg.StringLiteral("12345123456"), iceberg.NewLiteral(int64(12345123456))},
		{iceberg.StringLiteral("3.14"), iceberg.NewLiteral(float32(3.14))},
	}

	for _, tt := range tests {
		t.Run(tt.to.Type().String(), func(t *testing.T) {
			got, err := tt.from.To(tt.to.Type())
			require.NoError(t, err)
			assert.Truef(t, tt.to.Equals(got), "expected: %s, got: %s", tt.to, got)
		})
	}
}

func TestLiteralIdentityConversions(t *testing.T) {
	fixedLit, _ := iceberg.NewLiteral([]byte{0x01, 0x02, 0x03}).To(iceberg.FixedTypeOf(3))

	tests := []struct {
		lit iceberg.Literal
		typ iceberg.PrimitiveType
	}{
		{iceberg.NewLiteral(true), iceberg.PrimitiveTypes.Bool},
		{iceberg.NewLiteral(int32(34)), iceberg.PrimitiveTypes.Int32},
		{iceberg.NewLiteral(int64(340000000)), iceberg.PrimitiveTypes.Int64},
		{iceberg.NewLiteral(float32(34.11)), iceberg.PrimitiveTypes.Float32},
		{iceberg.NewLiteral(float64(3.5028235e38)), iceberg.PrimitiveTypes.Float64},
		{iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(3455), Scale: 2}),
			iceberg.DecimalTypeOf(9, 2)},
		{iceberg.NewLiteral(iceberg.Date(19079)), iceberg.PrimitiveTypes.Date},
		{iceberg.NewLiteral(iceberg.Timestamp(1503091261919234)),
			iceberg.PrimitiveTypes.Timestamp},
		{iceberg.NewLiteral("abc"), iceberg.PrimitiveTypes.String},
		{iceberg.NewLiteral(uuid.New()), iceberg.PrimitiveTypes.UUID},
		{fixedLit, iceberg.FixedTypeOf(3)},
		{iceberg.NewLiteral([]byte{0x01, 0x02, 0x03}), iceberg.PrimitiveTypes.Binary},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			expected, err := tt.lit.To(tt.typ)
			require.NoError(t, err)
			assert.Equal(t, expected, tt.lit)
		})
	}
}

func TestFixedLiteral(t *testing.T) {
	fixedLit012 := iceberg.FixedLiteral{0x00, 0x01, 0x02}
	fixedLit013 := iceberg.FixedLiteral{0x00, 0x01, 0x03}
	assert.True(t, fixedLit012.Equals(fixedLit012))
	assert.False(t, fixedLit012.Equals(fixedLit013))

	cmp := fixedLit012.Comparator()
	assert.Equal(t, -1, cmp(fixedLit012, fixedLit013))
	assert.Equal(t, 1, cmp(fixedLit013, fixedLit012))
	assert.Equal(t, 0, cmp(fixedLit013, fixedLit013))

	testUuid := uuid.New()
	lit, err := iceberg.NewLiteral(testUuid[:]).To(iceberg.FixedTypeOf(16))
	require.NoError(t, err)
	uuidLit, err := lit.To(iceberg.PrimitiveTypes.UUID)
	require.NoError(t, err)

	assert.EqualValues(t, uuidLit, testUuid)

	fixedUuid, err := uuidLit.To(iceberg.FixedTypeOf(16))
	require.NoError(t, err)
	assert.EqualValues(t, testUuid[:], fixedUuid)

	binUuid, err := uuidLit.To(iceberg.PrimitiveTypes.Binary)
	require.NoError(t, err)
	assert.EqualValues(t, testUuid[:], binUuid)

	binlit, err := fixedLit012.To(iceberg.PrimitiveTypes.Binary)
	require.NoError(t, err)
	assert.EqualValues(t, fixedLit012, binlit)
}

func TestBinaryLiteral(t *testing.T) {
	binLit012 := iceberg.NewLiteral([]byte{0x00, 0x01, 0x02}).(iceberg.BinaryLiteral)
	binLit013 := iceberg.NewLiteral([]byte{0x00, 0x01, 0x03}).(iceberg.BinaryLiteral)
	assert.True(t, binLit012.Equals(binLit012))
	assert.False(t, binLit012.Equals(binLit013))

	cmp := binLit012.Comparator()
	assert.Equal(t, -1, cmp(binLit012, binLit013))
	assert.Equal(t, 1, cmp(binLit013, binLit012))
	assert.Equal(t, 0, cmp(binLit013, binLit013))
}

func TestBinaryLiteralConversions(t *testing.T) {
	binLit012 := iceberg.NewLiteral([]byte{0x00, 0x01, 0x02})
	fixed, err := binLit012.To(iceberg.FixedTypeOf(3))
	require.NoError(t, err)
	assert.Equal(t, iceberg.FixedLiteral{0x00, 0x01, 0x02}, fixed)

	_, err = binLit012.To(iceberg.FixedTypeOf(4))
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, "cannot convert BinaryLiteral to fixed[4], different length - 3 <> 4")

	_, err = binLit012.To(iceberg.FixedTypeOf(2))
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, "cannot convert BinaryLiteral to fixed[2], different length - 3 <> 2")

	testUuid := uuid.New()
	lit := iceberg.NewLiteral(testUuid[:])
	uuidLit, err := lit.To(iceberg.PrimitiveTypes.UUID)
	require.NoError(t, err)
	assert.EqualValues(t, testUuid, uuidLit)

	_, err = binLit012.To(iceberg.PrimitiveTypes.UUID)
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, "cannot convert BinaryLiteral to UUID")
}

func testInvalidLiteralConversions(t *testing.T, lit iceberg.Literal, typs []iceberg.Type) {
	t.Run(lit.Type().String(), func(t *testing.T) {
		for _, tt := range typs {
			t.Run(tt.String(), func(t *testing.T) {
				_, err := lit.To(tt)
				assert.ErrorIs(t, err, iceberg.ErrBadCast)
			})
		}
	})
}

func TestInvalidBoolLiteralConversions(t *testing.T) {
	testInvalidLiteralConversions(t, iceberg.NewLiteral(true), []iceberg.Type{
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.PrimitiveTypes.Binary,
		iceberg.FixedTypeOf(2),
	})
}

func TestInvalidNumericConversions(t *testing.T) {
	testInvalidLiteralConversions(t, iceberg.NewLiteral(int32(34)), []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(1),
		iceberg.PrimitiveTypes.Binary,
	})

	testInvalidLiteralConversions(t, iceberg.NewLiteral(int64(34)), []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(1),
		iceberg.PrimitiveTypes.Binary,
	})

	testInvalidLiteralConversions(t, iceberg.NewLiteral(float32(34)), []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(1),
		iceberg.PrimitiveTypes.Binary,
	})

	testInvalidLiteralConversions(t, iceberg.NewLiteral(float64(34)), []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(1),
		iceberg.PrimitiveTypes.Binary,
	})

	testInvalidLiteralConversions(t, iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(3411), Scale: 2}),
		[]iceberg.Type{
			iceberg.PrimitiveTypes.Bool,
			iceberg.PrimitiveTypes.Date,
			iceberg.PrimitiveTypes.Time,
			iceberg.PrimitiveTypes.Timestamp,
			iceberg.PrimitiveTypes.TimestampTz,
			iceberg.PrimitiveTypes.String,
			iceberg.PrimitiveTypes.UUID,
			iceberg.FixedTypeOf(1),
			iceberg.PrimitiveTypes.Binary,
		})
}

func TestInvalidDateTimeLiteralConversions(t *testing.T) {
	lit, _ := iceberg.NewLiteral("2017-08-18").To(iceberg.PrimitiveTypes.Date)
	testInvalidLiteralConversions(t, lit, []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(1),
		iceberg.PrimitiveTypes.Binary,
	})

	lit, _ = iceberg.NewLiteral("14:21:01.919").To(iceberg.PrimitiveTypes.Time)
	testInvalidLiteralConversions(t, lit, []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(1),
		iceberg.PrimitiveTypes.Binary,
	})

	lit, _ = iceberg.NewLiteral("2017-08-18T14:21:01.919").To(iceberg.PrimitiveTypes.Timestamp)
	testInvalidLiteralConversions(t, lit, []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Time,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
		iceberg.FixedTypeOf(1),
		iceberg.PrimitiveTypes.Binary,
	})
}

func TestInvalidStringLiteralConversions(t *testing.T) {
	testInvalidLiteralConversions(t, iceberg.NewLiteral("abc"), []iceberg.Type{
		iceberg.FixedTypeOf(1), iceberg.PrimitiveTypes.Binary,
	})
}

func TestInvalidBinaryLiteralConversions(t *testing.T) {
	testInvalidLiteralConversions(t, iceberg.NewLiteral(uuid.New()), []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.String,
		iceberg.FixedTypeOf(1),
	})

	lit, _ := iceberg.NewLiteral([]byte{0x00, 0x01, 0x02}).To(iceberg.FixedTypeOf(3))
	testInvalidLiteralConversions(t, lit, []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
	})

	testInvalidLiteralConversions(t, iceberg.NewLiteral([]byte{0x00, 0x01, 0x02}), []iceberg.Type{
		iceberg.PrimitiveTypes.Bool,
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.String,
		iceberg.PrimitiveTypes.UUID,
	})
}

func TestBadStringLiteralCasts(t *testing.T) {
	tests := []iceberg.Type{
		iceberg.PrimitiveTypes.Int32,
		iceberg.PrimitiveTypes.Int64,
		iceberg.PrimitiveTypes.Float32,
		iceberg.PrimitiveTypes.Float64,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.PrimitiveTypes.Bool,
		iceberg.DecimalTypeOf(9, 2),
		iceberg.PrimitiveTypes.UUID,
	}

	for _, tt := range tests {
		t.Run(tt.String(), func(t *testing.T) {
			_, err := iceberg.NewLiteral("abc").To(tt)
			assert.ErrorIs(t, err, iceberg.ErrBadCast)
		})
	}
}

func TestStringLiteralToIntMaxMinValue(t *testing.T) {
	above, err := iceberg.NewLiteral(strconv.FormatInt(math.MaxInt32+1, 10)).
		To(iceberg.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Int32AboveMaxLiteral(), above)

	below, err := iceberg.NewLiteral(strconv.FormatInt(math.MinInt32-1, 10)).
		To(iceberg.PrimitiveTypes.Int32)
	require.NoError(t, err)
	assert.Equal(t, iceberg.Int32BelowMinLiteral(), below)
}
