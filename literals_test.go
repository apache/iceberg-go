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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
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
		{
			iceberg.DecimalTypeOf(9, 0),
			iceberg.Decimal{Val: decimal128.FromI64(34), Scale: 0},
		},
		{
			iceberg.DecimalTypeOf(9, 2),
			iceberg.Decimal{Val: decimal128.FromI64(3400), Scale: 2},
		},
		{
			iceberg.DecimalTypeOf(9, 4),
			iceberg.Decimal{Val: decimal128.FromI64(340000), Scale: 4},
		},
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

func TestTimestampNanoConversions(t *testing.T) {
	tests := []struct {
		name         string
		microValue   iceberg.Timestamp
		expectedNano iceberg.TimestampNano
	}{
		{
			name:         "basic conversion",
			microValue:   iceberg.Timestamp(1234567890000000),
			expectedNano: iceberg.TimestampNano(1234567890000000000),
		},
		{
			name:         "zero value",
			microValue:   iceberg.Timestamp(0),
			expectedNano: iceberg.TimestampNano(0),
		},
		{
			name:         "epoch",
			microValue:   iceberg.Timestamp(1000000),        // 1 second in microseconds
			expectedNano: iceberg.TimestampNano(1000000000), // 1 second in nanoseconds
		},
		{
			name:         "negative timestamp",
			microValue:   iceberg.Timestamp(-1000000),
			expectedNano: iceberg.TimestampNano(-1000000000),
		},
		{
			name:         "max safe conversion",
			microValue:   iceberg.Timestamp(9223372036854775), // max microseconds that fit in int64 when multiplied by 1000
			expectedNano: iceberg.TimestampNano(9223372036854775000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nanoValue := tt.microValue.ToNanos()
			assert.Equal(t, tt.expectedNano, nanoValue, "micro to nano conversion")

			backToMicro := nanoValue.ToMicros()
			assert.Equal(t, tt.microValue, backToMicro, "nano to micro round-trip")

			microTime := tt.microValue.ToTime()
			nanoTime := nanoValue.ToTime()
			assert.Equal(t, microTime, nanoTime, "ToTime() should produce same result")
		})
	}
}

func TestTimestampNanoLiteralConversions(t *testing.T) {
	tests := []struct {
		name              string
		nanoLit           iceberg.TimestampNsLiteral
		expectedMicro     iceberg.Timestamp
		expectedRoundTrip iceberg.TimestampNano
	}{
		{
			name:              "basic conversion with truncation",
			nanoLit:           iceberg.TimestampNsLiteral(1234567890123456789),
			expectedMicro:     iceberg.Timestamp(1234567890123456),        // truncates 789
			expectedRoundTrip: iceberg.TimestampNano(1234567890123456000), // last 3 digits become 000
		},
		{
			name:              "zero value",
			nanoLit:           iceberg.TimestampNsLiteral(0),
			expectedMicro:     iceberg.Timestamp(0),
			expectedRoundTrip: iceberg.TimestampNano(0),
		},
		{
			name:              "no truncation needed",
			nanoLit:           iceberg.TimestampNsLiteral(1234567890123456000), // ends in 000
			expectedMicro:     iceberg.Timestamp(1234567890123456),
			expectedRoundTrip: iceberg.TimestampNano(1234567890123456000),
		},
		{
			name:              "negative timestamp",
			nanoLit:           iceberg.TimestampNsLiteral(-1234567890123456789),
			expectedMicro:     iceberg.Timestamp(-1234567890123456),
			expectedRoundTrip: iceberg.TimestampNano(-1234567890123456000),
		},
		{
			name:              "maximum precision truncation",
			nanoLit:           iceberg.TimestampNsLiteral(1234567890123456999), // maximum nanosecond digits
			expectedMicro:     iceberg.Timestamp(1234567890123456),
			expectedRoundTrip: iceberg.TimestampNano(1234567890123456000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test nano to micro conversion
			microLit, err := tt.nanoLit.To(iceberg.PrimitiveTypes.Timestamp)
			require.NoError(t, err)
			assert.IsType(t, iceberg.TimestampLiteral(0), microLit)

			microValue, ok := microLit.(iceberg.TimestampLiteral)
			require.True(t, ok)
			assert.Equal(t, tt.expectedMicro, iceberg.Timestamp(microValue))

			// Test micro back to nano conversion (round-trip)
			backToNano, err := microLit.To(iceberg.PrimitiveTypes.TimestampNs)
			require.NoError(t, err)
			assert.IsType(t, iceberg.TimestampNsLiteral(0), backToNano)

			nanoValue, ok := backToNano.(iceberg.TimestampNsLiteral)
			require.True(t, ok)
			assert.Equal(t, tt.expectedRoundTrip, iceberg.TimestampNano(nanoValue))
		})
	}
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
		{
			iceberg.Int64Literal(1234567890123456789), iceberg.NewLiteral(iceberg.TimestampNano(1234567890123456789)),
		},
		{
			iceberg.Int64Literal(34),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(34), Scale: 0}),
		},
		{
			iceberg.Int64Literal(34),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(3400), Scale: 2}),
		},
		{
			iceberg.Int64Literal(34),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(340000), Scale: 4}),
		},
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
		{
			iceberg.Float32Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n1, Scale: 1}),
		},
		{
			iceberg.Float32Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n2, Scale: 2}),
		},
		{
			iceberg.Float32Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n3, Scale: 4}),
		},
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
		{
			iceberg.Float64Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n1, Scale: 1}),
		},
		{
			iceberg.Float64Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n2, Scale: 2}),
		},
		{
			iceberg.Float64Literal(34.56),
			iceberg.NewLiteral(iceberg.Decimal{Val: n3, Scale: 4}),
		},
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
		{
			iceberg.StringLiteral("2017-08-18"),
			iceberg.NewLiteral(iceberg.Date(arrow.Date32FromTime(tm))),
		},
		{
			iceberg.StringLiteral("14:21:01.919"),
			iceberg.NewLiteral(iceberg.Time(51661919000)),
		},
		{
			iceberg.StringLiteral("2017-08-18T14:21:01.919234"),
			iceberg.NewLiteral(iceberg.Timestamp(1503066061919234)),
		},
		{iceberg.StringLiteral(expected.String()), iceberg.NewLiteral(expected)},
		{
			iceberg.StringLiteral("34.560"),
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(34560), Scale: 3}),
		},
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

	lit := iceberg.StringLiteral("2017-08-18T14:21:01.919234-07:00")
	casted, err := lit.To(iceberg.PrimitiveTypes.TimestampTz)
	require.NoError(t, err)
	expectedTimestamp := iceberg.NewLiteral(iceberg.Timestamp(1503091261919234))
	assert.Truef(t, casted.Equals(expectedTimestamp), "expected: %s, got: %s",
		expectedTimestamp, casted)

	_, err = lit.To(iceberg.PrimitiveTypes.Timestamp)
	require.Error(t, err)
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, `parsing time "2017-08-18T14:21:01.919234-07:00": extra text: "-07:00"`)
	assert.ErrorContains(t, err, "invalid Timestamp format for casting from string")

	_, err = iceberg.StringLiteral("2017-08-18T14:21:01.919234").To(iceberg.PrimitiveTypes.TimestampTz)
	require.Error(t, err)
	assert.ErrorIs(t, err, iceberg.ErrBadCast)
	assert.ErrorContains(t, err, `cannot parse "" as "Z07:00"`)
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
		{
			iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(3455), Scale: 2}),
			iceberg.DecimalTypeOf(9, 2),
		},
		{iceberg.NewLiteral(iceberg.Date(19079)), iceberg.PrimitiveTypes.Date},
		{
			iceberg.NewLiteral(iceberg.Timestamp(1503091261919234)),
			iceberg.PrimitiveTypes.Timestamp,
		},
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
		iceberg.FixedTypeOf(1),
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

func TestUnmarshalBinary(t *testing.T) {
	tests := []struct {
		typ    iceberg.Type
		data   []byte
		result iceberg.Literal
	}{
		{iceberg.PrimitiveTypes.Bool, []byte{0x0}, iceberg.BoolLiteral(false)},
		{iceberg.PrimitiveTypes.Bool, []byte{0x1}, iceberg.BoolLiteral(true)},
		{iceberg.PrimitiveTypes.Int32, []byte{0xd2, 0x04, 0x00, 0x00}, iceberg.Int32Literal(1234)},
		{
			iceberg.PrimitiveTypes.Int64,
			[]byte{0xd2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.Int64Literal(1234),
		},
		{iceberg.PrimitiveTypes.Float32, []byte{0x00, 0x00, 0x90, 0xc0}, iceberg.Float32Literal(-4.5)},
		{
			iceberg.PrimitiveTypes.Float64,
			[]byte{0x8d, 0x97, 0x6e, 0x12, 0x83, 0xc0, 0xf3, 0x3f},
			iceberg.Float64Literal(1.2345),
		},
		{iceberg.PrimitiveTypes.Date, []byte{0xe8, 0x03, 0x00, 0x00}, iceberg.DateLiteral(1000)},
		{iceberg.PrimitiveTypes.Date, []byte{0xd2, 0x04, 0x00, 0x00}, iceberg.DateLiteral(1234)},
		{
			iceberg.PrimitiveTypes.Time,
			[]byte{0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.TimeLiteral(10000),
		},
		{
			iceberg.PrimitiveTypes.Time,
			[]byte{0x00, 0xe8, 0x76, 0x48, 0x17, 0x00, 0x00, 0x00},
			iceberg.TimeLiteral(100000000000),
		},
		{
			iceberg.PrimitiveTypes.TimestampTz,
			[]byte{0x80, 0x1a, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(400000),
		},
		{
			iceberg.PrimitiveTypes.TimestampTz,
			[]byte{0x00, 0xe8, 0x76, 0x48, 0x17, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(100000000000),
		},
		{
			iceberg.PrimitiveTypes.Timestamp,
			[]byte{0x80, 0x1a, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(400000),
		},
		{
			iceberg.PrimitiveTypes.Timestamp,
			[]byte{0x00, 0xe8, 0x76, 0x48, 0x17, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(100000000000),
		},
		{iceberg.PrimitiveTypes.String, []byte("ABC"), iceberg.StringLiteral("ABC")},
		{iceberg.PrimitiveTypes.String, []byte("foo"), iceberg.StringLiteral("foo")},
		{
			iceberg.PrimitiveTypes.UUID,
			[]byte{0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7},
			iceberg.UUIDLiteral(uuid.UUID{0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7}),
		},
		{iceberg.FixedTypeOf(3), []byte("foo"), iceberg.FixedLiteral([]byte("foo"))},
		{iceberg.PrimitiveTypes.Binary, []byte("foo"), iceberg.BinaryLiteral([]byte("foo"))},
		{
			iceberg.DecimalTypeOf(5, 2),
			[]byte{0x30, 0x39},
			iceberg.DecimalLiteral{Scale: 2, Val: decimal128.FromU64(12345)},
		},
		{
			iceberg.DecimalTypeOf(7, 4),
			[]byte{0x12, 0xd6, 0x87},
			iceberg.DecimalLiteral{Scale: 4, Val: decimal128.FromU64(1234567)},
		},
		{
			iceberg.DecimalTypeOf(7, 4),
			[]byte{0xff, 0xed, 0x29, 0x79},
			iceberg.DecimalLiteral{Scale: 4, Val: decimal128.FromI64(-1234567)},
		},
		{
			iceberg.DecimalTypeOf(8, 2),
			[]byte{0x00, 0xbc, 0x61, 0x4e},
			iceberg.DecimalLiteral{Scale: 2, Val: decimal128.FromI64(12345678)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			lit, err := iceberg.LiteralFromBytes(tt.typ, tt.data)
			require.NoError(t, err)

			assert.Truef(t, tt.result.Equals(lit), "expected: %s, got: %s", tt.result, lit)
		})
	}
}

func TestRoundTripLiteralBinary(t *testing.T) {
	tests := []struct {
		typ    iceberg.Type
		b      []byte
		result iceberg.Literal
	}{
		{iceberg.PrimitiveTypes.Bool, []byte{0x0}, iceberg.BoolLiteral(false)},
		{iceberg.PrimitiveTypes.Bool, []byte{0x1}, iceberg.BoolLiteral(true)},
		{iceberg.PrimitiveTypes.Int32, []byte{0xd2, 0x04, 0x00, 0x00}, iceberg.Int32Literal(1234)},
		{
			iceberg.PrimitiveTypes.Int64,
			[]byte{0xd2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.Int64Literal(1234),
		},
		{iceberg.PrimitiveTypes.Float32, []byte{0x00, 0x00, 0x90, 0xc0}, iceberg.Float32Literal(-4.5)},
		{iceberg.PrimitiveTypes.Float32, []byte{0x19, 0x04, 0x9e, 0x3f}, iceberg.Float32Literal(1.2345)},
		{
			iceberg.PrimitiveTypes.Float64,
			[]byte{0x8d, 0x97, 0x6e, 0x12, 0x83, 0xc0, 0xf3, 0x3f},
			iceberg.Float64Literal(1.2345),
		},
		{iceberg.PrimitiveTypes.Date, []byte{0xe8, 0x03, 0x00, 0x00}, iceberg.DateLiteral(1000)},
		{iceberg.PrimitiveTypes.Date, []byte{0xd2, 0x04, 0x00, 0x00}, iceberg.DateLiteral(1234)},
		{
			iceberg.PrimitiveTypes.Time,
			[]byte{0x10, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.TimeLiteral(10000),
		},
		{
			iceberg.PrimitiveTypes.Time,
			[]byte{0x00, 0xe8, 0x76, 0x48, 0x17, 0x00, 0x00, 0x00},
			iceberg.TimeLiteral(100000000000),
		},
		{
			iceberg.PrimitiveTypes.TimestampTz,
			[]byte{0x80, 0x1a, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(400000),
		},
		{
			iceberg.PrimitiveTypes.TimestampTz,
			[]byte{0x00, 0xe8, 0x76, 0x48, 0x17, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(100000000000),
		},
		{
			iceberg.PrimitiveTypes.Timestamp,
			[]byte{0x80, 0x1a, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(400000),
		},
		{
			iceberg.PrimitiveTypes.Timestamp,
			[]byte{0x00, 0xe8, 0x76, 0x48, 0x17, 0x00, 0x00, 0x00},
			iceberg.TimestampLiteral(100000000000),
		},
		{iceberg.PrimitiveTypes.String, []byte("ABC"), iceberg.StringLiteral("ABC")},
		{iceberg.PrimitiveTypes.String, []byte("foo"), iceberg.StringLiteral("foo")},
		{
			iceberg.PrimitiveTypes.UUID,
			[]byte{0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7},
			iceberg.UUIDLiteral(uuid.UUID{0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7}),
		},
		{iceberg.FixedTypeOf(3), []byte("foo"), iceberg.FixedLiteral([]byte("foo"))},
		{iceberg.PrimitiveTypes.Binary, []byte("foo"), iceberg.BinaryLiteral([]byte("foo"))},
		{
			iceberg.DecimalTypeOf(5, 2),
			[]byte{0x30, 0x39},
			iceberg.DecimalLiteral{Scale: 2, Val: decimal128.FromU64(12345)},
		},
		// decimal on 3-bytes to test that we use the minimum number of bytes and not a power of 2
		// 1234567 is 00010010|11010110|10000111 in binary
		// 00010010 -> 18, 11010110 -> 214, 10000111 -> 135
		{
			iceberg.DecimalTypeOf(7, 4),
			[]byte{0x12, 0xd6, 0x87},
			iceberg.DecimalLiteral{Scale: 4, Val: decimal128.FromU64(1234567)},
		},
		// negative decimal to test two's complement
		// -1234567 is 11101101|00101001|01111001 in binary
		// 11101101 -> 237, 00101001 -> 41, 01111001 -> 121
		{
			iceberg.DecimalTypeOf(7, 4),
			[]byte{0xed, 0x29, 0x79},
			iceberg.DecimalLiteral{Scale: 4, Val: decimal128.FromI64(-1234567)},
		},
		// test empty byte in decimal
		// 11 is 00001011 in binary
		// 00001011 -> 11
		{iceberg.DecimalTypeOf(10, 3), []byte{0x0b}, iceberg.DecimalLiteral{Scale: 3, Val: decimal128.FromU64(11)}},
		{iceberg.DecimalTypeOf(4, 2), []byte{0x04, 0xd2}, iceberg.DecimalLiteral{Scale: 2, Val: decimal128.FromU64(1234)}},
	}

	for _, tt := range tests {
		t.Run(tt.result.String(), func(t *testing.T) {
			lit, err := iceberg.LiteralFromBytes(tt.typ, tt.b)
			require.NoError(t, err)

			assert.True(t, lit.Equals(tt.result))

			data, err := lit.MarshalBinary()
			require.NoError(t, err)

			assert.Equal(t, tt.b, data)
		})
	}
}

func TestLargeDecimalRoundTrip(t *testing.T) {
	tests := []struct {
		typ iceberg.DecimalType
		b   []byte
		val string
	}{
		{
			iceberg.DecimalTypeOf(38, 21),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0xe9, 0x18, 0x30, 0x73, 0xb9, 0x1e,
				0x7e, 0xa2, 0xb3, 0x6a, 0x83,
			},
			"12345678912345678.123456789123456789123",
		},
		{
			iceberg.DecimalTypeOf(38, 22),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0xe9, 0x16, 0xbb, 0x01, 0x2f,
				0x4c, 0xc3, 0x2b, 0x42, 0x29, 0x22,
			},
			"1234567891234567.1234567891234567891234",
		},
		{
			iceberg.DecimalTypeOf(38, 23),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0xe9, 0x0a, 0x42, 0xa1, 0xad,
				0xe5, 0x2b, 0x33, 0x15, 0x9b, 0x59,
			},
			"123456789123456.12345678912345678912345",
		},
		{
			iceberg.DecimalTypeOf(38, 24),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0xe8, 0xa2, 0xbb, 0xe9, 0x67,
				0xba, 0x86, 0x77, 0xd8, 0x11, 0x80,
			},
			"12345678912345.123456789123456789123456",
		},
		{
			iceberg.DecimalTypeOf(38, 25),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0xe5, 0x6b, 0x3a, 0xd2, 0x78,
				0xdd, 0x04, 0xc8, 0x70, 0xaf, 0x07,
			},
			"1234567891234.1234567891234567891234567",
		},
		{
			iceberg.DecimalTypeOf(38, 26),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0xcd, 0x85, 0xc5, 0x03, 0x38, 0x37,
				0x3c, 0x38, 0x66, 0xd6, 0x4e,
			},
			"123456789123.12345678912345678912345678",
		},
		{
			iceberg.DecimalTypeOf(38, 27),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0x31, 0x46, 0xfd, 0xc7, 0x79,
				0xca, 0x39, 0x7c, 0x04, 0x5f, 0x15,
			},
			"12345678912.123456789123456789123456789",
		},
		{
			iceberg.DecimalTypeOf(38, 28),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x10, 0x52, 0x01, 0x72, 0x11, 0xda,
				0x08, 0x5b, 0x08, 0x2b, 0xb6, 0xd3,
			},
			"1234567891.1234567891234567891234567891",
		},
		{
			iceberg.DecimalTypeOf(38, 29),
			[]byte{
				0x09, 0x49, 0xb0, 0xf7, 0x13, 0xe9, 0x18, 0x5b, 0x37, 0xc1,
				0x78, 0x0b, 0x91, 0xb5, 0x24, 0x40,
			},
			"123456789.12345678912345678912345678912",
		},
		{
			iceberg.DecimalTypeOf(38, 30),
			[]byte{
				0x09, 0x49, 0xb0, 0xed, 0x1e, 0xdf, 0x80, 0x03, 0x47, 0x3b,
				0x16, 0x9b, 0xf1, 0x13, 0x6a, 0x83,
			},
			"12345678.123456789123456789123456789123",
		},
		{
			iceberg.DecimalTypeOf(38, 31),
			[]byte{
				0x09, 0x49, 0xb0, 0x96, 0x2b, 0xac, 0x29, 0x64, 0x28, 0x70,
				0x36, 0x29, 0xea, 0xc2, 0x29, 0x22,
			},
			"1234567.1234567891234567891234567891234",
		},
		{
			iceberg.DecimalTypeOf(38, 32),
			[]byte{
				0x09, 0x49, 0xad, 0xae, 0xe3, 0x68, 0xe7, 0x4f, 0xb5, 0x14,
				0xbc, 0xdc, 0x2b, 0x95, 0x9b, 0x59,
			},
			"123456.12345678912345678912345678912345",
		},
		{
			iceberg.DecimalTypeOf(38, 33),
			[]byte{
				0x09, 0x49, 0x95, 0x94, 0x3e, 0x35, 0x93, 0xde, 0xb9, 0x2e,
				0xef, 0x53, 0xb3, 0xd8, 0x11, 0x80,
			},
			"12345.123456789123456789123456789123456",
		},
		{
			iceberg.DecimalTypeOf(38, 34),
			[]byte{
				0x09, 0x48, 0xd5, 0xd7, 0x90, 0x78, 0xdf, 0x08, 0x1a, 0xf6,
				0x43, 0x09, 0x06, 0x70, 0xaf, 0x07,
			},
			"1234.1234567891234567891234567891234567",
		},
		{
			iceberg.DecimalTypeOf(38, 35),
			[]byte{
				0x09, 0x43, 0x45, 0x82, 0x85, 0xc7, 0x56, 0x66, 0x24, 0x4d,
				0x16, 0x82, 0x40, 0x66, 0xd6, 0x4e,
			},
			"123.12345678912345678912345678912345678",
		},
		{
			iceberg.DecimalTypeOf(21, 16),
			[]byte{0x06, 0xb1, 0x3a, 0xe3, 0xc4, 0x4e, 0x94, 0xaf, 0x07},
			"12345.1234567891234567",
		},
		{
			iceberg.DecimalTypeOf(22, 17),
			[]byte{0x42, 0xec, 0x4c, 0xe5, 0xab, 0x11, 0xce, 0xd6, 0x4e},
			"12345.12345678912345678",
		},
		{
			iceberg.DecimalTypeOf(23, 18),
			[]byte{0x02, 0x9d, 0x3b, 0x00, 0xf8, 0xae, 0xb2, 0x14, 0x5f, 0x15},
			"12345.123456789123456789",
		},
		{
			iceberg.DecimalTypeOf(24, 19),
			[]byte{0x1a, 0x24, 0x4e, 0x09, 0xb6, 0xd2, 0xf4, 0xcb, 0xb6, 0xd3},
			"12345.1234567891234567891",
		},
		{
			iceberg.DecimalTypeOf(25, 20),
			[]byte{0x01, 0x05, 0x6b, 0x0c, 0x61, 0x24, 0x3d, 0x8f, 0xf5, 0x24, 0x40},
			"12345.12345678912345678912",
		},
		{
			iceberg.DecimalTypeOf(26, 21),
			[]byte{0x0a, 0x36, 0x2e, 0x7b, 0xcb, 0x6a, 0x67, 0x9f, 0x93, 0x6a, 0x83},
			"12345.123456789123456789123",
		},
		{
			iceberg.DecimalTypeOf(27, 22),
			[]byte{0x66, 0x1d, 0xd0, 0xd5, 0xf2, 0x28, 0x0c, 0x3b, 0xc2, 0x29, 0x22},
			"12345.1234567891234567891234",
		},
		{
			iceberg.DecimalTypeOf(28, 23),
			[]byte{0x03, 0xfd, 0x2a, 0x28, 0x5b, 0x75, 0x90, 0x7a, 0x55, 0x95, 0x9b, 0x59},
			"12345.12345678912345678912345",
		},
		{
			iceberg.DecimalTypeOf(29, 24),
			[]byte{0x27, 0xe3, 0xa5, 0x93, 0x92, 0x97, 0xa4, 0xc7, 0x57, 0xd8, 0x11, 0x80},
			"12345.123456789123456789123456",
		},
		{
			iceberg.DecimalTypeOf(30, 25),
			[]byte{0x01, 0x8e, 0xe4, 0x77, 0xc3, 0xb9, 0xec, 0x6f, 0xc9, 0x6e, 0x70, 0xaf, 0x07},
			"12345.1234567891234567891234567",
		},
		{
			iceberg.DecimalTypeOf(31, 26),
			[]byte{0x0f, 0x94, 0xec, 0xad, 0xa5, 0x43, 0x3c, 0x5d, 0xde, 0x50, 0x66, 0xd6, 0x4e},
			"12345.12345678912345678912345678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.val, func(t *testing.T) {
			lit, err := iceberg.LiteralFromBytes(tt.typ, tt.b)
			require.NoError(t, err)

			v, err := decimal128.FromString(tt.val, int32(tt.typ.Precision()), int32(tt.typ.Scale()))
			require.NoError(t, err)

			assert.True(t, lit.Equals(iceberg.DecimalLiteral{Scale: tt.typ.Scale(), Val: v}))

			data, err := lit.MarshalBinary()
			require.NoError(t, err)

			assert.Equal(t, tt.b, data)
		})
	}
}

func TestDecimalMaxMinRoundTrip(t *testing.T) {
	tests := []struct {
		typ iceberg.DecimalType
		v   string
	}{
		{iceberg.DecimalTypeOf(6, 2), "9999.99"},
		{iceberg.DecimalTypeOf(10, 10), ".9999999999"},
		{iceberg.DecimalTypeOf(2, 1), "9.9"},
		{iceberg.DecimalTypeOf(38, 37), "9.9999999999999999999999999999999999999"},
		{iceberg.DecimalTypeOf(20, 1), "9999999999999999999.9"},
		{iceberg.DecimalTypeOf(6, 2), "-9999.99"},
		{iceberg.DecimalTypeOf(10, 10), "-.9999999999"},
		{iceberg.DecimalTypeOf(2, 1), "-9.9"},
		{iceberg.DecimalTypeOf(38, 37), "-9.9999999999999999999999999999999999999"},
		{iceberg.DecimalTypeOf(20, 1), "-9999999999999999999.9"},
	}

	for _, tt := range tests {
		t.Run(tt.v, func(t *testing.T) {
			v, err := decimal128.FromString(tt.v, int32(tt.typ.Precision()), int32(tt.typ.Scale()))
			require.NoError(t, err)

			lit := iceberg.DecimalLiteral{Scale: tt.typ.Scale(), Val: v}
			b, err := lit.MarshalBinary()
			require.NoError(t, err)
			val, err := iceberg.LiteralFromBytes(tt.typ, b)
			require.NoError(t, err)

			assert.True(t, val.Equals(lit))
		})
	}
}
