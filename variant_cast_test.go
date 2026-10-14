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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scalarVariant(t *testing.T, build func(*variant.Builder) error) variant.Value {
	t.Helper()
	var b variant.Builder
	require.NoError(t, build(&b))
	v, err := b.Build()
	require.NoError(t, err)

	return v
}

// TestCastVariantTimestampTZ pins tz-aware vs zoneless timestamp casting: a source's
// tz-awareness must match the target's, or the value is not castable.
func TestCastVariantTimestampTZ(t *testing.T) {
	// AppendTimestamp(value, isMicros, isTz).
	nanosNTZ := func(b *variant.Builder) error { return b.AppendTimestamp(arrow.Timestamp(1500), false, false) }
	nanosTZ := func(b *variant.Builder) error { return b.AppendTimestamp(arrow.Timestamp(1500), false, true) }
	microsNTZ := func(b *variant.Builder) error { return b.AppendTimestamp(arrow.Timestamp(5), true, false) }
	microsTZ := func(b *variant.Builder) error { return b.AppendTimestamp(arrow.Timestamp(5), true, true) }
	date := func(b *variant.Builder) error { return b.AppendDate(arrow.Date32(10)) }

	for _, tt := range []struct {
		name  string
		build func(*variant.Builder) error
		typ   PrimitiveType
		want  any // nil means not castable
	}{
		// zoneless nanos leaf -> micros: only the zoneless target accepts it.
		{"nanos NTZ to zoneless micros", nanosNTZ, PrimitiveTypes.Timestamp, Timestamp(1)},
		{"nanos NTZ to tz micros REJECTED", nanosNTZ, PrimitiveTypes.TimestampTz, nil},
		// tz nanos leaf -> micros: only the tz target accepts it.
		{"nanos TZ to tz micros", nanosTZ, PrimitiveTypes.TimestampTz, Timestamp(1)},
		{"nanos TZ to zoneless micros REJECTED", nanosTZ, PrimitiveTypes.Timestamp, nil},
		// zoneless micros leaf -> nanos: only the zoneless target accepts it.
		{"micros NTZ to zoneless nanos", microsNTZ, PrimitiveTypes.TimestampNs, TimestampNano(5000)},
		{"micros NTZ to tz nanos REJECTED", microsNTZ, PrimitiveTypes.TimestampTzNs, nil},
		// tz micros leaf -> nanos: only the tz target accepts it.
		{"micros TZ to tz nanos", microsTZ, PrimitiveTypes.TimestampTzNs, TimestampNano(5000)},
		{"micros TZ to zoneless nanos REJECTED", microsTZ, PrimitiveTypes.TimestampNs, nil},
		// date -> timestamp is zoneless-only; a tz target needs a zone.
		{"date to zoneless micros", date, PrimitiveTypes.Timestamp, Timestamp(10 * 86_400_000_000)},
		{"date to tz micros REJECTED", date, PrimitiveTypes.TimestampTz, nil},
		// timestamp -> date is zoneless-only.
		{"zoneless micros to date", microsNTZ, PrimitiveTypes.Date, Date(0)},
		{"tz micros to date REJECTED", microsTZ, PrimitiveTypes.Date, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lit, ok := CastVariantLiteral(scalarVariant(t, tt.build), tt.typ)
			if tt.want == nil {
				assert.False(t, ok, "expected not castable")

				return
			}
			require.True(t, ok)
			assert.Equal(t, tt.want, lit.Any())
		})
	}
}

func TestCastVariantLiteral(t *testing.T) {
	testUUID := uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	for _, tt := range []struct {
		name  string
		build func(*variant.Builder) error
		typ   PrimitiveType
		want  any // nil means not castable
	}{
		{"small int to int64", func(b *variant.Builder) error { return b.AppendInt(5) }, PrimitiveTypes.Int64, int64(5)},
		{"small int to int32", func(b *variant.Builder) error { return b.AppendInt(5) }, PrimitiveTypes.Int32, int32(5)},
		{"int64 exact", func(b *variant.Builder) error { return b.AppendInt(5_000_000_000) }, PrimitiveTypes.Int64, int64(5_000_000_000)},
		{"float32 widens to float64", func(b *variant.Builder) error { return b.AppendFloat32(1.5) }, PrimitiveTypes.Float64, float64(1.5)},
		{"boolean exact", func(b *variant.Builder) error { return b.AppendBool(true) }, PrimitiveTypes.Bool, true},
		{"string exact", func(b *variant.Builder) error { return b.AppendString("hi") }, PrimitiveTypes.String, "hi"},
		{"binary exact", func(b *variant.Builder) error { return b.AppendBinary([]byte{1, 2, 3}) }, PrimitiveTypes.Binary, []byte{1, 2, 3}},
		{"date exact", func(b *variant.Builder) error { return b.AppendDate(arrow.Date32(100)) }, PrimitiveTypes.Date, Date(100)},
		{"timestamp micros exact", func(b *variant.Builder) error { return b.AppendTimestamp(arrow.Timestamp(123), true, false) }, PrimitiveTypes.Timestamp, Timestamp(123)},
		{"uuid exact", func(b *variant.Builder) error { return b.AppendUUID(testUUID) }, PrimitiveTypes.UUID, testUUID},
		{"decimal scale match", func(b *variant.Builder) error { return b.AppendDecimal8(2, decimal.Decimal64(1234)) }, DecimalTypeOf(10, 2), Decimal{Val: decimal128.FromI64(1234), Scale: 2}},
		{"decimal32 scale match", func(b *variant.Builder) error { return b.AppendDecimal4(2, decimal.Decimal32(1234)) }, DecimalTypeOf(10, 2), Decimal{Val: decimal128.FromI64(1234), Scale: 2}},
		{"decimal128 scale match", func(b *variant.Builder) error { return b.AppendDecimal16(3, decimal128.FromI64(123456)) }, DecimalTypeOf(20, 3), Decimal{Val: decimal128.FromI64(123456), Scale: 3}},
		{"decimal32 scale mismatch", func(b *variant.Builder) error { return b.AppendDecimal4(2, decimal.Decimal32(1234)) }, DecimalTypeOf(10, 3), nil},
		{"decimal scale mismatch", func(b *variant.Builder) error { return b.AppendDecimal8(2, decimal.Decimal64(1234)) }, DecimalTypeOf(10, 3), nil},
		{"string not castable to int64", func(b *variant.Builder) error { return b.AppendString("hi") }, PrimitiveTypes.Int64, nil},
		{"nanos floor to micros pre-epoch", func(b *variant.Builder) error { return b.AppendTimestamp(arrow.Timestamp(-1500), false, false) }, PrimitiveTypes.Timestamp, Timestamp(-2)},
		{"date to nanos overflows int64", func(b *variant.Builder) error { return b.AppendDate(arrow.Date32(200_000)) }, PrimitiveTypes.TimestampNs, nil},
		{"micros to nanos overflows int64", func(b *variant.Builder) error {
			return b.AppendTimestamp(arrow.Timestamp(9_300_000_000_000_000), true, false)
		}, PrimitiveTypes.TimestampNs, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lit, ok := CastVariantLiteral(scalarVariant(t, tt.build), tt.typ)
			if tt.want == nil {
				assert.False(t, ok)

				return
			}
			require.True(t, ok)
			assert.Equal(t, tt.want, lit.Any())
		})
	}
}
