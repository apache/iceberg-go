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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
)

const (
	microsPerDay  = int64(86_400_000_000)
	nanosPerDay   = int64(86_400_000_000_000)
	nanosPerMicro = int64(1_000)
)

// CastVariantLiteral casts a leaf variant value to typ and wraps it as a Literal.
func CastVariantLiteral(v variant.Value, typ PrimitiveType) (Literal, bool) {
	result, ok := castVariantValue(v, typ)
	if !ok {
		return nil, false
	}

	lit := literalFromCastValue(result)
	if lit == nil {
		return nil, false
	}
	if !lit.Type().Equals(typ) {
		conv, err := lit.To(typ)
		if err != nil {
			return nil, false
		}

		lit = conv
	}

	return lit, true
}

// castVariantValue casts a leaf variant value to the Go value backing typ.
func castVariantValue(v variant.Value, typ PrimitiveType) (any, bool) {
	raw := v.Value()
	if raw == nil {
		return nil, false
	}

	if r, ok := exactVariantMatch(v.Type(), raw, typ); ok {
		return r, true
	}

	switch t := typ.(type) {
	case Int32Type:
		switch n := raw.(type) {
		case int8:
			return int32(n), true
		case int16:
			return int32(n), true
		}
	case Int64Type:
		switch n := raw.(type) {
		case int8:
			return int64(n), true
		case int16:
			return int64(n), true
		case int32:
			return int64(n), true
		}
	case Float64Type:
		if f, ok := raw.(float32); ok {
			return float64(f), true
		}
	case FixedType:
		if b, ok := raw.([]byte); ok && len(b) == t.Len() {
			return b, true
		}
	case DecimalType:
		return castVariantDecimal(raw, t)
	case BooleanType:
		if b, ok := raw.(bool); ok {
			return b, true
		}
	case TimestampType, TimestampTzType:
		return castVariantToMicros(v.Type(), raw)
	case TimestampNsType, TimestampTzNsType:
		return castVariantToNanos(v.Type(), raw)
	case DateType:
		return castVariantToDate(v.Type(), raw)
	}

	return nil, false
}

// exactVariantMatch returns raw coerced to the Go type backing typ when the variant physical type matches typ exactly.
func exactVariantMatch(pt variant.Type, raw any, typ PrimitiveType) (any, bool) {
	switch typ.(type) {
	case Int32Type:
		if pt == variant.Int32 {
			return raw.(int32), true
		}
	case Int64Type:
		if pt == variant.Int64 {
			return raw.(int64), true
		}
	case Float32Type:
		if pt == variant.Float {
			return raw.(float32), true
		}
	case Float64Type:
		if pt == variant.Double {
			return raw.(float64), true
		}
	case DateType:
		if pt == variant.Date {
			return Date(raw.(arrow.Date32)), true
		}
	case TimestampType:
		if pt == variant.TimestampMicrosNTZ {
			return Timestamp(raw.(arrow.Timestamp)), true
		}
	case TimestampTzType:
		if pt == variant.TimestampMicros {
			return Timestamp(raw.(arrow.Timestamp)), true
		}
	case TimestampNsType:
		if pt == variant.TimestampNanosNTZ {
			return TimestampNano(raw.(arrow.Timestamp)), true
		}
	case TimestampTzNsType:
		if pt == variant.TimestampNanos {
			return TimestampNano(raw.(arrow.Timestamp)), true
		}
	case TimeType:
		if pt == variant.Time {
			return Time(raw.(arrow.Time64)), true
		}
	case UUIDType:
		if pt == variant.UUID {
			return raw.(uuid.UUID), true
		}
	case StringType:
		if pt == variant.String {
			return raw.(string), true
		}
	case BinaryType:
		if pt == variant.Binary {
			return raw.([]byte), true
		}
	}

	return nil, false
}

func castVariantDecimal(raw any, typ DecimalType) (any, bool) {
	switch d := raw.(type) {
	case variant.DecimalValue[decimal.Decimal32]:
		if int(d.Scale) != typ.Scale() {
			return nil, false
		}

		return Decimal{Val: decimal128.FromI64(int64(d.Value.(decimal.Decimal32))), Scale: int(d.Scale)}, true
	case variant.DecimalValue[decimal.Decimal64]:
		if int(d.Scale) != typ.Scale() {
			return nil, false
		}

		return Decimal{Val: decimal128.FromI64(int64(d.Value.(decimal.Decimal64))), Scale: int(d.Scale)}, true
	case variant.DecimalValue[decimal.Decimal128]:
		if int(d.Scale) != typ.Scale() {
			return nil, false
		}

		return Decimal{Val: d.Value.(decimal.Decimal128), Scale: int(d.Scale)}, true
	}

	return nil, false
}

func castVariantToMicros(pt variant.Type, raw any) (any, bool) {
	switch pt {
	case variant.TimestampNanos, variant.TimestampNanosNTZ:
		return Timestamp(floorDiv(int64(raw.(arrow.Timestamp)), nanosPerMicro)), true
	case variant.Date:
		return Timestamp(int64(raw.(arrow.Date32)) * microsPerDay), true
	}

	return nil, false
}

func castVariantToNanos(pt variant.Type, raw any) (any, bool) {
	switch pt {
	case variant.TimestampMicros, variant.TimestampMicrosNTZ:
		return TimestampNano(int64(raw.(arrow.Timestamp)) * nanosPerMicro), true
	case variant.Date:
		return TimestampNano(int64(raw.(arrow.Date32)) * nanosPerDay), true
	}

	return nil, false
}

func castVariantToDate(pt variant.Type, raw any) (any, bool) {
	switch pt {
	case variant.TimestampMicros, variant.TimestampMicrosNTZ:
		return Date(floorDiv(int64(raw.(arrow.Timestamp)), microsPerDay)), true
	case variant.TimestampNanos, variant.TimestampNanosNTZ:
		return Date(floorDiv(int64(raw.(arrow.Timestamp)), nanosPerDay)), true
	}

	return nil, false
}

// floorDiv divides rounding toward negative infinity.
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}

	return q
}

func literalFromCastValue(result any) Literal {
	switch r := result.(type) {
	case bool:
		return NewLiteral(r)
	case int32:
		return NewLiteral(r)
	case int64:
		return NewLiteral(r)
	case float32:
		return NewLiteral(r)
	case float64:
		return NewLiteral(r)
	case Date:
		return NewLiteral(r)
	case Time:
		return NewLiteral(r)
	case Timestamp:
		return NewLiteral(r)
	case TimestampNano:
		return NewLiteral(r)
	case string:
		return NewLiteral(r)
	case []byte:
		return NewLiteral(r)
	case uuid.UUID:
		return NewLiteral(r)
	case Decimal:
		return NewLiteral(r)
	}

	return nil
}
