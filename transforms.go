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
	"encoding"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/google/uuid"
	"github.com/twmb/murmur3"
)

// ParseTransform takes the string representation of a transform as
// defined in the iceberg spec, and produces the appropriate Transform
// object or an error if the string is not a valid transform string.
func ParseTransform(s string) (Transform, error) {
	s = strings.ToLower(s)
	switch {
	case strings.HasPrefix(s, "bucket"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])

		return BucketTransform{NumBuckets: n}, nil
	case strings.HasPrefix(s, "truncate"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])

		return TruncateTransform{Width: n}, nil
	default:
		switch s {
		case "identity":
			return IdentityTransform{}, nil
		case "void":
			return VoidTransform{}, nil
		case "year":
			return YearTransform{}, nil
		case "month":
			return MonthTransform{}, nil
		case "day":
			return DayTransform{}, nil
		case "hour":
			return HourTransform{}, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrInvalidTransform, s)
}

// Transform is an interface for the various Transformation types
// in partition specs. Currently, they do not yet provide actual
// transformation functions or implementation. That will come later as
// data reading gets implemented.
type Transform interface {
	fmt.Stringer
	encoding.TextMarshaler
	CanTransform(t Type) bool
	ResultType(t Type) Type
	PreservesOrder() bool
	Equals(Transform) bool
	Apply(Optional[Literal]) Optional[Literal]
	Project(name string, pred BoundPredicate) (UnboundPredicate, error)

	ToHumanStr(any) string
}

// IdentityTransform uses the identity function, performing no transformation
// but instead partitioning on the value itself.
type IdentityTransform struct{}

func (t IdentityTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (IdentityTransform) String() string { return "identity" }

func (IdentityTransform) CanTransform(t Type) bool {
	_, ok := t.(PrimitiveType)

	return ok
}
func (IdentityTransform) ResultType(t Type) Type { return t }
func (IdentityTransform) PreservesOrder() bool   { return true }

func (IdentityTransform) Equals(other Transform) bool {
	_, ok := other.(IdentityTransform)

	return ok
}

func (IdentityTransform) Apply(value Optional[Literal]) Optional[Literal] {
	return value
}

func (IdentityTransform) ToHumanStr(val any) string {
	switch v := val.(type) {
	case nil:
		return "null"
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case bool:
		return strconv.FormatBool(v)
	case Date:
		return v.ToTime().Format("2006-01-02")
	case Time:
		return v.ToTime().Format("15:04:05.999999")
	case Timestamp:
		return v.ToTime().Format("2006-01-02T15:04:05.999999")
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (t IdentityTransform) Project(name string, pred BoundPredicate) (UnboundPredicate, error) {
	if _, ok := pred.Term().(*BoundTransform); ok {
		return projectTransformPredicate(t, name, pred)
	}

	switch p := pred.(type) {
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(name)), nil
	case BoundLiteralPredicate:
		return p.AsUnbound(Reference(name), p.Literal()), nil
	case BoundSetPredicate:
		return p.AsUnbound(Reference(name), p.Literals().Members()), nil
	}

	return nil, nil
}

// VoidTransform is a transformation that always returns nil.
type VoidTransform struct{}

func (t VoidTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (VoidTransform) String() string { return "void" }

func (VoidTransform) CanTransform(Type) bool { return true }
func (VoidTransform) ResultType(t Type) Type { return t }
func (VoidTransform) PreservesOrder() bool   { return false }

func (VoidTransform) Equals(other Transform) bool {
	_, ok := other.(VoidTransform)

	return ok
}

func (VoidTransform) Apply(value Optional[Literal]) Optional[Literal] {
	return Optional[Literal]{}
}

func (VoidTransform) ToHumanStr(any) string { return "null" }

func (VoidTransform) Project(string, BoundPredicate) (UnboundPredicate, error) {
	return nil, nil
}

// BucketTransform transforms values into a bucket partition value. It is
// parameterized by a number of buckets. Bucket partition transforms use
// a 32-bit hash of the source value to produce a positive value by mod
// the bucket number.
type BucketTransform struct {
	NumBuckets int
}

func (t BucketTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t BucketTransform) String() string { return fmt.Sprintf("bucket[%d]", t.NumBuckets) }

func (BucketTransform) CanTransform(t Type) bool {
	switch t.(type) {
	case Int32Type,
		DateType,
		Int64Type,
		TimeType,
		TimestampType,
		TimestampTzType,
		DecimalType,
		StringType,
		FixedType,
		BinaryType,
		UUIDType:
		return true
	default:
		return false
	}
}
func (BucketTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }
func (BucketTransform) PreservesOrder() bool { return false }

func hashHelperInt[T ~int32 | ~int64](v any) uint32 {
	var (
		val = uint64(v.(T))
		buf [8]byte
		b   = buf[:]
	)

	binary.LittleEndian.PutUint64(b, val)

	return murmur3.Sum32(b)
}

func (t BucketTransform) Equals(other Transform) bool {
	rhs, ok := other.(BucketTransform)
	if !ok {
		return false
	}

	return t.NumBuckets == rhs.NumBuckets
}

func (t BucketTransform) Apply(value Optional[Literal]) Optional[Literal] {
	if !value.Valid {
		return Optional[Literal]{}
	}

	var hash uint32
	switch v := value.Val.(type) {
	case TypedLiteral[[]byte]:
		hash = murmur3.Sum32(v.Value())
	case StringLiteral:
		hash = murmur3.Sum32(unsafe.Slice(unsafe.StringData(string(v)), len(v)))
	case UUIDLiteral:
		hash = murmur3.Sum32(v[:])
	case DecimalLiteral:
		b, _ := v.MarshalBinary()
		hash = murmur3.Sum32(b)
	case Int32Literal:
		hash = hashHelperInt[int64](int64(v))
	case Int64Literal:
		hash = hashHelperInt[int64](int64(v))
	case DateLiteral:
		hash = hashHelperInt[int64](int64(v))
	case TimeLiteral:
		hash = hashHelperInt[int64](int64(v))
	case TimestampLiteral:
		hash = hashHelperInt[int64](int64(v))
	default:
		return Optional[Literal]{}
	}

	return Optional[Literal]{
		Valid: true,
		Val:   Int32Literal((int32(hash) & math.MaxInt32) % int32(t.NumBuckets)),
	}
}

func (t BucketTransform) Transformer(src Type) func(any) Optional[int32] {
	var h func(any) uint32

	switch src.(type) {
	case Int32Type:
		h = hashHelperInt[int32]
	case DateType:
		h = hashHelperInt[Date]
	case Int64Type:
		h = hashHelperInt[int64]
	case TimeType:
		h = hashHelperInt[Time]
	case TimestampType:
		h = hashHelperInt[Timestamp]
	case TimestampTzType:
		h = hashHelperInt[Timestamp]
	case DecimalType:
		h = func(v any) uint32 {
			b, _ := DecimalLiteral(v.(Decimal)).MarshalBinary()

			return murmur3.Sum32(b)
		}
	case StringType, FixedType, BinaryType:
		h = func(v any) uint32 {
			if v, ok := v.([]byte); ok {
				return murmur3.Sum32(v)
			}

			str := v.(string)

			return murmur3.Sum32(unsafe.Slice(unsafe.StringData(str), len(str)))
		}
	case UUIDType:
		h = func(v any) uint32 {
			if v, ok := v.([]byte); ok {
				return murmur3.Sum32(v)
			}

			u := v.(uuid.UUID)

			return murmur3.Sum32(u[:])
		}
	}

	return func(v any) Optional[int32] {
		if v == nil {
			return Optional[int32]{}
		}

		return Optional[int32]{
			Valid: true,
			Val:   int32((int32(h(v)) & math.MaxInt32) % int32(t.NumBuckets)),
		}
	}
}

func (BucketTransform) ToHumanStr(val any) string {
	if val == nil {
		return "null"
	}

	return fmt.Sprintf("%v", val)
}

func (t BucketTransform) Project(name string, pred BoundPredicate) (UnboundPredicate, error) {
	if _, ok := pred.Term().(*BoundTransform); ok {
		return projectTransformPredicate(t, name, pred)
	}

	transformer := t.Transformer(pred.Term().Type())
	switch p := pred.(type) {
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(name)), nil
	case BoundLiteralPredicate:
		if p.Op() != OpEQ {
			break
		}

		return p.AsUnbound(Reference(name), transformLiteral(transformer, p.Literal())), nil
	case BoundSetPredicate:
		if p.Op() != OpIn {
			break
		}

		return setApplyTransform(name, p, transformer), nil
	}

	return nil, nil
}

// TruncateTransform is a transformation for truncating a value to a specified width.
type TruncateTransform struct {
	Width int
}

func (t TruncateTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t TruncateTransform) String() string { return fmt.Sprintf("truncate[%d]", t.Width) }

func (TruncateTransform) CanTransform(t Type) bool {
	switch t.(type) {
	case Int32Type,
		Int64Type,
		StringType,
		BinaryType,
		DecimalType:
		return true
	default:
		return false
	}
}
func (TruncateTransform) ResultType(t Type) Type { return t }
func (TruncateTransform) PreservesOrder() bool   { return true }
func (t TruncateTransform) Equals(other Transform) bool {
	rhs, ok := other.(TruncateTransform)
	if !ok {
		return false
	}

	return t.Width == rhs.Width
}

func (t TruncateTransform) Transformer(src Type) (func(any) any, error) {
	switch src.(type) {
	case Int32Type:
		return func(v any) any {
			if v == nil {
				return nil
			}

			val := v.(int32)

			return val - (val % int32(t.Width))
		}, nil
	case Int64Type:
		return func(v any) any {
			if v == nil {
				return nil
			}

			val := v.(int64)

			return val - (val % int64(t.Width))
		}, nil
	case StringType, BinaryType:
		return func(v any) any {
			switch v := v.(type) {
			case string:
				return v[:min(len(v), t.Width)]
			case []byte:
				return v[:min(len(v), t.Width)]
			default:
				return nil
			}
		}, nil
	case DecimalType:
		bigWidth := big.NewInt(int64(t.Width))

		return func(v any) any {
			if v == nil {
				return nil
			}

			val := v.(Decimal)
			unscaled := val.Val.BigInt()
			// unscaled - (((unscaled % width) + width) % width)
			applied := (&big.Int{}).Mod(unscaled, bigWidth)
			applied.Add(applied, bigWidth).Mod(applied, bigWidth)
			val.Val = decimal128.FromBigInt(unscaled.Sub(unscaled, applied))

			return val
		}, nil
	}

	return nil, fmt.Errorf("%w: cannot truncate for type %s",
		ErrInvalidArgument, src)
}

func (t TruncateTransform) Apply(value Optional[Literal]) (out Optional[Literal]) {
	if !value.Valid {
		return
	}

	fn, err := t.Transformer(value.Val.Type())
	if err != nil {
		return
	}

	out.Valid = true
	switch v := value.Val.(type) {
	case Int32Literal:
		out.Val = Int32Literal(fn(int32(v)).(int32))
	case Int64Literal:
		out.Val = Int64Literal(fn(int64(v)).(int64))
	case DecimalLiteral:
		out.Val = DecimalLiteral(fn(Decimal(v)).(Decimal))
	case StringLiteral:
		out.Val = StringLiteral(fn(string(v)).(string))
	case BinaryLiteral:
		out.Val = BinaryLiteral(fn([]byte(v)).([]byte))
	}

	return
}

func (TruncateTransform) ToHumanStr(val any) string {
	switch v := val.(type) {
	case nil:
		return "null"
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (t TruncateTransform) Project(name string, pred BoundPredicate) (UnboundPredicate, error) {
	if _, ok := pred.Term().(*BoundTransform); ok {
		return projectTransformPredicate(t, name, pred)
	}

	fieldType := pred.Term().Ref().Field().Type

	transformer, err := t.Transformer(fieldType)
	if err != nil {
		return nil, err
	}

	switch p := pred.(type) {
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(name)), nil
	case BoundSetPredicate:
		if p.Op() != OpIn {
			break
		}

		switch fieldType.(type) {
		case Int32Type:
			return setApplyTransform(name, p, wrapTransformFn[int32](transformer)), nil
		case Int64Type:
			return setApplyTransform(name, p, wrapTransformFn[int64](transformer)), nil
		case DecimalType:
			return setApplyTransform(name, p, wrapTransformFn[Decimal](transformer)), nil
		case StringType:
			return setApplyTransform(name, p, wrapTransformFn[string](transformer)), nil
		case BinaryType:
			return setApplyTransform(name, p, wrapTransformFn[[]byte](transformer)), nil
		}
	case BoundLiteralPredicate:
		switch fieldType.(type) {
		case Int32Type:
			return truncateNumber(name, p, wrapTransformFn[int32](transformer))
		case Int64Type:
			return truncateNumber(name, p, wrapTransformFn[int64](transformer))
		case DecimalType:
			return truncateNumber(name, p, wrapTransformFn[Decimal](transformer))
		case StringType:
			return truncateArray(name, p, wrapTransformFn[string](transformer))
		case BinaryType:
			return truncateArray(name, p, wrapTransformFn[[]byte](transformer))
		}
	}

	return nil, nil
}

var epochTM = time.Unix(0, 0).UTC()

type TimeTransform interface {
	Transform
	Transformer(Type) (func(any) Optional[int32], error)
}

func canTransformTime(t TimeTransform, sourceType Type) bool {
	switch sourceType.(type) {
	case DateType, TimestampType, TimestampTzType:
		return true
	default:
		return false
	}
}

func projectTimeTransform(t TimeTransform, name string, pred BoundPredicate) (UnboundPredicate, error) {
	if _, ok := pred.Term().(*BoundTransform); ok {
		return projectTransformPredicate(t, name, pred)
	}

	transformer, err := t.Transformer(pred.Term().Ref().Type())
	if err != nil {
		return nil, err
	}

	switch p := pred.(type) {
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(name)), nil
	case BoundLiteralPredicate:
		return truncateNumber(name, p, transformer)
	case BoundSetPredicate:
		if p.Op() != OpIn {
			break
		}

		return setApplyTransform(name, p, transformer), nil
	}

	return nil, nil
}

// YearTransform transforms a datetime value into a year value.
type YearTransform struct{}

func (t YearTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (YearTransform) String() string { return "year" }

func (t YearTransform) CanTransform(sourceType Type) bool { return canTransformTime(t, sourceType) }
func (YearTransform) ResultType(Type) Type                { return PrimitiveTypes.Int32 }
func (YearTransform) PreservesOrder() bool                { return true }

func (YearTransform) Equals(other Transform) bool {
	_, ok := other.(YearTransform)

	return ok
}

func (YearTransform) Transformer(src Type) (func(any) Optional[int32], error) {
	switch src.(type) {
	case DateType:
		return func(v any) Optional[int32] {
			if v == nil {
				return Optional[int32]{}
			}

			return Optional[int32]{
				Valid: true,
				Val:   int32(v.(Date).ToTime().Year() - epochTM.Year()),
			}
		}, nil
	case TimestampType, TimestampTzType:
		return func(v any) Optional[int32] {
			if v == nil {
				return Optional[int32]{}
			}

			return Optional[int32]{
				Valid: true,
				Val:   int32(v.(Timestamp).ToTime().Year() - epochTM.Year()),
			}
		}, nil
	}

	return nil, fmt.Errorf("%w: cannot apply year transform for type %s",
		ErrInvalidArgument, src)
}

func (YearTransform) Apply(value Optional[Literal]) (out Optional[Literal]) {
	if !value.Valid {
		return
	}

	switch v := value.Val.(type) {
	case DateLiteral:
		out.Valid = true
		out.Val = Int32Literal(Date(v).ToTime().Year() - epochTM.Year())
	case TimestampLiteral:
		out.Valid = true
		out.Val = Int32Literal(Timestamp(v).ToTime().Year() - epochTM.Year())
	}

	return
}

func (YearTransform) ToHumanStr(val any) string {
	switch v := val.(type) {
	case int32:
		return strconv.Itoa(int(v) + epochTM.Year())
	default:
		return "null"
	}
}

func (t YearTransform) Project(name string, pred BoundPredicate) (UnboundPredicate, error) {
	return projectTimeTransform(t, name, pred)
}

// MonthTransform transforms a datetime value into a month value.
type MonthTransform struct{}

func (t MonthTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (MonthTransform) String() string { return "month" }

func (t MonthTransform) CanTransform(sourceType Type) bool { return canTransformTime(t, sourceType) }
func (MonthTransform) ResultType(Type) Type                { return PrimitiveTypes.Int32 }
func (MonthTransform) PreservesOrder() bool                { return true }

func (MonthTransform) Equals(other Transform) bool {
	_, ok := other.(MonthTransform)

	return ok
}

func (MonthTransform) Transformer(src Type) (func(any) Optional[int32], error) {
	switch src.(type) {
	case DateType:
		return func(v any) Optional[int32] {
			if v == nil {
				return Optional[int32]{}
			}

			d := v.(Date).ToTime()

			return Optional[int32]{
				Valid: true,
				Val:   int32((d.Year()-epochTM.Year())*12 + (int(d.Month()) - int(epochTM.Month()))),
			}
		}, nil
	case TimestampType, TimestampTzType:
		return func(v any) Optional[int32] {
			if v == nil {
				return Optional[int32]{}
			}

			d := v.(Timestamp).ToTime()

			return Optional[int32]{
				Valid: true,
				Val:   int32((d.Year()-epochTM.Year())*12 + (int(d.Month()) - int(epochTM.Month()))),
			}
		}, nil

	}

	return nil, fmt.Errorf("%w: cannot apply month transform for type %s",
		ErrInvalidArgument, src)
}

func (MonthTransform) Apply(value Optional[Literal]) (out Optional[Literal]) {
	if !value.Valid {
		return
	}

	var tm time.Time
	switch v := value.Val.(type) {
	case DateLiteral:
		tm = Date(v).ToTime()
	case TimestampLiteral:
		tm = Timestamp(v).ToTime()
	default:
		return
	}

	out.Valid = true
	out.Val = Int32Literal(int32((tm.Year()-epochTM.Year())*12 + (int(tm.Month()) - int(epochTM.Month()))))

	return
}

func (t MonthTransform) ToHumanStr(val any) string {
	switch v := val.(type) {
	case int32:
		tm := epochTM.AddDate(0, int(v), 0)

		return tm.Format("2006-01")
	default:
		return "null"
	}
}

func (t MonthTransform) Project(name string, pred BoundPredicate) (UnboundPredicate, error) {
	return projectTimeTransform(t, name, pred)
}

// DayTransform transforms a datetime value into a date value.
type DayTransform struct{}

func (t DayTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (DayTransform) String() string { return "day" }

func (t DayTransform) CanTransform(sourceType Type) bool { return canTransformTime(t, sourceType) }
func (DayTransform) ResultType(Type) Type                { return PrimitiveTypes.Int32 }
func (DayTransform) PreservesOrder() bool                { return true }

func (DayTransform) Equals(other Transform) bool {
	_, ok := other.(DayTransform)

	return ok
}

func (DayTransform) Transformer(src Type) (func(any) Optional[int32], error) {
	switch src.(type) {
	case DateType:
		return func(v any) Optional[int32] {
			if v == nil {
				return Optional[int32]{}
			}

			return Optional[int32]{
				Valid: true,
				Val:   int32(v.(Date)),
			}
		}, nil
	case TimestampType, TimestampTzType:
		return func(v any) Optional[int32] {
			if v == nil {
				return Optional[int32]{}
			}

			return Optional[int32]{
				Valid: true,
				Val:   int32(v.(Timestamp).ToDate()),
			}
		}, nil
	}

	return nil, fmt.Errorf("%w: cannot apply day transform for type %s",
		ErrInvalidArgument, src)
}

func (DayTransform) Apply(value Optional[Literal]) (out Optional[Literal]) {
	if !value.Valid {
		return
	}

	switch v := value.Val.(type) {
	case DateLiteral:
		out.Valid, out.Val = true, Int32Literal(v)
	case TimestampLiteral:
		out.Valid, out.Val = true, Int32Literal(Timestamp(v).ToDate())
	}

	return
}

func (DayTransform) ToHumanStr(val any) string {
	switch v := val.(type) {
	case int32:
		tm := epochTM.AddDate(0, 0, int(v))

		return tm.Format("2006-01-02")
	default:
		return "null"
	}
}

func (t DayTransform) Project(name string, pred BoundPredicate) (UnboundPredicate, error) {
	return projectTimeTransform(t, name, pred)
}

// HourTransform transforms a datetime value into an hour value.
type HourTransform struct{}

func (t HourTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (HourTransform) String() string { return "hour" }

func (t HourTransform) CanTransform(sourceType Type) bool {
	switch sourceType.(type) {
	case TimestampType, TimestampTzType:
		return true
	default:
		return false
	}
}
func (HourTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }
func (HourTransform) PreservesOrder() bool { return true }

func (HourTransform) Equals(other Transform) bool {
	_, ok := other.(HourTransform)

	return ok
}

func (HourTransform) Transformer(src Type) (func(any) Optional[int32], error) {
	switch src.(type) {
	case TimestampType, TimestampTzType:
		const factor = int64(time.Hour / time.Microsecond)

		return func(v any) Optional[int32] {
			if v == nil {
				return Optional[int32]{}
			}

			return Optional[int32]{
				Valid: true,
				Val:   int32(int64(v.(Timestamp)) / factor),
			}
		}, nil
	}

	return nil, fmt.Errorf("%w: cannot apply hour transform for type %s",
		ErrInvalidArgument, src)
}

func (HourTransform) Apply(value Optional[Literal]) (out Optional[Literal]) {
	if !value.Valid {
		return
	}

	switch v := value.Val.(type) {
	case TimestampLiteral:
		const factor = int64(time.Hour / time.Microsecond)
		out.Valid, out.Val = true, Int32Literal(int32(int64(v)/factor))
	}

	return
}

func (HourTransform) ToHumanStr(val any) string {
	switch v := val.(type) {
	case int32:
		tm := epochTM.Add(time.Duration(v) * time.Hour)

		return tm.Format("2006-01-02-15")
	default:
		return "null"
	}
}

func (t HourTransform) Project(name string, pred BoundPredicate) (UnboundPredicate, error) {
	return projectTimeTransform(t, name, pred)
}

func removeTransform(partName string, pred BoundPredicate) (UnboundPredicate, error) {
	switch p := pred.(type) {
	case BoundUnaryPredicate:
		return p.AsUnbound(Reference(partName)), nil
	case BoundLiteralPredicate:
		return p.AsUnbound(Reference(partName), p.Literal()), nil
	case BoundSetPredicate:
		return p.AsUnbound(Reference(partName), p.Literals().Members()), nil
	}

	return nil, fmt.Errorf("%w: cannot replace transform in unknown predicate: %s",
		ErrInvalidArgument, pred)
}

func projectTransformPredicate(t Transform, partitionName string, pred BoundPredicate) (UnboundPredicate, error) {
	term := pred.Term()
	bt, ok := term.(*BoundTransform)
	if !ok || !t.Equals(bt.transform) {
		return nil, nil
	}

	return removeTransform(partitionName, pred)
}

func transformLiteral[T LiteralType](fn func(any) Optional[T], lit Literal) Literal {
	switch l := lit.(type) {
	case BoolLiteral:
		return NewLiteral(fn(bool(l)).Val)
	case Int32Literal:
		return NewLiteral(fn(int32(l)).Val)
	case Int64Literal:
		return NewLiteral(fn(int64(l)).Val)
	case Float32Literal:
		return NewLiteral(fn(float32(l)).Val)
	case Float64Literal:
		return NewLiteral(fn(float64(l)).Val)
	case DateLiteral:
		return NewLiteral(fn(Date(l)).Val)
	case TimeLiteral:
		return NewLiteral(fn(Time(l)).Val)
	case TimestampLiteral:
		return NewLiteral(fn(Timestamp(l)).Val)
	case StringLiteral:
		return NewLiteral(fn(string(l)).Val)
	case FixedLiteral:
		return NewLiteral(fn([]byte(l)).Val)
	case BinaryLiteral:
		return NewLiteral(fn([]byte(l)).Val)
	case UUIDLiteral:
		return NewLiteral(fn(uuid.UUID(l)).Val)
	case DecimalLiteral:
		return NewLiteral(fn(Decimal(l)).Val)
	}

	panic("invalid literal type")
}

func wrapTransformFn[T LiteralType](fn func(any) any) func(any) Optional[T] {
	return func(v any) Optional[T] {
		out := fn(v)
		if out == nil {
			return Optional[T]{}
		}

		return Optional[T]{Valid: true, Val: out.(T)}
	}
}

func truncateNumber[T LiteralType](name string, pred BoundLiteralPredicate, fn func(any) Optional[T]) (UnboundPredicate, error) {
	boundary, ok := pred.Literal().(NumericLiteral)
	if !ok {
		return nil, fmt.Errorf("%w: expected numeric literal, got %s",
			ErrInvalidArgument, boundary.Type())
	}

	switch pred.Op() {
	case OpLT:
		return LiteralPredicate(OpLTEQ, Reference(name),
			transformLiteral(fn, boundary.Decrement())), nil
	case OpLTEQ:
		return LiteralPredicate(OpLTEQ, Reference(name),
			transformLiteral(fn, boundary)), nil
	case OpGT:
		return LiteralPredicate(OpGTEQ, Reference(name),
			transformLiteral(fn, boundary.Increment())), nil
	case OpGTEQ:
		return LiteralPredicate(OpGTEQ, Reference(name),
			transformLiteral(fn, boundary)), nil
	case OpEQ:
		return LiteralPredicate(OpEQ, Reference(name),
			transformLiteral(fn, boundary)), nil
	}

	return nil, nil
}

func truncateArray[T LiteralType](name string, pred BoundLiteralPredicate, fn func(any) Optional[T]) (UnboundPredicate, error) {
	boundary := pred.Literal()

	switch pred.Op() {
	case OpLT, OpLTEQ:
		return LiteralPredicate(OpLTEQ, Reference(name),
			transformLiteral(fn, boundary)), nil
	case OpGT, OpGTEQ:
		return LiteralPredicate(OpGTEQ, Reference(name),
			transformLiteral(fn, boundary)), nil
	case OpEQ:
		return LiteralPredicate(OpEQ, Reference(name),
			transformLiteral(fn, boundary)), nil
	case OpStartsWith:
		return LiteralPredicate(OpStartsWith, Reference(name),
			transformLiteral(fn, boundary)), nil
	case OpNotStartsWith:
		return LiteralPredicate(OpNotStartsWith, Reference(name),
			transformLiteral(fn, boundary)), nil
	}

	return nil, nil
}

func setApplyTransform[T LiteralType](name string, pred BoundSetPredicate, fn func(any) Optional[T]) UnboundPredicate {
	lits := pred.Literals().Members()
	for i, l := range lits {
		lits[i] = transformLiteral(fn, l)
	}

	return pred.AsUnbound(Reference(name), lits)
}
