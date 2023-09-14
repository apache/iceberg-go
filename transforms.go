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
	"fmt"
	"strconv"
	"strings"
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
	ResultType(t Type) Type
}

// IdentityTransform uses the identity function, performing no transformation
// but instead partitioning on the value itself.
type IdentityTransform struct{}

func (t IdentityTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (IdentityTransform) String() string { return "identity" }

func (IdentityTransform) ResultType(t Type) Type { return t }

// VoidTransform is a transformation that always returns nil.
type VoidTransform struct{}

func (t VoidTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (VoidTransform) String() string { return "void" }

func (VoidTransform) ResultType(t Type) Type { return t }

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

func (BucketTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

// TruncateTransform is a transformation for truncating a value to a specified width.
type TruncateTransform struct {
	Width int
}

func (t TruncateTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t TruncateTransform) String() string { return fmt.Sprintf("truncate[%d]", t.Width) }

func (TruncateTransform) ResultType(t Type) Type { return t }

// YearTransform transforms a datetime value into a year value.
type YearTransform struct{}

func (t YearTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (YearTransform) String() string { return "year" }

func (YearTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

// MonthTransform transforms a datetime value into a month value.
type MonthTransform struct{}

func (t MonthTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (MonthTransform) String() string { return "month" }

func (MonthTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

// DayTransform transforms a datetime value into a date value.
type DayTransform struct{}

func (t DayTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (DayTransform) String() string { return "day" }

func (DayTransform) ResultType(Type) Type { return PrimitiveTypes.Date }

// HourTransform transforms a datetime value into an hour value.
type HourTransform struct{}

func (t HourTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (HourTransform) String() string { return "hour" }

func (HourTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }
