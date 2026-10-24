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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iceberg

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func checkCalendarDays(t *testing.T, days int64) {
	t.Helper()
	expected := epochTM.AddDate(0, 0, int(days))
	year, month := yearMonthFromUnixDays(days)
	if year != expected.Year() || month != int(expected.Month()) {
		t.Fatalf("days=%d: got %d-%02d; expected %d-%02d",
			days, year, month, expected.Year(), expected.Month())
	}
}

func TestYearMonthFromUnixDaysMatchesTime(t *testing.T) {
	for _, days := range []int64{
		math.MinInt32, -719469, -719468, -719467,
		-146097, -146096, -366, -365, -1, 0, 1, 59, 60,
		365, 366, 10957, 19782, 20000, 146097, math.MaxInt32,
	} {
		checkCalendarDays(t, days)
	}

	// Exercise Gregorian leap days, century boundaries, pre-epoch dates
	// and the entire signed Date range with a fixed seed.
	rng := rand.New(rand.NewSource(19700101))
	for range 30000 {
		checkCalendarDays(t, int64(int32(rng.Uint32())))
	}
}

func TestYearMonthPartitionTransformParity(t *testing.T) {
	testCases := []struct {
		name   string
		typ    Type
		values []any
	}{
		{
			name: "date",
			typ:  DateType{},
			values: []any{
				Date(math.MinInt32), Date(-719468), Date(-365), Date(-1),
				Date(0), Date(1), Date(18628), Date(20000), Date(math.MaxInt32),
			},
		},
		{
			name: "timestamp",
			typ:  TimestampType{},
			values: []any{
				Timestamp(math.MinInt64), Timestamp(-86_400_000_001),
				Timestamp(-1), Timestamp(0), Timestamp(1),
				Timestamp(86_399_999_999), Timestamp(86_400_000_000),
				Timestamp(math.MaxInt64),
			},
		},
		{
			name: "timestamp-nanoseconds",
			typ:  TimestampNsType{},
			values: []any{
				TimestampNano(math.MinInt64), TimestampNano(-86_400_000_000_001),
				TimestampNano(-1), TimestampNano(0), TimestampNano(1),
				TimestampNano(86_399_999_999_999), TimestampNano(86_400_000_000_000),
				TimestampNano(math.MaxInt64),
			},
		},
	}

	for _, tc := range testCases {
		for _, tr := range []struct {
			name      string
			transform TimeTransform
		}{
			{"year", YearTransform{}},
			{"month", MonthTransform{}},
		} {
			t.Run(tc.name+"/"+tr.name, func(t *testing.T) {
				fn, err := tr.transform.Transformer(tc.typ)
				if err != nil {
					t.Fatal(err)
				}
				if got := fn(nil); got.Valid {
					t.Fatal("nil input should return invalid optional")
				}

				check := func(value any) {
					t.Helper()
					var tm time.Time
					var literal Literal
					switch v := value.(type) {
					case Date:
						tm, literal = v.ToTime(), DateLiteral(v)
					case Timestamp:
						tm, literal = v.ToTime(), TimestampLiteral(v)
					case TimestampNano:
						tm, literal = v.ToTime(), TimestampNsLiteral(v)
					}
					expected := int32(tm.Year() - epochTM.Year())
					if tr.name == "month" {
						expected = int32((tm.Year()-epochTM.Year())*12 + int(tm.Month()) - int(epochTM.Month()))
					}

					if result := fn(value); !result.Valid || result.Val != expected {
						t.Fatalf("%s of %v: Transformer=%v, expected=%d",
							tr.name, value, result, expected)
					}

					if applied := tr.transform.Apply(Optional[Literal]{Valid: true, Val: literal}); !applied.Valid ||
						applied.Val != Int32Literal(expected) {
						t.Fatalf("%s of %v: Apply=%v, expected=%d",
							tr.name, value, applied, expected)
					}
				}

				for _, value := range tc.values {
					check(value)
				}
				rng := rand.New(rand.NewSource(934871))
				for range 5000 {
					switch tc.typ.(type) {
					case DateType:
						check(Date(int32(rng.Uint32())))
					case TimestampType:
						check(Timestamp(int64(rng.Uint64())))
					case TimestampNsType:
						check(TimestampNano(int64(rng.Uint64())))
					}
				}
			})
		}
	}
}

// referenceCalendarTransformer preserves the previous time.Time-based
// implementations for paired benchmarks. Keep each source type assertion
// inside its closure to compare equivalent work.
func referenceCalendarTransformer(src Type, month bool) func(any) Optional[int32] {
	switch src.(type) {
	case DateType:
		if month {
			return func(value any) Optional[int32] {
				if value == nil {
					return Optional[int32]{}
				}
				tm := value.(Date).ToTime()

				return Optional[int32]{
					Valid: true,
					Val:   int32((tm.Year()-epochTM.Year())*12 + int(tm.Month()) - int(epochTM.Month())),
				}
			}
		}

		return func(value any) Optional[int32] {
			if value == nil {
				return Optional[int32]{}
			}
			tm := value.(Date).ToTime()

			return Optional[int32]{Valid: true, Val: int32(tm.Year() - epochTM.Year())}
		}
	case TimestampType:
		if month {
			return func(value any) Optional[int32] {
				if value == nil {
					return Optional[int32]{}
				}
				tm := value.(Timestamp).ToTime()

				return Optional[int32]{
					Valid: true,
					Val:   int32((tm.Year()-epochTM.Year())*12 + int(tm.Month()) - int(epochTM.Month())),
				}
			}
		}

		return func(value any) Optional[int32] {
			if value == nil {
				return Optional[int32]{}
			}
			tm := value.(Timestamp).ToTime()

			return Optional[int32]{Valid: true, Val: int32(tm.Year() - epochTM.Year())}
		}
	case TimestampNsType:
		if month {
			return func(value any) Optional[int32] {
				if value == nil {
					return Optional[int32]{}
				}
				tm := value.(TimestampNano).ToTime()

				return Optional[int32]{
					Valid: true,
					Val:   int32((tm.Year()-epochTM.Year())*12 + int(tm.Month()) - int(epochTM.Month())),
				}
			}
		}

		return func(value any) Optional[int32] {
			if value == nil {
				return Optional[int32]{}
			}
			tm := value.(TimestampNano).ToTime()

			return Optional[int32]{Valid: true, Val: int32(tm.Year() - epochTM.Year())}
		}
	}

	return nil
}

var calendarBenchmarkSink int32

func BenchmarkCalendarPartitionTransform(b *testing.B) {
	cases := []struct {
		name   string
		typ    Type
		values []any
	}{
		{
			name:   "date",
			typ:    DateType{},
			values: []any{Date(0), Date(-10000), Date(19782), Date(20000), Date(math.MinInt32), Date(math.MaxInt32)},
		},
		{
			name:   "timestamp",
			typ:    TimestampType{},
			values: []any{Timestamp(0), Timestamp(-1), Timestamp(-1682982932131), Timestamp(17282931234567), Timestamp(math.MaxInt64)},
		},
		{
			name:   "timestamp-nanoseconds",
			typ:    TimestampNsType{},
			values: []any{TimestampNano(0), TimestampNano(-1), TimestampNano(-1682982932131), TimestampNano(17282931234567), TimestampNano(math.MaxInt64)},
		},
	}
	for _, tc := range cases {
		for _, tr := range []struct {
			name      string
			transform TimeTransform
			isMonth   bool
		}{
			{name: "year", transform: YearTransform{}},
			{name: "month", transform: MonthTransform{}, isMonth: true},
		} {
			optimized, err := tr.transform.Transformer(tc.typ)
			if err != nil {
				b.Fatal(err)
			}
			reference := referenceCalendarTransformer(tc.typ, tr.isMonth)
			for _, candidate := range []struct {
				name string
				fn   func(any) Optional[int32]
			}{
				{name: "reference", fn: reference},
				{name: "optimized", fn: optimized},
			} {
				b.Run(tc.name+"/"+tr.name+"/"+candidate.name, func(b *testing.B) {
					b.ReportAllocs()
					var sum int32
					for i := range b.N {
						sum += candidate.fn(tc.values[i%len(tc.values)]).Val
					}
					calendarBenchmarkSink = sum
				})
			}
		}
	}
}
