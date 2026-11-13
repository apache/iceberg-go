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
	"math"
	"testing"
)

func TestConvertPartitionInt32ChecksRange(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int32
		valid bool
	}{
		{"minimum", int64(math.MinInt32), math.MinInt32, true},
		{"maximum", int64(math.MaxInt32), math.MaxInt32, true},
		{"signed overflow", int64(math.MaxInt32) + 1, 0, false},
		{"signed underflow", int64(math.MinInt32) - 1, 0, false},
		{"unsigned maximum", uint64(math.MaxInt32), math.MaxInt32, true},
		{"unsigned overflow", uint64(math.MaxInt32) + 1, 0, false},
		{"unsigned 32-bit overflow", uint32(math.MaxInt32) + 1, 0, false},
		{"float within range", float64(-42), -42, true},
		{"float overflow", float64(math.MaxInt32) + 1, 0, false},
		{"fractional float", float64(42.5), 0, false},
		{"fractional float32", float32(-42.5), 0, false},
		{"NaN", math.NaN(), 0, false},
		{"infinity", math.Inf(1), 0, false},
		{"date", Date(-7), -7, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, valid := convertPartitionInt32(tt.value)
			if valid != tt.valid || (valid && got != tt.want) {
				t.Fatalf("convertPartitionInt32(%v) = (%d, %t), want (%d, %t)",
					tt.value, got, valid, tt.want, tt.valid)
			}
		})
	}
}

func TestConvertPartitionInt64ChecksRange(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int64
		valid bool
	}{
		{"minimum", int64(math.MinInt64), math.MinInt64, true},
		{"maximum", int64(math.MaxInt64), math.MaxInt64, true},
		{"uint64 within range", uint64(math.MaxInt64), math.MaxInt64, true},
		{"uint64 overflow", uint64(math.MaxInt64) + 1, 0, false},
		{"float within range", float64(1 << 40), 1 << 40, true},
		{"float minimum", float64(math.MinInt64), math.MinInt64, true},
		{"float rounded maximum", float64(math.MaxInt64), 0, false},
		{"float underflow", -0x1p64, 0, false},
		{"fractional float", float64(-42.5), 0, false},
		{"fractional float32", float32(42.5), 0, false},
		{"NaN", math.NaN(), 0, false},
		{"negative infinity", math.Inf(-1), 0, false},
		{"timestamp", Timestamp(-7), -7, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, valid := convertPartitionInt64(tt.value)
			if valid != tt.valid || (valid && got != tt.want) {
				t.Fatalf("convertPartitionInt64(%v) = (%d, %t), want (%d, %t)",
					tt.value, got, valid, tt.want, tt.valid)
			}
		})
	}
}

func TestPartitionFieldStatsRejectsInvalidIntegerBounds(t *testing.T) {
	for _, tt := range []struct {
		name  string
		typ   PrimitiveType
		value any
	}{
		{"int32 overflow", PrimitiveTypes.Int32, int64(math.MaxInt32) + 1},
		{"int32 fraction", PrimitiveTypes.Int32, 1.5},
		{"int64 overflow", PrimitiveTypes.Int64, uint64(math.MaxInt64) + 1},
		{"int64 fraction", PrimitiveTypes.Int64, 1.5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stats, err := newPartitionFieldStat(tt.typ)
			if err != nil {
				t.Fatal(err)
			}
			if err := stats.update(tt.value); err == nil {
				t.Fatalf("expected invalid %T partition bound to be rejected", tt.value)
			}
		})
	}
}
