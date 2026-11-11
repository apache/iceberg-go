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

func TestFloat32LiteralComparatorJavaOrder(t *testing.T) {
	negativeZero := math.Float32frombits(1 << 31)
	compare := iceberg.Float32Literal(0).Comparator()

	tests := []struct {
		name        string
		left, right float32
		want        int
	}{
		{"negative infinity before finite", float32(math.Inf(-1)), -1, -1},
		{"negative zero before positive zero", negativeZero, 0, -1},
		{"positive zero after negative zero", 0, negativeZero, 1},
		{"positive infinity before NaN", float32(math.Inf(1)), float32(math.NaN()), -1},
		{"NaN after finite", float32(math.NaN()), 42, 1},
		{"NaNs equal for comparison", float32(math.NaN()), float32(math.NaN()), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, compare(tt.left, tt.right))
			require.Equal(t, -tt.want, compare(tt.right, tt.left))
		})
	}
}

func TestFloat64LiteralComparatorJavaOrder(t *testing.T) {
	negativeZero := math.Float64frombits(1 << 63)
	compare := iceberg.Float64Literal(0).Comparator()

	tests := []struct {
		name        string
		left, right float64
		want        int
	}{
		{"negative infinity before finite", math.Inf(-1), -1, -1},
		{"negative zero before positive zero", negativeZero, 0, -1},
		{"positive zero after negative zero", 0, negativeZero, 1},
		{"positive infinity before NaN", math.Inf(1), math.NaN(), -1},
		{"NaN after finite", math.NaN(), 42, 1},
		{"NaNs equal for comparison", math.NaN(), math.NaN(), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, compare(tt.left, tt.right))
			require.Equal(t, -tt.want, compare(tt.right, tt.left))
		})
	}
}
