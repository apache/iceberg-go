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
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
)

func TestDecimalLiteralComparatorCrossScale(t *testing.T) {
	decimal := func(unscaled int64, scale int) iceberg.Decimal {
		return iceberg.Decimal{Val: decimal128.FromI64(unscaled), Scale: scale}
	}
	compare := (iceberg.DecimalLiteral{}).Comparator()

	tests := []struct {
		name        string
		left, right iceberg.Decimal
		want        int
	}{
		{"fractional digits", decimal(13, 1), decimal(125, 2), 1},
		{"fractional digits reversed", decimal(125, 2), decimal(13, 1), -1},
		{"equal different scales", decimal(120, 2), decimal(12, 1), 0},
		{"negative fractional digits", decimal(-12, 1), decimal(-125, 2), 1},
		{"negative fractional digits reversed", decimal(-125, 2), decimal(-12, 1), -1},
		{"zero different scales", decimal(0, 0), decimal(0, 38), 0},
		{"same scale", decimal(13, 2), decimal(125, 2), -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compare(tt.left, tt.right); got != tt.want {
				t.Errorf("compare(%v, %v) = %d, want %d", tt.left, tt.right, got, tt.want)
			}
			if got := compare(tt.right, tt.left); got != -tt.want {
				t.Errorf("reverse compare(%v, %v) = %d, want %d", tt.right, tt.left, got, -tt.want)
			}
		})
	}

	t.Run("decimal128 rescale overflow", func(t *testing.T) {
		// Increasing a 38-digit value's scale overflows decimal128.
		large, ok := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
		if !ok {
			t.Fatal("invalid decimal test value")
		}
		left := decimal(-1, 1)
		right := iceberg.Decimal{Val: decimal128.FromBigInt(large.Neg(large)), Scale: 0}

		if got := compare(left, right); got != 1 {
			t.Errorf("compare(-0.1, -max38) = %d, want 1", got)
		}
		if got := compare(right, left); got != -1 {
			t.Errorf("compare(-max38, -0.1) = %d, want -1", got)
		}
	})
}
