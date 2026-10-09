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
)

var benchmarkIntegerTruncateResult any

func BenchmarkTruncateTransformInteger(b *testing.B) {
	tests := []struct {
		name   string
		typ    iceberg.Type
		width  int
		values []any
	}{
		{
			name:  "int32/mixed",
			typ:   iceberg.PrimitiveTypes.Int32,
			width: 97,
			values: []any{
				int32(-1000), int32(-98), int32(-1), int32(0),
				int32(1), int32(96), int32(97), int32(1000),
			},
		},
		{
			name:  "int64/mixed",
			typ:   iceberg.PrimitiveTypes.Int64,
			width: 97,
			values: []any{
				int64(-1 << 40), int64(-1000), int64(-1), int64(0),
				int64(1), int64(1000), int64(1 << 40),
			},
		},
		{
			name:  "int32/width-max",
			typ:   iceberg.PrimitiveTypes.Int32,
			width: math.MaxInt32,
			values: []any{
				int32(math.MinInt32), int32(-1), int32(0),
				int32(math.MaxInt32 - 1), int32(math.MaxInt32),
			},
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			transformer, err := (iceberg.TruncateTransform{Width: tt.width}).Transformer(tt.typ)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			index := 0
			for b.Loop() {
				benchmarkIntegerTruncateResult = transformer(tt.values[index])
				index++
				if index == len(tt.values) {
					index = 0
				}
			}
		})
	}
}
