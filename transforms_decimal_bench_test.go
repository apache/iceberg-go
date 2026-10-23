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

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
)

var (
	benchmarkDecimalBucketLiteralResult   iceberg.Optional[iceberg.Literal]
	benchmarkDecimalBucketResult          iceberg.Optional[int32]
	benchmarkDecimalTruncateLiteralResult iceberg.Optional[iceberg.Literal]
	benchmarkDecimalTruncateResult        any
)

func BenchmarkBucketTransformDecimal(b *testing.B) {
	values := []iceberg.Decimal{
		{Val: decimal128.FromI64(0)},
		{Val: decimal128.FromI64(127)},
		{Val: decimal128.FromI64(128)},
		{Val: decimal128.FromI64(-128)},
		{Val: decimal128.New(0, 1<<63)},
		{Val: decimal128.New(-1, 0)},
		{Val: decimal128.New(-1, 1)},
	}
	literals := make([]iceberg.Literal, len(values))
	boxedValues := make([]any, len(values))
	for i, value := range values {
		literals[i] = iceberg.DecimalLiteral(value)
		boxedValues[i] = value
	}
	transform := iceberg.BucketTransform{NumBuckets: 97}

	b.Run("Apply", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			benchmarkDecimalBucketLiteralResult = transform.Apply(iceberg.Optional[iceberg.Literal]{
				Valid: true,
				Val:   literals[i%len(literals)],
			})
		}
	})

	b.Run("Transformer", func(b *testing.B) {
		transformer := transform.Transformer(iceberg.DecimalTypeOf(38, 0))
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			benchmarkDecimalBucketResult = transformer(boxedValues[i%len(boxedValues)])
		}
	})
}

func BenchmarkTruncateTransformDecimal(b *testing.B) {
	values := []iceberg.Decimal{
		{Val: decimal128.FromI64(0), Scale: 2},
		{Val: decimal128.FromI64(1065), Scale: 2},
		{Val: decimal128.FromI64(-1065), Scale: 2},
		{Val: decimal128.New(0x123456789abcdef, 0xfedcba9876543210), Scale: 2},
		{Val: decimal128.New(-0x123456789abcdef, 0xfedcba9876543210), Scale: 2},
	}
	tests := []struct {
		name   string
		width  int
		values []iceberg.Decimal
	}{
		{name: "zero", width: 50, values: values[:1]},
		{name: "positive", width: 50, values: values[1:2]},
		{name: "negative", width: 50, values: values[2:3]},
		{name: "wide-positive", width: 50, values: values[3:4]},
		{name: "wide-negative", width: 50, values: values[4:5]},
		{name: "width-1", width: 1, values: values},
		{name: "width-max", width: math.MaxInt32, values: values},
	}
	for _, tt := range tests {
		transform := iceberg.TruncateTransform{Width: tt.width}
		literals := make([]iceberg.Literal, len(tt.values))
		boxedValues := make([]any, len(tt.values))
		for i, value := range tt.values {
			literals[i] = iceberg.DecimalLiteral(value)
			boxedValues[i] = value
		}

		b.Run(tt.name+"/Apply", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				benchmarkDecimalTruncateLiteralResult = transform.Apply(iceberg.Optional[iceberg.Literal]{
					Valid: true,
					Val:   literals[i%len(literals)],
				})
			}
		})
		b.Run(tt.name+"/Transformer", func(b *testing.B) {
			transformer, err := transform.Transformer(iceberg.DecimalTypeOf(38, 2))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				benchmarkDecimalTruncateResult = transformer(boxedValues[i%len(boxedValues)])
			}
		})
	}
}
