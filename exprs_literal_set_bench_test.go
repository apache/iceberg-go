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
	"strconv"
	"testing"
)

var (
	benchmarkLiteralSetSink  Set[Literal]
	benchmarkBooleanExprSink BooleanExpression
)

func benchmarkLiteralSetValues(size int) []Literal {
	values := make([]Literal, size)
	for i := range values {
		values[i] = NewLiteral(int32(i))
	}

	return values
}

func BenchmarkNewLiteralSet(b *testing.B) {
	for _, size := range []int{8, 64, 1024, 8192} {
		values := benchmarkLiteralSetValues(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				benchmarkLiteralSetSink = newLiteralSet(values...)
			}
		})
	}
}

func BenchmarkBindLiteralSet(b *testing.B) {
	schema := NewSchema(1, NestedField{ID: 1, Name: "value", Type: PrimitiveTypes.Int32})
	for _, size := range []int{8, 64, 1024, 8192} {
		values := benchmarkLiteralSetValues(size)
		predicate := SetPredicate(OpIn, Reference("value"), values).(UnboundPredicate)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var err error
				benchmarkBooleanExprSink, err = predicate.Bind(schema, true)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
