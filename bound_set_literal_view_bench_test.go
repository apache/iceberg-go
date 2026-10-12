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
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
)

var benchmarkBoundSetLiteralsSink iceberg.Set[iceberg.Literal]

func BenchmarkBoundSetPredicateLiterals(b *testing.B) {
	for _, kind := range []string{"int32", "binary"} {
		for _, size := range []int{2, 8, 9, 64, 1024, 8192} {
			b.Run(kind+"/"+strconv.Itoa(size), func(b *testing.B) {
				typ := iceberg.Type(iceberg.PrimitiveTypes.Int32)
				values := make([]iceberg.Literal, size)
				for i := range values {
					if kind == "binary" {
						value := make([]byte, 16)
						binary.LittleEndian.PutUint64(value, uint64(i))
						values[i] = iceberg.NewLiteral(value)
						typ = iceberg.PrimitiveTypes.Binary
					} else {
						values[i] = iceberg.NewLiteral(int32(i))
					}
				}
				schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: typ})
				bound, err := iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("value"), values).(iceberg.UnboundPredicate).Bind(schema, true)
				if err != nil {
					b.Fatal(err)
				}
				predicate := bound.(iceberg.BoundSetPredicate)

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					benchmarkBoundSetLiteralsSink = predicate.Literals()
				}
			})
		}
	}
}

func BenchmarkBoundSetPredicateLiteralsFiltered(b *testing.B) {
	values := make([]iceberg.Literal, 8192)
	for i := range values {
		value := int64(i)
		if i >= 2 {
			value += math.MaxInt32
		}
		values[i] = iceberg.NewLiteral(value)
	}
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32})
	bound, err := iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("value"), values).(iceberg.UnboundPredicate).Bind(schema, true)
	if err != nil {
		b.Fatal(err)
	}
	predicate := bound.(iceberg.BoundSetPredicate)
	if predicate.Literals().Len() != 2 {
		b.Fatal("expected two literals after binding")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkBoundSetLiteralsSink = predicate.Literals()
	}
}
