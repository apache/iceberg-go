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

package substrait

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var benchmarkSubstraitLiteralSetResult any

func BenchmarkToSubstraitLiteralSet(b *testing.B) {
	cases := []struct {
		name string
		typ  iceberg.Type
		lit  func(int) iceberg.Literal
	}{
		{
			name: "int64",
			typ:  iceberg.PrimitiveTypes.Int64,
			lit: func(value int) iceberg.Literal {
				return iceberg.Int64Literal(1_000_000 + value)
			},
		},
		{
			name: "string",
			typ:  iceberg.PrimitiveTypes.String,
			lit: func(value int) iceberg.Literal {
				return iceberg.StringLiteral(fmt.Sprintf("value-%08d", value))
			},
		},
	}

	for _, testCase := range cases {
		b.Run(testCase.name, func(b *testing.B) {
			for _, size := range []int{10, 100, 1000} {
				template := make([]iceberg.Literal, size)
				for i := range template {
					template[i] = testCase.lit((i * 7919) % size)
				}
				lits := make([]iceberg.Literal, size)

				b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						copy(lits, template)
						benchmarkSubstraitLiteralSetResult = toSubstraitLiteralSet(testCase.typ, lits)
					}
				})
			}
		})
	}
}
