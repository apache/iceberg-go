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
	"testing"

	"github.com/apache/iceberg-go"
)

var parseTransformBenchmarkSink iceberg.Transform

func BenchmarkParseTransform(b *testing.B) {
	cases := []struct {
		name  string
		value string
	}{
		{name: "identity", value: "identity"},
		{name: "day", value: "day"},
		{name: "bucket", value: "bucket[16]"},
		{name: "truncate", value: "truncate[8]"},
		{name: "mixed simple", value: "IdEnTiTy"},
		{name: "mixed parameterized", value: "BuCkEt[16]"},
		{name: "unknown", value: "custom_transform[42]"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				transform, err := iceberg.ParseTransform(tc.value)
				if err != nil {
					b.Fatal(err)
				}
				parseTransformBenchmarkSink = transform
			}
		})
	}
}
