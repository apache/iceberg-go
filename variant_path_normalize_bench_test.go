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
	"strings"
	"testing"
)

var normalizedVariantPathBenchmarkResult string

func BenchmarkNormalizeVariantPath(b *testing.B) {
	cases := []struct {
		name   string
		fields []string
	}{
		{"single", []string{"event_id"}},
		{"nested4", []string{"location", "latitude", "metadata", "distance"}},
		{"nested16", strings.Fields(strings.Repeat("partition_column ", 16))},
		{"nested64", strings.Fields(strings.Repeat("partition_column ", 64))},
		{"escapedUnicode", []string{"東京\n", "naïve\\field", "🦊'report"}},
		{"unicode", []string{"naïve", "地名", "la🦊"}},
		{"escaped", []string{"o'brien", "a\\b", "a\nline", "a\x01b"}},
		{"controls", []string{strings.Repeat("a\x01\x02\x03\x04\x05\x1f", 8)}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				normalizedVariantPathBenchmarkResult = NormalizeVariantPath(tc.fields)
			}
		})
	}
}
