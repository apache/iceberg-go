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

package internal

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/apache/iceberg-go"
)

var (
	truncateUpperBoundTextBenchmarkSink string
	truncateVariantBoundBenchmarkSink   [2]iceberg.Literal
)

func BenchmarkTruncateUpperBoundText(b *testing.B) {
	for _, tc := range benchmarkTruncatedBoundValues() {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.value)))
			b.ResetTimer()

			for range b.N {
				truncateUpperBoundTextBenchmarkSink = TruncateUpperBoundText(tc.value, 16)
			}
		})
	}
}

func BenchmarkTruncateVariantBoundString(b *testing.B) {
	for _, tc := range benchmarkTruncatedBoundValues() {
		b.Run(tc.name, func(b *testing.B) {
			lower := iceberg.StringLiteral(tc.value)
			upper := iceberg.StringLiteral(tc.value)

			b.ReportAllocs()
			b.SetBytes(int64(len(tc.value)))
			b.ResetTimer()

			for range b.N {
				lo, hi := truncateVariantBound(iceberg.PrimitiveTypes.String, lower, upper, 16)
				truncateVariantBoundBenchmarkSink = [2]iceberg.Literal{lo, hi}
			}
		})
	}
}

func benchmarkTruncatedBoundValues() []struct {
	name  string
	value string
} {
	return []struct {
		name  string
		value string
	}{
		{name: "ascii-32B", value: strings.Repeat("a", 32)},
		{name: "ascii-1KB", value: strings.Repeat("a", 1024)},
		{name: "ascii-64KB", value: strings.Repeat("a", 64*1024)},
		{name: "utf8-32B", value: strings.Repeat("é", 15) + "ab"},
		{name: "utf8-1KB", value: strings.Repeat("é", 511) + "ab"},
		{name: "utf8-64KB", value: strings.Repeat("é", 32767) + "ab"},
		{name: "upper-surrogate-gap", value: strings.Repeat("a", 15) + string('\uD7FF') + "x"},
		{name: "upper-max-rune-carry", value: strings.Repeat("a", 15) + string(utf8.MaxRune) + "x"},
		{name: "upper-all-max-runes", value: strings.Repeat(string(utf8.MaxRune), 17)},
	}
}
