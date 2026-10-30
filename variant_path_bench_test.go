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

import "testing"

var (
	benchmarkVariantPathMember   string
	benchmarkVariantPathConsumed int
	benchmarkVariantPathFields   []string
)

// BenchmarkParseBracketSelector measures the selector fast path separately.
func BenchmarkParseBracketSelector(b *testing.B) {
	for _, tt := range []struct {
		name, selector string
	}{
		{"Short", "['id']"},
		{"Medium", "['event_type']"},
		{"Long", "['this_is_a_very_long_field_name_for_analytic_queries']"},
		{"UTF8", "['naïve_東京_データ']"},
		{"Escaped", "['o\\'brien']"},
		{"UnicodeEscape", "['\\uD840\\uDC00']"},
	} {
		b.Run(tt.name, func(b *testing.B) {
			path := "$" + tt.selector
			b.ReportAllocs()

			var member string
			var consumed int
			var err error
			for range b.N {
				member, consumed, err = parseBracketSelector(tt.selector, path)
			}
			if err != nil {
				b.Fatal(err)
			}
			benchmarkVariantPathMember = member
			benchmarkVariantPathConsumed = consumed
		})
	}
}

// BenchmarkParseVariantPath measures the complete parser, including slice allocations.
func BenchmarkParseVariantPath(b *testing.B) {
	for _, tt := range []struct {
		name, path string
	}{
		{"One", "$['event_type']"},
		{"Two", "$['location']['latitude']"},
		{"Four", "$['customer']['account']['profile']['event_type']"},
		{"UTF8", "$['naïve_東京_データ']"},
		{"Escaped", "$['o\\'brien']"},
		{"Mixed", "$['customer'].profile['created_at']"},
		{"DotOnly", "$.customer.profile.created_at"},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()

			var fields []string
			var err error
			for range b.N {
				fields, err = parseVariantPath(tt.path)
			}
			if err != nil {
				b.Fatal(err)
			}
			benchmarkVariantPathFields = fields
		})
	}
}
