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
	"net/url"
	"strings"
	"testing"
)

var queryEscapeBenchmarkSink string

// BenchmarkAppendQueryEscaped compares the existing url.QueryEscape + WriteString
// path with direct appending to the same builder and capacity.
func BenchmarkAppendQueryEscaped(b *testing.B) {
	cases := []struct {
		name  string
		value string
	}{
		{"ASCII-32", "customer-region-2026-oct-europe"},
		{"Escaped-48", "Paris / Île-de-France ? + travel / cities"},
		{"Long-escaped", strings.Repeat("some/part?=%\t+é", 24)},
		{"Long-plain", strings.Repeat("customerseg2026", 30)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for _, method := range []string{"QueryEscape", "AppendQueryEscaped"} {
				b.Run(method, func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						var builder strings.Builder
						builder.Grow(len(tc.value) + 20)
						builder.WriteString("partition=")
						if method == "QueryEscape" {
							builder.WriteString(url.QueryEscape(tc.value))
						} else {
							AppendQueryEscaped(&builder, tc.value)
						}
						queryEscapeBenchmarkSink = builder.String()
					}
				})
			}
		})
	}
}
