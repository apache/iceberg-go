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

// Keep the previous implementation here to make before/after comparisons
// reproducible on the same toolchain and machine.
func truncateStringBefore(s string, width int) string {
	for idx := range s {
		if width == 0 {
			return s[:idx]
		}
		width--
	}

	return s
}

func TestTruncateStringFastPath(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		width       int
		want        string
	}{
		{"short_ascii_unchanged", "us-east-1", 32, "us-east-1"},
		{"short_ascii_truncated", "us-east-1", 4, "us-e"},
		{"unicode_unchanged", "東京🧊Berlin", 32, "東京🧊Berlin"},
		{"unicode_truncated", "東京🧊Berlin", 4, "東京🧊B"},
		{"utf8_bytes_exceed_width", "éé", 3, "éé"},
		{"combining_codepoints", "e\u0301clair", 1, "e"},
		{"invalid_utf8_prefix", "a\xffb", 2, "a\xff"},
		{"negative_width", "iceberg", -1, "iceberg"},
		{"exact_width", "iceberg", 7, "iceberg"},
		{"empty", "", 1, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := truncateString(tc.input, tc.width); got != tc.want {
				t.Fatalf("truncateString(%q, %d) = %q, want %q", tc.input, tc.width, got, tc.want)
			}
		})
	}

	// Compare every width around both byte and code-point boundaries. The
	// invalid UTF-8 input verifies this optimization preserves the existing
	// behavior even for strings that cannot appear in spec-compliant data.
	for _, input := range []string{
		"", "us-east-1", "東京🧊Berlin", "éé", "a\xffb", strings.Repeat("é", 64),
	} {
		for width := 0; width <= len(input)+2; width++ {
			if got, want := truncateString(input, width), truncateStringBefore(input, width); got != want {
				t.Fatalf("truncateString(%q, %d) = %q, before = %q", input, width, got, want)
			}
		}
	}
}

var truncateStringBenchmarkSink string

func BenchmarkTruncateStringFastPath(b *testing.B) {
	for _, tc := range []struct {
		name, input string
		width       int
	}{
		{"ascii_short_noop", "us-east-1", 32},
		{"ascii_short_truncate", "us-east-1", 4},
		{"ascii_long_noop", strings.Repeat("a", 128), 256},
		{"unicode_short_noop", "東京🧊Berlin", 32},
		{"unicode_short_truncate", "東京🧊Berlin", 4},
		{"unicode_long_noop", strings.Repeat("é", 64), 256},
		{"unicode_long_truncate", strings.Repeat("é", 64), 16},
		{"utf8_bytes_exceed_width", "éé", 3},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for _, implementation := range []struct {
				name string
				run  func(string, int) string
			}{
				{"before", truncateStringBefore},
				{"after", truncateString},
			} {
				b.Run(implementation.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						truncateStringBenchmarkSink = implementation.run(tc.input, tc.width)
					}
				})
			}
		})
	}
}
