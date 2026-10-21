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

func TestAppendQueryEscapedMatchesURLQueryEscape(t *testing.T) {
	values := []string{
		"", "north-america", "spaces in value", "a/b?c=d&e+f", "x%y#z",
		"~._-", "\x00\x7f\xff", "東京都", "café/naïve", "a\nb\tc",
		strings.Repeat("/ +%", 100), strings.Repeat("ascii-09", 100),
	}
	// Cover every possible pair of input bytes, including invalid UTF-8.
	for i := range 256 {
		for j := range 256 {
			values = append(values, string([]byte{byte(i), byte(j)}))
		}
	}

	for _, value := range values {
		var builder strings.Builder
		builder.WriteString("partition=")
		AppendQueryEscaped(&builder, value)
		got := builder.String()
		want := "partition=" + url.QueryEscape(value)
		if got != want {
			t.Fatalf("encoding differs for %q: got %q, want %q", value, got, want)
		}
	}
}
