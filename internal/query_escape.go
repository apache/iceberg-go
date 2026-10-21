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

import "strings"

const queryEscapeHex = "0123456789ABCDEF"

// AppendQueryEscaped writes the exact encoding produced by url.QueryEscape
// directly to a builder. Avoiding the temporary encoded string saves work
// when building partition paths, especially for multiple escaped fields.
func AppendQueryEscaped(builder *strings.Builder, value string) {
	start := 0
	for i := range len(value) {
		char := value[i]
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '-' || char == '_' || char == '.' || char == '~' {
			continue
		}

		if start < i {
			builder.WriteString(value[start:i])
		}
		if char == ' ' {
			builder.WriteByte('+')
		} else {
			builder.WriteByte('%')
			builder.WriteByte(queryEscapeHex[char>>4])
			builder.WriteByte(queryEscapeHex[char&15])
		}
		start = i + 1
	}

	builder.WriteString(value[start:])
}
