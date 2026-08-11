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
	"fmt"
	"strings"
)

// NormalizeVariantPath renders member names as the spec's RFC-9535 normalized JSON path.
func NormalizeVariantPath(fields []string) string {
	if len(fields) == 0 {
		return "$"
	}

	var b strings.Builder
	b.WriteByte('$')
	for _, f := range fields {
		b.WriteString("['")
		b.WriteString(rfc9535Escape(f))
		b.WriteString("']")
	}

	return b.String()
}

func rfc9535Escape(name string) string {
	if strings.IndexFunc(name, func(r rune) bool {
		return r < 0x20 || r == '\'' || r == '\\'
	}) < 0 {
		return name
	}

	var b strings.Builder
	b.Grow(len(name) + 4)
	for _, r := range name {
		switch r {
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\'':
			b.WriteString(`\'`)
		case '\\':
			b.WriteString(`\\`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}

	return b.String()
}

// parseVariantPath parses a dot-shorthand variant path ($.a.b) into its member names.
func parseVariantPath(path string) ([]string, error) {
	if strings.ContainsAny(path, "[]") {
		return nil, fmt.Errorf("%w: unsupported variant path, contains bracket: %q", ErrInvalidArgument, path)
	}
	if strings.Contains(path, "*") {
		return nil, fmt.Errorf("%w: unsupported variant path, contains wildcard: %q", ErrInvalidArgument, path)
	}
	if strings.Contains(path, "..") {
		return nil, fmt.Errorf("%w: unsupported variant path, contains recursive descent: %q", ErrInvalidArgument, path)
	}

	parts := strings.Split(path, ".")
	if parts[0] != "$" {
		return nil, fmt.Errorf("%w: invalid variant path, does not start with $: %q", ErrInvalidArgument, path)
	}

	names := parts[1:]
	for _, name := range names {
		if !isRFC9535MemberName(name) {
			return nil, fmt.Errorf("%w: invalid variant path %q (%q has invalid characters)", ErrInvalidArgument, path, name)
		}
	}

	return names, nil
}

// isRFC9535MemberName reports whether name is a valid RFC-9535 member-name shorthand.
func isRFC9535MemberName(name string) bool {
	for i, r := range name {
		if i == 0 {
			if !isRFC9535NameFirst(r) {
				return false
			}

			continue
		}
		if isRFC9535NameFirst(r) || (r >= '0' && r <= '9') {
			continue
		}

		return false
	}

	return name != ""
}

func isRFC9535NameFirst(r rune) bool {
	return r == '_' ||
		(r >= 'A' && r <= 'Z') ||
		(r >= 'a' && r <= 'z') ||
		(r >= 0x80 && r <= 0xD7FF) ||
		(r >= 0xE000 && r <= 0x10FFFF)
}
