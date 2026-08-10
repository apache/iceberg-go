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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeVariantPathEscaping(t *testing.T) {
	for _, tt := range []struct {
		name   string
		fields []string
		want   string
	}{
		{"root", nil, "$"},
		{"plain", []string{"event_type"}, "$['event_type']"},
		{"dotted name kept literal", []string{"user.name"}, "$['user.name']"},
		{"nested", []string{"location", "latitude"}, "$['location']['latitude']"},
		{"single quote escaped", []string{"o'brien"}, `$['o\'brien']`},
		{"backslash escaped", []string{`a\b`}, `$['a\\b']`},
		{"newline escaped", []string{"a\nb"}, `$['a\nb']`},
		{"other control char hex-escaped", []string{"a\x01b"}, "$['a\\u0001b']"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NormalizeVariantPath(tt.fields))
		})
	}
}

func TestParseVariantPath(t *testing.T) {
	for _, tt := range []struct {
		name string
		path string
		want []string
	}{
		{"root", "$", []string{}},
		{"single member", "$.event_id", []string{"event_id"}},
		{"nested members", "$.location.latitude", []string{"location", "latitude"}},
		{"underscore and digits", "$._a1.b2", []string{"_a1", "b2"}},
		{"non-ascii first char", "$.naïve", []string{"naïve"}},
		{"bracket notation", "$['event_id']", []string{"event_id"}},
		{"bracket nested", "$['location']['latitude']", []string{"location", "latitude"}},
		{"bracket dotted name", "$['user.name']", []string{"user.name"}},
		{"bracket leading digit", "$['1abc']", []string{"1abc"}},
		{"bracket escaped quote", `$['o\'brien']`, []string{"o'brien"}},
		{"bracket escaped backslash", `$['a\\b']`, []string{`a\b`}},
		{"bracket star is literal", "$['a*b']", []string{"a*b"}},
		{"mixed dot and bracket", "$['a'].b", []string{"a", "b"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseVariantPath(tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestParseVariantPathRoundTrip proves NormalizeVariantPath output parses back to the same members.
func TestParseVariantPathRoundTrip(t *testing.T) {
	for _, fields := range [][]string{
		{"event_id"},
		{"location", "latitude"},
		{"user.name"},
		{"o'brien"},
		{`a\b`},
		{"a\nb"},
		{"a\x01b"},
		{"a*b"},
		{"1abc"},
	} {
		got, err := parseVariantPath(NormalizeVariantPath(fields))
		require.NoError(t, err)
		assert.Equal(t, fields, got)
	}
}

func TestParseVariantPathRejects(t *testing.T) {
	for _, tt := range []struct {
		name string
		path string
	}{
		{"wildcard", "$.*"},
		{"recursive descent", "$..event_id"},
		{"missing root", "event_id"},
		{"leading digit member", "$.1abc"},
		{"empty member", "$."},
		{"array index", "$[0]"},
		{"bracket wildcard", "$[*]"},
		{"unquoted bracket", "$[event_id]"},
		{"unterminated bracket", "$['event_id"},
		{"missing close bracket", "$['event_id'"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseVariantPath(tt.path)
			require.ErrorIs(t, err, ErrInvalidArgument)
		})
	}
}
