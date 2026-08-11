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
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseVariantPath(tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseVariantPathRejects(t *testing.T) {
	for _, tt := range []struct {
		name string
		path string
	}{
		{"bracket notation", "$['event_id']"},
		{"wildcard", "$.*"},
		{"recursive descent", "$..event_id"},
		{"missing root", "event_id"},
		{"leading digit member", "$.1abc"},
		{"empty member", "$."},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseVariantPath(tt.path)
			require.ErrorIs(t, err, ErrInvalidArgument)
		})
	}
}
