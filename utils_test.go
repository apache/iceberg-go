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

	"github.com/stretchr/testify/require"
)

func TestDifference(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		expected []string
	}{
		{
			name:     "No elements in common",
			a:        []string{"a", "b", "c"},
			b:        []string{"d", "e", "f"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Some elements in common",
			a:        []string{"a", "b", "c"},
			b:        []string{"b"},
			expected: []string{"a", "c"},
		},
		{
			name:     "All elements in common",
			a:        []string{"a", "b", "c"},
			b:        []string{"a", "b", "c"},
			expected: []string{},
		},
		{
			name:     "Empty slice a",
			a:        []string{},
			b:        []string{"a", "b", "c"},
			expected: []string{},
		},
		{
			name:     "Empty slice b",
			a:        []string{"a", "b", "c"},
			b:        []string{},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "No elements in slice b present in slice a",
			a:        []string{"a", "b", "c"},
			b:        []string{"x", "y", "z"},
			expected: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)
			result := Difference(tt.a, tt.b)
			assert.ElementsMatch(tt.expected, result)
		})
	}
}
