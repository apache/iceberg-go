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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithSelectedFieldsDoesNotAliasCallerSlice(t *testing.T) {
	fields := []string{"id", "name"}

	option := WithSelectedFields(fields...)
	scan := &Scan{}
	option(scan)

	fields[0] = "updated_id"

	assert.Equal(t, []string{"id", "name"}, scan.selectedFields)
	assert.NotEqual(t, fields[0], scan.selectedFields[0])
}

func TestWithMaxConcurrency(t *testing.T) {
	cases := []struct {
		name string
		n    int
		want int
	}{
		{"positive sets concurrency", 4, 4},
		{"zero is a no-op", 0, 0},
		{"negative is a no-op", -1, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scan := &Scan{}
			WithMaxConcurrency(tc.n)(scan)
			assert.Equal(t, tc.want, scan.concurrency)
		})
	}
}

// The deprecated WitMaxConcurrency alias must stay in lock-step with the
// canonical WithMaxConcurrency so callers on the old name don't drift.
func TestWitMaxConcurrencyAliasParity(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 8} {
		canonical, alias := &Scan{}, &Scan{}
		WithMaxConcurrency(n)(canonical)
		WitMaxConcurrency(n)(alias)
		assert.Equal(t, canonical.concurrency, alias.concurrency, "n=%d", n)
	}
}
