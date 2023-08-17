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

package iceberg_test

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTransform(t *testing.T) {
	tests := []struct {
		toparse  string
		expected iceberg.Transform
	}{
		{"identity", iceberg.IdentityTransform{}},
		{"IdEnTiTy", iceberg.IdentityTransform{}},
		{"void", iceberg.VoidTransform{}},
		{"VOId", iceberg.VoidTransform{}},
		{"year", iceberg.YearTransform{}},
		{"yEAr", iceberg.YearTransform{}},
		{"month", iceberg.MonthTransform{}},
		{"MONtH", iceberg.MonthTransform{}},
		{"day", iceberg.DayTransform{}},
		{"DaY", iceberg.DayTransform{}},
		{"hour", iceberg.HourTransform{}},
		{"hOuR", iceberg.HourTransform{}},
		{"bucket[5]", iceberg.BucketTransform{N: 5}},
		{"bucket[100]", iceberg.BucketTransform{N: 100}},
		{"BUCKET[5]", iceberg.BucketTransform{N: 5}},
		{"bUCKeT[100]", iceberg.BucketTransform{N: 100}},
		{"truncate[10]", iceberg.TruncateTransform{W: 10}},
		{"truncate[255]", iceberg.TruncateTransform{W: 255}},
		{"TRUNCATE[10]", iceberg.TruncateTransform{W: 10}},
		{"tRuNCATe[255]", iceberg.TruncateTransform{W: 255}},
	}

	for _, tt := range tests {
		t.Run(tt.toparse, func(t *testing.T) {
			transform, err := iceberg.ParseTransform(tt.toparse)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, transform)

			txt, err := transform.MarshalText()
			assert.NoError(t, err)
			assert.Equal(t, strings.ToLower(tt.toparse), string(txt))
		})
	}

	errorTests := []struct {
		name    string
		toparse string
	}{
		{"foobar", "foobar"},
		{"bucket no brackets", "bucket"},
		{"truncate no brackets", "truncate"},
		{"bucket no val", "bucket[]"},
		{"truncate no val", "truncate[]"},
		{"bucket neg", "bucket[-1]"},
		{"truncate neg", "truncate[-1]"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := iceberg.ParseTransform(tt.toparse)
			assert.Nil(t, tr)
			assert.ErrorIs(t, err, iceberg.ErrInvalidTransform)
			assert.ErrorContains(t, err, tt.toparse)
		})
	}
}
