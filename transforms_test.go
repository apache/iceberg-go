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

	"github.com/apache/arrow-go/v18/arrow/decimal"
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
		{"bucket[5]", iceberg.BucketTransform{NumBuckets: 5}},
		{"bucket[100]", iceberg.BucketTransform{NumBuckets: 100}},
		{"BUCKET[5]", iceberg.BucketTransform{NumBuckets: 5}},
		{"bUCKeT[100]", iceberg.BucketTransform{NumBuckets: 100}},
		{"truncate[10]", iceberg.TruncateTransform{Width: 10}},
		{"truncate[255]", iceberg.TruncateTransform{Width: 255}},
		{"TRUNCATE[10]", iceberg.TruncateTransform{Width: 10}},
		{"tRuNCATe[255]", iceberg.TruncateTransform{Width: 255}},
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

func TestToHumanString(t *testing.T) {
	decVal, _ := decimal.Decimal128FromString("14.21", 4, 2)
	negDecVal, _ := decimal.Decimal128FromString("-1.50", 9, 2)

	tests := []struct {
		transform iceberg.Transform
		input     any
		expected  string
	}{
		{iceberg.YearTransform{}, int32(47), "2017"},
		{iceberg.MonthTransform{}, int32(575), "2017-12"},
		{iceberg.DayTransform{}, int32(17501), "2017-12-01"},
		{iceberg.YearTransform{}, nil, "null"},
		{iceberg.MonthTransform{}, nil, "null"},
		{iceberg.DayTransform{}, nil, "null"},
		{iceberg.HourTransform{}, nil, "null"},
		{iceberg.HourTransform{}, int32(420042), "2017-12-01-18"},
		{iceberg.YearTransform{}, int32(-1), "1969"},
		{iceberg.MonthTransform{}, int32(-1), "1969-12"},
		{iceberg.DayTransform{}, int32(-1), "1969-12-31"},
		{iceberg.HourTransform{}, int32(-1), "1969-12-31-23"},
		{iceberg.YearTransform{}, int32(0), "1970"},
		{iceberg.MonthTransform{}, int32(0), "1970-01"},
		{iceberg.DayTransform{}, int32(0), "1970-01-01"},
		{iceberg.HourTransform{}, int32(0), "1970-01-01-00"},
		{iceberg.VoidTransform{}, nil, "null"},
		{iceberg.IdentityTransform{}, nil, "null"},
		{iceberg.TruncateTransform{Width: 1}, []byte{0x00, 0x01, 0x02, 0x03}, "AAECAw=="},
		{iceberg.TruncateTransform{Width: 1}, iceberg.Decimal{
			Val: decVal, Scale: 2,
		}, "14.21"},
		{iceberg.TruncateTransform{Width: 1}, int32(123), "123"},
		{iceberg.TruncateTransform{Width: 1}, int64(123), "123"},
		{iceberg.TruncateTransform{Width: 1}, "foo", "foo"},
		{iceberg.IdentityTransform{}, nil, "null"},
		{iceberg.IdentityTransform{}, iceberg.Date(17501), "2017-12-01"},
		{iceberg.IdentityTransform{}, iceberg.Time(36775038194), "10:12:55.038194"},
		{iceberg.IdentityTransform{}, iceberg.Timestamp(1512151975038194), "2017-12-01T18:12:55.038194"},
		{iceberg.IdentityTransform{}, int64(-1234567890000), "-1234567890000"},
		{iceberg.IdentityTransform{}, "a/b/c=d", "a/b/c=d"},
		{iceberg.IdentityTransform{}, []byte("foo"), "Zm9v"},
		{iceberg.IdentityTransform{}, iceberg.Decimal{Val: negDecVal, Scale: 2}, "-1.50"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.transform.ToHumanStr(tt.input))
		})
	}
}
