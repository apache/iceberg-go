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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestStringLiteralTimestampNanoRange(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		typ     iceberg.Type
		want    iceberg.TimestampNsLiteral
		wantErr bool
	}{
		{"timestamp min", "1677-09-21T00:12:43.145224192", iceberg.PrimitiveTypes.TimestampNs, math.MinInt64, false},
		{"timestamp max", "2262-04-11T23:47:16.854775807", iceberg.PrimitiveTypes.TimestampNs, math.MaxInt64, false},
		{"timestamp below min by one ns", "1677-09-21T00:12:43.145224191", iceberg.PrimitiveTypes.TimestampNs, 0, true},
		{"timestamp above max by one ns", "2262-04-11T23:47:16.854775808", iceberg.PrimitiveTypes.TimestampNs, 0, true},
		{"timestamp far past", "1600-01-01T00:00:00", iceberg.PrimitiveTypes.TimestampNs, 0, true},
		{"timestamp far future", "2300-01-01T00:00:00", iceberg.PrimitiveTypes.TimestampNs, 0, true},
		{"timestamptz min", "1677-09-21T00:12:43.145224192Z", iceberg.PrimitiveTypes.TimestampTzNs, math.MinInt64, false},
		{"timestamptz max", "2262-04-11T23:47:16.854775807Z", iceberg.PrimitiveTypes.TimestampTzNs, math.MaxInt64, false},
		{"timestamptz min with offset", "1677-09-20T23:12:43.145224192-01:00", iceberg.PrimitiveTypes.TimestampTzNs, math.MinInt64, false},
		{"timestamptz max with offset", "2262-04-12T00:47:16.854775807+01:00", iceberg.PrimitiveTypes.TimestampTzNs, math.MaxInt64, false},
		{"timestamptz below min with offset", "1677-09-20T23:12:43.145224191-01:00", iceberg.PrimitiveTypes.TimestampTzNs, 0, true},
		{"timestamptz above max with offset", "2262-04-12T00:47:16.854775808+01:00", iceberg.PrimitiveTypes.TimestampTzNs, 0, true},
		{"timestamptz far past", "1600-01-01T00:00:00Z", iceberg.PrimitiveTypes.TimestampTzNs, 0, true},
		{"timestamptz far future", "2300-01-01T00:00:00Z", iceberg.PrimitiveTypes.TimestampTzNs, 0, true},
		{"timestamptz normal offset", "1970-01-01T01:00:00+01:00", iceberg.PrimitiveTypes.TimestampTzNs, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := iceberg.StringLiteral(tt.input).To(tt.typ)
			if tt.wantErr {
				require.ErrorIs(t, err, iceberg.ErrBadCast)
				require.Nil(t, got)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
