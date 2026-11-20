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

package compaction

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAppendPartitionTupleDeterministic(t *testing.T) {
	values := []any{
		nil, "", "a=b;:é", -123, int32(-45), iceberg.Date(-1),
		iceberg.Date(20500), int64(1234567890123), true, false,
		math.NaN(), math.Inf(1), math.Copysign(0, -1), float32(3.5),
		[]byte{0, 255},
		uuid.UUID{1, 2, 3},
	}
	for _, fields := range []int{0, 1, 3, 8, 9, 16, 32} {
		for seed := range 32 {
			partition := make(map[int]any, fields)
			for i := range fields {
				partition[1000+i*7] = values[(i*3+seed)%len(values)]
			}
			ids := make([]int, 0, len(partition))
			for id := range partition {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			reversed := make(map[int]any, len(partition))
			for _, id := range slices.Backward(ids) {
				reversed[id] = partition[id]
			}
			for _, prefix := range []string{"", "42:"} {
				got, err := tblutils.AppendPartitionTuple([]byte(prefix), partition)
				require.NoError(t, err)
				want, err := tblutils.AppendPartitionTuple([]byte(prefix), reversed)
				require.NoError(t, err)
				if !bytes.Equal(got, want) {
					t.Fatalf("fields=%d seed=%d prefix=%q: got %q, want %q",
						fields, seed, prefix, got, want)
				}
				if !bytes.HasPrefix(got, []byte(prefix)) {
					t.Fatalf("fields=%d prefix=%q: encoding lost prefix: %q", fields, prefix, got)
				}
			}
		}
	}
}

func TestAppendPartitionTupleCollisionResistance(t *testing.T) {
	tests := []struct {
		name string
		a, b map[int]any
	}{
		{
			name: "separators across two fields",
			a:    map[int]any{1: "a;2=b", 2: "c"},
			b:    map[int]any{1: "a", 2: "b;2=c"},
		},
		{
			name: "nil vs literal nil",
			a:    map[int]any{1: nil}, b: map[int]any{1: "<nil>"},
		},
		{
			name: "boolean vs text",
			a:    map[int]any{1: true}, b: map[int]any{1: "true"},
		},
		{
			name: "positive and negative zero",
			a:    map[int]any{1: math.Copysign(0, -1)}, b: map[int]any{1: float64(0)},
		},
		{
			name: "binary vs text",
			a:    map[int]any{1: []byte{0, ';', '='}},
			b:    map[int]any{1: string([]byte{0, ';', '='})},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := tblutils.CanonicalPartitionKey(7, tt.a)
			require.NoError(t, err)
			b, err := tblutils.CanonicalPartitionKey(7, tt.b)
			require.NoError(t, err)
			require.NotEqual(t, a, b)
		})
	}
}

func TestAppendPartitionTupleNormalizesDecodedValues(t *testing.T) {
	tests := []struct {
		name string
		a, b any
	}{
		{name: "int32 and int64", a: int32(7), b: int64(7)},
		{name: "date and integer", a: iceberg.Date(-20), b: int32(-20)},
		{name: "float32 and float64", a: float32(1.5), b: float64(1.5)},
		{name: "uuid and binary", a: uuid.UUID{1, 2, 3}, b: []byte{1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{name: "different NaN payloads", a: math.Float64frombits(0x7ff8000000000001), b: math.Float64frombits(0x7ff8000000000002)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := tblutils.CanonicalPartitionKey(1, map[int]any{1000: tt.a})
			require.NoError(t, err)
			b, err := tblutils.CanonicalPartitionKey(1, map[int]any{1000: tt.b})
			require.NoError(t, err)
			if a != b {
				t.Fatalf("equivalent partition values have different keys %q and %q", a, b)
			}
		})
	}
}

func TestCanonicalPartitionKeyKeepsSpecPrefix(t *testing.T) {
	for _, specID := range []int32{math.MinInt32, -1, 0, 1, math.MaxInt32} {
		for _, partition := range []map[int]any{nil, {1000: strings.Repeat("x", 128)}} {
			got, err := tblutils.CanonicalPartitionKey(specID, partition)
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(got, fmt.Sprintf("%d:", specID)))
		}
	}
}

func TestCanonicalPartitionKeySeparatesEmptyAndDifferentFieldIDs(t *testing.T) {
	cases := []struct {
		name string
		a, b map[int]any
	}{
		{name: "empty versus null field", a: nil, b: map[int]any{1000: nil}},
		{name: "empty versus empty string", a: nil, b: map[int]any{1000: ""}},
		{name: "empty versus empty binary", a: nil, b: map[int]any{1000: []byte{}}},
		{name: "field id matters", a: map[int]any{1000: "x"}, b: map[int]any{1001: "x"}},
		{name: "field ordering matters", a: map[int]any{1000: "x", 1001: "y"}, b: map[int]any{1000: "y", 1001: "x"}},
		{name: "binary length matters", a: map[int]any{1000: []byte{0}}, b: map[int]any{1000: []byte{0, 0}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, err := tblutils.CanonicalPartitionKey(3, tc.a)
			require.NoError(t, err)
			second, err := tblutils.CanonicalPartitionKey(3, tc.b)
			require.NoError(t, err)
			require.NotEqual(t, first, second)
		})
	}

	partition := map[int]any{1000: "same"}
	first, err := tblutils.CanonicalPartitionKey(1, partition)
	require.NoError(t, err)
	second, err := tblutils.CanonicalPartitionKey(2, partition)
	require.NoError(t, err)
	require.NotEqual(t, first, second, "the partition spec is part of the key")
}

func TestCanonicalPartitionKeyNormalizesTemporalRepresentations(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b any
	}{
		{name: "time and integer", a: iceberg.Time(1234), b: int64(1234)},
		{name: "timestamp and integer", a: iceberg.Timestamp(-1234), b: int64(-1234)},
		{name: "timestamp nanos and integer", a: iceberg.TimestampNano(1234), b: int64(1234)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, err := tblutils.CanonicalPartitionKey(1, map[int]any{1000: tc.a})
			require.NoError(t, err)
			second, err := tblutils.CanonicalPartitionKey(1, map[int]any{1000: tc.b})
			require.NoError(t, err)
			require.Equal(t, first, second)
		})
	}
}

