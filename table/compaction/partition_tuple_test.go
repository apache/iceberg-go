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
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type partitionTupleStringer string

func (s partitionTupleStringer) String() string { return "custom:" + string(s) }

func TestAppendPartitionTupleDeterministic(t *testing.T) {
	values := []any{
		nil, "", "a=b;:é", -123, int32(-45), iceberg.Date(-1),
		iceberg.Date(20500), int64(1234567890123), true, false,
		uint64(1 << 63), math.NaN(), math.Inf(1), math.Copysign(0, -1),
		float32(3.5), []byte{0, 255}, []int{1, 2},
		partitionTupleStringer("x"), time.Unix(0, 0).UTC(), (*int)(nil),
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
			for i := len(ids) - 1; i >= 0; i-- {
				reversed[ids[i]] = partition[ids[i]]
			}
			for _, prefix := range []string{"", "42:"} {
				got := appendPartitionTuple([]byte(prefix), partition)
				want := appendPartitionTuple([]byte(prefix), reversed)
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
		{name: "separators across two fields",
			a: map[int]any{1: "a;2=b", 2: "c"},
			b: map[int]any{1: "a", 2: "b;2=c"}},
		{name: "nil vs literal nil",
			a: map[int]any{1: nil}, b: map[int]any{1: "<nil>"}},
		{name: "boolean vs text",
			a: map[int]any{1: true}, b: map[int]any{1: "true"}},
		{name: "positive and negative zero",
			a: map[int]any{1: math.Copysign(0, -1)}, b: map[int]any{1: float64(0)}},
		{name: "binary vs text",
			a: map[int]any{1: []byte{0, ';', '='}},
			b: map[int]any{1: string([]byte{0, ';', '='})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodedA := appendPartitionTuple(nil, tt.a)
			encodedB := appendPartitionTuple(nil, tt.b)
			if bytes.Equal(encodedA, encodedB) {
				t.Fatalf("different partitions have identical identity %q", encodedA)
			}
			if partitionBucketKey(7, tt.a) == partitionBucketKey(7, tt.b) {
				t.Fatal("different partitions were grouped under the same key")
			}
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
			a := partitionBucketKey(1, map[int]any{1000: tt.a})
			b := partitionBucketKey(1, map[int]any{1000: tt.b})
			if a != b {
				t.Fatalf("equivalent partition values have different keys %q and %q", a, b)
			}
		})
	}
}

func TestPartitionBucketKeyFromTupleMatchesFmt(t *testing.T) {
	for _, specID := range []int32{math.MinInt32, -1, 0, 1, math.MaxInt32} {
		for _, tuple := range []string{"", "1000=abc;", strings.Repeat("x", 128)} {
			got := partitionBucketKeyFromTuple(specID, []byte(tuple))
			want := fmt.Sprintf("%d:%s", specID, tuple)
			if got != want {
				t.Fatalf("spec ID %d: got %q, want %q", specID, got, want)
			}
		}
	}
}
