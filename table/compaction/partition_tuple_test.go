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
)

type partitionTupleStringer string

func (s partitionTupleStringer) String() string { return "custom:" + string(s) }

func TestAppendPartitionTupleMatchesFmt(t *testing.T) {
	values := []any{
		nil,
		"",
		"a=b;:é",
		-123,
		int32(-45),
		iceberg.Date(-1),
		iceberg.Date(20500),
		int64(1234567890123),
		true,
		false,
		uint64(1 << 63),
		math.NaN(),
		math.Inf(1),
		math.Copysign(0, -1),
		float32(3.5),
		[]byte{0, 255},
		[]int{1, 2},
		partitionTupleStringer("x"),
		time.Unix(0, 0).UTC(),
		(*int)(nil),
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

			for _, prefix := range []string{"", "42:"} {
				want := []byte(prefix)
				for _, id := range ids {
					want = fmt.Appendf(want, "%d=%v;", id, partition[id])
				}
				got := appendPartitionTuple([]byte(prefix), partition)
				if !bytes.Equal(got, want) {
					t.Fatalf("fields=%d seed=%d prefix=%q: got %q, want %q",
						fields, seed, prefix, got, want)
				}
			}
		}
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
