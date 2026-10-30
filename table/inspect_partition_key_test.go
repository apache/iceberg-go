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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

// Keep the old formatter as the oracle for uncommon value types and edge cases.
func inspectPartitionKeyFmtReference(partition map[int]any) string {
	ids := make([]int, 0, len(partition))
	for id := range partition {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	var key strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&key, "%d:%T:%v;", id, partition[id], partition[id])
	}

	return key.String()
}

func TestInspectPartitionKeyMatchesFmt(t *testing.T) {
	check := func(partition map[int]any) {
		t.Helper()
		want := inspectPartitionKeyFmtReference(partition)
		if got := inspectPartitionKey(partition); got != want {
			t.Fatalf("partition key mismatch for %v: got %q, want %q", partition, got, want)
		}
	}

	for _, partition := range []map[int]any{
		nil,
		{},
		{101: int32(-2147483648), 12: int64(-9223372036854775807 - 1), -1: false},
		{2: "unicode / 😀", 99: []byte{0, 127, 255}, 4: true},
		{30: []byte(nil), 40: []byte{}, 50: nil},
		{7: math.NaN(), 8: math.Inf(-1), 1: time.Unix(0, 0).UTC()},
		{1: struct{ Tag string }{"fallback"}},
	} {
		check(partition)
	}

	rng := rand.New(rand.NewSource(19))
	for range 1000 {
		partition := make(map[int]any)
		for range rng.Intn(24) {
			id := rng.Intn(50) - 10
			switch rng.Intn(9) {
			case 0:
				partition[id] = rng.Int31()
			case 1:
				partition[id] = rng.Int63()
			case 2:
				partition[id] = fmt.Sprintf("str-%d", rng.Int63())
			case 3:
				partition[id] = rng.Intn(2) == 1
			case 4:
				partition[id] = []byte{byte(rng.Intn(256)), byte(rng.Intn(256))}
			case 5:
				partition[id] = []byte(nil)
			case 6:
				partition[id] = math.Copysign(float64(rng.Intn(100)), -1)
			case 7:
				partition[id] = nil
			case 8:
				partition[id] = uint32(rng.Uint32())
			}
		}
		check(partition)
	}
}

var inspectPartitionKeyBenchmarkSink string

func BenchmarkInspectPartitionKey(b *testing.B) {
	for _, fieldCount := range []int{1, 8, 32} {
		for _, kind := range []string{"int32", "string", "binary", "mixed"} {
			partitions := make([]map[int]any, 1000)
			for i := range partitions {
				partition := make(map[int]any, fieldCount)
				for j := range fieldCount {
					id := j + 1000
					switch kind {
					case "int32":
						partition[id] = int32(i + j)
					case "string":
						partition[id] = fmt.Sprintf("part-%d-field-%d", i, j)
					case "binary":
						partition[id] = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
					case "mixed":
						switch j % 4 {
						case 0:
							partition[id] = int32(i + j)
						case 1:
							partition[id] = int64(-i - j)
						case 2:
							partition[id] = fmt.Sprintf("p-%d", i)
						case 3:
							partition[id] = []byte{1, 2, 3, 4, 5}
						}
					}
				}
				partitions[i] = partition
			}

			b.Run(fmt.Sprintf("%s/%d_fields", kind, fieldCount), func(b *testing.B) {
				for _, benchmark := range []struct {
					name string
					key  func(map[int]any) string
				}{
					{"fmt", inspectPartitionKeyFmtReference},
					{"optimized", inspectPartitionKey},
				} {
					b.Run(benchmark.name, func(b *testing.B) {
						b.ReportAllocs()
						for i := range b.N {
							inspectPartitionKeyBenchmarkSink = benchmark.key(partitions[i%len(partitions)])
						}
					})
				}
			})
		}
	}
}
