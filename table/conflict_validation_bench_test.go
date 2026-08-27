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
)

var canonicalPartitionKeyBenchmarkSink string

func BenchmarkCanonicalPartitionKey(b *testing.B) {
	for _, tc := range []struct {
		name      string
		partition map[int]any
	}{
		{
			name:      "fields=1/int32",
			partition: map[int]any{1: int32(1)},
		},
		{
			name: "fields=2/int32",
			partition: map[int]any{
				1: int32(1), 2: int32(2),
			},
		},
		{
			name: "fields=4/int32",
			partition: map[int]any{
				1: int32(1), 2: int32(2), 3: int32(3), 4: int32(4),
			},
		},
		{
			name: "fields=8/mixed",
			partition: map[int]any{
				1: int32(1), 2: "two", 3: int64(3), 4: "four",
				5: int32(5), 6: "six", 7: int64(7), 8: "eight",
			},
		},
		{
			name: "fields=4/binary-string",
			partition: map[int]any{
				1: []byte("one"), 2: "two", 3: []byte("three"), 4: "four",
			},
		},
		{
			name: "fields=16/mixed",
			partition: map[int]any{
				1: int32(1), 2: "two", 3: int64(3), 4: "four",
				5: int32(5), 6: "six", 7: int64(7), 8: "eight",
				9: int32(9), 10: "ten", 11: int64(11), 12: "twelve",
				13: int32(13), 14: "fourteen", 15: int64(15), 16: "sixteen",
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				key, err := canonicalPartitionKey(7, tc.partition)
				if err != nil {
					b.Fatal(err)
				}
				canonicalPartitionKeyBenchmarkSink = key
			}
		})
	}
}
