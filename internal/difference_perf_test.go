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

package internal_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/iceberg-go/internal"
)

func TestDifferenceFastPaths(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b []string
		want []string
	}{
		{name: "nil", want: []string{}},
		{name: "empty input", b: []string{"a", "b", "c"}, want: []string{}},
		{name: "no exclusions", a: []string{"b", "a", "a"}, want: []string{"b", "a", "a"}},
		{name: "one exclusion", a: []string{"a", "b", "a", "c"}, b: []string{"a"}, want: []string{"b", "c"}},
		{name: "two exclusions", a: []string{"a", "b", "c", "b", "d"}, b: []string{"b", "d"}, want: []string{"a", "c"}},
		{name: "many exclusions", a: []string{"a", "b", "c", "d", "e", "d"}, b: []string{"b", "d", "x", "d"}, want: []string{"a", "c", "e"}},
		{name: "all excluded", a: []string{"a", "a"}, b: []string{"a"}, want: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := internal.Difference(tc.a, tc.b); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Difference(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

var differenceBenchSink []string

func BenchmarkDifference(b *testing.B) {
	for _, size := range []int{1_024, 8_192} {
		paths := make([]string, size)
		for i := range paths {
			paths[i] = fmt.Sprintf("data/partition=%06d/file-%06d.parquet", i, i)
		}
		for _, excluded := range []int{0, 1, size / 2, size} {
			b.Run(fmt.Sprintf("entries=%d/exclusions=%d", size, excluded), func(b *testing.B) {
				removed := paths[:excluded]
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					differenceBenchSink = internal.Difference(paths, removed)
				}
			})
		}
	}
}
