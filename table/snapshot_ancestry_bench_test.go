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
	"testing"
)

var (
	benchmarkSnapshotAncestrySnapshots []Snapshot
	benchmarkSnapshotAncestryFound     bool
)

// End-to-end ancestry traversals, including snapshot lookups and result slices.
// Chain setup is outside the timed region. The depth=17 case exercises the
// transition from inline IDs to the map fallback.
func BenchmarkSnapshotAncestry(b *testing.B) {
	for _, depth := range []int64{1, 4, 8, 9, 16, 17, 32, 64, 256} {
		lookup := buildChain(depth)

		b.Run(fmt.Sprintf("is_ancestor/reachable/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkSnapshotAncestryFound = IsAncestorOf(depth, 1, lookup)
			}
			if !benchmarkSnapshotAncestryFound {
				b.Fatal("expected root to be reachable")
			}
		})

		b.Run(fmt.Sprintf("is_ancestor/missing/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkSnapshotAncestryFound = IsAncestorOf(depth, -1, lookup)
			}
			if benchmarkSnapshotAncestryFound {
				b.Fatal("missing ancestor was marked reachable")
			}
		})

		b.Run(fmt.Sprintf("ancestors_between/reachable/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkSnapshotAncestrySnapshots, benchmarkSnapshotAncestryFound = AncestorsBetween(depth, 1, lookup)
			}
			if !benchmarkSnapshotAncestryFound || int64(len(benchmarkSnapshotAncestrySnapshots)) != depth-1 {
				b.Fatal("incorrect ancestry between snapshots")
			}
		})

		b.Run(fmt.Sprintf("ancestors_between/missing/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkSnapshotAncestrySnapshots, benchmarkSnapshotAncestryFound = AncestorsBetween(depth, -1, lookup)
			}
			if benchmarkSnapshotAncestryFound || int64(len(benchmarkSnapshotAncestrySnapshots)) != depth {
				b.Fatal("unexpected complete ancestry for missing base")
			}
		})

		b.Run(fmt.Sprintf("ancestors_of/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkSnapshotAncestrySnapshots, benchmarkSnapshotAncestryFound = AncestorsOfChecked(depth, lookup)
			}
			if !benchmarkSnapshotAncestryFound || int64(len(benchmarkSnapshotAncestrySnapshots)) != depth {
				b.Fatal("incorrect complete ancestry")
			}
		})
	}
}
