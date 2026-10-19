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

// Malformed cycles must be detected on both sides of the inline-to-map
// transition, without incorrectly rejecting ancestors inside the cycle.
func TestSnapshotAncestryCycleAcrossInlineBoundary(t *testing.T) {
	for _, count := range []int{2, 8, 9, 16, 17, 32, 128} {
		t.Run(fmt.Sprintf("snapshots=%d", count), func(t *testing.T) {
			lookup := buildLongSnapshotCycle(count)

			ancestors, complete := AncestorsOfChecked(1, lookup)
			if complete || len(ancestors) != count {
				t.Fatalf("AncestorsOfChecked: complete=%t, count=%d; want false, %d",
					complete, len(ancestors), count)
			}
			for i, snap := range ancestors {
				if snap.SnapshotID != int64(i+1) {
					t.Fatalf("ancestor[%d]=%d, want %d", i, snap.SnapshotID, i+1)
				}
			}

			between, baseFound := AncestorsBetween(1, -1, lookup)
			if baseFound || len(between) != count {
				t.Fatalf("AncestorsBetween: baseFound=%t, count=%d; want false, %d",
					baseFound, len(between), count)
			}
			if IsAncestorOf(1, -1, lookup) {
				t.Fatal("missing ancestor must not be reachable through a cycle")
			}
			if !IsAncestorOf(1, int64(count), lookup) {
				t.Fatal("reachable ancestor inside a cycle was rejected")
			}
		})
	}
}

func TestSnapshotAncestryMissingParentAcrossInlineBoundary(t *testing.T) {
	for _, count := range []int64{8, 9, 16, 17, 32, 128} {
		t.Run(fmt.Sprintf("snapshots=%d", count), func(t *testing.T) {
			chain := buildChain(count)
			lookup := func(id int64) *Snapshot {
				if id == 1 {
					return nil
				}

				return chain(id)
			}

			ancestors, complete := AncestorsOfChecked(count, lookup)
			if complete || int64(len(ancestors)) != count-1 {
				t.Fatalf("AncestorsOfChecked: complete=%t, count=%d; want false, %d",
					complete, len(ancestors), count-1)
			}

			between, found := AncestorsBetween(count, -1, lookup)
			if found || int64(len(between)) != count-1 {
				t.Fatalf("AncestorsBetween: found=%t, count=%d; want false, %d",
					found, len(between), count-1)
			}
			if IsAncestorOf(count, 1, lookup) {
				t.Fatal("an unresolved ancestor must not match")
			}
		})
	}
}

func buildLongSnapshotCycle(count int) SnapshotLookup {
	snapshots := make(map[int64]*Snapshot, count)
	for i := 1; i <= count; i++ {
		parent := int64(i + 1)
		if i == count {
			parent = 1
		}
		snapshots[int64(i)] = &Snapshot{
			SnapshotID:       int64(i),
			ParentSnapshotID: &parent,
		}
	}

	return func(id int64) *Snapshot { return snapshots[id] }
}
