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

	"github.com/stretchr/testify/assert"
)

// buildChain produces a linear ancestry chain [1, 2, ..., n] where snapshot i
// has parent i-1, and returns a SnapshotLookup over it.
func buildChain(n int64) SnapshotLookup {
	snaps := make(map[int64]*Snapshot, int(n))
	for i := int64(1); i <= n; i++ {
		s := Snapshot{SnapshotID: i}
		if i > 1 {
			parent := i - 1
			s.ParentSnapshotID = &parent
		}
		snaps[i] = &s
	}

	return func(id int64) *Snapshot {
		return snaps[id]
	}
}

// buildCycle produces a malformed 2-snapshot cycle: 1 ← 2 ← 1.
func buildCycle() SnapshotLookup {
	parentOf1 := int64(2)
	parentOf2 := int64(1)
	snaps := map[int64]*Snapshot{
		1: {SnapshotID: 1, ParentSnapshotID: &parentOf1},
		2: {SnapshotID: 2, ParentSnapshotID: &parentOf2},
	}

	return func(id int64) *Snapshot { return snaps[id] }
}

func TestAncestorsOf_LinearChain(t *testing.T) {
	lookup := buildChain(5)

	got := AncestorsOf(5, lookup)

	assert.Equal(t, []int64{5, 4, 3, 2, 1}, snapshotIDs(got))
}

func TestAncestorsOf_RootSnapshot(t *testing.T) {
	lookup := buildChain(3)

	got := AncestorsOf(1, lookup)

	assert.Equal(t, []int64{1}, snapshotIDs(got))
}

func TestAncestorsOf_Unknown(t *testing.T) {
	lookup := buildChain(3)

	got := AncestorsOf(99, lookup)

	assert.Empty(t, got)
}

func TestAncestorsOf_BrokenChain(t *testing.T) {
	// Chain is 1 ← 2 ← 3, but snapshot 2 is missing from the lookup.
	chain := buildChain(3)
	lookup := func(id int64) *Snapshot {
		if id == 2 {
			return nil
		}

		return chain(id)
	}

	got := AncestorsOf(3, lookup)

	// Should yield only 3 before the chain breaks at the missing parent.
	assert.Equal(t, []int64{3}, snapshotIDs(got))
}

func TestAncestorsOf_CycleDefense(t *testing.T) {
	got := AncestorsOf(1, buildCycle())

	// Walks 1 → 2 → 1, stops on cycle detection with both snapshots visited.
	assert.Equal(t, []int64{1, 2}, snapshotIDs(got))
}

func TestAncestorsOfChecked_LinearChain(t *testing.T) {
	got, complete := AncestorsOfChecked(5, buildChain(5))

	assert.True(t, complete, "walk reached the root")
	assert.Equal(t, []int64{5, 4, 3, 2, 1}, snapshotIDs(got))
}

func TestAncestorsOfChecked_Unknown(t *testing.T) {
	got, complete := AncestorsOfChecked(99, buildChain(3))

	assert.False(t, complete, "unresolvable starting snapshot is not a complete walk")
	assert.Empty(t, got)
}

func TestAncestorsOfChecked_BrokenChain(t *testing.T) {
	// Chain 1 ← 2 ← 3 with snapshot 2 missing — the walk stops at 3 and
	// the missing parent must be reported as truncation.
	chain := buildChain(3)
	lookup := func(id int64) *Snapshot {
		if id == 2 {
			return nil
		}

		return chain(id)
	}

	got, complete := AncestorsOfChecked(3, lookup)

	assert.False(t, complete, "missing intermediate must surface as truncation")
	assert.Equal(t, []int64{3}, snapshotIDs(got))
}

func TestAncestorsOfChecked_CycleDefense(t *testing.T) {
	got, complete := AncestorsOfChecked(1, buildCycle())

	assert.False(t, complete, "cycle must surface as truncation")
	assert.Equal(t, []int64{1, 2}, snapshotIDs(got))
}

func TestAncestorsBetween_LinearChain(t *testing.T) {
	lookup := buildChain(5)

	got, ok := AncestorsBetween(5, 2, lookup)

	assert.True(t, ok)
	assert.Equal(t, []int64{5, 4, 3}, snapshotIDs(got))
}

func TestAncestorsBetween_Adjacent(t *testing.T) {
	lookup := buildChain(3)

	got, ok := AncestorsBetween(3, 2, lookup)

	assert.True(t, ok)
	assert.Equal(t, []int64{3}, snapshotIDs(got))
}

func TestAncestorsBetween_SameSnapshot(t *testing.T) {
	lookup := buildChain(3)

	got, ok := AncestorsBetween(3, 3, lookup)

	assert.True(t, ok, "same snapshot is trivially its own base")
	assert.Empty(t, got)
}

func TestAncestorsBetween_BaseNotInChain(t *testing.T) {
	// Writer's base (999) is not in the chain of latest (5).
	// Should return the full chain with baseFound=false to signal divergent.
	lookup := buildChain(5)

	got, ok := AncestorsBetween(5, 999, lookup)

	assert.False(t, ok)
	assert.Equal(t, []int64{5, 4, 3, 2, 1}, snapshotIDs(got))
}

func TestAncestorsBetween_LatestUnknown(t *testing.T) {
	lookup := buildChain(3)

	got, ok := AncestorsBetween(99, 1, lookup)

	assert.False(t, ok)
	assert.Empty(t, got)
}

func TestAncestorsBetween_BrokenMidChain(t *testing.T) {
	// Chain 1 ← 2 ← 3 ← 4 ← 5, with snapshot 3 missing mid-chain.
	// Writer's base is 1, latest is 5; walk breaks at 3 and cannot reach base.
	chain := buildChain(5)
	lookup := func(id int64) *Snapshot {
		if id == 3 {
			return nil
		}

		return chain(id)
	}

	got, ok := AncestorsBetween(5, 1, lookup)

	// Partial walk before break: [5, 4]. baseFound=false signals caller
	// must treat this as divergent, not as complete result.
	assert.False(t, ok)
	assert.Equal(t, []int64{5, 4}, snapshotIDs(got))
}

func TestAncestorsBetween_CycleDefense(t *testing.T) {
	// Cycle 1 ↔ 2, writer's base is 999 (not in cycle).
	got, ok := AncestorsBetween(1, 999, buildCycle())

	assert.False(t, ok)
	assert.Equal(t, []int64{1, 2}, snapshotIDs(got))
}

func TestIsAncestorOf_Self(t *testing.T) {
	lookup := buildChain(3)

	assert.True(t, IsAncestorOf(2, 2, lookup))
}

func TestIsAncestorOf_DirectParent(t *testing.T) {
	lookup := buildChain(3)

	assert.True(t, IsAncestorOf(3, 2, lookup))
	assert.True(t, IsAncestorOf(3, 1, lookup))
}

func TestIsAncestorOf_Descendant(t *testing.T) {
	lookup := buildChain(5)

	// 5 is a descendant of 3, not an ancestor.
	assert.False(t, IsAncestorOf(3, 5, lookup))
}

func TestIsAncestorOf_UnknownSnapshot(t *testing.T) {
	lookup := buildChain(3)

	assert.False(t, IsAncestorOf(99, 1, lookup))
}

func TestIsAncestorOf_BothUnknown(t *testing.T) {
	// Neither id exists — must not return true just because id == ancestorID.
	lookup := buildChain(3)

	assert.False(t, IsAncestorOf(99, 99, lookup))
}

func TestIsAncestorOf_UnknownAncestor(t *testing.T) {
	lookup := buildChain(3)

	assert.False(t, IsAncestorOf(3, 99, lookup))
}

func TestIsAncestorOf_MissingMidChain(t *testing.T) {
	// Chain 1 ← 2 ← 3 with snapshot 2 missing.
	// 1 is not reachable from 3 through the broken chain.
	chain := buildChain(3)
	lookup := func(id int64) *Snapshot {
		if id == 2 {
			return nil
		}

		return chain(id)
	}

	assert.False(t, IsAncestorOf(3, 1, lookup))
}

func TestIsAncestorOf_CycleDefense(t *testing.T) {
	// Cycle contains 1 and 2 but not 99 — must return false, not loop.
	assert.False(t, IsAncestorOf(1, 99, buildCycle()))
}

func TestIsAncestorOf_AncestorReachableInsideCycle(t *testing.T) {
	// In cycle 1 ↔ 2, snapshot 2 IS reachable from 1 via parent pointer.
	// Must return true before cycle detection triggers.
	assert.True(t, IsAncestorOf(1, 2, buildCycle()))
	assert.True(t, IsAncestorOf(2, 1, buildCycle()))
}

func snapshotIDs(snaps []Snapshot) []int64 {
	ids := make([]int64, len(snaps))
	for i, s := range snaps {
		ids[i] = s.SnapshotID
	}

	return ids
}
