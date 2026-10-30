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

import "slices"

// SnapshotLookup returns the snapshot for the given id, or nil if not found.
// It matches the signature of Metadata.SnapshotByID.
type SnapshotLookup func(id int64) *Snapshot

// Most ancestry queries traverse only a few snapshots. Keep those IDs inline
// instead of hashing every hop. Longer or malformed chains still use a map
// so cycle detection stays linear and cannot loop forever.
const inlineSnapshotVisitLimit = 16

type snapshotVisitSet struct {
	inline   [inlineSnapshotVisitLimit]int64
	count    int
	overflow map[int64]struct{}
}

// seenOrAdd reports whether id was already visited.
func (s *snapshotVisitSet) seenOrAdd(id int64) bool {
	if s.overflow != nil {
		if _, seen := s.overflow[id]; seen {
			return true
		}
		s.overflow[id] = struct{}{}

		return false
	}

	if slices.Contains(s.inline[:s.count], id) {
		return true
	}
	if s.count < len(s.inline) {
		s.inline[s.count] = id
		s.count++

		return false
	}

	s.overflow = make(map[int64]struct{}, 2*len(s.inline))
	for _, previous := range s.inline {
		s.overflow[previous] = struct{}{}
	}
	s.overflow[id] = struct{}{}

	return false
}

// ancestryWalkMode selects how a snapshot ancestry walk ends.
// An expired base ID can end a conflict-detection walk, but checking whether
// an ID is an ancestor requires resolving that snapshot.
type ancestryWalkMode uint8

const (
	ancestryToRoot ancestryWalkMode = iota
	ancestryBeforeBase
	ancestryToResolvedAncestor
)

// walkSnapshotAncestry shares parent traversal, missing-snapshot handling and
// cycle detection. Only the first two modes accumulate snapshot values.
func walkSnapshotAncestry(startID, boundaryID int64, lookup SnapshotLookup, mode ancestryWalkMode) ([]Snapshot, bool) {
	var history []Snapshot
	var visited snapshotVisitSet

	id := startID
	for {
		// A base may have expired, so stop before resolving it.
		if mode == ancestryBeforeBase && id == boundaryID {
			return history, true
		}
		if visited.seenOrAdd(id) {
			return history, false
		}
		snap := lookup(id)
		if snap == nil {
			return history, false
		}

		// Membership requires the candidate ancestor to resolve.
		if mode == ancestryToResolvedAncestor && id == boundaryID {
			return nil, true
		}
		if mode != ancestryToResolvedAncestor {
			history = append(history, *snap)
		}
		if snap.ParentSnapshotID == nil {
			return history, mode == ancestryToRoot
		}
		id = *snap.ParentSnapshotID
	}
}

// AncestorsOf walks the parent chain starting from snapshotID and yields
// every snapshot reachable via ParentSnapshotID, including the starting
// snapshot itself. Iteration stops when a snapshot has no parent, the
// parent cannot be resolved via lookup, or a cycle is detected in
// malformed metadata.
//
// Returns snapshots by value in reverse-chronological order (newest first).
// Returns an empty slice if snapshotID cannot be resolved.
//
// The returned slice may be truncated if an intermediate snapshot is
// missing from the lookup (e.g. expired) or if a cycle is encountered.
// Callers that need to distinguish a complete walk from a truncated one
// should use AncestorsOfChecked instead.
func AncestorsOf(snapshotID int64, lookup SnapshotLookup) []Snapshot {
	ancestors, _ := AncestorsOfChecked(snapshotID, lookup)

	return ancestors
}

// AncestorsOfChecked is AncestorsOf with completeness tracking. The
// second return value is true when the walk terminated at a snapshot
// with no parent (a clean root). It is false when the walk was
// truncated by an unresolvable starting snapshot, a missing intermediate
// snapshot, or a cycle in malformed metadata.
//
// Callers performing conflict detection (where a truncated ancestry
// equates to under-counting concurrent snapshots) MUST treat
// complete=false as divergent and refuse the commit, mirroring
// AncestorsBetween's baseFound=false contract. When complete is false
// the returned slice is the partial walk before truncation —
// diagnostic context only, NOT an enumerable ancestry.
//
// Snapshots are returned by value in reverse-chronological order.
// Returns an empty slice and false when snapshotID cannot be resolved.
func AncestorsOfChecked(snapshotID int64, lookup SnapshotLookup) ([]Snapshot, bool) {
	return walkSnapshotAncestry(snapshotID, 0, lookup, ancestryToRoot)
}

// AncestorsBetween returns the snapshots from latestID (inclusive) down
// to but not including baseID, by walking the parent chain from
// latestID backward. The second return value (baseFound) is true when
// baseID was actually reached during the walk.
//
// These snapshots are the "concurrent" snapshots that a writer based on
// baseID needs to examine for conflict detection.
//
// When baseFound is false, the walk terminated without reaching baseID
// — either because latestID was unknown, baseID is not in latestID's
// ancestry (diverged branch or expired base), the chain was broken by
// a missing intermediate snapshot, or a cycle was detected in
// malformed metadata. In all of these cases the returned slice is
// diagnostic context only — it is NOT an enumerable "concurrent
// snapshots" list. Callers performing conflict detection MUST treat
// baseFound=false as divergent and refuse the commit.
//
// Returns (nil, true) when latestID == baseID (no concurrent snapshots).
//
// Snapshots are returned by value in reverse-chronological order.
func AncestorsBetween(latestID, baseID int64, lookup SnapshotLookup) ([]Snapshot, bool) {
	if latestID == baseID {
		return nil, true
	}

	return walkSnapshotAncestry(latestID, baseID, lookup, ancestryBeforeBase)
}

// IsAncestorOf returns true if ancestorID is in the parent chain of
// snapshotID (or equal to snapshotID, provided snapshotID resolves).
//
// Returns false if snapshotID cannot be resolved, if ancestorID is not
// reachable from snapshotID via ParentSnapshotID, or if a cycle
// prevents reaching ancestorID.
//
// Note: every snapshot on the walked chain must be resolvable via
// lookup. This diverges from Java's SnapshotUtil.isAncestorOf which
// walks parent *ids* regardless of snapshot resolvability. The Go
// semantics are stricter (conservative) — broken chains return false
// rather than matching an unreachable ancestor id.
func IsAncestorOf(snapshotID, ancestorID int64, lookup SnapshotLookup) bool {
	_, found := walkSnapshotAncestry(snapshotID, ancestorID, lookup, ancestryToResolvedAncestor)

	return found
}
