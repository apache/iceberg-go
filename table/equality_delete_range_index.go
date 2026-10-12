// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"cmp"
	"slices"
	"sort"
	"sync"
)

// These floors do not guarantee that the first lookup amortizes construction.
// BenchmarkEqualityDeleteRangeIndexThreshold measures the cold and repeated
// lookup tradeoff against linear matching.
const (
	// Small buckets rarely amortize tree construction; keep them linear.
	equalityDeleteRangeIndexMinEntries = 256
	// Avoid a separate tree for a field represented by only a few deletes.
	equalityDeleteRangeIndexMinGroupEntries = 64
	// Even a large bucket should use its sequence-sorted suffix directly once
	// fewer than 128 entries remain. At that floor, the half-remaining limit
	// below allows fewer than 64 candidates.
	equalityDeleteRangeIndexMinLookupEntries = 128
)

type equalityDeleteRangeIndex struct {
	// Entries and their decoded bounds are immutable after index construction.
	// Build the tree only when a lookup has a large enough sequence suffix, so
	// unvisited partitions do not pay for sorting or allocating range nodes.
	entries  []equalityDeleteIndexEntry
	once     sync.Once
	groups   []equalityDeleteRangeGroup
	fallback []int
}

type equalityDeleteRangeItem struct {
	entryIndex int
	lower      equalityDeleteMetricValue
	upper      equalityDeleteMetricValue
	sequence   int64
}

type equalityDeleteRangeNode struct {
	entryIndex  int
	left        int
	right       int
	lower       equalityDeleteMetricValue
	upper       equalityDeleteMetricValue
	maxUpper    equalityDeleteMetricValue
	sequence    int64
	maxSequence int64
}

type equalityDeleteRangeGroup struct {
	field equalityDeleteFieldMetrics
	root  int
	nodes []equalityDeleteRangeNode
}

func newEqualityDeleteRangeIndex(entries []equalityDeleteIndexEntry) *equalityDeleteRangeIndex {
	if len(entries) < equalityDeleteRangeIndexMinEntries {
		return nil
	}

	return &equalityDeleteRangeIndex{entries: entries}
}

func (idx *equalityDeleteRangeIndex) build() {
	entries := idx.entries
	itemsByField := make(map[int][]equalityDeleteRangeItem)
	fieldsByID := make(map[int]equalityDeleteFieldMetrics)
	fallback := make([]int, 0)
	for entryIndex := range entries {
		field, ok := equalityDeleteRangeIndexField(entries[entryIndex].fields)
		if !ok {
			fallback = append(fallback, entryIndex)

			continue
		}

		itemsByField[field.fieldID] = append(itemsByField[field.fieldID], equalityDeleteRangeItem{
			entryIndex: entryIndex,
			lower:      field.lowerValue,
			upper:      field.upperValue,
			sequence:   entries[entryIndex].entry.SequenceNum(),
		})
		if _, found := fieldsByID[field.fieldID]; !found {
			fieldsByID[field.fieldID] = *field
		}
	}

	fieldIDs := make([]int, 0, len(itemsByField))
	for fieldID := range itemsByField {
		fieldIDs = append(fieldIDs, fieldID)
	}
	slices.Sort(fieldIDs)

	groups := make([]equalityDeleteRangeGroup, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		items := itemsByField[fieldID]
		if len(items) < equalityDeleteRangeIndexMinGroupEntries {
			for _, item := range items {
				fallback = append(fallback, item.entryIndex)
			}

			continue
		}

		groups = append(groups, newEqualityDeleteRangeGroup(fieldsByID[fieldID], items))
	}

	slices.Sort(fallback)
	idx.groups = groups
	idx.fallback = fallback
}

// The index may exclude only deletes that equalityDeleteCanContainData would
// also reject. A ranged field must prove it cannot match null or NaN keys before
// the tree can use its bounds. Anything uncertain stays on the linear path.
func equalityDeleteRangeIndexField(
	fields []equalityDeleteFieldMetrics,
) (*equalityDeleteFieldMetrics, bool) {
	for i := range fields {
		field := &fields[i]
		if !field.primitive || !field.canUseRange || !field.hasDecodedBounds {
			continue
		}
		if !field.required && (!field.hasNullCount || field.nullCount != 0) {
			continue
		}
		if field.floatType && (!field.hasNaNCount || field.nanCount != 0) {
			continue
		}

		// Index one field per delete to keep candidates unique. The first safe
		// field may be less selective than a later composite-key field; the
		// candidate limit falls back to linear matching in that case.
		return field, true
	}

	return nil, false
}

func newEqualityDeleteRangeGroup(
	field equalityDeleteFieldMetrics,
	items []equalityDeleteRangeItem,
) equalityDeleteRangeGroup {
	slices.SortFunc(items, func(a, b equalityDeleteRangeItem) int {
		if order := equalityDeleteMetricValueCompare(&a.lower, &b.lower); order != 0 {
			return order
		}

		return cmp.Compare(a.entryIndex, b.entryIndex)
	})

	group := equalityDeleteRangeGroup{
		field: field,
		root:  -1,
		nodes: make([]equalityDeleteRangeNode, len(items)),
	}
	var build func(int, int) int
	build = func(start, end int) int {
		if start >= end {
			return -1
		}

		middle := start + (end-start)/2
		item := items[middle]
		nodeIndex := middle

		left := build(start, middle)
		right := build(middle+1, end)
		// Bound values copy scalars or references to bytes already owned by
		// entries. Nodes do not clone the string/binary payload again.
		node := equalityDeleteRangeNode{
			entryIndex:  item.entryIndex,
			left:        left,
			right:       right,
			lower:       item.lower,
			upper:       item.upper,
			maxUpper:    item.upper,
			sequence:    item.sequence,
			maxSequence: item.sequence,
		}
		for _, childIndex := range [...]int{left, right} {
			if childIndex < 0 {
				continue
			}
			child := &group.nodes[childIndex]
			if equalityDeleteMetricValueCompare(&child.maxUpper, &node.maxUpper) > 0 {
				node.maxUpper = child.maxUpper
			}
			if child.maxSequence > node.maxSequence {
				node.maxSequence = child.maxSequence
			}
		}
		group.nodes[nodeIndex] = node

		return nodeIndex
	}

	group.root = build(0, len(items))

	return group
}

func (idx *equalityDeleteRangeIndex) candidates(
	start int,
	dataSeqNum int64,
	dataStats *equalityDeleteDataFileStats,
) ([]int, bool) {
	idx.once.Do(idx.build)
	if len(idx.groups) == 0 {
		return nil, false
	}

	remaining := len(idx.entries) - start
	// Use the linear path once candidates include at least half the applicable entries.
	// Stop at that threshold instead of allocating a full candidate list that
	// will be discarded before the same entries are scanned again.
	candidateLimit := remaining / 2
	fallbackStart := sort.Search(len(idx.fallback), func(i int) bool {
		return idx.fallback[i] >= start
	})
	if len(idx.fallback)-fallbackStart >= candidateLimit {
		return nil, false
	}

	candidates := make([]int, 0, min(candidateLimit, 128))
	candidates = append(candidates, idx.fallback[fallbackStart:]...)

	for i := range idx.groups {
		var ok bool
		candidates, ok = idx.groups[i].appendCandidates(candidates, dataStats, dataSeqNum, candidateLimit)
		if !ok || len(candidates) >= candidateLimit {
			return nil, false
		}
	}

	slices.Sort(candidates)

	return candidates, true
}

func (group *equalityDeleteRangeGroup) appendCandidates(
	out []int,
	dataStats *equalityDeleteDataFileStats,
	dataSeqNum int64,
	candidateLimit int,
) ([]int, bool) {
	field := &group.field
	if field.floatType && !equalityDeleteFloatRangesAreKnown(*dataStats, field) {
		return out, false
	}

	dataBounds := equalityDeleteDataFileBoundsFor(dataStats, field)
	if !dataBounds.usable {
		return out, false
	}

	return group.appendNodeCandidates(out, group.root, dataBounds, dataSeqNum, candidateLimit), true
}

func (group *equalityDeleteRangeGroup) appendNodeCandidates(
	out []int,
	nodeIndex int,
	dataBounds *equalityDeleteDataFileBounds,
	dataSeqNum int64,
	candidateLimit int,
) []int {
	if nodeIndex < 0 || len(out) >= candidateLimit {
		return out
	}

	node := &group.nodes[nodeIndex]
	// These strict disjoint-range checks must remain a subset of the rejects
	// in equalityDeleteCanContainData. Equal endpoints still overlap, and the
	// strict sequence comparison retains only deletes newer than the data.
	if node.maxSequence <= dataSeqNum ||
		equalityDeleteMetricValueCompare(&node.maxUpper, &dataBounds.lower) < 0 {
		return out
	}

	out = group.appendNodeCandidates(out, node.left, dataBounds, dataSeqNum, candidateLimit)
	if len(out) >= candidateLimit ||
		equalityDeleteMetricValueCompare(&node.lower, &dataBounds.upper) > 0 {
		return out
	}

	if node.sequence > dataSeqNum &&
		equalityDeleteMetricValueCompare(&node.upper, &dataBounds.lower) >= 0 {
		out = append(out, node.entryIndex)
	}

	return group.appendNodeCandidates(out, node.right, dataBounds, dataSeqNum, candidateLimit)
}
