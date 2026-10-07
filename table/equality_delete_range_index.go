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
)

const (
	equalityDeleteRangeIndexMinEntries       = 256
	equalityDeleteRangeIndexMinGroupEntries  = 64
	equalityDeleteRangeIndexMinLookupEntries = 128
)

type equalityDeleteRangeIndex struct {
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

	if len(groups) == 0 {
		return nil
	}

	slices.Sort(fallback)

	return &equalityDeleteRangeIndex{
		groups:   groups,
		fallback: fallback,
	}
}

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

		return field, true
	}

	return nil, false
}

func newEqualityDeleteRangeGroup(
	field equalityDeleteFieldMetrics,
	items []equalityDeleteRangeItem,
) equalityDeleteRangeGroup {
	slices.SortStableFunc(items, func(a, b equalityDeleteRangeItem) int {
		if order := equalityDeleteMetricValueCompare(&a.lower, &b.lower); order != 0 {
			return order
		}

		return cmp.Compare(a.entryIndex, b.entryIndex)
	})

	group := equalityDeleteRangeGroup{
		field: field,
		root:  -1,
		nodes: make([]equalityDeleteRangeNode, 0, len(items)),
	}
	var build func(int, int) int
	build = func(start, end int) int {
		if start >= end {
			return -1
		}

		middle := start + (end-start)/2
		item := items[middle]
		nodeIndex := len(group.nodes)
		group.nodes = append(group.nodes, equalityDeleteRangeNode{})

		left := build(start, middle)
		right := build(middle+1, end)
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
	entries []equalityDeleteIndexEntry,
	start int,
	dataSeqNum int64,
	dataStats *equalityDeleteDataFileStats,
) ([]int, bool) {
	remaining := len(entries) - start
	candidateCapacity := min(remaining, 128)
	candidates := make([]int, 0, candidateCapacity)
	fallbackStart := sort.Search(len(idx.fallback), func(i int) bool {
		return idx.fallback[i] >= start
	})
	candidates = append(candidates, idx.fallback[fallbackStart:]...)

	for i := range idx.groups {
		var ok bool
		candidates, ok = idx.groups[i].appendCandidates(candidates, dataStats, dataSeqNum)
		if !ok {
			return nil, false
		}
	}

	if remaining > 0 && len(candidates) >= remaining/2 {
		return nil, false
	}

	slices.Sort(candidates)

	return candidates, true
}

func (group *equalityDeleteRangeGroup) appendCandidates(
	out []int,
	dataStats *equalityDeleteDataFileStats,
	dataSeqNum int64,
) ([]int, bool) {
	field := &group.field
	if field.floatType && !equalityDeleteFloatRangesAreKnown(*dataStats, field) {
		return out, false
	}

	dataBounds := equalityDeleteDataFileBoundsFor(dataStats, field)
	if !dataBounds.usable {
		return out, false
	}

	return group.appendNodeCandidates(out, group.root, dataBounds, dataSeqNum), true
}

func (group *equalityDeleteRangeGroup) appendNodeCandidates(
	out []int,
	nodeIndex int,
	dataBounds *equalityDeleteDataFileBounds,
	dataSeqNum int64,
) []int {
	if nodeIndex < 0 {
		return out
	}

	node := &group.nodes[nodeIndex]
	if node.maxSequence <= dataSeqNum ||
		equalityDeleteMetricValueCompare(&node.maxUpper, &dataBounds.lower) < 0 {
		return out
	}

	out = group.appendNodeCandidates(out, node.left, dataBounds, dataSeqNum)
	if equalityDeleteMetricValueCompare(&node.lower, &dataBounds.upper) > 0 {
		return out
	}

	if node.sequence > dataSeqNum &&
		equalityDeleteMetricValueCompare(&node.upper, &dataBounds.lower) >= 0 {
		out = append(out, node.entryIndex)
	}

	return group.appendNodeCandidates(out, node.right, dataBounds, dataSeqNum)
}
