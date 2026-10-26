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

package internal

import (
	"iter"
	"math"
)

// Large FIFO plans spend most of their time searching the open bins.
// Index only when there are enough items to amortize the tree and ring buffer.
// Smaller plans, largest-bin-first packing, and potentially overflowing
// weights keep the existing linear implementation.
func (s *SlicePacker[T]) shouldUseIndexedFIFO(itemCount int) bool {
	return !s.LargestBinFirst &&
		s.Lookback >= 32 && s.Lookback <= 8192 &&
		itemCount >= 2*s.Lookback &&
		s.TargetWeight >= 0 && s.TargetWeight <= math.MaxInt64/2
}

// packingWeightIndex tracks the minimum weight in each subtree. Finding the
// first bin that can accept an item then takes O(log lookback) instead of
// scanning up to lookback open bins.
type packingWeightIndex struct {
	size    int
	weights []int64
}

func newPackingWeightIndex(capacity int) packingWeightIndex {
	size := 1
	for size < capacity {
		size *= 2
	}

	weights := make([]int64, 2*size)
	for i := range weights {
		weights[i] = math.MaxInt64
	}

	return packingWeightIndex{size: size, weights: weights}
}

func (idx *packingWeightIndex) update(bin int, weight int64) {
	node := idx.size + bin
	idx.weights[node] = weight

	for node /= 2; node > 0; node /= 2 {
		minimum := min(idx.weights[node*2], idx.weights[node*2+1])
		if idx.weights[node] == minimum {
			// An unchanged minimum cannot affect any ancestor.
			break
		}
		idx.weights[node] = minimum
	}
}

func (idx *packingWeightIndex) firstFit(start, end int, maxWeight int64) int {
	return idx.firstFitNode(1, 0, idx.size, start, end, maxWeight)
}

func (idx *packingWeightIndex) firstFitNode(node, lo, hi, start, end int, maxWeight int64) int {
	if lo >= end || hi <= start || idx.weights[node] > maxWeight {
		return -1
	}
	if hi-lo == 1 {
		return lo
	}

	mid := (lo + hi) / 2
	if bin := idx.firstFitNode(node*2, lo, mid, start, end, maxWeight); bin >= 0 {
		return bin
	}

	return idx.firstFitNode(node*2+1, mid, hi, start, end, maxWeight)
}

// indexedFIFOPackingIterator preserves first-fit selection and FIFO eviction.
// The ring keeps bin order stable when the oldest bin is emitted. The index is
// built lazily once 32 bins are open so easy-to-pack inputs remain inexpensive.
func indexedFIFOPackingIterator[T any](
	input iter.Seq[T], targetWeight int64, lookback int, weightFunc func(T) int64,
) iter.Seq[[]T] {
	capacity := lookback + 1
	bins := make([]Bin[T], capacity)

	var weights packingWeightIndex
	head, openBins := 0, 0
	safeToIndex, indexed := true, false

	return func(yield func([]T) bool) {
		for item := range input {
			weight := weightFunc(item)
			if weight < 0 || weight > targetWeight {
				// Preserve Bin.CanAdd's signed arithmetic for unusual inputs.
				// The indexed path assumes non-negative, bounded weights.
				safeToIndex = false
				indexed = false
			}

			bin := -1
			if indexed {
				maxBinWeight := targetWeight - weight
				bin = weights.firstFit(head, capacity, maxBinWeight)
				if bin < 0 && head > 0 {
					bin = weights.firstFit(0, head, maxBinWeight)
				}
			} else {
				for i := range openBins {
					j := (head + i) % capacity
					if bins[j].CanAdd(weight) {
						bin = j

						break
					}
				}
			}

			if bin >= 0 {
				bins[bin].Add(item, weight)
				if indexed {
					weights.update(bin, bins[bin].Weight())
				}

				continue
			}

			slot := (head + openBins) % capacity
			bins[slot] = Bin[T]{targetWeight: targetWeight}
			bins[slot].Add(item, weight)
			if indexed {
				weights.update(slot, weight)
			}
			openBins++

			if safeToIndex && !indexed && openBins == 32 {
				weights = newPackingWeightIndex(capacity)
				for i := range openBins {
					j := (head + i) % capacity
					weights.update(j, bins[j].Weight())
				}
				indexed = true
			}

			if openBins > lookback {
				out := bins[head].items
				bins[head] = Bin[T]{}
				if indexed {
					weights.update(head, math.MaxInt64)
				}
				head = (head + 1) % capacity
				openBins--

				if !yield(out) {
					return
				}
			}
		}

		for openBins > 0 {
			out := bins[head].items
			bins[head] = Bin[T]{}
			if indexed {
				weights.update(head, math.MaxInt64)
			}
			head = (head + 1) % capacity
			openBins--

			if !yield(out) {
				return
			}
		}
	}
}
