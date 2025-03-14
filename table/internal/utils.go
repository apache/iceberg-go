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
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// Enumerated is a quick way to represent a sequenced value that can
// be processed in parallel and then needs to be reordered.
type Enumerated[T any] struct {
	Value T
	Index int
	Last  bool
}

// a simple priority queue
type pqueue[T any] struct {
	queue   []*T
	compare func(a, b *T) bool
}

func (pq *pqueue[T]) Len() int { return len(pq.queue) }
func (pq *pqueue[T]) Less(i, j int) bool {
	return pq.compare(pq.queue[i], pq.queue[j])
}

func (pq *pqueue[T]) Swap(i, j int) {
	pq.queue[i], pq.queue[j] = pq.queue[j], pq.queue[i]
}

func (pq *pqueue[T]) Push(x any) {
	pq.queue = append(pq.queue, x.(*T))
}

func (pq *pqueue[T]) Pop() any {
	old := pq.queue
	n := len(old)

	item := old[n-1]
	old[n-1] = nil
	pq.queue = old[0 : n-1]

	return item
}

// MakeSequencedChan creates a channel that outputs values in a given order
// based on the comesAfter and isNext functions. The values are read in from
// the provided source and then re-ordered before being sent to the output.
func MakeSequencedChan[T any](bufferSize uint, source <-chan T, comesAfter, isNext func(a, b *T) bool, initial T) <-chan T {
	pq := pqueue[T]{queue: make([]*T, 0), compare: comesAfter}
	heap.Init(&pq)
	previous, out := &initial, make(chan T, bufferSize)
	go func() {
		defer close(out)
		for val := range source {
			heap.Push(&pq, &val)
			for pq.Len() > 0 && isNext(previous, pq.queue[0]) {
				previous = heap.Pop(&pq).(*T)
				out <- *previous
			}
		}
	}()

	return out
}

func u64FromBigEndianShifted(buf []byte) uint64 {
	var bytes [8]byte
	copy(bytes[8-len(buf):], buf)

	return binary.BigEndian.Uint64(bytes[:])
}

func BigEndianToDecimal(buf []byte) (decimal.Decimal128, error) {
	const (
		minDecBytes = 1
		maxDecBytes = 16
	)

	if len(buf) < minDecBytes || len(buf) > maxDecBytes {
		return decimal.Decimal128{},
			fmt.Errorf("invalid length for conversion to decimal: %d, must be between %d and %d",
				len(buf), minDecBytes, maxDecBytes)
	}

	// big endian, so first byte is the MSB and host the sign bit
	isNeg := int8(buf[0]) < 0

	var hi, lo int64
	// step 1. extract high bits
	highBitsOffset := max(0, len(buf)-8)
	highBits := u64FromBigEndianShifted(buf[:highBitsOffset])

	if highBitsOffset == 8 {
		hi = int64(highBits)
	} else {
		if isNeg && len(buf) < maxDecBytes {
			hi = -1
		}

		hi = int64(uint64(hi) << (uint64(highBitsOffset) * 8))
		hi |= int64(highBits)
	}

	// step 2. extract low bits
	lowBitsOffset := min(len(buf), 8)
	lowBits := u64FromBigEndianShifted(buf[highBitsOffset:])
	if lowBitsOffset == 8 {
		lo = int64(lowBits)
	} else {
		if isNeg && len(buf) < 8 {
			lo = -1
		}

		lo = int64(uint64(lo) << (uint64(lowBitsOffset) * 8))
		lo |= int64(lowBits)
	}

	return decimal128.New(hi, uint64(lo)), nil
}
