// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	arrowBufferMinClass  = 6
	arrowBufferMaxClass  = 20
	arrowBuffersPerClass = 4
)

type arrowBufferBucket struct {
	mu      sync.Mutex
	count   int
	buffers [arrowBuffersPerClass][]byte
}

// arrowBufferPool keeps at most four released buffers per size class, for
// less than 8 MiB total. Live buffers are owned by their Arrow references.
type arrowBufferPool struct {
	backing *memory.GoAllocator
	closed  atomic.Bool
	buckets [arrowBufferMaxClass - arrowBufferMinClass + 1]arrowBufferBucket
}

func (p *arrowBufferPool) Allocate(size int) []byte {
	if size <= 0 || size > 1<<arrowBufferMaxClass || p.closed.Load() {
		return p.backing.Allocate(size)
	}
	class := max(arrowBufferMinClass, bits.Len(uint(size-1)))
	bucket := &p.buckets[class-arrowBufferMinClass]
	bucket.mu.Lock()
	if bucket.count == 0 {
		bucket.mu.Unlock()

		return p.backing.Allocate(1 << class)[:size]
	}
	bucket.count--
	buf := bucket.buffers[bucket.count]
	bucket.buffers[bucket.count] = nil
	bucket.mu.Unlock()

	// A caller can reslice to capacity, so clear the entire size class.
	clear(buf)

	return buf[:size]
}

func (p *arrowBufferPool) Reallocate(size int, buf []byte) []byte {
	if size <= cap(buf) && size >= cap(buf)/2 {
		next := buf[:size]
		if size > len(buf) {
			clear(next[len(buf):])
		}

		return next
	}
	next := p.Allocate(size)
	copy(next, buf)
	p.Free(buf)

	return next
}

func (p *arrowBufferPool) Free(buf []byte) {
	capacity := cap(buf)
	if capacity < 1<<arrowBufferMinClass || capacity > 1<<arrowBufferMaxClass ||
		capacity&(capacity-1) != 0 || p.closed.Load() {
		return
	}
	class := bits.TrailingZeros(uint(capacity))
	bucket := &p.buckets[class-arrowBufferMinClass]
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if !p.closed.Load() && bucket.count < len(bucket.buffers) {
		bucket.buffers[bucket.count] = buf[:capacity]
		bucket.count++
	}
}

func (p *arrowBufferPool) close() {
	if p.closed.Swap(true) {
		return
	}
	for i := range p.buckets {
		bucket := &p.buckets[i]
		bucket.mu.Lock()
		clear(bucket.buffers[:])
		bucket.count = 0
		bucket.mu.Unlock()
	}
}
