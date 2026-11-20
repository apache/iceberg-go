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

	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	arrowBufferMinClass   = 6
	arrowBufferMaxClass   = 20
	arrowBuffersPerClass  = 16
	arrowBufferCacheBytes = 8 << 20
)

type arrowBufferBucket struct {
	count   int
	buffers [arrowBuffersPerClass][]byte
}

// arrowBufferPool shares a fixed byte budget among size classes so buffers
// released by several scan workers can fill the classes they actually use.
// Live buffers are owned by their Arrow references.
type arrowBufferPool struct {
	backing     *memory.GoAllocator
	mu          sync.Mutex
	closed      bool
	cachedBytes int
	buckets     [arrowBufferMaxClass - arrowBufferMinClass + 1]arrowBufferBucket
}

func (p *arrowBufferPool) Allocate(size int) []byte {
	if size <= 0 || size > 1<<arrowBufferMaxClass {
		return p.backing.Allocate(size)
	}
	class := max(arrowBufferMinClass, bits.Len(uint(size-1)))
	bucket := &p.buckets[class-arrowBufferMinClass]
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		return p.backing.Allocate(size)
	}
	if bucket.count == 0 {
		p.mu.Unlock()

		return p.backing.Allocate(1 << class)[:size]
	}
	bucket.count--
	buf := bucket.buffers[bucket.count]
	bucket.buffers[bucket.count] = nil
	p.cachedBytes -= cap(buf)
	p.mu.Unlock()

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
		capacity&(capacity-1) != 0 {
		return
	}
	class := bits.TrailingZeros(uint(capacity))
	bucket := &p.buckets[class-arrowBufferMinClass]
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed && bucket.count < len(bucket.buffers) && p.cachedBytes+capacity <= arrowBufferCacheBytes {
		bucket.buffers[bucket.count] = buf[:capacity]
		bucket.count++
		p.cachedBytes += capacity
	}
}

func (p *arrowBufferPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	clear(p.buckets[:])
	p.cachedBytes = 0
}
