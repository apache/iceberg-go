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
	"sync"
	"testing"
)

func TestEqualityDeletePositionBufferConcurrent(t *testing.T) {
	const (
		workers          = 12
		positionsPerBatch = 32
	)

	buffers := [2]*equalityDeletePositionBuffer{{}, {}}
	for _, buffer := range buffers {
		buffer.addPositions(nil)
		if buffer.bitmap != nil {
			t.Fatal("empty positions must not allocate a bitmap")
		}
	}

	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			positions := make([]int64, positionsPerBatch)
			for i := range positions {
				positions[i] = int64(worker*positionsPerBatch + i)
			}
			buffer := buffers[worker%len(buffers)]
			buffer.addPositions(positions)
			buffer.addPositions(positions[:1]) // A repeated match must be deduplicated.
		}(worker)
	}
	wg.Wait()

	for index, buffer := range buffers {
		if buffer.bitmap == nil {
			t.Fatalf("buffer %d never allocated its bitmap", index)
		}
		const expectedPerBuffer = (workers / 2) * positionsPerBatch
		if got := buffer.bitmap.Cardinality(); got != expectedPerBuffer {
			t.Errorf("buffer %d contains %d positions, want %d", index, got, expectedPerBuffer)
		}

		for worker := range workers {
			for i := range positionsPerBatch {
				position := uint64(worker*positionsPerBatch + i)
				want := worker%len(buffers) == index
				if got := buffer.bitmap.Contains(position); got != want {
					t.Errorf("buffer %d position %d: got %t, want %t", index, position, got, want)
				}
			}
		}
	}
}
