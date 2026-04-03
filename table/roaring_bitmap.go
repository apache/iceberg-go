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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/v2"
)

// RoaringPositionBitmap supports 64-bit positions using an array of
// 32-bit Roaring bitmaps. Positions are split into a 32-bit key
// (high bits) and 32-bit value (low bits).
//
// Compatible with the Java Iceberg RoaringPositionBitmap serialization format.
type RoaringPositionBitmap struct {
	bitmaps []*roaring.Bitmap // index = high 32 bits (key)
}

// NewRoaringPositionBitmap creates an empty bitmap.
func NewRoaringPositionBitmap() *RoaringPositionBitmap {
	return &RoaringPositionBitmap{}
}

// Set marks a position in the bitmap.
func (b *RoaringPositionBitmap) Set(pos int64) {
	key := int(pos >> 32)
	low := uint32(pos)
	b.grow(key + 1)
	b.bitmaps[key].Add(low)
}

// SetRange marks all positions in [start, end) as set.
func (b *RoaringPositionBitmap) SetRange(start, end int64) {
	for pos := start; pos < end; pos++ {
		b.Set(pos)
	}
}

// SetAll merges all positions from other into this bitmap.
func (b *RoaringPositionBitmap) SetAll(other *RoaringPositionBitmap) {
	b.grow(len(other.bitmaps))
	for key, bm := range other.bitmaps {
		b.bitmaps[key].Or(bm)
	}
}

// RunOptimize applies run-length encoding where space-efficient.
// Call before Serialize for smaller output.
func (b *RoaringPositionBitmap) RunOptimize() {
	for _, bm := range b.bitmaps {
		bm.RunOptimize()
	}
}

// Contains checks if a position is set.
func (b *RoaringPositionBitmap) Contains(pos int64) bool {
	key := int(pos >> 32)
	low := uint32(pos)
	if key >= len(b.bitmaps) {
		return false
	}
	return b.bitmaps[key].Contains(low)
}

// IsEmpty returns true if no positions are set.
func (b *RoaringPositionBitmap) IsEmpty() bool {
	return b.Cardinality() == 0
}

// Cardinality returns the total number of set positions.
func (b *RoaringPositionBitmap) Cardinality() int64 {
	var c int64
	for _, bm := range b.bitmaps {
		c += int64(bm.GetCardinality())
	}
	return c
}

// ForEach iterates all set positions in ascending order.
func (b *RoaringPositionBitmap) ForEach(fn func(int64)) {
	for key, bm := range b.bitmaps {
		high := int64(key) << 32
		it := bm.Iterator()
		for it.HasNext() {
			low := it.Next()
			fn(high | int64(low)&0xFFFFFFFF)
		}
	}
}

// SerializedSizeInBytes returns the number of bytes needed to serialize.
func (b *RoaringPositionBitmap) SerializedSizeInBytes() int64 {
	size := int64(8) // bitmap count
	for _, bm := range b.bitmaps {
		size += 4 + int64(bm.GetSerializedSizeInBytes()) // key + data
	}
	return size
}

// Serialize writes in the Iceberg portable format (little-endian):
//   - bitmap count (8 bytes, LE)
//   - for each bitmap: key (4 bytes, LE) + roaring portable data
func (b *RoaringPositionBitmap) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, int64(len(b.bitmaps))); err != nil {
		return fmt.Errorf("write bitmap count: %w", err)
	}
	for key, bm := range b.bitmaps {
		if err := binary.Write(w, binary.LittleEndian, uint32(key)); err != nil {
			return fmt.Errorf("write key %d: %w", key, err)
		}
		if _, err := bm.WriteTo(w); err != nil {
			return fmt.Errorf("write bitmap %d: %w", key, err)
		}
	}
	return nil
}

// DeserializeRoaringPositionBitmap reads a bitmap from the Iceberg portable format.
func DeserializeRoaringPositionBitmap(r io.Reader) (*RoaringPositionBitmap, error) {
	var count int64
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("read bitmap count: %w", err)
	}
	if count < 0 {
		return nil, fmt.Errorf("invalid bitmap count: %d", count)
	}

	b := &RoaringPositionBitmap{}
	lastKey := -1

	for i := int64(0); i < count; i++ {
		var key uint32
		if err := binary.Read(r, binary.LittleEndian, &key); err != nil {
			return nil, fmt.Errorf("read key %d: %w", i, err)
		}
		if int(key) <= lastKey {
			return nil, fmt.Errorf("keys must be ascending: got %d after %d", key, lastKey)
		}

		// fill gaps with empty bitmaps
		for len(b.bitmaps) < int(key) {
			b.bitmaps = append(b.bitmaps, roaring.New())
		}

		bm := roaring.New()
		if _, err := bm.ReadFrom(r); err != nil {
			return nil, fmt.Errorf("read bitmap for key %d: %w", key, err)
		}
		b.bitmaps = append(b.bitmaps, bm)
		lastKey = int(key)
	}

	return b, nil
}

func (b *RoaringPositionBitmap) grow(required int) {
	for len(b.bitmaps) < required {
		b.bitmaps = append(b.bitmaps, roaring.New())
	}
}
