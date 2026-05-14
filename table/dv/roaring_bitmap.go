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

package dv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/iceberg-go/puffin"
)

// maxBitmapCount is the maximum number of 32-bit bitmap keys allowed during
// deserialization. This prevents CPU/memory exhaustion from absurd counts
// in malformed input. Derived from puffin.DefaultMaxBlobSize / 8 (minimum
// per-bitmap overhead: 4-byte key + at least 4 bytes of roaring data).
var maxBitmapCount = uint64(puffin.DefaultMaxBlobSize / 8)

// RoaringPositionBitmap supports 64-bit positions using a sparse map of
// 32-bit Roaring bitmaps. Positions are split into a 32-bit key
// (high bits) and 32-bit value (low bits).
//
// Compatible with the Java Iceberg RoaringPositionBitmap serialization format.
type RoaringPositionBitmap struct {
	bitmaps map[uint32]*roaring.Bitmap
}

// NewRoaringPositionBitmap creates an empty bitmap.
func NewRoaringPositionBitmap() *RoaringPositionBitmap {
	return &RoaringPositionBitmap{
		bitmaps: make(map[uint32]*roaring.Bitmap),
	}
}

// Set marks a position in the bitmap.
func (b *RoaringPositionBitmap) Set(pos uint64) {
	key := uint32(pos >> 32)
	low := uint32(pos)
	bm, ok := b.bitmaps[key]
	if !ok {
		bm = roaring.New()
		b.bitmaps[key] = bm
	}
	bm.Add(low)
}

// Contains checks if a position is set.
func (b *RoaringPositionBitmap) Contains(pos uint64) bool {
	key := uint32(pos >> 32)
	low := uint32(pos)
	bm, ok := b.bitmaps[key]
	if !ok {
		return false
	}

	return bm.Contains(low)
}

// IsEmpty returns true if no positions are set. O(1): zero-cardinality
// bitmaps are not written to the map (Serialize skips them and the
// deserializer reads only what was written), so an empty bitmaps map is
// equivalent to zero cardinality.
func (b *RoaringPositionBitmap) IsEmpty() bool {
	return len(b.bitmaps) == 0
}

// Cardinality returns the total number of set positions.
func (b *RoaringPositionBitmap) Cardinality() int64 {
	var c int64
	for _, bm := range b.bitmaps {
		c += int64(bm.GetCardinality())
	}

	return c
}

// Iter yields the set positions in ascending order. Lazy — preferred over
// ToArray when the caller can consume positions one-at-a-time (e.g. feeding
// an Arrow builder), since a DV bitmap can hold millions of positions.
//
// The bitmap must not be modified while an iterator is running: the closure
// captures b by pointer and the underlying roaring.Bitmap library does not
// support concurrent Set/Iterator. Single-goroutine consumption is the only
// supported pattern.
func (b *RoaringPositionBitmap) Iter() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for _, key := range b.sortedKeys() {
			hi := uint64(key) << 32
			// roaring.Bitmap.Iterator yields values in ascending order;
			// iterating per sorted key gives an overall ascending sequence.
			it := b.bitmaps[key].Iterator()
			for it.HasNext() {
				if !yield(hi | uint64(it.Next())) {
					return
				}
			}
		}
	}
}

// ToArray materializes the set positions in ascending order. Convenience over
// Iter for callers that need an addressable slice; allocates Cardinality()
// elements up front, so prefer Iter for large bitmaps.
func (b *RoaringPositionBitmap) ToArray() []uint64 {
	return slices.AppendSeq(make([]uint64, 0, b.Cardinality()), b.Iter())
}

// Serialize writes in the Iceberg portable format (little-endian):
//   - bitmap count (8 bytes, LE): number of non-empty bitmaps
//   - for each bitmap in ascending key order: key (4 bytes, LE) + roaring portable data
//
// Only non-empty bitmaps are written, matching Java Iceberg behavior.
func (b *RoaringPositionBitmap) Serialize(w io.Writer) error {
	keys := make([]uint32, 0, len(b.bitmaps))
	for k, bm := range b.bitmaps {
		if bm.GetCardinality() > 0 {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	if err := binary.Write(w, binary.LittleEndian, int64(len(keys))); err != nil {
		return fmt.Errorf("write bitmap count: %w", err)
	}
	for _, key := range keys {
		if err := binary.Write(w, binary.LittleEndian, key); err != nil {
			return fmt.Errorf("write key %d: %w", key, err)
		}
		if _, err := b.bitmaps[key].WriteTo(w); err != nil {
			return fmt.Errorf("write bitmap %d: %w", key, err)
		}
	}

	return nil
}

// DeserializeRoaringPositionBitmap reads a bitmap from the Iceberg portable format.
// Format: [count] { [key][bitmap] } .....{[key_n][bitmap_n]}
func DeserializeRoaringPositionBitmap(data []byte) (*RoaringPositionBitmap, error) {
	r := bytes.NewReader(data)

	var count uint64
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("read bitmap count: %w", err)
	}
	if count > maxBitmapCount {
		return nil, fmt.Errorf("bitmap count %d exceeds maximum allowed %d", count, maxBitmapCount)
	}

	b := &RoaringPositionBitmap{
		bitmaps: make(map[uint32]*roaring.Bitmap, count),
	}
	var lastKey uint32
	hasLastKey := false

	for i := range count {
		var key uint32
		if err := binary.Read(r, binary.LittleEndian, &key); err != nil {
			return nil, fmt.Errorf("read key %d: %w", i, err)
		}
		if hasLastKey && key <= lastKey {
			return nil, fmt.Errorf("keys must be ascending: got %d after %d", key, lastKey)
		}

		bm := roaring.New()
		if _, err := bm.ReadFrom(r); err != nil {
			return nil, fmt.Errorf("read bitmap for key %d: %w", key, err)
		}
		b.bitmaps[key] = bm
		lastKey = key
		hasLastKey = true
	}

	return b, nil
}

// sortedKeys returns the bitmap keys in ascending order.
func (b *RoaringPositionBitmap) sortedKeys() []uint32 {
	return slices.Sorted(maps.Keys(b.bitmaps))
}
