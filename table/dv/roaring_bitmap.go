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
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

// IsEmpty returns true if no positions are set. Returns true both for the
// no-bucket case and for the (currently impossible-via-public-API) case
// where a bucket exists but its inner roaring bitmap has zero cardinality
// — the latter only matters if a future Remove-style method ever lets a
// bucket drop to empty without being deleted from the map.
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

// KeepMaskBytes returns a bit-packed []byte of length ⌈length/8⌉ where bit i
// (LSB-first within a byte) is 1 iff position i is NOT in the bitmap. The
// layout matches Arrow Boolean buffer convention so callers can wrap the
// result via memory.NewBufferBytes / array.NewBoolean without re-packing.
//
// length bounds the range of positions the caller cares about — typically the
// data file's row count. Bits past length-1 in the final byte are cleared.
// Positions in the bitmap that fall outside [0, length) are ignored, so a
// caller can safely pass a length smaller than the bitmap's max position
// (e.g. when the file row count is below a stale upper bound).
//
// Bucket-key arithmetic is exact: each 32-bit bucket covers exactly 2^32
// positions, so per-bucket bit offsets are 8-byte-aligned and the writer can
// pack inverted dense words straight in. bitutil.BitmapWordWriter handles
// host-endianness internally (PutNextWord LE-packs regardless of platform),
// so the helper is portable on any GOARCH.
func (b *RoaringPositionBitmap) KeepMaskBytes(length int64) []byte {
	if length <= 0 {
		return nil
	}
	out := make([]byte, bitutil.BytesForBits(length))
	// Pre-fill ALL bits to 1 (keep) before any bucket write. BitmapWordWriter
	// does full-word stores (not read-modify-write), so the un-deleted bits
	// in each 8-byte word land at 1 only because this initialization
	// happened. A future refactor that moves bucket writes before the
	// memory.Set, or removes it entirely, would silently corrupt the mask.
	memory.Set(out, 0xFF)

	for key, bm := range b.bitmaps {
		bucketBitBase := int64(key) << 32
		if bucketBitBase >= length {
			continue
		}
		dense := bm.ToDense()
		if len(dense) == 0 {
			continue
		}
		// Cap the bucket's bit range to what fits in `length`.
		bucketBits := int64(len(dense)) * 64
		if bucketBitBase+bucketBits > length {
			bucketBits = length - bucketBitBase
		}
		// bucketBitBase = key << 32 is always 8-byte-aligned, so the
		// BitmapWordWriter runs with offset=0 internally. The trailing-byte
		// loop below relies on that alignment — PutNextTrailingByte's
		// validBits=8 path is byte-aligned only when offset == 0.
		wr := bitutil.NewBitmapWordWriter(out, int(bucketBitBase), int(bucketBits))
		full := int(bucketBits / 64)
		for i := 0; i < full; i++ {
			wr.PutNextWord(^dense[i])
		}
		if rem := int(bucketBits & 63); rem > 0 {
			last := ^dense[full]
			for rem > 0 {
				valid := 8
				if rem < 8 {
					valid = rem
				}
				wr.PutNextTrailingByte(byte(last), valid)
				last >>= 8
				rem -= valid
			}
		}
	}
	// Pad bits past length-1 in the trailing byte must be 0; the writer can
	// paint into them when bucketBits stops mid-byte, so the clear has to
	// happen after all bucket writes complete.
	if pad := int64(len(out))*8 - length; pad > 0 {
		bitutil.SetBitsTo(out, length, pad, false)
	}

	return out
}
