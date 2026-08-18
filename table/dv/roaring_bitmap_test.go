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
	"maps"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Why: this type owns compatibility with Iceberg's Java roaring bitmap format, including the empty case.
// Condition: deserialize a Java-produced bitmap with zero stored positions.
// Assertion: no error, bitmap is empty, and cardinality is 0.
func TestDeserializeRoaringBitmapJavaEmpty(t *testing.T) {
	data := readDVTestData(t, "64mapempty.bin")

	bm, err := DeserializeRoaringPositionBitmap(data)
	require.NoError(t, err)

	assert.True(t, bm.IsEmpty())
	assert.Equal(t, int64(0), bm.Cardinality())
}

// Why: the bitmap layer owns 64-bit position mapping across multiple high-bit keys.
// Condition: deserialize a Java-produced bitmap with keys 0..9 and low values 0..9.
// Assertion: no error, cardinality is 100, representative positions across keys exist, and a position beyond the stored range is absent.
func TestDeserializeRoaringBitmapJavaSpreadValues(t *testing.T) {
	data := readDVTestData(t, "64mapspreadvals.bin")

	bm, err := DeserializeRoaringPositionBitmap(data)
	require.NoError(t, err)

	assert.Equal(t, int64(100), bm.Cardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains((uint64(3)<<32)|7))
	assert.True(t, bm.Contains((uint64(9)<<32)|9))
	assert.False(t, bm.Contains(uint64(10)<<32))
}

// Why: validates cross-impl compatibility for a simple 32-bit-only bitmap.
// Condition: deserialize a Java-produced bitmap with positions [0..9] in a single key.
// Assertion: no error, cardinality is 10, all expected positions present.
func TestDeserializeRoaringBitmapJava32BitValues(t *testing.T) {
	data := readDVTestData(t, "64map32bitvals.bin")

	bm, err := DeserializeRoaringPositionBitmap(data)
	require.NoError(t, err)

	assert.Equal(t, int64(10), bm.Cardinality())
	for i := range uint64(10) {
		assert.True(t, bm.Contains(i), "expected position %d to be set", i)
	}
	assert.False(t, bm.Contains(10))
}

// Why: the deserializer must fail cleanly when the outer bitmap count cannot be read.
// Condition: empty input stream.
// Assertion: returns an error containing "read bitmap count".
func TestDeserializeRoaringBitmapTruncatedInput(t *testing.T) {
	_, err := DeserializeRoaringPositionBitmap(nil)
	assert.ErrorContains(t, err, "read bitmap count")
}

// Why: counts with the high bit set (e.g. an int64 -1 written to disk) decode
// as a huge uint64 and must be rejected by the upper-bound check, not silently
// accepted as a small value or panicked on by make(map, hugeHint).
// Condition: count field encoded as int64(-1) (= 0xFFFF_FFFF_FFFF_FFFF on disk).
// Assertion: returns an error containing "exceeds maximum".
func TestDeserializeRoaringBitmapHighBitCount(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, int64(-1)))

	_, err := DeserializeRoaringPositionBitmap(buf.Bytes())
	assert.ErrorContains(t, err, "exceeds maximum")
}

// Why: absurdly large counts should be rejected to prevent CPU/memory exhaustion.
// Condition: count field set to maxBitmapCount + 1.
// Assertion: returns an error containing "exceeds maximum".
func TestDeserializeRoaringBitmapExcessiveCount(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, maxBitmapCount+1))

	_, err := DeserializeRoaringPositionBitmap(buf.Bytes())
	assert.ErrorContains(t, err, "exceeds maximum")
}

// Why: each bitmap entry must start with a key; premature EOF before that key is a distinct decode failure.
// Condition: count says 1, but no key bytes follow.
// Assertion: returns an error containing "read key 0".
func TestDeserializeRoaringBitmapTruncatedBeforeKey(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, int64(1)))

	_, err := DeserializeRoaringPositionBitmap(buf.Bytes())
	assert.ErrorContains(t, err, "read key 0")
}

// Why: the on-wire format requires keys to be strictly ascending so the decoder can rebuild the sparse key space correctly.
// Condition: encoded bitmap count is 2, but entries are written with key 5 before key 3.
// Assertion: returns an error containing "keys must be ascending".
func TestDeserializeRoaringBitmapNonAscendingKeys(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set((uint64(5) << 32) | 1)
	bm.Set((uint64(3) << 32) | 1)

	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, int64(2)))
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(5)))
	_, err := bm.bitmaps[5].WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(3)))
	_, err = bm.bitmaps[3].WriteTo(&buf)
	require.NoError(t, err)

	_, err = DeserializeRoaringPositionBitmap(buf.Bytes())
	assert.ErrorContains(t, err, "keys must be ascending")
}

// Why: after a valid key is read, the decoder still needs a full roaring bitmap payload for that key.
// Condition: count says 1 and key 0 is present, but no roaring bitmap bytes follow.
// Assertion: returns an error containing "read bitmap for key 0".
func TestDeserializeRoaringBitmapTruncatedAfterKey(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, int64(1)))
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(0)))

	_, err := DeserializeRoaringPositionBitmap(buf.Bytes())
	assert.ErrorContains(t, err, "read bitmap for key 0")
}

// Why: the outer count fully frames the portable bitmap and extra bytes are
// malformed rather than a forward-compatible extension.
// Condition: append bytes after otherwise valid empty and non-empty bitmaps.
// Assertion: returns an error containing "trailing data".
func TestDeserializeRoaringBitmapTrailingData(t *testing.T) {
	for _, tt := range []struct {
		name      string
		positions []uint64
	}{
		{name: "empty"},
		{name: "non-empty", positions: []uint64{1, (uint64(3) << 32) | 7}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bm := NewRoaringPositionBitmap()
			for _, position := range tt.positions {
				bm.Set(position)
			}
			var buf bytes.Buffer
			require.NoError(t, bm.Serialize(&buf))
			buf.Write([]byte{0xde, 0xad})

			_, err := DeserializeRoaringPositionBitmap(buf.Bytes())
			assert.ErrorContains(t, err, "trailing data")
		})
	}
}

// TestKeepMaskBytes pins KeepMaskBytes' contract: a bit-packed []byte where
// bit i is 1 iff position i is NOT in the bitmap, sized to ⌈length/8⌉ bytes,
// matching Arrow Boolean buffer layout so it can be wrapped without re-packing.
func TestKeepMaskBytes(t *testing.T) {
	// bitAt returns the LSB-first bit at position i in mask.
	bitAt := func(mask []byte, i int64) bool {
		return mask[i>>3]&(1<<uint(i&7)) != 0
	}

	t.Run("empty bitmap: all bits kept", func(t *testing.T) {
		bm := NewRoaringPositionBitmap()
		mask := bm.KeepMaskBytes(10)
		require.Len(t, mask, 2) // ⌈10/8⌉ = 2
		for i := range int64(10) {
			assert.True(t, bitAt(mask, i), "bit %d should be kept", i)
		}
		// Bits 10..15 in the trailing byte must be cleared so callers
		// inspecting raw bytes don't see phantom keep bits past length.
		assert.Equal(t, byte(0b00000011), mask[1])
	})

	t.Run("single bucket: deleted bits cleared, others kept", func(t *testing.T) {
		bm := NewRoaringPositionBitmap()
		bm.Set(0)
		bm.Set(3)
		bm.Set(7)
		bm.Set(9)

		mask := bm.KeepMaskBytes(16)
		require.Len(t, mask, 2)
		for _, deleted := range []int64{0, 3, 7, 9} {
			assert.False(t, bitAt(mask, deleted), "bit %d should be deleted", deleted)
		}
		for _, kept := range []int64{1, 2, 4, 5, 6, 8, 10, 11, 12, 13, 14, 15} {
			assert.True(t, bitAt(mask, kept), "bit %d should be kept", kept)
		}
	})

	t.Run("length zero returns nil", func(t *testing.T) {
		bm := NewRoaringPositionBitmap()
		bm.Set(0)
		assert.Nil(t, bm.KeepMaskBytes(0))
	})

	t.Run("length shorter than bitmap: extra positions ignored", func(t *testing.T) {
		// Bitmap holds a delete at position 50, but the caller asks for only
		// the first 32 positions. The mask must not extend to position 50,
		// and the in-range bits must all be kept.
		bm := NewRoaringPositionBitmap()
		bm.Set(50)

		mask := bm.KeepMaskBytes(32)
		require.Len(t, mask, 4)
		for i := range int64(32) {
			assert.True(t, bitAt(mask, i), "bit %d should be kept", i)
		}
	})

	t.Run("length not byte-aligned: trailing bits cleared", func(t *testing.T) {
		// Length 13 → 2 bytes, bits 13..15 in byte 1 must be 0 even when no
		// deletes touch them. Pins the trailing-byte mask logic.
		bm := NewRoaringPositionBitmap()
		mask := bm.KeepMaskBytes(13)
		require.Len(t, mask, 2)
		assert.Equal(t, byte(0xFF), mask[0])
		assert.Equal(t, byte(0b00011111), mask[1])
	})

	t.Run("multi-bucket: deletes from a non-zero bucket are out of range and ignored", func(t *testing.T) {
		// Spec parity case: a DV bitmap with positions in bucket 1
		// (position >= 2^32) cannot fit in a realistic caller-supplied
		// length, since allocating ⌈length/8⌉ bytes for length >= 2^32
		// would require >= 512 MiB. KeepMaskBytes must correctly walk every
		// bucket and skip those whose byte-offset already exceeds the mask
		// length — without this guard the bucket-1 ToDense allocation would
		// run anyway and (worse) try to index past out[len(out)-1].
		bm := NewRoaringPositionBitmap()
		bm.Set(5)              // bucket 0
		bm.Set(1<<32 | 7)      // bucket 1 — out of range for length 64
		bm.Set(3<<32 | 999999) // bucket 3 — out of range for length 64

		mask := bm.KeepMaskBytes(64)
		require.Len(t, mask, 8)
		assert.False(t, bitAt(mask, 5))
		for i := range int64(64) {
			if i == 5 {
				continue
			}
			assert.True(t, bitAt(mask, i), "bit %d should be kept", i)
		}
	})

	t.Run("multi-bucket within range: bucket-1 deletes land at correct offset", func(t *testing.T) {
		// Construct the bitmaps map manually with a synthetic small-key
		// "bucket 1" so the test can verify cross-bucket bit placement
		// without allocating 512 MiB. The internal layout is exactly the
		// same as Set would produce for a real position >= 2^32; this just
		// dodges the high-position allocation. Verifies that the per-bucket
		// bucketBitBase = key << 32 (bit offset 2^32, i.e. byte offset 2^29)
		// places bucket-1 deletes at byte 2^29, not at byte 0.
		const length int64 = (1 << 32) + 16 // just past the bucket-0/1 boundary
		bm := &RoaringPositionBitmap{bitmaps: make(map[uint32]*roaring.Bitmap)}
		bm.Set(5) // bucket 0
		// Directly install a bucket-1 entry with low-bits 7 set, which is
		// what Set((1<<32)+7) would produce — but avoids the 512 MiB mask.
		bucket1 := roaring.New()
		bucket1.Add(7)
		bm.bitmaps[1] = bucket1

		mask := bm.KeepMaskBytes(length)
		require.Len(t, mask, int((length+7)/8))
		assert.False(t, bitAt(mask, 5), "bucket-0 delete at position 5 must land in byte 0")
		assert.False(t, bitAt(mask, (1<<32)+7), "bucket-1 delete at position 2^32+7 must land at byte 2^29")
		// Adjacent positions in each bucket should be kept.
		assert.True(t, bitAt(mask, 4))
		assert.True(t, bitAt(mask, 6))
		assert.True(t, bitAt(mask, (1<<32)+6))
		assert.True(t, bitAt(mask, (1<<32)+8))
	})

	// Pins PutNextTrailingByte's per-byte loop in KeepMaskBytes: when a
	// bucket's bit range stops mid-word (bucketBits & 63 != 0), the loop
	// must place delete bits inside the trailing partial word. Without
	// coverage, an off-by-one in `last >>= 8` or `valid = min(8, rem)`
	// would silently keep a deleted row visible.
	for _, tc := range []struct {
		name       string
		length     int64
		deletePos  uint64
		expectBits []int64 // additional positions that must be kept
	}{
		// rem = length % 64. For each rem in {1, 7, 8, 63}, place a
		// deletion at the LAST in-range bit so the trailing loop must
		// reach validBits = rem on its final iteration.
		{name: "rem=1: delete at length-1", length: 65, deletePos: 64, expectBits: []int64{0, 32, 63}},
		{name: "rem=7: delete at length-1", length: 71, deletePos: 70, expectBits: []int64{0, 63, 64, 69}},
		{name: "rem=8: delete at length-1", length: 72, deletePos: 71, expectBits: []int64{0, 64, 70}},
		{name: "rem=63: delete at length-1", length: 127, deletePos: 126, expectBits: []int64{0, 64, 125}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bm := NewRoaringPositionBitmap()
			bm.Set(tc.deletePos)

			mask := bm.KeepMaskBytes(tc.length)
			require.Len(t, mask, int((tc.length+7)/8))
			assert.False(t, bitAt(mask, int64(tc.deletePos)),
				"position %d (length-1) must be deleted", tc.deletePos)
			for _, kept := range tc.expectBits {
				assert.True(t, bitAt(mask, kept),
					"position %d must be kept", kept)
			}
		})
	}
}

func TestRoaringBitmapSetContainsAndCardinality(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	bm.Set(0)
	bm.Set(42)
	bm.Set(1000)
	bm.Set((uint64(1) << 32) | 5)
	bm.Set((uint64(1) << 32) | 999)
	bm.Set((uint64(3) << 32) | 1)

	assert.False(t, bm.IsEmpty())
	assert.Equal(t, int64(6), bm.Cardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains((uint64(1)<<32)|999))
	assert.True(t, bm.Contains((uint64(3)<<32)|1))
	assert.False(t, bm.Contains(1))
	assert.False(t, bm.Contains((uint64(1)<<32)|6))
	assert.False(t, bm.Contains((uint64(2)<<32)|1))
	assert.False(t, bm.Contains(uint64(100)<<32))
}

// Why: SetRange has to split a 64-bit range across 32-bit buckets, and every
// boundary in that split is a place an off-by-one silently deletes the wrong
// rows. Condition: ranges wholly inside a bucket, ending and starting exactly
// on a bucket boundary, spanning two and three buckets, plus the degenerate
// empty and inverted cases. Assertion: exactly the in-range positions are set,
// the neighbours on both sides are not, and no bucket is allocated for a range
// that does not reach it.
func TestRoaringBitmapSetRange(t *testing.T) {
	const bucket = uint64(1) << 32

	for _, tt := range []struct {
		name        string
		start, end  uint64
		cardinality int64
		set         []uint64
		unset       []uint64
		buckets     []uint32
	}{
		{
			name: "within one bucket", start: 10, end: 15, cardinality: 5,
			set: []uint64{10, 11, 14}, unset: []uint64{9, 15}, buckets: []uint32{0},
		},
		{
			name: "single position", start: 7, end: 8, cardinality: 1,
			set: []uint64{7}, unset: []uint64{6, 8}, buckets: []uint32{0},
		},
		{
			name: "crossing a roaring container boundary", start: 65530, end: 65540, cardinality: 10,
			set: []uint64{65530, 65535, 65536, 65539}, unset: []uint64{65529, 65540}, buckets: []uint32{0},
		},
		{
			// The last included position is bucket 0's highest, so bucket 1
			// must not be allocated at all — bucketing endExclusive instead of
			// endExclusive-1 would create an empty bucket 1 and change the
			// serialized bitmap count.
			name: "ending exactly on a bucket boundary", start: bucket - 4, end: bucket, cardinality: 4,
			set: []uint64{bucket - 4, bucket - 1}, unset: []uint64{bucket - 5, bucket}, buckets: []uint32{0},
		},
		{
			name: "starting exactly on a bucket boundary", start: bucket, end: bucket + 3, cardinality: 3,
			set: []uint64{bucket, bucket + 2}, unset: []uint64{bucket - 1, bucket + 3}, buckets: []uint32{1},
		},
		{
			name: "spanning a bucket boundary", start: bucket - 3, end: bucket + 4, cardinality: 7,
			set:   []uint64{bucket - 3, bucket - 1, bucket, bucket + 3},
			unset: []uint64{bucket - 4, bucket + 4}, buckets: []uint32{0, 1},
		},
		{
			// Three buckets exercises the middle-bucket loop, where every
			// position of bucket 1 must be set without any range arithmetic
			// derived from the caller's start or end.
			name: "spanning three buckets", start: 2*bucket - 1, end: 3*bucket + 1,
			cardinality: int64(bucket) + 2,
			set:         []uint64{2*bucket - 1, 2 * bucket, 3*bucket - 1, 3 * bucket},
			unset:       []uint64{2*bucket - 2, 3*bucket + 1}, buckets: []uint32{1, 2, 3},
		},
		{
			name: "empty range", start: 5, end: 5,
			unset: []uint64{4, 5, 6},
		},
		{
			// Java's setRange throws on an inverted range; the Go port has no
			// error channel here, so the documented behaviour is a no-op.
			name: "inverted range", start: 9, end: 5,
			unset: []uint64{4, 5, 8, 9, 10},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bm := NewRoaringPositionBitmap()
			bm.SetRange(tt.start, tt.end)

			assert.Equal(t, tt.cardinality, bm.Cardinality())
			for _, pos := range tt.set {
				assert.Truef(t, bm.Contains(pos), "position %d should be set", pos)
			}
			for _, pos := range tt.unset {
				assert.Falsef(t, bm.Contains(pos), "position %d should not be set", pos)
			}
			assert.ElementsMatch(t, tt.buckets, slices.Collect(maps.Keys(bm.bitmaps)),
				"allocated buckets")
		})
	}
}

// Why: SetRange is only a shortcut if it produces exactly what the per-position
// Set loop it replaces would have produced; a divergence would delete or spare
// rows depending on which API the writer happened to use.
// Condition: ranges inside one bucket, across a bucket boundary, and appended to
// an already-populated bitmap, each mirrored by a Set loop over every position.
// Assertion: identical membership and cardinality (container encoding may
// differ — SetRange yields runs where Set yields arrays).
func TestRoaringBitmapSetRangeMatchesPerPositionSet(t *testing.T) {
	const bucket = uint64(1) << 32

	for _, tt := range []struct {
		name       string
		seed       []uint64
		start, end uint64
	}{
		{name: "within one bucket", start: 1000, end: 5000},
		{name: "spanning a bucket boundary", start: bucket - 100, end: bucket + 100},
		{name: "merged into existing positions", seed: []uint64{0, 2500, 999999}, start: 1000, end: 5000},
		{name: "overlapping itself", seed: []uint64{1000, 1001, 1002}, start: 1000, end: 1003},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ranged, looped := NewRoaringPositionBitmap(), NewRoaringPositionBitmap()
			for _, pos := range tt.seed {
				ranged.Set(pos)
				looped.Set(pos)
			}

			ranged.SetRange(tt.start, tt.end)
			for pos := tt.start; pos < tt.end; pos++ {
				looped.Set(pos)
			}

			assertSamePositions(t, looped, ranged)
		})
	}
}

// Why: KeepMaskBytes reads each bucket through roaring's dense conversion, a
// path that until SetRange existed only ever saw array and bitmap containers in
// Go-built bitmaps. A range write installs run containers instead, so the mask
// has to be correct for those too — otherwise a reader would keep rows the
// writer deleted.
// Condition: a range that stops mid-word, alongside a scattered position.
// Assertion: exactly the in-range positions are dropped from the keep mask.
func TestKeepMaskBytesAfterSetRange(t *testing.T) {
	bitAt := func(mask []byte, i int64) bool {
		return mask[i>>3]&(1<<uint(i&7)) != 0
	}

	bm := NewRoaringPositionBitmap()
	bm.SetRange(10, 70)
	bm.Set(100)

	mask := bm.KeepMaskBytes(128)
	require.Len(t, mask, 16)
	for i := range int64(128) {
		deleted := (i >= 10 && i < 70) || i == 100
		assert.Equalf(t, !deleted, bitAt(mask, i), "position %d: deleted=%v", i, deleted)
	}
}

// Why: run-length encoding is the whole reason a range-shaped deletion set is
// cheap on the wire, and it must not disturb the positions it re-encodes.
// Condition: a bitmap built one position at a time over a 100,000-position
// contiguous stretch — two dense bitmap containers — serialized before and
// after RunLengthEncode.
// Assertion: the encoded bitmap shrinks, membership and cardinality survive,
// and the shrunken bytes still round-trip through the deserializer.
func TestRoaringBitmapRunLengthEncode(t *testing.T) {
	const positions = 100_000

	bm := NewRoaringPositionBitmap()
	for pos := range uint64(positions) {
		bm.Set(pos)
	}

	var before bytes.Buffer
	require.NoError(t, bm.Serialize(&before))

	bm.RunLengthEncode()

	var after bytes.Buffer
	require.NoError(t, bm.Serialize(&after))
	t.Logf("bare bitmap serialization: %d bytes before RunLengthEncode, %d after", before.Len(), after.Len())

	assert.Less(t, after.Len(), before.Len(), "run encoding must shrink a contiguous stretch")
	assert.Equal(t, int64(positions), bm.Cardinality())

	got, err := DeserializeRoaringPositionBitmap(after.Bytes())
	require.NoError(t, err)
	assertSamePositions(t, bm, got)
	assert.False(t, got.Contains(positions))
}

// Why: run encoding must be a no-op, not a corruption, for the scattered
// low-cardinality bitmaps that make up most deletion vectors.
// Condition: alternating positions that roaring keeps as an array container.
// Assertion: membership and cardinality are unchanged and the bitmap still
// serializes and round-trips.
func TestRoaringBitmapRunLengthEncodeScatteredPositions(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	positions := []uint64{1, 3, 5, 7, 9, (uint64(1) << 32) | 11}
	for _, pos := range positions {
		bm.Set(pos)
	}

	bm.RunLengthEncode()

	assert.Equal(t, int64(len(positions)), bm.Cardinality())
	for _, pos := range positions {
		assert.Truef(t, bm.Contains(pos), "position %d should survive run encoding", pos)
	}

	var buf bytes.Buffer
	require.NoError(t, bm.Serialize(&buf))
	got, err := DeserializeRoaringPositionBitmap(buf.Bytes())
	require.NoError(t, err)
	assertSamePositions(t, bm, got)
}

// assertSamePositions asserts two bitmaps hold the same positions. It compares
// buckets with roaring's Equals, which tests membership rather than container
// encoding, so a run-encoded bucket matches an array-encoded bucket holding the
// same positions.
func assertSamePositions(t *testing.T, want, got *RoaringPositionBitmap) {
	t.Helper()

	require.Equal(t, want.Cardinality(), got.Cardinality(), "cardinality")
	require.ElementsMatch(t, slices.Collect(maps.Keys(want.bitmaps)), slices.Collect(maps.Keys(got.bitmaps)),
		"allocated buckets")
	for key, wantBucket := range want.bitmaps {
		assert.Truef(t, wantBucket.Equals(got.bitmaps[key]), "bucket %d holds different positions", key)
	}
}

// Why: Serialize and DeserializeRoaringPositionBitmap together define the Go encoding contract for non-empty bitmaps.
// Condition: round-trip a bitmap with positions spread across multiple keys and an internal key gap.
// Assertion: serialization succeeds, deserialization succeeds, cardinality is preserved, and all original positions remain present.
func TestRoaringBitmapSerializeRoundTrip(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	positions := []uint64{
		0,
		1,
		100,
		65535,
		(uint64(1) << 32) | 42,
		(uint64(1) << 32) | 9999,
		uint64(5) << 32,
		(uint64(5) << 32) | 1,
	}
	for _, pos := range positions {
		bm.Set(pos)
	}

	var buf bytes.Buffer
	require.NoError(t, bm.Serialize(&buf))

	got, err := DeserializeRoaringPositionBitmap(buf.Bytes())
	require.NoError(t, err)

	assert.Equal(t, bm.Cardinality(), got.Cardinality())
	for _, pos := range positions {
		assert.True(t, got.Contains(pos), "round-trip lost position %d", pos)
	}
}

// Why: empty serialization is a separate boundary case because the on-wire count is zero and no key/bitmap pairs follow.
// Condition: round-trip an empty bitmap through Serialize and DeserializeRoaringPositionBitmap.
// Assertion: serialization succeeds, deserialization succeeds, bitmap is empty, and cardinality is 0.
func TestRoaringBitmapEmptyRoundTrip(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	var buf bytes.Buffer
	require.NoError(t, bm.Serialize(&buf))

	got, err := DeserializeRoaringPositionBitmap(buf.Bytes())
	require.NoError(t, err)

	assert.True(t, got.IsEmpty())
	assert.Equal(t, int64(0), got.Cardinality())
}

// Why: Positions is the supported way to enumerate a deletion vector's
// contents; without it consumers must round-trip the serialized format.
// Condition: iterate an empty bitmap.
// Assertion: no positions are yielded.
func TestRoaringBitmapPositionsEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	for pos := range bm.Positions() {
		t.Fatalf("empty bitmap yielded position %d", pos)
	}
}

// Why: the single-position case pins down that Positions yields exactly what Set stored.
// Condition: set one position above the 32-bit boundary and iterate.
// Assertion: exactly that position is yielded.
func TestRoaringBitmapPositionsSingle(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	want := (uint64(7) << 32) | 123
	bm.Set(want)

	var got []uint64
	for pos := range bm.Positions() {
		got = append(got, pos)
	}

	assert.Equal(t, []uint64{want}, got)
}

// Why: positions spanning multiple 32-bit keys must come back in ascending
// 64-bit order, with the count agreeing with Cardinality.
// Condition: set positions across keys 0, 1, and 5 (crossing the 32-bit
// boundary) in shuffled insertion order, then iterate.
// Assertion: yielded positions are the sorted originals and their count
// equals Cardinality.
func TestRoaringBitmapPositionsAscendingAcrossKeys(t *testing.T) {
	positions := []uint64{
		(uint64(5) << 32) | 1,
		0,
		(uint64(1) << 32) | 42,
		65535,
		uint64(5) << 32,
		(uint64(1) << 32) | 9999,
		1,
	}

	bm := NewRoaringPositionBitmap()
	for _, pos := range positions {
		bm.Set(pos)
	}

	var got []uint64
	for pos := range bm.Positions() {
		got = append(got, pos)
	}

	want := slices.Clone(positions)
	slices.Sort(want)
	assert.Equal(t, want, got)
	assert.Equal(t, bm.Cardinality(), int64(len(got)))
}

// Why: iter.Seq consumers may stop early (e.g. range-over-func with break);
// the iterator must honor that instead of continuing to yield.
// Condition: set positions in two keys and break after the first yield.
// Assertion: exactly one position (the smallest) is observed.
func TestRoaringBitmapPositionsEarlyBreak(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(10)
	bm.Set((uint64(2) << 32) | 3)

	var got []uint64
	for pos := range bm.Positions() {
		got = append(got, pos)

		break
	}

	assert.Equal(t, []uint64{10}, got)
}
