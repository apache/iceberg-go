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
	"testing"

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
	for i := uint64(0); i < 10; i++ {
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

// Why: Set, Contains, Cardinality, and gap handling are the core in-memory behaviors of this type.
// Condition: set positions across keys 0, 1, and 3, leaving key 2 absent.
// Assertion: cardinality counts all set positions, expected positions are present, and unset positions in the same key, a gap key, and a far key are absent.
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
