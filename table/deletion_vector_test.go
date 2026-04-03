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
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serializeDVForTest is a test-only helper that serializes a bitmap
// into the DV binary format (length + magic + bitmap + CRC-32).
func serializeDVForTest(t *testing.T, bitmap *RoaringPositionBitmap) []byte {
	t.Helper()
	bitmap.RunOptimize()

	var bitmapBuf bytes.Buffer
	require.NoError(t, bitmap.Serialize(&bitmapBuf))
	bitmapBytes := bitmapBuf.Bytes()

	bitmapDataLength := dvMagicSize + len(bitmapBytes)
	totalSize := dvLengthSize + bitmapDataLength + dvCRCSize
	out := make([]byte, totalSize)

	binary.BigEndian.PutUint32(out[0:dvLengthSize], uint32(bitmapDataLength))
	binary.LittleEndian.PutUint32(out[dvLengthSize:dvLengthSize+dvMagicSize], DVMagicNumber)
	copy(out[dvLengthSize+dvMagicSize:], bitmapBytes)

	crc := crc32.ChecksumIEEE(out[dvLengthSize : totalSize-dvCRCSize])
	binary.BigEndian.PutUint32(out[totalSize-dvCRCSize:], crc)

	return out
}

// --- DeserializeDV tests ---

func TestDeserializeDVTooShort(t *testing.T) {
	_, err := DeserializeDV([]byte{0, 1, 2}, -1)
	assert.ErrorContains(t, err, "too short")
}

func TestDeserializeDVBadMagic(t *testing.T) {
	// valid length, wrong magic, padding, valid CRC placeholder
	data := make([]byte, dvMinSize)
	// length = 4 (magic only, no bitmap)
	data[0], data[1], data[2], data[3] = 0, 0, 0, 4
	// bad magic
	data[4], data[5], data[6], data[7] = 0xFF, 0xFF, 0xFF, 0xFF
	_, err := DeserializeDV(data, -1)
	assert.ErrorContains(t, err, "invalid deletion vector magic")
}

func TestDeserializeDVBadLength(t *testing.T) {
	data := make([]byte, dvMinSize)
	// wrong length value
	data[0], data[1], data[2], data[3] = 0, 0, 0, 99
	_, err := DeserializeDV(data, -1)
	assert.ErrorContains(t, err, "length mismatch")
}

// --- Java golden file tests: DV envelope ---

func TestDeserializeDVJavaEmpty(t *testing.T) {
	data := readTestData(t, "empty-position-index.bin")

	bm, err := DeserializeDV(data, 0)
	require.NoError(t, err)
	assert.True(t, bm.IsEmpty())
	assert.Equal(t, int64(0), bm.Cardinality())
}

func TestDeserializeDVJavaSmallAlternatingValues(t *testing.T) {
	data := readTestData(t, "small-alternating-values-position-index.bin")

	bm, err := DeserializeDV(data, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), bm.Cardinality())

	// positions: 1, 3, 5, 7, 9
	for _, pos := range []int64{1, 3, 5, 7, 9} {
		assert.True(t, bm.Contains(pos), "should contain %d", pos)
	}
	for _, pos := range []int64{0, 2, 4, 6, 8, 10} {
		assert.False(t, bm.Contains(pos), "should not contain %d", pos)
	}
}

func TestDeserializeDVJavaSmallAndLargeValues(t *testing.T) {
	data := readTestData(t, "small-and-large-values-position-index.bin")

	bm, err := DeserializeDV(data, 4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), bm.Cardinality())

	// positions: 100, 101, MaxInt32+100, MaxInt32+101
	assert.True(t, bm.Contains(100))
	assert.True(t, bm.Contains(101))
	assert.True(t, bm.Contains(int64(math.MaxInt32)+100))
	assert.True(t, bm.Contains(int64(math.MaxInt32)+101))
	assert.False(t, bm.Contains(102))
}

func TestDeserializeDVJavaAllContainerTypes(t *testing.T) {
	data := readTestData(t, "all-container-types-position-index.bin")

	bm, err := DeserializeDV(data, -1) // skip cardinality check
	require.NoError(t, err)
	assert.False(t, bm.IsEmpty())
	assert.Greater(t, bm.Cardinality(), int64(0))
}

func TestDeserializeDVCardinalityMismatch(t *testing.T) {
	data := readTestData(t, "small-alternating-values-position-index.bin")

	_, err := DeserializeDV(data, 999)
	assert.ErrorContains(t, err, "cardinality mismatch")
}

// --- SerializeDV tests ---

func TestSerializeDVEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	data := serializeDVForTest(t, bm)

	got, err := DeserializeDV(data, 0)
	require.NoError(t, err)
	assert.True(t, got.IsEmpty())
}

func TestSerializeDVSmallValues(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(1)
	bm.Set(3)
	bm.Set(5)
	bm.Set(7)
	bm.Set(9)

	data := serializeDVForTest(t, bm)

	got, err := DeserializeDV(data, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), got.Cardinality())
	for _, pos := range []int64{1, 3, 5, 7, 9} {
		assert.True(t, got.Contains(pos))
	}
}

func TestSerializeDVLargePositions(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(100)
	bm.Set(101)
	bm.Set(int64(math.MaxInt32) + 100)
	bm.Set(int64(math.MaxInt32) + 101)

	data := serializeDVForTest(t, bm)

	got, err := DeserializeDV(data, 4)
	require.NoError(t, err)
	assert.Equal(t, int64(4), got.Cardinality())
	assert.True(t, got.Contains(100))
	assert.True(t, got.Contains(101))
	assert.True(t, got.Contains(int64(math.MaxInt32)+100))
	assert.True(t, got.Contains(int64(math.MaxInt32)+101))
}

// --- Round-trip: Java golden files → deserialize → serialize → deserialize ---

func TestSerializeDVRoundTripJavaGoldenFiles(t *testing.T) {
	files := []struct {
		name        string
		cardinality int64
	}{
		{"empty-position-index.bin", 0},
		{"small-alternating-values-position-index.bin", 5},
		{"small-and-large-values-position-index.bin", 4},
	}

	for _, f := range files {
		t.Run(f.name, func(t *testing.T) {
			// Deserialize from Java golden file
			javaBytes := readTestData(t, f.name)
			bm, err := DeserializeDV(javaBytes, f.cardinality)
			require.NoError(t, err)

			// Re-serialize with Go
			goBytes := serializeDVForTest(t, bm)

			// Deserialize again and verify all positions match
			bm2, err := DeserializeDV(goBytes, f.cardinality)
			require.NoError(t, err)
			assert.Equal(t, bm.Cardinality(), bm2.Cardinality())

			bm.ForEach(func(pos int64) {
				assert.True(t, bm2.Contains(pos), "missing position %d after round-trip", pos)
			})
		})
	}
}

// --- Byte-exact match with Java golden files ---

func TestSerializeDVMatchesJavaBytes(t *testing.T) {
	files := []struct {
		name        string
		cardinality int64
	}{
		{"empty-position-index.bin", 0},
		{"small-alternating-values-position-index.bin", 5},
		{"small-and-large-values-position-index.bin", 4},
	}

	for _, f := range files {
		t.Run(f.name, func(t *testing.T) {
			javaBytes := readTestData(t, f.name)

			// Deserialize from Java, re-serialize with Go
			bm, err := DeserializeDV(javaBytes, f.cardinality)
			require.NoError(t, err)

			goBytes := serializeDVForTest(t, bm)

			// Bytes should be identical
			assert.Equal(t, javaBytes, goBytes, "Go serialization does not match Java golden file")
		})
	}
}

func readTestData(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return data
}
