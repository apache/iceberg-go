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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Java golden file deserialization tests ---

func TestDeserializeRoaringBitmapJavaEmpty(t *testing.T) {
	data := readTestData(t, "64mapempty.bin")
	r := bytes.NewReader(data)

	bm, err := DeserializeRoaringPositionBitmap(r)
	require.NoError(t, err)
	assert.True(t, bm.IsEmpty())
	assert.Equal(t, int64(0), bm.Cardinality())
}

func TestDeserializeRoaringBitmapJava32BitVals(t *testing.T) {
	data := readTestData(t, "64map32bitvals.bin")
	r := bytes.NewReader(data)

	bm, err := DeserializeRoaringPositionBitmap(r)
	require.NoError(t, err)
	assert.False(t, bm.IsEmpty())
	assert.Greater(t, bm.Cardinality(), int64(0))
}

func TestDeserializeRoaringBitmapJavaSpreadVals(t *testing.T) {
	data := readTestData(t, "64mapspreadvals.bin")
	r := bytes.NewReader(data)

	bm, err := DeserializeRoaringPositionBitmap(r)
	require.NoError(t, err)
	assert.False(t, bm.IsEmpty())
	assert.Greater(t, bm.Cardinality(), int64(0))
}

// --- Bitmap operation tests ---

func TestRoaringBitmapSetAndContains(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	// Positions within 32-bit key 0
	bm.Set(0)
	bm.Set(42)
	bm.Set(1000)

	// Positions in key 1 (high 32 bits = 1)
	high := int64(1) << 32
	bm.Set(high + 5)
	bm.Set(high + 999)

	// Positions in key 3 (skip key 2)
	high3 := int64(3) << 32
	bm.Set(high3 + 1)

	assert.Equal(t, int64(6), bm.Cardinality())

	// Verify all set positions
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains(42))
	assert.True(t, bm.Contains(1000))
	assert.True(t, bm.Contains(high+5))
	assert.True(t, bm.Contains(high+999))
	assert.True(t, bm.Contains(high3+1))

	// Verify positions not set
	assert.False(t, bm.Contains(1))
	assert.False(t, bm.Contains(high+6))
	assert.False(t, bm.Contains(int64(2)<<32+1))
	assert.False(t, bm.Contains(high3+2))
}

func TestRoaringBitmapSerializeRoundTrip(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	positions := []int64{
		0, 1, 100, 65535,
		int64(1)<<32 + 42,
		int64(1)<<32 + 9999,
		int64(5)<<32 + 0,
		int64(5)<<32 + 1,
	}
	for _, pos := range positions {
		bm.Set(pos)
	}

	// Serialize
	var buf bytes.Buffer
	err := bm.Serialize(&buf)
	require.NoError(t, err)

	// Deserialize
	got, err := DeserializeRoaringPositionBitmap(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	assert.Equal(t, bm.Cardinality(), got.Cardinality())

	for _, pos := range positions {
		assert.True(t, got.Contains(pos), "round-trip lost position %d", pos)
	}
}

func TestRoaringBitmapSetRange(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.SetRange(5, 10) // should set 5, 6, 7, 8, 9

	assert.Equal(t, int64(5), bm.Cardinality())

	for pos := int64(5); pos < 10; pos++ {
		assert.True(t, bm.Contains(pos), "should contain %d", pos)
	}

	// Boundaries
	assert.False(t, bm.Contains(4), "should not contain 4")
	assert.False(t, bm.Contains(10), "should not contain 10")
}

func TestRoaringBitmapSetAll(t *testing.T) {
	a := NewRoaringPositionBitmap()
	a.Set(1)
	a.Set(3)
	a.Set(5)

	b := NewRoaringPositionBitmap()
	b.Set(2)
	b.Set(4)
	b.Set(5) // overlap

	a.SetAll(b)

	assert.Equal(t, int64(5), a.Cardinality())
	for _, pos := range []int64{1, 2, 3, 4, 5} {
		assert.True(t, a.Contains(pos), "union should contain %d", pos)
	}
}

func TestRoaringBitmapForEachOrder(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	// Set positions across multiple keys in non-sorted insertion order
	high := int64(1) << 32
	bm.Set(high + 10)
	bm.Set(3)
	bm.Set(high + 1)
	bm.Set(100)
	bm.Set(0)

	var collected []int64
	bm.ForEach(func(pos int64) {
		collected = append(collected, pos)
	})

	expected := []int64{0, 3, 100, high + 1, high + 10}
	assert.Equal(t, expected, collected, "ForEach should yield positions in ascending order")
}
