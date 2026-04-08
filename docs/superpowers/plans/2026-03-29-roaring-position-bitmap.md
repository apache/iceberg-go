# Roaring Position Bitmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a 64-bit roaring position bitmap for deletion vectors, compatible with Java's `RoaringPositionBitmap` serialization format.

**Architecture:** A `RoaringPositionBitmap` struct in `table/` that splits 64-bit positions into high 32-bit keys and low 32-bit values, storing each key's values in a standard 32-bit `roaring.Bitmap`. Serialization uses the Iceberg portable format: `[bitmap count (8B LE)][key (4B LE) + roaring data]*`. This is the core data structure used by both DV reader and writer.

**Tech Stack:** Go, `github.com/RoaringBitmap/roaring/v2`, `github.com/stretchr/testify`

---

## File Structure

| File | Responsibility |
|---|---|
| `table/roaring_position_bitmap.go` | `RoaringPositionBitmap` struct: Set, SetRange, Contains, Cardinality, Serialize, Deserialize, SetAll, ForEach, RunOptimize |
| `table/roaring_position_bitmap_test.go` | Unit tests + Java golden file interop tests |
| `table/testdata/64mapempty.bin` | Java golden file: empty bitmap |
| `table/testdata/64map32bitvals.bin` | Java golden file: values fitting in 32 bits |
| `table/testdata/64mapspreadvals.bin` | Java golden file: values spread across multiple keys |

---

### Task 1: Add roaring dependency

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add the roaring bitmap dependency**

```bash
cd /Users/gladium/opensource/iceberg-go && go get github.com/RoaringBitmap/roaring/v2
```

- [ ] **Step 2: Verify it was added**

```bash
grep roaring go.mod
```

Expected: `github.com/RoaringBitmap/roaring/v2 v2.x.x`

- [ ] **Step 3: Tidy**

```bash
go mod tidy
```

---

### Task 2: Write failing tests for Set/Contains/Cardinality

**Files:**
- Create: `table/roaring_position_bitmap_test.go`

- [ ] **Step 1: Write the basic tests**

```go
package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoaringPositionBitmapSetAndContains(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	bm.Set(10)
	assert.True(t, bm.Contains(10))

	bm.Set(0)
	assert.True(t, bm.Contains(0))

	// duplicate add
	bm.Set(10)
	assert.True(t, bm.Contains(10))

	// not present
	assert.False(t, bm.Contains(99))
}

func TestRoaringPositionBitmapMultipleKeys(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	pos1 := int64(0)<<32 | 10   // key=0, low=10
	pos2 := int64(1)<<32 | 20   // key=1, low=20
	pos3 := int64(2)<<32 | 30   // key=2, low=30
	pos4 := int64(100)<<32 | 40 // key=100, low=40

	bm.Set(pos1)
	bm.Set(pos2)
	bm.Set(pos3)
	bm.Set(pos4)

	assert.True(t, bm.Contains(pos1))
	assert.True(t, bm.Contains(pos2))
	assert.True(t, bm.Contains(pos3))
	assert.True(t, bm.Contains(pos4))
	assert.Equal(t, int64(4), bm.Cardinality())
}

func TestRoaringPositionBitmapEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	assert.True(t, bm.IsEmpty())
	assert.Equal(t, int64(0), bm.Cardinality())

	bm.Set(5)
	assert.False(t, bm.IsEmpty())
}

func TestRoaringPositionBitmapCardinality(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	bm.Set(10)
	bm.Set(20)
	bm.Set(30)
	assert.Equal(t, int64(3), bm.Cardinality())

	// duplicate
	bm.Set(10)
	assert.Equal(t, int64(3), bm.Cardinality())
}

func TestRoaringPositionBitmapCardinalitySparseKeys(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	bm.Set(int64(0)<<32 | 100)
	bm.Set(int64(0)<<32 | 101)
	bm.Set(int64(0)<<32 | 105)
	bm.Set(int64(1)<<32 | 200)
	bm.Set(int64(100)<<32 | 300)
	assert.Equal(t, int64(5), bm.Cardinality())
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run TestRoaringPositionBitmap -v -count=1
```

Expected: FAIL — `NewRoaringPositionBitmap` undefined

---

### Task 3: Implement Set/Contains/Cardinality/IsEmpty

**Files:**
- Create: `table/roaring_position_bitmap.go`

- [ ] **Step 1: Write the struct and basic methods**

```go
package table

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/v2"
)

// RoaringPositionBitmap is a bitmap that supports positive 64-bit positions,
// optimized for cases where most positions fit in 32 bits. It uses an array
// of 32-bit Roaring bitmaps. Positions are split into a 32-bit key (high bits)
// and a 32-bit value (low bits).
//
// This is compatible with the Java Iceberg RoaringPositionBitmap serialization format.
type RoaringPositionBitmap struct {
	bitmaps []*roaring.Bitmap
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

// Contains checks if a position is set in the bitmap.
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

// Cardinality returns the number of set positions.
func (b *RoaringPositionBitmap) Cardinality() int64 {
	var c int64
	for _, bm := range b.bitmaps {
		c += int64(bm.GetCardinality())
	}
	return c
}

// grow ensures the bitmaps slice has at least 'required' entries,
// filling gaps with empty bitmaps.
func (b *RoaringPositionBitmap) grow(required int) {
	for len(b.bitmaps) < required {
		b.bitmaps = append(b.bitmaps, roaring.New())
	}
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run TestRoaringPositionBitmap -v -count=1
```

Expected: All 5 tests PASS

- [ ] **Step 3: Commit**

```bash
git add table/roaring_position_bitmap.go table/roaring_position_bitmap_test.go go.mod go.sum
git commit -m "feat(table): add RoaringPositionBitmap with Set/Contains/Cardinality"
```

---

### Task 4: Write failing tests for SetAll and ForEach

**Files:**
- Modify: `table/roaring_position_bitmap_test.go`

- [ ] **Step 1: Add tests**

Append to `table/roaring_position_bitmap_test.go`:

```go
func TestRoaringPositionBitmapSetAll(t *testing.T) {
	bm1 := NewRoaringPositionBitmap()
	bm1.Set(10)
	bm1.Set(20)

	bm2 := NewRoaringPositionBitmap()
	bm2.Set(30)
	bm2.Set(40)
	bm2.Set(int64(2) << 32)

	bm1.SetAll(bm2)

	assert.True(t, bm1.Contains(10))
	assert.True(t, bm1.Contains(20))
	assert.True(t, bm1.Contains(30))
	assert.True(t, bm1.Contains(40))
	assert.True(t, bm1.Contains(int64(2)<<32))
	assert.Equal(t, int64(5), bm1.Cardinality())

	// bm2 should be unmodified
	assert.False(t, bm2.Contains(10))
	assert.Equal(t, int64(3), bm2.Cardinality())
}

func TestRoaringPositionBitmapSetAllEmpty(t *testing.T) {
	bm1 := NewRoaringPositionBitmap()
	bm1.Set(10)

	bm1.SetAll(NewRoaringPositionBitmap())

	assert.True(t, bm1.Contains(10))
	assert.Equal(t, int64(1), bm1.Cardinality())
}

func TestRoaringPositionBitmapSetAllOverlapping(t *testing.T) {
	bm1 := NewRoaringPositionBitmap()
	bm1.Set(10)
	bm1.Set(20)
	bm1.Set(30)

	bm2 := NewRoaringPositionBitmap()
	bm2.Set(20)
	bm2.Set(40)

	bm1.SetAll(bm2)
	assert.Equal(t, int64(4), bm1.Cardinality())
}

func TestRoaringPositionBitmapSetRange(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.SetRange(10, 20) // [10, 20)

	for i := int64(10); i < 20; i++ {
		assert.True(t, bm.Contains(i), "should contain %d", i)
	}
	assert.False(t, bm.Contains(9))
	assert.False(t, bm.Contains(20))
	assert.Equal(t, int64(10), bm.Cardinality())
}

func TestRoaringPositionBitmapSetRangeEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.SetRange(10, 10) // empty range
	assert.True(t, bm.IsEmpty())
}

func TestRoaringPositionBitmapForEach(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	pos1 := int64(10)
	pos2 := int64(1) << 33
	pos3 := pos2 + 1
	pos4 := int64(2) << 33

	bm.Set(pos1)
	bm.Set(pos4)
	bm.Set(pos2)
	bm.Set(pos3)

	var collected []int64
	bm.ForEach(func(pos int64) {
		collected = append(collected, pos)
	})

	// must be sorted ascending across keys
	require.Equal(t, []int64{pos1, pos2, pos3, pos4}, collected)
}

func TestRoaringPositionBitmapForEachEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	var collected []int64
	bm.ForEach(func(pos int64) {
		collected = append(collected, pos)
	})
	assert.Empty(t, collected)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run "TestRoaringPositionBitmap(SetAll|ForEach)" -v -count=1
```

Expected: FAIL — `SetRange`, `SetAll`, and `ForEach` undefined

---

### Task 5: Implement SetAll and ForEach

**Files:**
- Modify: `table/roaring_position_bitmap.go`

- [ ] **Step 1: Add SetRange, SetAll, and ForEach methods**

Add to `table/roaring_position_bitmap.go` before the `grow` method:

```go
// SetRange marks all positions in [posStart, posEnd) as set.
func (b *RoaringPositionBitmap) SetRange(posStart, posEnd int64) {
	for pos := posStart; pos < posEnd; pos++ {
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
```

- [ ] **Step 2: Run all tests**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run TestRoaringPositionBitmap -v -count=1
```

Expected: All 12 tests PASS

- [ ] **Step 3: Commit**

```bash
git add table/roaring_position_bitmap.go table/roaring_position_bitmap_test.go
git commit -m "feat(table): add SetRange, SetAll, and ForEach to RoaringPositionBitmap"
```

---

### Task 6: Write failing tests for Serialize/Deserialize round-trip

**Files:**
- Modify: `table/roaring_position_bitmap_test.go`

- [ ] **Step 1: Add serialization round-trip tests**

Append to `table/roaring_position_bitmap_test.go`:

```go
import (
	"bytes"
	// ... existing imports
)

func TestRoaringPositionBitmapSerializeEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	var buf bytes.Buffer
	require.NoError(t, bm.Serialize(&buf))

	got, err := DeserializeRoaringPositionBitmap(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.True(t, got.IsEmpty())
	assert.Equal(t, int64(0), got.Cardinality())
}

func TestRoaringPositionBitmapSerializeSmallValues(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(1)
	bm.Set(3)
	bm.Set(5)
	bm.Set(7)
	bm.Set(9)

	var buf bytes.Buffer
	require.NoError(t, bm.Serialize(&buf))

	got, err := DeserializeRoaringPositionBitmap(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, int64(5), got.Cardinality())
	for _, pos := range []int64{1, 3, 5, 7, 9} {
		assert.True(t, got.Contains(pos))
	}
}

func TestRoaringPositionBitmapSerializeMultipleKeys(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(100)
	bm.Set(101)
	bm.Set(int64(1)<<32 + 100) // key=1
	bm.Set(int64(1)<<32 + 101) // key=1

	var buf bytes.Buffer
	require.NoError(t, bm.Serialize(&buf))

	got, err := DeserializeRoaringPositionBitmap(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, int64(4), got.Cardinality())
	assert.True(t, got.Contains(100))
	assert.True(t, got.Contains(101))
	assert.True(t, got.Contains(int64(1)<<32+100))
	assert.True(t, got.Contains(int64(1)<<32+101))
}

func TestRoaringPositionBitmapSerializeRoundTripPreservesAllPositions(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	// array container: sparse values
	bm.Set(5)
	bm.Set(7)

	// run container: consecutive range
	for i := int64(65536 + 1); i < 65536+1000; i++ {
		bm.Set(i)
	}

	// second key
	bm.Set(int64(1)<<32 + 10)
	bm.Set(int64(1)<<32 + 20)

	bm.RunOptimize()

	var buf bytes.Buffer
	require.NoError(t, bm.Serialize(&buf))

	got, err := DeserializeRoaringPositionBitmap(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, bm.Cardinality(), got.Cardinality())

	bm.ForEach(func(pos int64) {
		assert.True(t, got.Contains(pos), "missing position %d", pos)
	})
	got.ForEach(func(pos int64) {
		assert.True(t, bm.Contains(pos), "extra position %d", pos)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run "TestRoaringPositionBitmapSerialize" -v -count=1
```

Expected: FAIL — `Serialize` and `DeserializeRoaringPositionBitmap` undefined

---

### Task 7: Implement Serialize, Deserialize, RunOptimize, SerializedSizeInBytes

**Files:**
- Modify: `table/roaring_position_bitmap.go`

- [ ] **Step 1: Add the serialization methods**

Add to `table/roaring_position_bitmap.go` before the `grow` method:

```go
// RunOptimize applies run-length encoding where it is more space efficient.
func (b *RoaringPositionBitmap) RunOptimize() {
	for _, bm := range b.bitmaps {
		bm.RunOptimize()
	}
}

// SerializedSizeInBytes returns the number of bytes needed to serialize this bitmap.
func (b *RoaringPositionBitmap) SerializedSizeInBytes() int64 {
	size := int64(8) // bitmap count (8 bytes)
	for _, bm := range b.bitmaps {
		size += 4 + int64(bm.GetSerializedSizeInBytes()) // key (4) + bitmap data
	}
	return size
}

// Serialize writes the bitmap in the Iceberg portable format (little-endian):
//   - bitmap count (8 bytes, LE)
//   - for each bitmap: key (4 bytes, LE) + roaring portable format data
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
		if err == io.EOF {
			return NewRoaringPositionBitmap(), nil
		}
		return nil, fmt.Errorf("read bitmap count: %w", err)
	}
	if count < 0 || count > int64(^uint32(0)) {
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
			return nil, fmt.Errorf("keys must be in ascending order: got %d after %d", key, lastKey)
		}

		// fill gaps with empty bitmaps (sparse array)
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
```

- [ ] **Step 2: Run all serialization tests**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run "TestRoaringPositionBitmapSerialize" -v -count=1
```

Expected: All 4 tests PASS

- [ ] **Step 3: Run all bitmap tests**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run TestRoaringPositionBitmap -v -count=1
```

Expected: All 16 tests PASS

- [ ] **Step 4: Commit**

```bash
git add table/roaring_position_bitmap.go table/roaring_position_bitmap_test.go
git commit -m "feat(table): add Serialize/Deserialize to RoaringPositionBitmap"
```

---

### Task 8: Java golden file interop tests

**Files:**
- Create: `table/testdata/64mapempty.bin` (copy from Java)
- Create: `table/testdata/64map32bitvals.bin` (copy from Java)
- Create: `table/testdata/64mapspreadvals.bin` (copy from Java)
- Modify: `table/roaring_position_bitmap_test.go`

- [ ] **Step 1: Copy golden files from Java repo**

```bash
mkdir -p /Users/gladium/opensource/iceberg-go/table/testdata
cp /Users/gladium/opensource/iceberg/core/src/test/resources/org/apache/iceberg/deletes/64mapempty.bin /Users/gladium/opensource/iceberg-go/table/testdata/
cp /Users/gladium/opensource/iceberg/core/src/test/resources/org/apache/iceberg/deletes/64map32bitvals.bin /Users/gladium/opensource/iceberg-go/table/testdata/
cp /Users/gladium/opensource/iceberg/core/src/test/resources/org/apache/iceberg/deletes/64mapspreadvals.bin /Users/gladium/opensource/iceberg-go/table/testdata/
```

- [ ] **Step 2: Add Java interop tests**

Append to `table/roaring_position_bitmap_test.go`:

```go
import (
	"os"
	"path/filepath"
	// ... existing imports
)

func TestRoaringPositionBitmapDeserializeJavaEmpty(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "64mapempty.bin"))
	require.NoError(t, err)

	bm, err := DeserializeRoaringPositionBitmap(bytes.NewReader(data))
	require.NoError(t, err)
	assert.True(t, bm.IsEmpty())
	assert.Equal(t, int64(0), bm.Cardinality())
}

func TestRoaringPositionBitmapDeserializeJava32BitVals(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "64map32bitvals.bin"))
	require.NoError(t, err)

	bm, err := DeserializeRoaringPositionBitmap(bytes.NewReader(data))
	require.NoError(t, err)
	assert.False(t, bm.IsEmpty())
	assert.Greater(t, bm.Cardinality(), int64(0))
}

func TestRoaringPositionBitmapDeserializeJavaSpreadVals(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "64mapspreadvals.bin"))
	require.NoError(t, err)

	bm, err := DeserializeRoaringPositionBitmap(bytes.NewReader(data))
	require.NoError(t, err)
	assert.False(t, bm.IsEmpty())
	// Spread values means positions exist across multiple keys
	assert.Greater(t, bm.Cardinality(), int64(0))
}

func TestRoaringPositionBitmapDeserializeJavaAndReserialize(t *testing.T) {
	for _, name := range []string{"64mapempty.bin", "64map32bitvals.bin", "64mapspreadvals.bin"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", name))
			require.NoError(t, err)

			bm, err := DeserializeRoaringPositionBitmap(bytes.NewReader(data))
			require.NoError(t, err)

			// re-serialize
			var buf bytes.Buffer
			require.NoError(t, bm.Serialize(&buf))

			// deserialize again
			bm2, err := DeserializeRoaringPositionBitmap(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)

			assert.Equal(t, bm.Cardinality(), bm2.Cardinality())
			bm.ForEach(func(pos int64) {
				assert.True(t, bm2.Contains(pos), "missing position %d after round-trip", pos)
			})
		})
	}
}
```

- [ ] **Step 3: Run the interop tests**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run "TestRoaringPositionBitmapDeserializeJava" -v -count=1
```

Expected: All 4 tests PASS. If any fail, the serialization format is incompatible with Java — fix before proceeding.

- [ ] **Step 4: Run all bitmap tests one final time**

```bash
cd /Users/gladium/opensource/iceberg-go && go test ./table/ -run TestRoaringPositionBitmap -v -count=1
```

Expected: All 20 tests PASS

- [ ] **Step 5: Commit**

```bash
git add table/testdata/64mapempty.bin table/testdata/64map32bitvals.bin table/testdata/64mapspreadvals.bin table/roaring_position_bitmap_test.go
git commit -m "test(table): add Java golden file interop tests for RoaringPositionBitmap"
```
