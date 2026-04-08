# The Bitmap That Roars

I've been working on adding deletion vector support to [iceberg-go](https://github.com/apache/iceberg-go), the Go implementation of Apache Iceberg. The idea is simple: when you delete rows from an Iceberg table, instead of rewriting the entire data file, you record *which* row positions were deleted in a small side file. At read time, you skip those positions.

Simple enough. But the moment I started thinking about how to actually store those positions efficiently, things got interesting.

The Iceberg spec says deletion vectors use **Roaring Bitmaps**. I'd heard the name before but never looked under the hood. So I went digging.

This post is what I found.

---

## The Problem: Tracking Deleted Rows

Say you have a Parquet data file with 10 million rows. You delete rows at positions 42, 1337, 5000000, and 9999999.

You need a data structure that can:

1. Store arbitrary row positions (potentially up to billions)
2. Check membership fast -- "is row 42 deleted?"
3. Serialize compactly -- this gets written to disk
4. Merge efficiently -- multiple deletes might target the same file

Your first instinct might be a sorted list: `[42, 1337, 5000000, 9999999]`. That works for four positions. But what if you delete a million rows? Or a contiguous range of 500,000 rows? A sorted list of 64-bit integers at 8 bytes each burns 8 MB for a million deletes. And membership checks are O(log n).

There's a classic answer to this kind of problem.

---

## Enter: The Bitmap

A bitmap is the brute-force approach to set membership. You allocate one bit per possible position. Bit is 1? Position is in the set. Bit is 0? It's not.

```
Positions: {2, 5, 7}

Bitmap:  0 0 1 0 0 1 0 1
Index:   0 1 2 3 4 5 6 7
```

**Membership check**: O(1). Just read the bit at position `i`.

**Set a position**: O(1). Flip the bit.

**Merge two sets**: Bitwise OR. Doesn't get faster than that.

Bitmaps are beautifully simple. But they have a fatal flaw: **size is proportional to the universe, not the data.**

If your positions go up to 10 million, you need a 10-million-bit bitmap -- about 1.2 MB. Even if you only deleted 4 rows. The bitmap doesn't care how many bits are set; it allocates for the entire range.

And it gets worse. Iceberg uses 64-bit row positions (to support very large tables). A bitmap covering 64-bit space would need... 2^64 bits. That's 2 exabytes. Not practical.

```
Universe: 10,000,000 positions

Sorted list (4 deletes):     32 bytes
Bitmap (4 deletes):      1,250,000 bytes   ← 39,000× larger

Sorted list (1M deletes):  8,000,000 bytes
Bitmap (1M deletes):      1,250,000 bytes   ← 6× smaller
```

So bitmaps win when data is dense and lose badly when it's sparse. Sorted lists are the opposite. What if there was a structure that adapted to the data?

---

## Roaring Bitmaps: The Best of Both Worlds

This is exactly what [Roaring Bitmaps](https://roaringbitmap.org/) solve. The core idea is deceptively simple: **don't commit to one storage strategy. Let the data decide.**

### The Chunking Trick

A Roaring Bitmap splits the 32-bit space into **chunks** of 2^16 (65,536) values each. The high 16 bits of a value determine which chunk it belongs to. The low 16 bits determine the position within that chunk.

```
Value: 0x00050042 (327,746 in decimal)

High 16 bits: 0x0005 → chunk 5
Low 16 bits:  0x0042 → position 66 within chunk 5
```

Each chunk then independently chooses its own container type based on how many values it holds:

### Three Container Types

**1. Array Container** -- for sparse chunks

When a chunk has fewer than 4,096 values, store them as a sorted array of 16-bit integers.

```
Chunk 5 (sparse, 3 values): [66, 1024, 8192]

Storage: 3 values × 2 bytes = 6 bytes
```

Why 4,096? Because 4,096 × 2 bytes = 8,192 bytes, which is exactly the size of a 65,536-bit bitmap. Below this threshold, an array is smaller.

**2. Bitmap Container** -- for dense chunks

When a chunk has 4,096 or more values, switch to a traditional 65,536-bit bitmap.

```
Chunk 12 (dense, 30,000 values):

Storage: 65,536 bits = 8,192 bytes (fixed)
```

At 30,000 values, a sorted array would cost 60,000 bytes. The bitmap costs 8,192 bytes regardless. Clear win.

**3. Run Container** -- for consecutive sequences

When values form runs (e.g., "all positions from 1000 to 5000"), store them as `[start, length]` pairs.

```
Chunk 0 (contiguous range 1000-5000):

Run container: [(1000, 4001)]
Storage: 1 run × 4 bytes = 4 bytes

vs. Array container: 4,001 values × 2 bytes = 8,002 bytes
vs. Bitmap container: 8,192 bytes
```

This is where the "Roaring" name comes from -- runs make the bitmap roar through contiguous data.

### The Threshold Visualized

```
Values in chunk │ Best container │ Size
────────────────┼────────────────┼─────────────
             10 │ Array          │ 20 bytes
            100 │ Array          │ 200 bytes
          1,000 │ Array          │ 2,000 bytes
          4,095 │ Array          │ 8,190 bytes
─ ─ ─ ─ ─ ─ ─ ─┼─ crossover ─ ─ ┼ ─ ─ ─ ─ ─ ─
          4,096 │ Bitmap         │ 8,192 bytes
         10,000 │ Bitmap         │ 8,192 bytes
         50,000 │ Bitmap         │ 8,192 bytes
─ ─ ─ ─ ─ ─ ─ ─┼────────────────┼─────────────
  1000 to 5000  │ Run            │ 4 bytes
     (4001 vals)│                │
```

The magic is that each chunk makes its own choice. In a single Roaring Bitmap, chunk 0 might be an array, chunk 5 a bitmap, and chunk 12 a run container. The data dictates the representation.

---

## Operations Stay Fast

The container-per-chunk design keeps operations efficient:

**Membership check**: O(1) to find the chunk (it's indexed by the high 16 bits), then O(1) for bitmap containers, O(log n) for array containers, and O(log n) for run containers.

**Union (OR)**: Merge chunk by chunk. Two bitmap containers? Bitwise OR (fast). Two array containers? Sorted merge. The result container type is chosen based on the output cardinality.

**Intersection (AND)**: Same idea. Two bitmaps? Bitwise AND. Bitmap + array? Check each array element against the bitmap. Always picks the fastest approach for the container pair.

---

## 64-Bit Extension: Stacking Roaring Bitmaps

Standard Roaring Bitmaps operate on 32-bit integers. But Iceberg needs 64-bit positions -- a file could theoretically have more than 4 billion rows.

The solution is a layer on top: split the 64-bit position into a 32-bit **key** (high bits) and a 32-bit **value** (low bits). Each key maps to its own 32-bit Roaring Bitmap.

```
Position: 0x00000001_0000002A (4,294,967,338 in decimal)

Key (high 32):  0x00000001 → Roaring Bitmap at index 1
Value (low 32): 0x0000002A → stored in that bitmap
```

In practice, most Iceberg tables have far fewer than 4 billion rows per file, so you usually end up with a single key (0) and one Roaring Bitmap. But the structure is ready when you need it.

This is exactly what I implemented in iceberg-go:

```go
type RoaringPositionBitmap struct {
    bitmaps []*roaring.Bitmap // index = high 32 bits
}

func (b *RoaringPositionBitmap) Set(pos int64) {
    key := int(pos >> 32)
    low := uint32(pos)
    b.grow(key + 1)
    b.bitmaps[key].Add(low)
}

func (b *RoaringPositionBitmap) Contains(pos int64) bool {
    key := int(pos >> 32)
    low := uint32(pos)
    if key >= len(b.bitmaps) {
        return false
    }
    return b.bitmaps[key].Contains(low)
}
```

Split the 64-bit position, route to the right bitmap, done.

---

## Deletion Vectors: Putting It All Together

Back to Iceberg. A deletion vector is essentially a serialized Roaring Bitmap wrapped with some framing:

```
┌──────────────┬──────────────┬─────────────────┬──────────────┐
│ Length (4B)   │ Magic (4B)   │ Roaring Bitmap  │ CRC-32 (4B)  │
│ big-endian    │ 0x6439D3D1   │ (variable)      │ big-endian    │
└──────────────┴──────────────┴─────────────────┴──────────────┘
```

The length field tells you how many bytes to read. The magic number identifies the format. The CRC-32 catches corruption. And in the middle sits the Roaring Bitmap containing all deleted row positions.

These blobs live inside Puffin files -- Iceberg's general-purpose blob storage format. When reading a table, the scan engine:

1. Reads the manifest to find deletion vector files
2. Opens the Puffin file
3. Deserializes the Roaring Bitmap
4. Skips deleted positions during the scan

Because Roaring Bitmaps are compact and fast to query, this check adds negligible overhead to the scan path. Much cheaper than rewriting entire data files on every delete.

---

## Why Not Just Use a Hash Set?

Fair question. A `map[int64]struct{}` in Go would work. Membership check is O(1) amortized. So why Roaring?

```
1 million deleted positions:

Hash set (Go map):  ~40-50 MB (overhead per entry: hash, pointer, bucket)
Sorted slice:       ~8 MB    (8 bytes per int64)
Roaring Bitmap:     ~2 MB    (depends on distribution, but usually much smaller)
```

The difference compounds when you consider that deletion vectors get serialized to disk and read back on every scan. Roaring Bitmaps have a well-defined portable serialization format. Hash sets don't. You'd have to invent your own wire format, and it'd almost certainly be larger.

Plus, set operations. Merging two deletion vectors (when compacting or committing concurrent deletes) is a single `bitmap.Or(other)` call. With hash sets, you're iterating and inserting.

---

## Key Takeaways

| Concept | What it does |
|---|---|
| **Plain Bitmap** | 1 bit per possible position -- fast but size scales with universe |
| **Sorted Array** | Compact for sparse data, but slow for membership checks and merges |
| **Roaring Bitmap** | Chunks the space, picks the best container per chunk -- adapts to the data |
| **Array Container** | Sorted 16-bit array for sparse chunks (< 4,096 values) |
| **Bitmap Container** | Traditional bitmap for dense chunks (≥ 4,096 values) |
| **Run Container** | Start/length pairs for consecutive sequences |
| **64-bit Extension** | Array of 32-bit Roaring Bitmaps, keyed by high 32 bits |

The thing that clicked for me: Roaring Bitmaps aren't a single clever trick. They're an *engineering* answer -- the realization that no single representation works for all data distributions, so you let each chunk pick what's best for it. The 4,096-value crossover point isn't arbitrary; it's the exact point where an array of 16-bit values costs the same as a 65,536-bit bitmap.

I came into this just needing "a way to store deleted row IDs." I came out appreciating how a thoughtful data structure can eliminate the tradeoff between space and speed entirely. Hopefully this gives you the same intuition.

## References

- [Roaring Bitmap: Better Compressed Bitsets](https://arxiv.org/abs/1603.06549) -- The academic paper
- [roaringbitmap.org](https://roaringbitmap.org/) -- Official site with implementations in many languages
- [Apache Iceberg Deletion Vector Spec](https://iceberg.apache.org/spec/#deletion-vectors) -- How Iceberg uses them
- [Puffin File Format](https://iceberg.apache.org/puffin-spec/) -- The blob container format
