# Equality Delete Filter: Implementation Comparison

Research comparing three approaches for equality delete filtering in iceberg-go.

## Approaches

### 1. Hash (current)
Per-row: `encodeArrowValue` into `bytes.Buffer` → `bufString` zero-copy map lookup → bitmap.
~83 allocs at 1M rows. Bottleneck: per-row type switch in `encodeArrowValue`.

### 2. Arrow Compute (`is_in`)
Encode rows into Binary array → `is_in` kernel (vectorized set membership) → `not` → filter.
**Blocked by arrow-go escape analysis bug**: `BinaryMemoTable.Exists` causes 1 heap alloc
per row because `[]byte` val escapes through closure chain in `visitBinary` → `isInKernelExec`
→ `Lookup(h, func(i int32) bool { bytes.Equal(val, ...) })`.

**With `noescape` patch** (unsafe.Pointer trick to hide from escape analysis): allocs drop from
1M to ~225 at 1M rows. Performance ~10% slower than hash due to extra memcpy building Binary array.

Root cause: `internal/hashing/xxh3_memo_table_types.go` `HashTable.Lookup` takes `cmp func(T) bool`
closure, causing the `[]byte` parameter to escape. Fix options:
- `noescape` trick (validated, works)
- Restructure to accept `(buf []byte, offset, length int)` instead of `[]byte`
- Add specialized `ExistsDirect` that inlines the probe loop

**Arrow-go issue TODO**: file upstream about BinaryMemoTable.Exists heap escape.

### 3. Columnar (winner)
Extract raw typed slices (`Int64Values()`) once per column → encode directly from `[]int64`
without type switch → same `bufString` map lookup → bitmap. No arrow-go changes needed.
20-35% faster than hash at 1M rows. Same allocs (~85).

## Benchmark Results (Apple M3, 1M rows)

| Deletes | Hash (ms) | Compute+noescape (ms) | Columnar (ms) |
|---------|-----------|----------------------|---------------|
| 10      | 27        | 35                   | 20            |
| 100     | 35        | 39                   | 29            |
| 10K     | 37        | 40                   | 30            |

## Arrow-go noescape patch location
Patched files saved at `/tmp/arrow-go-patched/`:
- `internal/hashing/xxh3_memo_table.go` — `ExistsDirect` + `noescape`/`noescapeBytes`
- `arrow/compute/internal/kernels/scalar_set_lookup.go` — `isInBinaryDirect` + `binaryMemoExists`
