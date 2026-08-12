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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dvBinaryFieldID is the iceberg field ID for the single binary "payload"
// column used in the var-binary DV scan tests.
//
// IMPORTANT: NewMetadata calls AssignFreshSchemaIDs which always assigns the
// first field ID 1. The Parquet writer stamps PARQUET:field_id on each column
// from this constant, and the scan resolves column indices by matching those
// file-level field IDs to the table schema's (post-reassignment) IDs. If the
// two values diverge, PrunedSchema finds no matching column and the scan
// yields "no leaf column readers matched col indices".
//
// The existing dvScanFieldID = 1 (dv_scanner_end_to_end_test.go) follows the
// same convention. The two tests use separate Table objects and separate
// Parquet files, so the identical constant value causes no conflict.
const dvBinaryFieldID = 1

// _dvPanicEnv is the environment variable that gates the crash-subprocess path
// inside TestBinaryOffsetCorruptionPanicChain.
const _dvPanicEnv = "_ICEBERG_DV_CORRUPTION_PANIC"

// TestBinaryOffsetCorruptionPanicChain is the reproduction for the production
// crash. It documents the exact failure chain and confirms the mechanism is
// real by running it in a subprocess:
//
//  1. pqarrow builds *array.Binary (int32 offsets) for Parquet BYTE_ARRAY
//     columns. BinaryBuilder.appendNextOffset stores int32(b.values.Len()),
//     guarded only by debug.Assert which is an empty function under the
//     default !assert build tag — inert in every production binary.
//
//  2. A read batch whose aggregate BYTE_ARRAY payload exceeds 2^31-1 bytes
//     wraps int32 silently, writing negative offsets into the buffer.
//
//  3. compute.FilterRecordBatch dispatches to VarBinaryImpl[int32]
//     (arrow/compute/internal/kernels/vector_selection.go:1703), which does:
//     rawData[valOffset : valOffset+valSize]
//     A negative valOffset (sign-extended from int32) causes a slice-bounds
//     panic that is unrecoverable — the panic fires in a goroutine spawned by
//     Arrow's compute executor, so no outer recover() can catch it. The
//     process exits with code 2.
//
// The fix (GetReader calling arrProps.SetForceLarge for every leaf column)
// makes overflow structurally impossible by always producing *array.LargeBinary
// (int64 offsets). The end-to-end regression is TestDVScanWithBinaryColumn.
//
// Subprocess pattern: the outer test spawns itself with _ICEBERG_DV_CORRUPTION_PANIC=1
// and asserts the subprocess exits non-zero with the expected panic message.
// The subprocess runs the crash directly — it will always die. This is the
// canonical Go approach for testing fatal/unrecoverable errors.
func TestBinaryOffsetCorruptionPanicChain(t *testing.T) {
	if os.Getenv(_dvPanicEnv) == "1" {
		// Subprocess path: run the corruption and die.
		runCorruptBinaryFilterForSubprocess()
		return
	}

	// Outer test: spawn a subprocess and assert it crashes with the right panic.
	cmd := exec.Command(os.Args[0], "-test.run=TestBinaryOffsetCorruptionPanicChain", "-test.v")
	cmd.Env = append(os.Environ(), _dvPanicEnv+"=1")
	out, err := cmd.CombinedOutput()
	t.Logf("subprocess output:\n%s", out)

	require.Error(t, err, "subprocess must exit non-zero — the corrupt binary offsets must panic inside VarBinaryImpl[int32]")
	assert.Contains(t, string(out), "slice bounds out of range",
		"subprocess must emit the expected panic string from vector_selection.go:1703")
}

// runCorruptBinaryFilterForSubprocess is called inside the subprocess path of
// TestBinaryOffsetCorruptionPanicChain. It builds a *array.Binary with
// hand-crafted negative int32 offsets — what pqarrow produces silently after
// int32 wrap-around — and passes it through filterByDeletionVector.
//
// The compute kernel panics unconditionally on corrupt offsets. This function
// is not expected to return.
func runCorruptBinaryFilterForSubprocess() {
	ctx := context.Background()

	// Offsets: [0, 5, int32(-2000000000), int32(-2000000005)] for a 3-row array.
	//   row 0: bytes[0:5]            — valid ("hello")
	//   row 1: start=5, end wraps to int32(-2000000000)
	//   row 2: both bounds corrupt
	//
	// When the filter tries to copy row 1:
	//   valOffset = 5, valSize = -2000000000 - 5 = -2000000005 (negative)
	//   rawData[5 : 5 + (-2000000005)] = rawData[5 : -2000000000]
	//   → panic: slice bounds out of range [5:-2000000000]
	//
	// Empirically the Arrow presize calculation at line 1661 runs with
	// dataLength = rawOffsets[3]-rawOffsets[0] = -2000000005 (negative int32),
	// which produces a negative estimatedTotalSize clamped to 0 by
	// min(..., 16777216). The actual panic fires at line 1703 during the
	// per-row copy loop.
	offsets := []int32{0, 5, -2000000000, -2000000005}
	offsetBuf := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(offsets))
	dataBuf := memory.NewBufferBytes([]byte("hello"))

	arrData := array.NewData(
		arrow.BinaryTypes.Binary,
		3,
		[]*memory.Buffer{nil, offsetBuf, dataBuf},
		nil,
		0,
		0,
	)
	corruptBinary := array.NewBinaryData(arrData)
	arrData.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: arrow.BinaryTypes.Binary}}, nil)

	// DV deletes row 2, keeping rows 0 and 1. The filter reads row 1's
	// corrupt end-offset, triggering the slice-bounds panic.
	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(2)
	filter := filterByDeletionVector(ctx, bitmap, 3)

	// filter takes ownership of batch (calls defer r.Release() internally).
	batch := array.NewRecordBatch(schema, []arrow.Array{corruptBinary}, 3)
	corruptBinary.Release()

	out, _ := filter(batch)
	// Unreachable: the compute kernel panics before returning.
	if out != nil {
		out.Release()
	}
}

// TestFilterByDeletionVectorZeroRowBatch verifies that a zero-row batch passes
// through filterByDeletionVector without advancing the absolute-position
// counter (nextIdx), so subsequent batches remain aligned with the keep-mask.
//
// Zero-row batches are a valid (if rare) output of Parquet row-group readers
// when all rows in the final partial page fell into the preceding batch. The
// filter must not trip on them.
func TestFilterByDeletionVectorZeroRowBatch(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx = compute.WithAllocator(ctx, mem)

	schema := arrow.NewSchema([]arrow.Field{{Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, nil)

	mkBatch := func(vals ...int64) arrow.RecordBatch {
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		bldr.AppendValues(vals, nil)
		col := bldr.NewArray()
		defer col.Release()
		return array.NewRecordBatch(schema, []arrow.Array{col}, int64(len(vals)))
	}

	// DV deletes rows 1 and 3 out of a 4-row file.
	// We deliver: [batch of 0 rows] [batch of rows 0-1] [batch of rows 2-3]
	// nextIdx must correctly index into the keep-mask despite the empty
	// leading batch.
	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(1)
	bitmap.Set(3)
	filter := filterByDeletionVector(ctx, bitmap, 4)

	// First: empty batch. Must pass through without panicking or advancing nextIdx.
	empty := array.NewRecordBatch(schema, nil, 0)
	outEmpty, err := filter(empty)
	require.NoError(t, err)
	require.NotNil(t, outEmpty)
	assert.Equal(t, int64(0), outEmpty.NumRows(), "empty batch must stay empty")
	outEmpty.Release()

	// Second: rows 0-1 (absolute positions 0 and 1). Row 1 is deleted.
	batch1 := mkBatch(0, 1)
	out1, err := filter(batch1)
	require.NoError(t, err)
	defer out1.Release()
	require.Equal(t, int64(1), out1.NumRows())
	assert.Equal(t, int64(0), out1.Column(0).(*array.Int64).Value(0),
		"row 1 (absolute pos 1) must be deleted; only row 0 survives")

	// Third: rows 2-3 (absolute positions 2 and 3). Row 3 is deleted.
	batch2 := mkBatch(2, 3)
	out2, err := filter(batch2)
	require.NoError(t, err)
	defer out2.Release()
	require.Equal(t, int64(1), out2.NumRows())
	assert.Equal(t, int64(2), out2.Column(0).(*array.Int64).Value(0),
		"row 3 (absolute pos 3) must be deleted; only row 2 survives")
}

// TestFilterByDeletionVectorRowCountGuard verifies that filterByDeletionVector
// returns a descriptive error (not a panic) when the Parquet reader delivers
// more rows than the file metadata claims.
//
// Without this guard, array.NewSlice would panic with an out-of-bounds error
// that is unrecoverable from outside the goroutine.
func TestFilterByDeletionVectorRowCountGuard(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx = compute.WithAllocator(ctx, mem)

	schema := arrow.NewSchema([]arrow.Field{{Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, nil)

	// rowCount=2 but the batch delivers 5 rows — simulates a corrupted or
	// mismatched file where the data has more rows than the metadata claims.
	bitmap := dv.NewRoaringPositionBitmap()
	filter := filterByDeletionVector(ctx, bitmap, 2 /* rowCount */)

	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.AppendValues([]int64{0, 1, 2, 3, 4}, nil)
	col := bldr.NewArray()
	defer col.Release()
	bigBatch := array.NewRecordBatch(schema, []arrow.Array{col}, 5)
	// filter takes ownership of bigBatch; releasing bigBatch here would
	// double-release since filter calls defer r.Release() internally.

	out, err := filter(bigBatch)
	require.Error(t, err, "exceeding rowCount must return an error, not panic")
	assert.Nil(t, out)
	assert.Contains(t, err.Error(), "file count=2",
		"error must quote the file row count for diagnosability")
	assert.Contains(t, err.Error(), "batch rows=5",
		"error must quote the batch row count for diagnosability")
}

// writeBinaryParquetWithFieldID writes a single-column Binary (BYTE_ARRAY)
// Parquet file at path containing the supplied values, with PARQUET:field_id
// dvBinaryFieldID. Returns a mockDataFile shaped like a v3 manifest entry.
func writeBinaryParquetWithFieldID(t *testing.T, fs iceio.WriteFileIO, path string, values [][]byte) iceberg.DataFile {
	t.Helper()
	mem := memory.DefaultAllocator

	arrSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "payload",
		Type:     arrow.BinaryTypes.Binary,
		Nullable: true,
		Metadata: arrow.MetadataFrom(map[string]string{
			"PARQUET:field_id": strconv.Itoa(dvBinaryFieldID),
		}),
	}}, nil)

	bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer bldr.Release()
	for _, v := range values {
		if v == nil {
			bldr.AppendNull()
		} else {
			bldr.Append(v)
		}
	}
	col := bldr.NewArray()
	defer col.Release()

	n := int64(len(values))
	rb := array.NewRecordBatch(arrSchema, []arrow.Array{col}, n)
	defer rb.Release()
	arrTbl := array.NewTableFromRecords(arrSchema, []arrow.RecordBatch{rb})
	defer arrTbl.Release()

	fo, err := fs.Create(path)
	require.NoError(t, err)
	defer fo.Close()
	require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(), nil,
		pqarrow.DefaultWriterProps()))

	return &mockDataFile{
		path:        path,
		contentType: iceberg.EntryContentData,
		format:      iceberg.ParquetFile,
		count:       n,
	}
}

// buildDVScanBinaryTestTable returns a *Table backed by v3 metadata with a
// single Binary "payload" column (field ID dvBinaryFieldID), unpartitioned and
// unsorted. Used by the var-binary DV scan end-to-end test.
func buildDVScanBinaryTestTable(t *testing.T, fs iceio.IO, location string) *Table {
	t.Helper()

	sc := iceberg.NewSchema(
		0,
		iceberg.NestedField{
			ID:       dvBinaryFieldID,
			Name:     "payload",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: false,
		},
	)
	meta, err := NewMetadata(sc, iceberg.UnpartitionedSpec, UnsortedSortOrder, location,
		iceberg.Properties{PropertyFormatVersion: "3"})
	require.NoError(t, err)

	fsF := func(_ context.Context) (iceio.IO, error) { return fs, nil }
	return New(Identifier{"default", "dv_binary_scan"}, meta, "", fsF, nil)
}

// collectDVBinaryScanRows drains a scan iterator into a [][]byte of surviving
// "payload" values, sorted lexicographically for deterministic comparison
// across goroutine-ordered scans.
func collectDVBinaryScanRows(t *testing.T, scan *Scan, tasks []FileScanTask) [][]byte {
	t.Helper()

	_, iter, err := scan.ReadTasks(t.Context(), tasks)
	require.NoError(t, err)

	var got [][]byte
	for rec, recErr := range iter {
		require.NoError(t, recErr)
		require.NotNil(t, rec)

		col := rec.Column(0)
		for i := range col.Len() {
			if col.IsNull(i) {
				got = append(got, nil)
				continue
			}
			// ToRequestedSchema with useLargeTypes=false (the default) casts
			// LargeBinary back to Binary after the filter. Handle both types so
			// the test is not brittle against that detail — if useLargeTypes is
			// ever wired through, the assertion still holds.
			switch c := col.(type) {
			case *array.Binary:
				v := c.Value(i)
				cp := make([]byte, len(v))
				copy(cp, v)
				got = append(got, cp)
			case *array.LargeBinary:
				v := c.Value(i)
				cp := make([]byte, len(v))
				copy(cp, v)
				got = append(got, cp)
			default:
				t.Fatalf("unexpected column type %T for payload column", col)
			}
		}
		rec.Release()
	}

	slices.SortFunc(got, func(a, b []byte) int {
		la, lb := len(a), len(b)
		for i := range min(la, lb) {
			if a[i] != b[i] {
				return int(a[i]) - int(b[i])
			}
		}
		return la - lb
	})

	return got
}

// TestDVScanWithBinaryColumn is the end-to-end regression test for the binary
// offset overflow crash. It verifies that the DV filter path works correctly
// for a real Parquet file with a Binary (BYTE_ARRAY) column.
//
// Full chain exercised: GetReader (SetForceLarge forces LargeBinary / int64
// offsets) → filterByDeletionVector (compute.FilterRecordBatch on LargeBinary)
// → ToRequestedSchema (casts back to Binary for the caller).
//
// Prior DV tests only used Int64 columns. This is the first DV test with a
// var-binary column — the gap that allowed the overflow to ship undetected.
// If SetForceLarge is removed from GetReader and the test fixture is small
// enough to not overflow int32, this test still passes, but the mechanism is
// documented in TestBinaryOffsetCorruptionPanicChain.
func TestDVScanWithBinaryColumn(t *testing.T) {
	fs := iceio.LocalFS{}
	tmp := t.TempDir()
	tbl := buildDVScanBinaryTestTable(t, fs, tmp)

	payloads := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
		[]byte("elderberry"),
	}

	t.Run("no DV — all binary rows survive", func(t *testing.T) {
		dataPath := filepath.Join(tmp, "binary-1.parquet")
		df := writeBinaryParquetWithFieldID(t, fs, dataPath, payloads)

		got := collectDVBinaryScanRows(t, tbl.Scan(), []FileScanTask{{File: df}})
		require.Len(t, got, len(payloads), "all rows must survive with no DV")
	})

	t.Run("DV deletes interior binary rows {1,3}", func(t *testing.T) {
		dataPath := filepath.Join(tmp, "binary-2.parquet")
		df := writeBinaryParquetWithFieldID(t, fs, dataPath, payloads)
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{1, 3}, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVBinaryScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})

		// Positions 1 ("banana") and 3 ("date") are deleted.
		// Survivors: "apple"(0), "cherry"(2), "elderberry"(4) — sorted.
		want := [][]byte{[]byte("apple"), []byte("cherry"), []byte("elderberry")}
		assert.Equal(t, want, got,
			"DV positions {1,3} must remove banana and date from Binary column output")
	})

	t.Run("DV deletes all binary rows", func(t *testing.T) {
		dataPath := filepath.Join(tmp, "binary-3.parquet")
		df := writeBinaryParquetWithFieldID(t, fs, dataPath, payloads)
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{0, 1, 2, 3, 4}, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVBinaryScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})
		assert.Empty(t, got, "fully-deleted Binary file must yield zero rows")
	})

	t.Run("DV deletes boundary binary rows {0,4}", func(t *testing.T) {
		dataPath := filepath.Join(tmp, "binary-4.parquet")
		df := writeBinaryParquetWithFieldID(t, fs, dataPath, payloads)
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{0, 4}, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVBinaryScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})

		// "apple"(0) and "elderberry"(4) deleted; survivors: banana, cherry, date.
		want := [][]byte{[]byte("banana"), []byte("cherry"), []byte("date")}
		assert.Equal(t, want, got, "first and last Binary rows must be removable")
	})
}
