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
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeDVPuffinFixture writes a puffin file containing one deletion-vector-v1
// blob for the given positions and returns the on-disk path plus the blob's
// offset/length within the file (what a v3 manifest entry stores as
// content_offset / content_size_in_bytes).
//
// The inner payload is wrapped in the spec-canonical [4B BE length][4B LE
// magic 0xD1D33964][bitmap][4B BE CRC32] envelope so dv.DeserializeDV
// accepts it. A Go-built payload suffices here; the cross-impl byte pin
// against a Java-produced fixture is the job of #1041, not this test.
//
// TODO(#1041): replace the hand-built envelope with a dv.SerializeDV helper
// once that PR exports one. Today the dv package only exports the read side.
func writeDVPuffinFixture(t *testing.T, positions []uint64, referencedDataFile string) (path string, offset, length int64) {
	t.Helper()

	bitmap := dv.NewRoaringPositionBitmap()
	for _, p := range positions {
		bitmap.Set(p)
	}

	var bitmapBuf bytes.Buffer
	require.NoError(t, bitmap.Serialize(&bitmapBuf))

	// Length covers magic + bitmap, excludes CRC.
	magicAndBitmap := make([]byte, 4+bitmapBuf.Len())
	binary.LittleEndian.PutUint32(magicAndBitmap[:4], dv.DVMagicNumber)
	copy(magicAndBitmap[4:], bitmapBuf.Bytes())

	payload := make([]byte, 4+len(magicAndBitmap)+4)
	binary.BigEndian.PutUint32(payload[:4], uint32(len(magicAndBitmap)))
	copy(payload[4:4+len(magicAndBitmap)], magicAndBitmap)
	binary.BigEndian.PutUint32(payload[4+len(magicAndBitmap):], crc32.ChecksumIEEE(magicAndBitmap))

	var puffinBuf bytes.Buffer
	w, err := puffin.NewWriter(&puffinBuf)
	require.NoError(t, err)
	blobMeta, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDeletionVector,
		SnapshotID:     -1,
		SequenceNumber: -1,
		Fields:         []int32{},
		Properties: map[string]string{
			"referenced-data-file": referencedDataFile,
			"cardinality":          strconv.FormatInt(bitmap.Cardinality(), 10),
		},
	}, payload)
	require.NoError(t, err)
	require.NoError(t, w.Finish())

	path = filepath.Join(t.TempDir(), "dv.puffin")
	require.NoError(t, os.WriteFile(path, puffinBuf.Bytes(), 0o644))

	return path, blobMeta.Offset, blobMeta.Length
}

// newDVMockDataFile builds a manifest-entry-shaped mock for a DV puffin blob.
// FileSizeBytes is intentionally left at zero (mockDataFile's default): the
// scanner read path consults ContentOffset / ContentSizeInBytes only, and
// setting filesize to the blob length (rather than the puffin file size)
// invites a misleading test fixture.
func newDVMockDataFile(puffinPath, referencedDataFile string, offset, contentSize int64) *dvMockDataFile {
	return &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        puffinPath,
			contentType: iceberg.EntryContentPosDeletes,
			format:      iceberg.PuffinFile,
		},
		referencedDataFile: strPtr(referencedDataFile),
		contentOffset:      int64Ptr(offset),
		contentSizeInBytes: int64Ptr(contentSize),
	}
}

// Compile-time assertion of readAllDeletionVectors' signature — pins the
// `perFileDVBitmaps` return type so GetRecords' downstream wiring through
// recordBatchesFromTasksAndDeletes can't silently shift to a different shape
// without breaking the build first. Placed at package scope so the contract
// is visible at the top of the file.
var _ func(context.Context, iceio.IO, []FileScanTask, int) (perFileDVBitmaps, error) = readAllDeletionVectors

// TestReadAllDeletionVectors verifies the scanner-side DV loader produces a
// perFileDVBitmaps map keyed by referenced data-file path, with the spec-
// correctness invariants the loader is the right place to enforce: dedup by
// referenced data file (not by puffin path, which would silently drop blobs
// from multi-DV puffin files), rejection of missing referenced_data_file /
// content_offset, and rejection of two distinct DV blobs targeting the same
// data file (over-deletion guard, matching Java's DeleteFileIndex
// ValidationException behaviour for that case).
//
// End-to-end coverage (DV positions actually filtered out of GetRecords
// output for a real Parquet data file) is deferred to a follow-up — would
// need v3 table-metadata scaffolding that lives outside this PR's scope.
func TestReadAllDeletionVectors(t *testing.T) {
	ctx := context.Background()
	fs := iceio.LocalFS{}

	t.Run("decodes positions and keys by referenced data file", func(t *testing.T) {
		const dataFilePath = "file:///table/data/data-001.parquet"
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{1, 3, 5, 7, 9}, dataFilePath)

		tasks := []FileScanTask{{DeletionVectorFiles: []iceberg.DataFile{
			newDVMockDataFile(puffinPath, dataFilePath, offset, length),
		}}}

		got, err := readAllDeletionVectors(ctx, fs, tasks, 1)
		require.NoError(t, err)
		require.Contains(t, got, dataFilePath,
			"map must be keyed by the referenced data file, not by the DV puffin path")

		// Bitmap should expose the spec-mandated positions via Contains
		// (the lookup path filterByDeletionVector will use per batch).
		bitmap := got[dataFilePath]
		require.NotNil(t, bitmap)
		assert.Equal(t, int64(5), bitmap.Cardinality())
		for _, pos := range []uint64{1, 3, 5, 7, 9} {
			assert.True(t, bitmap.Contains(pos), "expected %d to be in bitmap", pos)
		}
		// Spot-check a non-deleted position too.
		assert.False(t, bitmap.Contains(0))
	})

	t.Run("dedups identical DV referenced by two tasks", func(t *testing.T) {
		const dataFilePath = "file:///table/data/data-002.parquet"
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{2, 4}, dataFilePath)
		dvFile := newDVMockDataFile(puffinPath, dataFilePath, offset, length)

		tasks := []FileScanTask{
			{DeletionVectorFiles: []iceberg.DataFile{dvFile}},
			{DeletionVectorFiles: []iceberg.DataFile{dvFile}},
		}

		got, err := readAllDeletionVectors(ctx, fs, tasks, 2)
		require.NoError(t, err)
		// Same (puffin path, content offset) across both tasks is the
		// same blob — the second occurrence is a dedup no-op, so the
		// single map entry stays a single bitmap (not a list of two).
		require.NotNil(t, got[dataFilePath])
		assert.Equal(t, int64(2), got[dataFilePath].Cardinality())
	})

	t.Run("no tasks -> empty map, no error", func(t *testing.T) {
		got, err := readAllDeletionVectors(ctx, fs, nil, 1)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("DV missing referenced_data_file is rejected", func(t *testing.T) {
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{0}, "file:///placeholder.parquet")

		dvFile := &dvMockDataFile{
			mockDataFile: mockDataFile{
				path:        puffinPath,
				contentType: iceberg.EntryContentPosDeletes,
				format:      iceberg.PuffinFile,
			},
			referencedDataFile: nil, // spec violation
			contentOffset:      int64Ptr(offset),
			contentSizeInBytes: int64Ptr(length),
		}

		_, err := readAllDeletionVectors(ctx, fs, []FileScanTask{{DeletionVectorFiles: []iceberg.DataFile{dvFile}}}, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing referenced_data_file")
	})

	t.Run("DV missing content_offset is rejected", func(t *testing.T) {
		// content_offset is required by spec; without it ReadDV can't
		// locate the blob. Pinned at the pre-pass so two such entries
		// for the same data file don't silently collide as a misleading
		// "multiple deletion vectors" dedup error.
		dvFile := &dvMockDataFile{
			mockDataFile: mockDataFile{
				path:        "/irrelevant.puffin",
				contentType: iceberg.EntryContentPosDeletes,
				format:      iceberg.PuffinFile,
			},
			referencedDataFile: strPtr("file:///table/data/data.parquet"),
			contentOffset:      nil, // spec violation
			contentSizeInBytes: int64Ptr(42),
		}

		_, err := readAllDeletionVectors(ctx, fs, []FileScanTask{{DeletionVectorFiles: []iceberg.DataFile{dvFile}}}, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing content_offset")
	})

	t.Run("multiple distinct DV blobs for same data file are rejected", func(t *testing.T) {
		// Two separate puffin files both claiming to be the DV for the same
		// data file. Java surfaces this as a ValidationException; silently
		// unioning the bitmaps would over-delete. The loader is also the
		// right place to catch the case where two blobs share a puffin file
		// but live at different content offsets — sameDVBlob compares both.
		const dataFilePath = "file:///table/data/data-003.parquet"
		puffinA, offsetA, lengthA := writeDVPuffinFixture(t, []uint64{1, 2}, dataFilePath)
		puffinB, offsetB, lengthB := writeDVPuffinFixture(t, []uint64{3, 4}, dataFilePath)

		tasks := []FileScanTask{{DeletionVectorFiles: []iceberg.DataFile{
			newDVMockDataFile(puffinA, dataFilePath, offsetA, lengthA),
			newDVMockDataFile(puffinB, dataFilePath, offsetB, lengthB),
		}}}

		_, err := readAllDeletionVectors(ctx, fs, tasks, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "multiple deletion vectors for data file")
	})

	t.Run("nil referenced_data_file is rejected before any goroutine launches", func(t *testing.T) {
		// The prior implementation validated inside the goroutine-dispatch
		// loop, so a bad entry hit AFTER one or more g.Go() calls would
		// defer-close the result channel while in-flight workers were still
		// sending to it (panic: send on closed channel). The v2 structure
		// makes the original buggy state unreachable by construction —
		// uniqueDVs is fully built and validated before any goroutine fans
		// out. This test pins the invariant: with both a valid and a
		// nil-ref entry in the input, the call returns a clean error and
		// does not panic, regardless of how concurrency is configured.
		const dataFilePath = "file:///table/data/data-004.parquet"
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{42}, dataFilePath)
		valid := newDVMockDataFile(puffinPath, dataFilePath, offset, length)
		broken := &dvMockDataFile{
			mockDataFile: mockDataFile{
				path:        "/nonexistent.puffin",
				contentType: iceberg.EntryContentPosDeletes,
				format:      iceberg.PuffinFile,
			},
			referencedDataFile: nil,
			contentOffset:      int64Ptr(0),
			contentSizeInBytes: int64Ptr(0),
		}

		var err error
		assert.NotPanics(t, func() {
			_, err = readAllDeletionVectors(ctx, fs, []FileScanTask{
				{DeletionVectorFiles: []iceberg.DataFile{valid}},
				{DeletionVectorFiles: []iceberg.DataFile{broken}},
			}, 4)
		})
		require.Error(t, err)
	})
}

// TestFilterByDeletionVector pins the per-batch pipeline step that applies
// a RoaringPositionBitmap to a record batch via Boolean keep-mask +
// compute.Filter — the load-bearing optimization over materializing the
// bitmap into a position set and using compute.Take.
//
// Spans two batches so the closure-captured absolute-position counter is
// exercised: positions 1,3 deleted from batch 1 (rows 0-4), positions 7,9
// from batch 2 (rows 5-9). The keep-mask logic is "false = drop, true =
// keep", so the surviving rows must be 0,2,4,6,8 across both batches.
func TestFilterByDeletionVector(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	bitmap := dv.NewRoaringPositionBitmap()
	for _, p := range []uint64{1, 3, 7, 9} {
		bitmap.Set(p)
	}
	filter := filterByDeletionVector(ctx, bitmap)

	mkBatch := func(start, end int64) arrow.RecordBatch {
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		for i := start; i < end; i++ {
			bldr.Append(i)
		}
		col := bldr.NewArray()
		defer col.Release()
		schema := arrow.NewSchema([]arrow.Field{{Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, nil)

		return array.NewRecordBatch(schema, []arrow.Array{col}, end-start)
	}

	t.Run("drops deleted rows across multiple batches", func(t *testing.T) {
		batch1 := mkBatch(0, 5)
		out1, err := filter(batch1)
		require.NoError(t, err)
		defer out1.Release()
		assert.Equal(t, []int64{0, 2, 4},
			out1.Column(0).(*array.Int64).Int64Values())

		batch2 := mkBatch(5, 10)
		out2, err := filter(batch2)
		require.NoError(t, err)
		defer out2.Release()
		assert.Equal(t, []int64{5, 6, 8},
			out2.Column(0).(*array.Int64).Int64Values())
	})
}
