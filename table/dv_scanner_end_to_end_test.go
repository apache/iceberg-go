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
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dvScanFieldID is the iceberg field ID of the single Int64 "id" column the
// end-to-end DV scan tests build their fixtures around. Kept as a constant so
// the parquet writer's PARQUET:field_id metadata, the iceberg.Schema
// declaration, and the mock data file all agree on the same number.
const dvScanFieldID = 1

// writeIntParquetWithFieldID writes a single-column Int64 parquet file at path
// containing row values [start, start+n), with PARQUET:field_id stamped on the
// column so the scanner can map the parquet column to the iceberg schema's
// field. The start parameter lets multi-file tests use disjoint row-value
// ranges so a bitmap-swap regression between files is observable in the
// aggregated output. Returns a mockDataFile shaped like a v3 manifest entry:
// ParquetFile format, Count() matching the actual row count
// (filterByDeletionVector uses File.Count() as the keep-mask length, so this
// must be exact).
func writeIntParquetWithFieldID(t *testing.T, fs iceio.WriteFileIO, path string, start, n int) iceberg.DataFile {
	t.Helper()
	mem := memory.DefaultAllocator

	arrSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "id",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: false,
		Metadata: arrow.MetadataFrom(map[string]string{
			"PARQUET:field_id": strconv.Itoa(dvScanFieldID),
		}),
	}}, nil)

	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	for i := range n {
		bldr.Append(int64(start + i))
	}
	col := bldr.NewArray()
	defer col.Release()
	rb := array.NewRecordBatch(arrSchema, []arrow.Array{col}, int64(n))
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
		count:       int64(n),
	}
}

// buildDVScanTestTable returns a *Table backed by an in-memory v3 metadata
// (format-version=3, single Int64 "id" column, field ID dvScanFieldID,
// unpartitioned, unsorted) and an FS factory that hands the same fs to all
// scan paths. No metadata.json on disk — the test reads parquet + puffin
// directly via the FS, never resolves manifests.
func buildDVScanTestTable(t *testing.T, fs iceio.IO, location string) *Table {
	t.Helper()

	sc := iceberg.NewSchema(
		0,
		iceberg.NestedField{
			ID:       dvScanFieldID,
			Name:     "id",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
	)
	meta, err := NewMetadata(sc, iceberg.UnpartitionedSpec, UnsortedSortOrder, location,
		iceberg.Properties{PropertyFormatVersion: "3"})
	require.NoError(t, err)

	fsF := func(_ context.Context) (iceio.IO, error) { return fs, nil }

	return New(Identifier{"default", "dv_scan_e2e"}, meta, "", fsF, nil)
}

// collectDVScanRows drains a scan iterator into a []int64 of surviving "id"
// values. Sorted ascending so multi-file scans are deterministic regardless
// of goroutine ordering (the scanner uses runtime.GOMAXPROCS workers by
// default and we don't constrain it — sorting decouples the contract under
// test from any worker-ordering assumption).
func collectDVScanRows(t *testing.T, scan *Scan, tasks []FileScanTask) []int64 {
	t.Helper()

	_, iter, err := scan.ReadTasks(t.Context(), tasks)
	require.NoError(t, err)

	var got []int64
	for rec, recErr := range iter {
		require.NoError(t, recErr)
		require.NotNil(t, rec)
		require.IsType(t, &array.Int64{}, rec.Column(0),
			"the test schema declares a single Int64 column; if the scanner reshapes it, the failure here is clearer than a bare type-assertion panic")
		col := rec.Column(0).(*array.Int64)
		for i := range col.Len() {
			got = append(got, col.Value(i))
		}
		rec.Release()
	}

	slices.Sort(got)

	return got
}

// TestDVScanEndToEnd locks the contract that DV-deleted row positions actually
// disappear from scan output for a real on-disk Parquet data file. The loader
// (readAllDeletionVectors) and the per-batch filter (filterByDeletionVector)
// are unit-tested separately in dv_scanner_read_test.go; this is the missing
// link asserting they compose correctly through the public ReadTasks API and
// that nothing in recordBatchesFromTasksAndDeletes drops the wiring.
//
// Out of scope here (covered elsewhere or deferred): the PlanFiles ->
// matchDVToData integration path (dv_scan_planning_test.go pins the
// sequence-number guard unit), DV + equality-delete composition, DV +
// multi-batch Parquet (TestFilterByDeletionVector covers the cross-batch
// nextIdx counter directly), and cross-client byte fixtures (#1041).
//
// Empirically verified to catch a regression: temporarily replacing the
// filterByDeletionVector body with a passthrough makes cases 2/3/4/6 fail
// with surviving-row mismatches. See PR description for details.
func TestDVScanEndToEnd(t *testing.T) {
	t.Run("no DV — all rows survive", func(t *testing.T) {
		fs := iceio.LocalFS{}
		tmp := t.TempDir()
		tbl := buildDVScanTestTable(t, fs, tmp)

		df := writeIntParquetWithFieldID(t, fs, filepath.Join(tmp, "data-1.parquet"), 0, 10)

		got := collectDVScanRows(t, tbl.Scan(), []FileScanTask{{File: df}})
		assert.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got,
			"no DV must leave all rows intact")
	})

	t.Run("DV deletes interior positions {1,3,5,7,9}", func(t *testing.T) {
		fs := iceio.LocalFS{}
		tmp := t.TempDir()
		tbl := buildDVScanTestTable(t, fs, tmp)

		dataPath := filepath.Join(tmp, "data-2.parquet")
		df := writeIntParquetWithFieldID(t, fs, dataPath, 0, 10)
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{1, 3, 5, 7, 9}, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})
		assert.Equal(t, []int64{0, 2, 4, 6, 8}, got,
			"DV positions {1,3,5,7,9} must be filtered out of scan output")
	})

	t.Run("DV deletes boundary positions {0,4}", func(t *testing.T) {
		fs := iceio.LocalFS{}
		tmp := t.TempDir()
		tbl := buildDVScanTestTable(t, fs, tmp)

		dataPath := filepath.Join(tmp, "data-3.parquet")
		df := writeIntParquetWithFieldID(t, fs, dataPath, 0, 5)
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{0, 4}, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})
		assert.Equal(t, []int64{1, 2, 3}, got,
			"first and last positions must be removable")
	})

	t.Run("DV deletes all rows", func(t *testing.T) {
		fs := iceio.LocalFS{}
		tmp := t.TempDir()
		tbl := buildDVScanTestTable(t, fs, tmp)

		dataPath := filepath.Join(tmp, "data-4.parquet")
		df := writeIntParquetWithFieldID(t, fs, dataPath, 0, 5)
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{0, 1, 2, 3, 4}, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})
		assert.Empty(t, got, "fully-deleted file must yield zero rows, not error")
	})

	t.Run("empty DV bitmap — all rows survive", func(t *testing.T) {
		fs := iceio.LocalFS{}
		tmp := t.TempDir()
		tbl := buildDVScanTestTable(t, fs, tmp)

		dataPath := filepath.Join(tmp, "data-5.parquet")
		df := writeIntParquetWithFieldID(t, fs, dataPath, 0, 5)
		puffinPath, offset, length := writeDVPuffinFixture(t, nil, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})
		assert.Equal(t, []int64{0, 1, 2, 3, 4}, got,
			"empty DV must be a no-op, not skip the file")
	})

	t.Run("two data files with independent DVs", func(t *testing.T) {
		// Pins that each data file's DV is applied only to its own rows.
		// Data ranges and DV positions are BOTH asymmetric between A and B so
		// a bitmap-swap regression (file A picks up B's bitmap and vice versa)
		// produces a distinguishably different aggregate. Symmetric setups
		// (identical row data, identical DV positions) hide swap bugs because
		// the aggregate sum is invariant under the swap.
		//
		// A: rows [0..4], DV deletes positions {1,3} → survives [0,2,4]
		// B: rows [10..14], DV deletes positions {0,2} → survives row values
		//    at positions {1,3,4} = [11,13,14]
		// Correct sorted aggregate: [0,2,4,11,13,14].
		// Swapped (A gets {0,2}, B gets {1,3}): [1,3,4,10,12,14] — different.
		fs := iceio.LocalFS{}
		tmp := t.TempDir()
		tbl := buildDVScanTestTable(t, fs, tmp)

		dataPathA := filepath.Join(tmp, "data-6a.parquet")
		dataPathB := filepath.Join(tmp, "data-6b.parquet")
		dfA := writeIntParquetWithFieldID(t, fs, dataPathA, 0, 5)
		dfB := writeIntParquetWithFieldID(t, fs, dataPathB, 10, 5)
		puffinA, offA, lenA := writeDVPuffinFixture(t, []uint64{1, 3}, dataPathA)
		puffinB, offB, lenB := writeDVPuffinFixture(t, []uint64{0, 2}, dataPathB)

		tasks := []FileScanTask{
			{File: dfA, DeletionVectorFiles: []iceberg.DataFile{
				newDVMockDataFile(puffinA, dataPathA, offA, lenA),
			}},
			{File: dfB, DeletionVectorFiles: []iceberg.DataFile{
				newDVMockDataFile(puffinB, dataPathB, offB, lenB),
			}},
		}
		got := collectDVScanRows(t, tbl.Scan(), tasks)
		assert.Equal(t, []int64{0, 2, 4, 11, 13, 14}, got,
			"each file's DV must apply only to that file's rows; a bitmap-swap "+
				"between files would yield [1,3,4,10,12,14] instead")
	})

	t.Run("DV with out-of-range position is a no-op for missing rows", func(t *testing.T) {
		// Stale or over-aggressive DV may carry positions past the data
		// file's row count. The contract being pinned is
		// dv.RoaringPositionBitmap.KeepMaskBytes(length): the keep-mask is
		// sized to `length` (= File.Count()), so bitmap positions >= length
		// have no mask slot and are silently ignored rather than crashing
		// or affecting present rows. Matches Java's behaviour. If
		// KeepMaskBytes ever grows an explicit bounds-check that errors on
		// out-of-range positions, this case is the first place to revisit.
		fs := iceio.LocalFS{}
		tmp := t.TempDir()
		tbl := buildDVScanTestTable(t, fs, tmp)

		dataPath := filepath.Join(tmp, "data-7.parquet")
		df := writeIntParquetWithFieldID(t, fs, dataPath, 0, 5)
		puffinPath, offset, length := writeDVPuffinFixture(t, []uint64{100}, dataPath)
		dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length)

		got := collectDVScanRows(t, tbl.Scan(), []FileScanTask{{
			File:                df,
			DeletionVectorFiles: []iceberg.DataFile{dvFile},
		}})
		assert.Equal(t, []int64{0, 1, 2, 3, 4}, got,
			"out-of-range DV position must not affect present rows")
	})
}
