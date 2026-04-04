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

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestReadPerfUnderDeletePressure measures how scan latency degrades
// as equality delete files accumulate. This documents the baseline
// degradation that compaction (#832) and deletion vectors (#589) will fix.
//
// Each delete file is committed individually to simulate real CDC workloads
// where deletes accumulate one-per-commit. This is the worst case because
// each delete file requires a separate hash-set anti-join during scan.
//
// Note: each commit also creates a new snapshot and manifest, so the
// measurement includes metadata traversal overhead, not just delete
// application cost.
func TestReadPerfUnderDeletePressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping read-perf benchmark in short mode")
	}

	tbl := newReadPerfTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Write a data file with 1000 rows
	const numRows = 1000
	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, generateRowsJSON(numRows))

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Baseline: scan with no deletes
	baselineDuration, baselineRows := scanAndMeasure(t, t.Context(), tbl)
	t.Logf("Baseline: %d rows in %v", baselineRows, baselineDuration)
	require.Equal(t, int64(numRows), baselineRows)

	// Equality delete schema: delete by id
	delSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	delArrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(t, err)

	// Progressive delete pressure: add individual equality delete files
	measurePoints := []int{5, 10, 25, 50, 100}
	nextMeasure := 0
	totalDeleteFiles := 0
	nextDeleteID := 1

	for nextMeasure < len(measurePoints) {
		target := measurePoints[nextMeasure]

		for totalDeleteFiles < target {
			deleteJSON := generateDeleteJSON(nextDeleteID, 1)
			nextDeleteID++

			rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, delArrowSc, strings.NewReader(deleteJSON))
			require.NoError(t, err)

			records := func(yield func(arrow.RecordBatch, error) bool) {
				yield(rec, nil)
			}

			tx := tbl.NewTransaction()
			deleteFiles, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
			require.NoError(t, err)
			rec.Release()

			rd := tx.NewRowDelta(nil)
			for _, df := range deleteFiles {
				rd.AddDeletes(df)
			}
			require.NoError(t, rd.Commit(t.Context()))

			tbl, err = tx.Commit(t.Context())
			require.NoError(t, err)

			totalDeleteFiles++
		}

		duration, rows := scanAndMeasure(t, t.Context(), tbl)
		expectedRows := int64(numRows - totalDeleteFiles)
		slowdown := float64(duration) / float64(baselineDuration)

		t.Logf("After %3d eq-delete files: %4d rows in %v (%.1fx baseline)",
			totalDeleteFiles, rows, duration, slowdown)

		require.Equal(t, expectedRows, rows,
			"expected %d rows after %d deletes", expectedRows, totalDeleteFiles)

		nextMeasure++
	}

	finalDuration, finalRows := scanAndMeasure(t, t.Context(), tbl)
	finalSlowdown := float64(finalDuration) / float64(baselineDuration)

	t.Logf("\nSummary:")
	t.Logf("  Data rows:       %d", numRows)
	t.Logf("  Delete files:    %d", totalDeleteFiles)
	t.Logf("  Remaining rows:  %d", finalRows)
	t.Logf("  Baseline scan:   %v", baselineDuration)
	t.Logf("  Final scan:      %v (%.1fx slowdown)", finalDuration, finalSlowdown)
}

// BenchmarkScanWithEqualityDeletes provides stable benchmark numbers
// for scan performance under varying delete pressure.
func BenchmarkScanWithEqualityDeletes(b *testing.B) {
	deleteCounts := []int{0, 10, 50, 100, 200}

	for _, numDeletes := range deleteCounts {
		b.Run(fmt.Sprintf("deletes=%d", numDeletes), func(b *testing.B) {
			tbl := setupTableWithDeletes(b, 1000, numDeletes)
			ctx := context.Background()

			b.ResetTimer()
			for b.Loop() {
				_, itr, err := tbl.Scan().ToArrowRecords(ctx)
				require.NoError(b, err)

				var rows int64
				for rec, err := range itr {
					require.NoError(b, err)
					rows += rec.NumRows()
					rec.Release()
				}

				if rows != int64(1000-numDeletes) {
					b.Fatalf("expected %d rows, got %d", 1000-numDeletes, rows)
				}
			}
		})
	}
}

// ============================================================
// Helpers
// ============================================================

func newReadPerfTestTable(tb testing.TB) *table.Table {
	tb.Helper()

	location := filepath.ToSlash(tb.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(tb, err)

	return table.New(
		table.Identifier{"db", "read_perf_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func setupTableWithDeletes(tb testing.TB, numRows, numDeletes int) *table.Table {
	tb.Helper()

	tbl := newReadPerfTestTable(tb)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(tb, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(tb, dataPath, arrowSc, generateRowsJSON(numRows))

	ctx := context.Background()
	tx := tbl.NewTransaction()
	require.NoError(tb, tx.AddFiles(ctx, []string{dataPath}, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(tb, err)

	if numDeletes == 0 {
		return tbl
	}

	delSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	delArrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(tb, err)

	for i := range numDeletes {
		deleteJSON := generateDeleteJSON(i+1, 1)
		rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, delArrowSc, strings.NewReader(deleteJSON))
		require.NoError(tb, err)

		records := func(yield func(arrow.RecordBatch, error) bool) {
			yield(rec, nil)
		}

		tx = tbl.NewTransaction()
		deleteFiles, err := tx.WriteEqualityDeletes(ctx, []int{1}, records)
		require.NoError(tb, err)
		rec.Release()

		rd := tx.NewRowDelta(nil)
		for _, df := range deleteFiles {
			rd.AddDeletes(df)
		}
		require.NoError(tb, rd.Commit(ctx))

		tbl, err = tx.Commit(ctx)
		require.NoError(tb, err)
	}

	return tbl
}

func scanAndMeasure(tb testing.TB, ctx context.Context, tbl *table.Table) (time.Duration, int64) {
	tb.Helper()

	start := time.Now()
	_, itr, err := tbl.Scan().ToArrowRecords(ctx)
	require.NoError(tb, err)

	var total int64
	for rec, err := range itr {
		require.NoError(tb, err)
		total += rec.NumRows()
		rec.Release()
	}

	return time.Since(start), total
}

func generateRowsJSON(n int) string {
	var b strings.Builder
	b.WriteByte('[')
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id": %d, "data": "row-%06d"}`, i+1, i+1)
	}
	b.WriteByte(']')

	return b.String()
}

func generateDeleteJSON(startID, count int) string {
	var b strings.Builder
	b.WriteByte('[')
	for i := range count {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id": %d}`, startID+i)
	}
	b.WriteByte(']')

	return b.String()
}
