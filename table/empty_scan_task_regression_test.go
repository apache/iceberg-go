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

// Regression test for: filtered scans silently returning zero rows when an
// earlier-ordered scan task produces no record batches.
//
// createIterator sequences per-task records and only releases task N+1's
// records after task N emitted a record with Last=true. A task whose file is
// statically dropped (dropFile) emits an empty sentinel batch, but a task
// whose parquet reader yields no batches — every row group pruned by
// row-group statistics — emitted nothing. The first such task stalled the
// sequenced channel and, on close, the iterator's deferred drain released
// every queued record from the tasks behind it without yielding them: the
// scan reported success with rows silently missing.
//
// The trigger needs a data file whose manifest entry carries no column
// bounds (so file-level pruning keeps it) but whose parquet row-group stats
// eliminate all row groups under the filter. DataFiles registered via
// NewDataFileBuilder/AddDataFiles — the pattern external committers use —
// have exactly that shape.

import (
	"context"
	"iter"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestScanSurvivesFullyPrunedTask scans a two-file table where the filter
// prunes every row group of the first file. The second file's rows must
// still be returned.
//
// Before fix: 0 rows (first task emits nothing, second task's records are
// discarded on close). After fix: the second file's rows.
func TestScanSurvivesFullyPrunedTask(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx = compute.WithAllocator(ctx, mem)

	// The field-id metadata matters: row-group statistics are keyed by
	// parquet field id, and without it the stats never bind to the filter
	// column, no row group is pruned, and the empty-task condition never
	// arises.
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"}),
		},
	}, nil)

	writeParquet := func(path string, vals []int32) int64 {
		bldr := array.NewInt32Builder(memory.DefaultAllocator)
		defer bldr.Release()

		bldr.AppendValues(vals, nil)
		col := bldr.NewArray()
		defer col.Release()

		rec := array.NewRecordBatch(arrowSchema, []arrow.Array{col}, int64(len(vals)))
		defer rec.Release()

		arrTbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
		defer arrTbl.Release()

		fo, err := fs.Create(path)
		require.NoError(t, err)
		require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(),
			nil, pqarrow.DefaultWriterProps()))

		st, err := fs.Open(path)
		require.NoError(t, err)
		defer st.Close()
		info, err := st.Stat()
		require.NoError(t, err)

		return info.Size()
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, nil)
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "test_empty_scan_task"},
		meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil },
		cat,
	)

	// File A: only row group is fully pruned by the id >= 1000 filter.
	// File B: matches the filter.
	pathA := dir + "/data-a.parquet"
	pathB := dir + "/data-b.parquet"
	sizeA := writeParquet(pathA, []int32{1, 2, 3})
	sizeB := writeParquet(pathB, []int32{1000, 1001, 1002})

	buildDataFile := func(path string, size int64) iceberg.DataFile {
		bldr, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
			iceberg.EntryContentData, path, iceberg.ParquetFile,
			nil, nil, nil, 3, size)
		require.NoError(t, err)

		return bldr.Build()
	}

	// One commit, one manifest: task order over {A, B} is deterministic.
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(ctx,
		[]iceberg.DataFile{buildDataFile(pathA, sizeA), buildDataFile(pathB, sizeB)}, nil))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	scan := tbl.Scan(table.WithRowFilter(
		iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(1000))))

	// Both files must be planned (their manifest entries carry no column
	// bounds, so file-level pruning cannot eliminate A), with A ordered
	// first — the arrangement that leaves an empty task ahead of B's rows.
	tasks, err := scan.PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, pathA, tasks[0].File.FilePath())
	require.Equal(t, pathB, tasks[1].File.FilePath())

	_, itr, err := scan.ToArrowRecords(ctx)
	require.NoError(t, err)

	next, stop := iter.Pull2(itr)
	defer stop()

	var got []int32
	for {
		rec, err, valid := next()
		if !valid {
			break
		}
		require.NoError(t, err)
		col := rec.Column(0).(*array.Int32)
		for i := 0; i < col.Len(); i++ {
			got = append(got, col.Value(i))
		}
		rec.Release()
	}

	require.Equal(t, []int32{1000, 1001, 1002}, got,
		"rows queued behind a fully row-group-pruned task must not be dropped")
}
