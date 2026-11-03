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
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func idSortOrder(t *testing.T, id int, transform iceberg.Transform) table.SortOrder {
	t.Helper()

	order, err := table.NewSortOrder(id, []table.SortField{{
		SourceIDs: []int{1},
		Transform: transform,
		Direction: table.SortASC,
		NullOrder: table.NullsFirst,
	}})
	require.NoError(t, err)

	return order
}

func newSortCompactionTestTable(t *testing.T, spec *iceberg.PartitionSpec) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, spec, idSortOrder(t, 1, iceberg.IdentityTransform{}), location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "sort_compaction_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta},
	)
}

// addSortTestFile adds one data file with the given ids, all in partition
// data=partition. The file is written unsorted.
func addSortTestFile(t *testing.T, tbl *table.Table, name, partition string, ids ...int64) *table.Table {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	rows := make([]string, len(ids))
	for i, id := range ids {
		rows[i] = fmt.Sprintf(`{"id": %d, "data": %q}`, id, partition)
	}
	dataPath := tbl.Location() + "/data/" + name + ".parquet"
	writeParquetFile(t, dataPath, arrowSc, "["+strings.Join(rows, ",")+"]")

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

func sortTestGroup(t *testing.T, tbl *table.Table) table.CompactionTaskGroup {
	t.Helper()

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	slices.SortFunc(tasks, func(a, b table.FileScanTask) int {
		return strings.Compare(a.File.FilePath(), b.File.FilePath())
	})
	group := table.CompactionTaskGroup{PartitionKey: "group", Tasks: tasks}
	for _, task := range tasks {
		group.TotalSizeBytes += task.File.FileSizeBytes()
	}

	return group
}

func readInt64Column(t *testing.T, path, column string) []int64 {
	t.Helper()

	pf, err := file.OpenParquetFile(strings.TrimPrefix(path, "file://"), false)
	require.NoError(t, err)
	defer pf.Close()

	rdr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := rdr.ReadTable(t.Context())
	require.NoError(t, err)
	defer tbl.Release()

	idx := tbl.Schema().FieldIndices(column)
	require.Len(t, idx, 1)
	var out []int64
	for _, chunk := range tbl.Column(idx[0]).Data().Chunks() {
		out = append(out, chunk.(*array.Int64).Int64Values()...)
	}

	return out
}

func TestExecuteCompactionGroup_SortOrder(t *testing.T) {
	tbl := newSortCompactionTestTable(t, iceberg.UnpartitionedSpec)
	tbl = addSortTestFile(t, tbl, "f0", "x", 7, 3, 11)
	tbl = addSortTestFile(t, tbl, "f1", "x", 2, 9, 5)
	tbl = addSortTestFile(t, tbl, "f2", "x", 12, 1, 8)
	tbl = addSortTestFile(t, tbl, "f3", "x", 4, 10, 6)
	group := sortTestGroup(t, tbl)

	// Five-row output batches span several source files, and a one-byte
	// target rolls a file after every batch.
	res, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionSortOrder(tbl.SortOrder()),
		table.WithCompactionArrowBatchSize(5),
		table.WithCompactionTargetFileSize(1))
	require.NoError(t, err)
	require.Len(t, res.NewDataFiles, 3)
	assert.Len(t, res.OldDataFiles, 4)

	var all []int64
	for _, df := range res.NewDataFiles {
		ids := readInt64Column(t, df.FilePath(), "id")
		assert.True(t, slices.IsSorted(ids), "file %s is not sorted: %v", df.FilePath(), ids)
		all = append(all, ids...)

		require.NotNil(t, df.SortOrderID())
		assert.Equal(t, tbl.SortOrder().OrderID(), *df.SortOrderID())
	}
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, all,
		"files must hold consecutive, non-overlapping ranges of the sorted group")
}

func TestExecuteCompactionGroup_SortOrderSplitsPartitions(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "data",
	})
	tbl := newSortCompactionTestTable(t, &spec)
	// Partitions interleave across the group's task order, as they do when
	// a group spans files written under an older spec.
	tbl = addSortTestFile(t, tbl, "f0", "a", 5, 1)
	tbl = addSortTestFile(t, tbl, "f1", "b", 4, 2)
	tbl = addSortTestFile(t, tbl, "f2", "a", 6, 3)
	group := sortTestGroup(t, tbl)

	res, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionSortOrder(tbl.SortOrder()))
	require.NoError(t, err)
	require.Len(t, res.NewDataFiles, 2)

	got := make(map[string][]int64)
	for _, df := range res.NewDataFiles {
		part, ok := df.Partition()[1000].(string)
		require.True(t, ok)
		got[part] = readInt64Column(t, df.FilePath(), "id")
	}
	assert.Equal(t, map[string][]int64{"a": {1, 3, 5, 6}, "b": {2, 4}}, got)
}

func TestExecuteCompactionGroup_SortOrderIDClaim(t *testing.T) {
	tbl := newSortCompactionTestTable(t, iceberg.UnpartitionedSpec)
	tbl = addSortTestFile(t, tbl, "f0", "x", 3, 1)
	tbl = addSortTestFile(t, tbl, "f1", "x", 2, 4)
	group := sortTestGroup(t, tbl)

	tests := []struct {
		name  string
		order table.SortOrder
	}{
		{"not a table sort order", idSortOrder(t, 7, iceberg.IdentityTransform{})},
		{"transform does not preserve order", idSortOrder(t, 1, iceberg.BucketTransform{NumBuckets: 4})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
				table.WithCompactionSortOrder(tt.order))
			require.NoError(t, err)
			require.Len(t, res.NewDataFiles, 1)
			assert.Equal(t, []int64{1, 2, 3, 4}, readInt64Column(t, res.NewDataFiles[0].FilePath(), "id"))
			assert.Nil(t, res.NewDataFiles[0].SortOrderID())
		})
	}
}

func TestExecuteCompactionGroup_UnsortedOrderRejected(t *testing.T) {
	tbl := newSortCompactionTestTable(t, iceberg.UnpartitionedSpec)
	tbl = addSortTestFile(t, tbl, "f0", "x", 1)

	_, err := table.ExecuteCompactionGroup(t.Context(), tbl, sortTestGroup(t, tbl),
		table.WithCompactionSortOrder(table.UnsortedSortOrder))
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}

func TestRewriteDataFiles_SortOrder(t *testing.T) {
	tbl := newSortCompactionTestTable(t, iceberg.UnpartitionedSpec)
	tbl = addSortTestFile(t, tbl, "f0", "x", 6, 2)
	tbl = addSortTestFile(t, tbl, "f1", "x", 5, 1)
	tbl = addSortTestFile(t, tbl, "f2", "x", 4, 3)

	res, err := tbl.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{sortTestGroup(t, tbl)},
		table.RewriteDataFilesOptions{
			GroupOptions: []table.CompactionGroupOption{table.WithCompactionSortOrder(tbl.SortOrder())},
		})
	require.NoError(t, err)
	require.NotNil(t, res.Table)
	assert.Equal(t, 3, res.RemovedDataFiles)
	assert.Equal(t, 1, res.AddedDataFiles)
	assertRowCount(t, res.Table, 6)

	tasks, err := res.Table.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].File.SortOrderID())
	assert.Equal(t, tbl.SortOrder().OrderID(), *tasks[0].File.SortOrderID())
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, readInt64Column(t, tasks[0].File.FilePath(), "id"))
}

// TestExecuteCompactionGroup_MergesSortedFiles compacts files that already
// record the sort order, which takes the streaming merge path, with one
// position-delete file shared by both inputs.
func TestExecuteCompactionGroup_MergesSortedFiles(t *testing.T) {
	tbl := newSortCompactionTestTable(t, iceberg.UnpartitionedSpec)
	tbl = addSortTestFile(t, tbl, "f0", "x", 7, 3, 11)
	tbl = addSortTestFile(t, tbl, "f1", "x", 2, 9, 5)
	tbl = addSortTestFile(t, tbl, "f2", "x", 12, 1, 8)
	tbl = addSortTestFile(t, tbl, "f3", "x", 4, 10, 6)
	unsorted := sortTestGroup(t, tbl)

	res, err := tbl.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{
		{PartitionKey: "a", Tasks: unsorted.Tasks[:2]},
		{PartitionKey: "b", Tasks: unsorted.Tasks[2:]},
	}, table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{table.WithCompactionSortOrder(tbl.SortOrder())},
	})
	require.NoError(t, err)
	tbl = res.Table

	group := sortTestGroup(t, tbl)
	require.Len(t, group.Tasks, 2)
	byFirstID := make(map[int64]table.FileScanTask)
	for _, task := range group.Tasks {
		require.NotNil(t, task.File.SortOrderID())
		require.Equal(t, tbl.SortOrder().OrderID(), *task.File.SortOrderID())
		byFirstID[readInt64Column(t, task.File.FilePath(), "id")[0]] = task
	}
	// Files hold 2,3,5,7,9,11 and 1,4,6,8,10,12. Delete 2 and 11 from the
	// first and 4 from the second.
	first, second := byFirstID[2].File.FilePath(), byFirstID[1].File.FilePath()
	delPath := tbl.Location() + "/data/pos-del.parquet"
	writeParquetFile(t, delPath, table.PositionalDeleteArrowSchema, fmt.Sprintf(
		`[{"file_path": %q, "pos": 0}, {"file_path": %q, "pos": 5}, {"file_path": %q, "pos": 1}]`,
		first, first, second))
	bldr, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		delPath, iceberg.ParquetFile, nil, nil, nil, 3, 1)
	require.NoError(t, err)
	posDel := bldr.Build()
	for i := range group.Tasks {
		group.Tasks[i].DeleteFiles = []iceberg.DataFile{posDel}
	}

	out, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionSortOrder(tbl.SortOrder()),
		table.WithCompactionArrowBatchSize(4),
		table.WithCompactionTargetFileSize(1))
	require.NoError(t, err)
	require.Len(t, out.NewDataFiles, 3)

	var all []int64
	for _, df := range out.NewDataFiles {
		ids := readInt64Column(t, df.FilePath(), "id")
		assert.True(t, slices.IsSorted(ids), "file %s is not sorted: %v", df.FilePath(), ids)
		all = append(all, ids...)
		require.NotNil(t, df.SortOrderID())
		assert.Equal(t, tbl.SortOrder().OrderID(), *df.SortOrderID())
	}
	assert.Equal(t, []int64{1, 3, 5, 6, 7, 8, 9, 10, 12}, all)
}
