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
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type WriteRecordsSortTestSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator
	ctx context.Context
}

func TestWriteRecordsSort(t *testing.T) {
	suite.Run(t, new(WriteRecordsSortTestSuite))
}

func (s *WriteRecordsSortTestSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.ctx = compute.WithAllocator(context.Background(), s.mem)
}

func (s *WriteRecordsSortTestSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

// TestPartitionedTableSortedPerFile exercises the hash-fanout write path:
// a partition-by-category table with a sort order on id ASC must produce
// one data file per partition, each with rows sorted by id.
func (s *WriteRecordsSortTestSuite) TestPartitionedTableSortedPerFile() {
	loc := filepath.ToSlash(s.T().TempDir())

	iceSch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "category", Transform: iceberg.IdentityTransform{}},
	)
	sortOrder, err := table.NewSortOrder(1, []table.SortField{
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsLast},
	})
	s.Require().NoError(err)

	meta, err := table.NewMetadata(iceSch, &spec, sortOrder, loc, iceberg.Properties{
		table.PropertyFormatVersion: "2",
	})
	s.Require().NoError(err)

	tbl := table.New(
		table.Identifier{"db", "partitioned_sorted"},
		meta,
		filepath.Join(loc, "metadata", "v1.metadata.json"),
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		nil,
	)

	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	// Mixed partitions, rows intentionally unsorted within and across.
	type row struct {
		id  int32
		cat string
	}
	rows := []row{
		{5, "a"},
		{2, "b"},
		{1, "a"},
		{7, "b"},
		{3, "a"},
		{4, "b"},
		{6, "a"},
		{0, "b"},
	}
	bldr := array.NewRecordBuilder(s.mem, arrSchema)
	for _, r := range rows {
		bldr.Field(0).(*array.Int32Builder).Append(r.id)
		bldr.Field(1).(*array.StringBuilder).Append(r.cat)
	}
	rec := bldr.NewRecordBatch()
	bldr.Release()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}

	var dataFiles []iceberg.DataFile
	for df, err := range table.WriteRecords(s.ctx, tbl, arrSchema, records) {
		s.Require().NoError(err)
		dataFiles = append(dataFiles, df)
	}
	s.Require().Len(dataFiles, 2, "expected one file per partition")

	// Group expected ids by category for cross-checking.
	expectedByCat := map[string][]int32{}
	for _, r := range rows {
		expectedByCat[r.cat] = append(expectedByCat[r.cat], r.id)
	}
	for cat := range expectedByCat {
		sort.Slice(expectedByCat[cat], func(i, j int) bool {
			return expectedByCat[cat][i] < expectedByCat[cat][j]
		})
	}

	for _, df := range dataFiles {
		// Per-batch sorting only, so no per-file sort order claim.
		s.Nil(df.SortOrderID(), "data file must not claim a sort order id")

		ids, cats := readIDsAndCategories(s.T(), df.FilePath())
		s.Require().NotEmpty(cats, "file should have at least one row")

		// All rows in a partitioned file share the partition value.
		cat := cats[0]
		for _, c := range cats {
			s.Equal(cat, c, "partitioned file should contain a single category")
		}

		s.Equal(expectedByCat[cat], ids, "rows must be sorted by id ASC within partition %q", cat)
	}
}

// TestUnpartitionedTableMultipleBatchesEachSorted documents the per-batch
// sort guarantee: each Add() call is locally sorted, but rows aren't merged
// across batches. Two unsorted batches in arrival order produce a file with
// two locally-sorted runs concatenated. This matches PyIceberg's behavior.
func (s *WriteRecordsSortTestSuite) TestUnpartitionedTableMultipleBatchesEachSorted() {
	loc := filepath.ToSlash(s.T().TempDir())

	iceSch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)
	spec := iceberg.NewPartitionSpec()
	sortOrder, err := table.NewSortOrder(1, []table.SortField{
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsLast},
	})
	s.Require().NoError(err)
	meta, err := table.NewMetadata(iceSch, &spec, sortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)

	tbl := table.New(
		table.Identifier{"db", "unpartitioned_sorted"},
		meta,
		filepath.Join(loc, "metadata", "v1.metadata.json"),
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		nil,
	)

	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	makeBatch := func(vals []int32) arrow.RecordBatch {
		bldr := array.NewRecordBuilder(s.mem, arrSchema)
		for _, v := range vals {
			bldr.Field(0).(*array.Int32Builder).Append(v)
		}
		rec := bldr.NewRecordBatch()
		bldr.Release()

		return rec
	}

	// Interleaved ranges, so per-batch and global sort give different results.
	batch1 := makeBatch([]int32{5, 3, 1})
	batch2 := makeBatch([]int32{4, 2, 0})
	records := func(yield func(arrow.RecordBatch, error) bool) {
		if !yield(batch1, nil) {
			return
		}
		yield(batch2, nil)
	}

	var dataFiles []iceberg.DataFile
	for df, err := range table.WriteRecords(s.ctx, tbl, arrSchema, records) {
		s.Require().NoError(err)
		dataFiles = append(dataFiles, df)
	}
	s.Require().Len(dataFiles, 1)

	ids, _ := readIDsAndCategories(s.T(), dataFiles[0].FilePath())
	// Runs concatenated in arrival order; a global sort would give [0..5].
	s.Equal([]int32{1, 3, 5, 0, 2, 4}, ids,
		"batches are sorted independently and emitted in arrival order, not merged")
}

func readIDsAndCategories(t *testing.T, path string) ([]int32, []string) {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := arrowRdr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	var ids []int32
	var cats []string

	idCol := tbl.Schema().FieldIndices("id")
	if len(idCol) > 0 {
		for _, chunk := range tbl.Column(idCol[0]).Data().Chunks() {
			arr := chunk.(*array.Int32)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					ids = append(ids, arr.Value(i))
				}
			}
		}
	}
	catCol := tbl.Schema().FieldIndices("category")
	if len(catCol) > 0 {
		for _, chunk := range tbl.Column(catCol[0]).Data().Chunks() {
			arr := chunk.(*array.String)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					cats = append(cats, arr.Value(i))
				}
			}
		}
	}

	return ids, cats
}
