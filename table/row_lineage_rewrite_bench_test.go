// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

import (
	"context"
	"io/fs"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

type rowLineageRewriteBenchmarkFixture struct {
	tbl          *table.Table
	catalog      *concurrentTestCatalog
	fs           *iceio.MemFS
	location     string
	baseMetadata table.Metadata
	initialFiles map[string]struct{}
}

func BenchmarkRowLineageRewriteRowGroupPruning(b *testing.B) {
	const numRows = 1_000_000

	for _, tc := range []struct {
		name         string
		deleteFilter iceberg.BooleanExpression
	}{
		{
			name:         "keep-1-of-100",
			deleteFilter: iceberg.LessThan(iceberg.Reference("id"), int64(numRows-10_000)),
		},
		{
			name:         "keep-10-of-100",
			deleteFilter: iceberg.LessThan(iceberg.Reference("id"), int64(numRows-100_000)),
		},
		{
			name:         "keep-100-of-100",
			deleteFilter: iceberg.EqualTo(iceberg.Reference("id"), int64(0)),
		},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			fixture := newRowLineageRewriteBenchmarkFixture(b, numRows)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := fixture.tbl.Delete(ctx, tc.deleteFilter, nil, table.WithDeleteConcurrency(1)); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				fixture.reset(b)
				b.StartTimer()
			}
		})
	}
}

func newRowLineageRewriteBenchmarkFixture(b *testing.B, numRows int) *rowLineageRewriteBenchmarkFixture {
	b.Helper()

	fsys := iceio.NewMemFS()
	location := "mem://row-lineage-rewrite-benchmark"
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:   "3",
			table.ParquetRowGroupLimitKey: "10000",
		})
	require.NoError(b, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return fsys, nil }
	catalog := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "row_lineage_rewrite_benchmark"}, meta, metaLoc, fsF, catalog)

	data := newRowLineageRewriteBenchmarkData(b, numRows)
	defer data.Release()
	tbl, err = tbl.Append(context.Background(), array.NewTableReader(data, -1), nil)
	require.NoError(b, err)

	return &rowLineageRewriteBenchmarkFixture{
		tbl:          tbl,
		catalog:      catalog,
		fs:           fsys,
		location:     location,
		baseMetadata: tbl.Metadata(),
		initialFiles: rowLineageRewriteBenchmarkFiles(b, fsys, location),
	}
}

func newRowLineageRewriteBenchmarkData(b *testing.B, numRows int) arrow.Table {
	b.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	idBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	defer idBuilder.Release()
	dataBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	defer dataBuilder.Release()
	idBuilder.Reserve(numRows)
	dataBuilder.Reserve(numRows)
	for i := range numRows {
		idBuilder.Append(int64(i))
		dataBuilder.Append("payload")
	}

	id := idBuilder.NewArray()
	defer id.Release()
	data := dataBuilder.NewArray()
	defer data.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{id, data}, int64(numRows))
	defer record.Release()

	return array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
}

func (f *rowLineageRewriteBenchmarkFixture) reset(b *testing.B) {
	b.Helper()
	for path := range rowLineageRewriteBenchmarkFiles(b, f.fs, f.location) {
		if _, ok := f.initialFiles[path]; ok {
			continue
		}
		require.NoError(b, f.fs.Remove(path))
	}
	f.catalog.metadata = f.baseMetadata
}

func rowLineageRewriteBenchmarkFiles(b *testing.B, fsys *iceio.MemFS, location string) map[string]struct{} {
	b.Helper()
	files := make(map[string]struct{})
	require.NoError(b, fsys.WalkDir(location, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			files[path] = struct{}{}
		}

		return nil
	}))

	return files
}
