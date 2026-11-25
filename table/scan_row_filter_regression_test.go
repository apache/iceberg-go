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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func newTestScanTable(t *testing.T, name string, schema *iceberg.Schema) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", name}, meta, metaLoc, fsF, cat)
}

func scanIDsWithCheckedAlloc(t *testing.T, tbl *table.Table, filter iceberg.BooleanExpression) []int64 {
	t.Helper()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	ctx := compute.WithAllocator(t.Context(), mem)
	out, err := tbl.Scan(table.WithRowFilter(filter)).ToArrowTable(ctx)
	require.NoError(t, err)
	defer out.Release()

	ids := []int64{}
	for _, chunk := range out.Column(0).Data().Chunks() {
		ids = append(ids, chunk.(*array.Int64).Int64Values()...)
	}

	return ids
}

func TestScanRowFilterStartsWith(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	t.Run("StartsWith", func(t *testing.T) {
		tbl := newTestScanTable(t, "starts_with", schema)

		bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer bldr.Release()

		idBldr := bldr.Field(0).(*array.Int64Builder)
		nameBldr := bldr.Field(1).(*array.StringBuilder)
		idBldr.AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
		nameBldr.Append("alpha")
		nameBldr.Append("alpine")
		nameBldr.Append("beta")
		nameBldr.Append("alphabet")
		nameBldr.Append("")
		nameBldr.AppendNull()

		rec := bldr.NewRecordBatch()
		defer rec.Release()

		rdr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
		require.NoError(t, err)
		defer rdr.Release()

		tbl, err = tbl.Append(t.Context(), rdr, nil)
		require.NoError(t, err)

		name := iceberg.Reference("name")
		require.Equal(t, []int64{1, 2, 4}, scanIDsWithCheckedAlloc(t, tbl, iceberg.StartsWith(name, "alp")))
		require.Equal(t, []int64{}, scanIDsWithCheckedAlloc(t, tbl, iceberg.StartsWith(name, "zzz")))
	})

	t.Run("NotStartsWith", func(t *testing.T) {
		tbl := newTestScanTable(t, "not_starts_with", schema)

		bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer bldr.Release()

		idBldr := bldr.Field(0).(*array.Int64Builder)
		nameBldr := bldr.Field(1).(*array.StringBuilder)
		idBldr.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		nameBldr.AppendValues([]string{"alpha", "alpine", "beta", "alphabet", ""}, nil)

		rec := bldr.NewRecordBatch()
		defer rec.Release()

		rdr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
		require.NoError(t, err)
		defer rdr.Release()

		tbl, err = tbl.Append(t.Context(), rdr, nil)
		require.NoError(t, err)

		name := iceberg.Reference("name")
		require.Equal(t, []int64{3, 5}, scanIDsWithCheckedAlloc(t, tbl, iceberg.NotStartsWith(name, "alp")))
		require.Equal(t, []int64{1, 2, 3, 4, 5}, scanIDsWithCheckedAlloc(t, tbl, iceberg.NotStartsWith(name, "zzz")))
	})
}

func TestScanRowFilterEmbeddedDictionary(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "cat", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	tbl := newTestScanTable(t, "embedded_dict", schema)

	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "1"}),
		},
		{
			Name:     "cat",
			Type:     dictType,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{table.ArrowParquetFieldIDKey: "2"}),
		},
	}, nil)

	writeMem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer writeMem.AssertSize(t, 0)

	bldr := array.NewRecordBuilder(writeMem, arrowSchema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	catBldr := bldr.Field(1).(*array.BinaryDictionaryBuilder)
	require.NoError(t, catBldr.AppendString("red"))
	require.NoError(t, catBldr.AppendString("blue"))
	catBldr.AppendNull()
	require.NoError(t, catBldr.AppendString("red"))
	require.NoError(t, catBldr.AppendString("green"))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	dataDir := filepath.Join(tbl.Location(), "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	parquetPath := filepath.Join(dataDir, "dict.parquet")

	f, err := os.Create(parquetPath)
	require.NoError(t, err)
	writer, err := pqarrow.NewFileWriter(
		arrowSchema,
		f,
		parquet.NewWriterProperties(parquet.WithAllocator(writeMem)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(writeMem), pqarrow.WithStoreSchema()),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{filepath.ToSlash(parquetPath)}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	cat := iceberg.Reference("cat")
	require.Equal(t, []int64{1, 4}, scanIDsWithCheckedAlloc(t, tbl, iceberg.EqualTo(cat, "red")))
	require.Equal(t, []int64{2, 5}, scanIDsWithCheckedAlloc(t, tbl, iceberg.IsIn(cat, "blue", "green")))
	require.Equal(t, []int64{3}, scanIDsWithCheckedAlloc(t, tbl, iceberg.IsNull(cat)))
	require.Equal(t, []int64{1, 2, 4, 5}, scanIDsWithCheckedAlloc(t, tbl, iceberg.NotNull(cat)))
}

func TestScanRowFilterDecimalUnaryAndIn(t *testing.T) {
	decIcebergType, err := iceberg.DecimalTypeOf(10, 2)
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "amount", Type: decIcebergType, Required: false},
	)
	tbl := newTestScanTable(t, "decimal_filter", schema)

	decType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "amount", Type: decType, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	decBldr := bldr.Field(1).(*array.Decimal128Builder)
	decBldr.Append(decimal128.FromI64(1050))
	decBldr.AppendNull()
	decBldr.Append(decimal128.FromI64(2500))
	decBldr.Append(decimal128.FromI64(-525))
	decBldr.Append(decimal128.FromI64(1050))

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	rdr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()

	tbl, err = tbl.Append(t.Context(), rdr, nil)
	require.NoError(t, err)

	amount := iceberg.Reference("amount")
	require.Equal(t, []int64{2}, scanIDsWithCheckedAlloc(t, tbl, iceberg.IsNull(amount)))
	require.Equal(t, []int64{1, 3, 4, 5}, scanIDsWithCheckedAlloc(t, tbl, iceberg.NotNull(amount)))
	require.Equal(t, []int64{1, 4, 5}, scanIDsWithCheckedAlloc(t, tbl, iceberg.IsIn(
		amount,
		iceberg.Decimal{Val: decimal128.FromI64(1050), Scale: 2},
		iceberg.Decimal{Val: decimal128.FromI64(-525), Scale: 2},
	)))
}
