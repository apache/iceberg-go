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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestScanImportedSignedZeroBounds(t *testing.T) {
	ctx := t.Context()
	cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Float64})
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "zeros"), sc)
	require.NoError(t, err)
	arrowSchema, err := table.SchemaToArrowSchema(sc, nil, false, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Float64Builder).Append(math.Copysign(0, -1))
	rec := builder.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()
	tbl, err = tbl.Append(ctx, rdr, nil)
	require.NoError(t, err)
	raw, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer raw.Release()
	// Attach exact valid signed-zero bounds, like an external writer can produce.
	filesBefore, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	originalFile := filesBefore[0].File
	bound, err := iceberg.Float64Literal(math.Copysign(0, -1)).MarshalBinary()
	require.NoError(t, err)
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, originalFile.FilePath(), iceberg.ParquetFile, nil, nil, nil, 1, originalFile.FileSizeBytes())
	require.NoError(t, err)
	df := b.ValueCounts(map[int]int64{1: 1}).NullValueCounts(map[int]int64{1: 0}).NaNValueCounts(map[int]int64{1: 0}).LowerBoundValues(map[int][]byte{1: bound}).UpperBoundValues(map[int][]byte{1: bound}).Build()
	imported, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "imported_zeros"), sc)
	require.NoError(t, err)
	tx := imported.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(df).Commit(ctx))
	imported, err = tx.Commit(ctx)
	require.NoError(t, err)
	tbl = imported
	filter := iceberg.EqualTo(iceberg.Reference("value"), float64(0))
	files, err := tbl.Scan(table.WithRowFilter(filter)).PlanFiles(ctx)
	require.NoError(t, err)
	result, err := tbl.Scan(table.WithRowFilter(filter)).ToArrowTable(ctx)
	require.NoError(t, err)
	defer result.Release()

	require.EqualValues(t, 1, raw.NumRows())
	require.Len(t, files, 1)
	require.EqualValues(t, 1, result.NumRows())
}
