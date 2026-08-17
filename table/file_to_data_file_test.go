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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestFileToDataFile(t *testing.T) {
	ctx := context.Background()
	fs := iceio.LocalFS{}
	path := t.TempDir() + "/data.parquet"

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	bldr := array.NewInt32Builder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.AppendValues([]int32{3, 7}, nil)
	col := bldr.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{col}, 2)
	defer rec.Release()
	arrTbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer arrTbl.Release()

	fo, err := fs.Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(), nil, pqarrow.DefaultWriterProps()))

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	df, err := table.FileToDataFile(ctx, fs, path, schema, *iceberg.UnpartitionedSpec, 0, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, df.Count())
	require.Positive(t, df.FileSizeBytes())
	require.NotEmpty(t, df.LowerBoundValues()[1])
	require.NotEmpty(t, df.UpperBoundValues()[1])
	require.EqualValues(t, 2, df.ValueCounts()[1])
	require.EqualValues(t, 0, df.NullValueCounts()[1])
	require.Nil(t, df.SortOrderID(), "sortOrderID 0 must leave sort_order_id unset")

	const claimedOrder = 3
	sorted, err := table.FileToDataFile(ctx, fs, path, schema, *iceberg.UnpartitionedSpec, claimedOrder, nil)
	require.NoError(t, err)
	require.NotNil(t, sorted.SortOrderID())
	require.Equal(t, claimedOrder, *sorted.SortOrderID())

	_, err = table.FileToDataFile(ctx, fs, path+".missing", schema, *iceberg.UnpartitionedSpec, 0, nil)
	require.Error(t, err)
}
