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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestChangelogReadRejectsNilScan(t *testing.T) {
	var scan *IncrementalChangelogScan
	_, _, err := scan.Read(context.Background(), nil)
	require.ErrorIs(t, err, ErrInvalidOperation)
}

func TestChangelogReadAcceptsDeletedRowTasks(t *testing.T) {
	data := changelogTestDataFile(t, "data/f2.parquet", iceberg.EntryContentData, iceberg.ParquetFile)
	added := changelogTestDataFile(t, "deletes/d2.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile)
	task, err := NewDeletedRowsScanTask(data, []iceberg.DataFile{added}, nil, 1, 44)
	require.NoError(t, err)

	require.NoError(t, readableChangelogTask(task))
}

func TestStampChangelogBatchCutsToTheRemainingBudget(t *testing.T) {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues([]int64{1, 2, 3}, nil)
	values := builder.NewArray()
	defer values.Release()
	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	rec := array.NewRecordBatch(dataSchema, []arrow.Array{values}, int64(values.Len()))
	defer rec.Release()

	out := dataSchema
	var err error
	for _, field := range changelogArrowFields(false) {
		out, err = out.AddField(out.NumFields(), field)
		require.NoError(t, err)
	}
	file := changelogTestDataFile(t, "data.parquet", iceberg.EntryContentData, iceberg.ParquetFile)
	task, err := NewAddedRowsScanTask(file, nil, 0, 7)
	require.NoError(t, err)

	stamped, rows, err := stampChangelogBatch(context.Background(), out, false, rec, task, 2, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), rows)
	require.Equal(t, int64(2), stamped.NumRows())
	t.Cleanup(stamped.Release)
	require.Equal(t, []int64{1, 2}, stamped.Column(0).(*array.Int64).Int64Values())
}

func TestChangelogArrowSchemaRejectsReservedName(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: ChangelogChangeTypeColumn, Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://changelog-reserved", nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "changelog"}, meta, "metadata.json", nil, nil)

	_, _, err = changelogArrowSchema(tbl.NewIncrementalChangelogScan().scan)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	require.ErrorContains(t, err, ChangelogChangeTypeColumn)
}
