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
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pruningFilterTestReader struct {
	schema *arrow.Schema
	tester *tblutils.ParquetRowGroupTester
}

func (r *pruningFilterTestReader) Close() error { return nil }

func (r *pruningFilterTestReader) Metadata() tblutils.Metadata { return nil }

func (r *pruningFilterTestReader) SourceFileSize() int64 { return 1 }

func (r *pruningFilterTestReader) Schema() (*arrow.Schema, error) {
	return r.schema, nil
}

func (r *pruningFilterTestReader) PrunedSchema(map[int]struct{}, iceberg.NameMapping) (*arrow.Schema, []int, error) {
	return r.schema, []int{0}, nil
}

func (r *pruningFilterTestReader) GetRecords(_ context.Context, _ []int, tester any) (array.RecordReader, error) {
	r.tester = tester.(*tblutils.ParquetRowGroupTester)

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	values := builder.NewArray()
	builder.Release()

	record := array.NewRecordBatch(r.schema, []arrow.Array{values}, 1)
	values.Release()
	reader, err := array.NewRecordReader(r.schema, []arrow.RecordBatch{record})
	record.Release()

	return reader, err
}

func (r *pruningFilterTestReader) ReadTable(context.Context) (arrow.Table, error) {
	return nil, nil
}

func TestProcessRecordsUsesRowGroupFilterForPruning(t *testing.T) {
	ctx := context.Background()
	fileSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	rowGroupFilter, err := iceberg.BindExpr(fileSchema, iceberg.NewNot(
		iceberg.NotEqualTo(iceberg.Reference("id"), int64(1))), true)
	require.NoError(t, err)

	dataFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		"file:///test.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)

	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	reader := &pruningFilterTestReader{schema: arrowSchema}
	out := make(chan enumeratedRecord, 1)
	err = (&arrowScan{rowGroupFilter: rowGroupFilter, projectedSchema: fileSchema}).processRecords(
		ctx,
		tblutils.Enumerated[FileScanTask]{Value: FileScanTask{File: dataFileBuilder.Build()}},
		fileSchema, iceberg.AlwaysTrue{}, reader, []int{0}, nil, nil, out)
	require.NoError(t, err)

	require.NotNil(t, reader.tester)
	require.Len(t, reader.tester.BloomPreds, 1)
	assert.Equal(t, 1, reader.tester.BloomPreds[0].FieldID)
	assert.Len(t, reader.tester.BloomPreds[0].PhysBytes, 1)

	result := <-out
	result.Record.Value.Release()
}

func TestBindTaskFilterValidatesBoundSchema(t *testing.T) {
	int64Schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	bound, err := iceberg.BindExpr(int64Schema,
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)), true)
	require.NoError(t, err)

	t.Run("matching schema is accepted", func(t *testing.T) {
		got, err := bindTaskFilter(int64Schema, bound, true)
		require.NoError(t, err)
		require.Same(t, bound, got)
	})

	t.Run("missing field is rejected", func(t *testing.T) {
		otherSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 2, Name: "other", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		)
		_, err := bindTaskFilter(otherSchema, bound, true)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.ErrorContains(t, err, "field ID 1")
	})

	t.Run("type mismatch is rejected", func(t *testing.T) {
		otherSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		)
		_, err := bindTaskFilter(otherSchema, bound, true)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.ErrorContains(t, err, "type")
	})

	t.Run("accessor path mismatch is rejected", func(t *testing.T) {
		field := iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}
		originalSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 10, Name: "payload", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{field}}},
		)
		boundNested, err := iceberg.BindExpr(originalSchema,
			iceberg.EqualTo(iceberg.Reference("payload.id"), int64(1)), true)
		require.NoError(t, err)
		otherSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 11, Name: "other", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 10, Name: "payload", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{field}}},
		)

		_, err = bindTaskFilter(otherSchema, boundNested, true)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.ErrorContains(t, err, "accessor path")
	})
}
