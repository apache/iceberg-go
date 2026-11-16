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
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestParquetReadPageLimitsScanOverrideAndDelete(t *testing.T) {
	tbl := newMergeOnReadTestTable(t)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{
		table.ParquetCompressionKey: "uncompressed", table.ParquetPageSizeBytesKey: "134217728",
		"parquet.enable.dictionary": "false",
	}))
	tbl, err := txn.Commit(t.Context())
	require.NoError(t, err)
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	payload := strings.Repeat("x", 70*1024*1024)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{payload, "small"}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	defer reader.Release()
	tbl, err = tbl.Append(t.Context(), reader, nil)
	require.NoError(t, err)

	_, records, err := tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)
	var readErr error
	for record, err := range records {
		if record != nil {
			record.Release()
		}
		readErr = errors.Join(readErr, err)
	}
	require.ErrorContains(t, readErr, "compressed page size")

	_, records, err = tbl.Scan(table.WithOptions(iceberg.Properties{
		table.ParquetMaxCompressedPageSizeBytesKey: "100663296",
	})).ToArrowRecords(t.Context())
	require.NoError(t, err)
	var rows int64
	for record, err := range records {
		require.NoError(t, err)
		rows += record.NumRows()
		if record.NumRows() > 0 && record.Column(0).(*array.Int64).Value(0) == 1 {
			require.Equal(t, payload, record.Column(1).(*array.String).Value(0))
		}
		record.Release()
	}
	require.Equal(t, int64(2), rows)
	require.NotContains(t, tbl.Metadata().Properties(), table.ParquetMaxCompressedPageSizeBytesKey)

	txn = tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.ParquetMaxCompressedPageSizeBytesKey: "100663296"}))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)
	tbl, err = tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	_, records, err = tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)
	var ids []int64
	for record, err := range records {
		require.NoError(t, err)
		ids = append(ids, record.Column(0).(*array.Int64).Int64Values()...)
		record.Release()
	}
	require.Equal(t, []int64{1}, ids)

	txn = tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.ParquetMaxCompressedPageSizeBytesKey: "1"}))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)
	positions, err := tbl.Inspect().PositionDeletes(t.Context())
	require.NoError(t, err)
	for positions.Next() {
	}
	require.ErrorContains(t, positions.Err(), "compressed page size")
	positions.Release()
	_, err = tbl.NewTransaction().RewritePositionDeleteFiles(t.Context(), table.PositionDeleteRewriteOptions{MinInputFiles: 1})
	require.ErrorContains(t, err, "compressed page size")
	txn = tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.ParquetMaxCompressedPageSizeBytesKey: "100663296"}))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	_, _, err = tbl.Scan(table.WithOptions(iceberg.Properties{table.ParquetMaxCompressedPageSizeBytesKey: "0"})).ToArrowRecords(t.Context())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.Equal(t, "100663296", tbl.Metadata().Properties()[table.ParquetMaxCompressedPageSizeBytesKey])
	txn = tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.ParquetMaxCompressedPageSizeBytesKey: "0"}))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)
	_, err = tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestParquetPageLimitValidationBeforeDeleteConversion(t *testing.T) {
	tbl := newMergeOnReadTestTableVersion(t, "3")
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.ParquetMaxUncompressedPageSizeBytesKey: "0"}))
	tbl, err := txn.Commit(t.Context())
	require.NoError(t, err)
	_, err = tbl.NewTransaction().ConvertEqualityDeletesToDeletionVectors(t.Context())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = tbl.Inspect().PositionDeletes(t.Context())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}
