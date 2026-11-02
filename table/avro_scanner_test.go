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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
	avro "github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

func avroScanFile(t *testing.T, fs *iceio.MemFS, rows ...any) iceberg.DataFile {
	t.Helper()
	const path = "mem://avro/data.avro"
	sc, err := avro.Parse(`{"type":"record","name":"row","fields":[{"name":"old_id","type":"long","field-id":1},{"name":"category","type":"string","field-id":2}]}`)
	require.NoError(t, err)
	var buf bytes.Buffer
	writer, err := ocf.NewWriter(&buf, sc)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, writer.Encode(row))
	}
	require.NoError(t, writer.Close())
	require.NoError(t, fs.WriteFile(t.Context(), path, buf.Bytes()))
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.AvroFile, nil, nil, nil, int64(len(rows)), int64(buf.Len()))
	require.NoError(t, err)

	return builder.Build()
}

func runAvroScan(t *testing.T, fs *iceio.MemFS, task FileScanTask, filter iceberg.BooleanExpression) ([]int64, error) {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	projected := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://avro/table", iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
	scan := &arrowScan{
		fs: fs, metadata: metadata, scanSchema: schema, projectedSchema: projected,
		filterSchema: schema, boundRowFilter: bound, caseSensitive: true, rowLimit: ScanNoLimit, concurrency: 1,
	}
	_, records, err := scan.GetRecords(context.Background(), []FileScanTask{task})
	if err != nil {
		return nil, err
	}
	var ids []int64
	for record, err := range records {
		if err != nil {
			return nil, err
		}
		col := record.Column(0).(*array.Int64)
		for i := range col.Len() {
			ids = append(ids, col.Value(i))
		}
		record.Release()
	}

	return ids, nil
}

func TestAvroScannerProjectionFilterAndPositionDeletes(t *testing.T) {
	fs := iceio.NewMemFS()
	data := avroScanFile(t, fs,
		map[string]any{"old_id": int64(1), "category": "keep"},
		map[string]any{"old_id": int64(2), "category": "keep"},
		map[string]any{"old_id": int64(3), "category": "drop"},
		map[string]any{"old_id": int64(4), "category": "keep"},
	)
	const deletePath = "mem://avro/delete.parquet"
	writePosDeleteParquetToMemFS(t, fs, deletePath, `[{"file_path":"mem://avro/data.avro","pos":1}]`)
	deleteFile := newPosDeleteFile(t, deletePath, 1, 128)
	filter := iceberg.EqualTo(iceberg.Reference("category"), "keep")
	task := FileScanTask{File: data, DeleteFiles: []iceberg.DataFile{deleteFile}}
	ids, err := runAvroScan(t, fs, task, filter)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 4}, ids)
	task.Residual = iceberg.NewAnd(filter, iceberg.GreaterThan(iceberg.Reference("id"), int64(1)))
	ids, err = runAvroScan(t, fs, task, filter)
	require.NoError(t, err)
	require.Equal(t, []int64{4}, ids)
}

func TestAvroScannerEmptyAndPartialTasks(t *testing.T) {
	fs := iceio.NewMemFS()
	data := avroScanFile(t, fs)
	ids, err := runAvroScan(t, fs, FileScanTask{File: data}, iceberg.AlwaysTrue{})
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = runAvroScan(t, fs, FileScanTask{File: data, Start: 1, Length: 5}, iceberg.AlwaysTrue{})
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}
