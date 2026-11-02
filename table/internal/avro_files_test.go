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
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
	avro "github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

func writeAvroOCF(t *testing.T, schemaText string, rows ...any) string {
	t.Helper()
	sc, err := avro.Parse(schemaText)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "data.avro")
	out, err := os.Create(path)
	require.NoError(t, err)
	writer, err := ocf.NewWriter(out, sc)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, writer.Encode(row))
	}
	require.NoError(t, writer.Close())
	require.NoError(t, out.Close())

	return path
}

func openAvroFile(t *testing.T, path string, rows int64) internal.FileReader {
	t.Helper()
	stat, err := os.Stat(path)
	require.NoError(t, err)
	df, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path,
		iceberg.AvroFile, nil, nil, nil, rows, stat.Size())
	require.NoError(t, err)
	src, err := internal.GetFile(context.Background(), iceio.LocalFS{}, df.Build(), false)
	require.NoError(t, err)
	reader, err := src.GetReader(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	return reader
}

func TestAvroReadByFieldIDAfterRename(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"old_name","type":"long","field-id":1},{"name":"unused","type":"string","field-id":2}]}`
	path := writeAvroOCF(t, schema, map[string]any{"old_name": int64(42), "unused": "ignored"})
	reader := openAvroFile(t, path, 1)
	raw, _, err := reader.PrunedSchema(map[int]struct{}{1: {}, 3: {}}, nil)
	require.NoError(t, err)
	id, found := raw.Field(0).Metadata.GetValue("PARQUET:field_id")
	require.True(t, found)
	require.Equal(t, "1", id)
	fileSchema, err := table.ArrowSchemaToIceberg(raw, false, nil)
	require.NoError(t, err)

	r, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer r.Release()
	require.True(t, r.Next())
	req := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "new_name", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "added", Type: iceberg.PrimitiveTypes.String},
	)
	projected, err := table.ToRequestedSchema(context.Background(), req, fileSchema, r.RecordBatch(), table.SchemaOptions{})
	require.NoError(t, err)
	defer projected.Release()
	require.Equal(t, []string{"new_name", "added"}, []string{projected.Schema().Field(0).Name, projected.Schema().Field(1).Name})
	require.Equal(t, int64(42), projected.Column(0).(*array.Int64).Value(0))
	require.True(t, projected.Column(1).IsNull(0))
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestAvroNestedFieldIDs(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"profile","field-id":1,"type":{"type":"record","name":"profile_type","fields":[{"name":"score","type":"long","field-id":2}]}},{"name":"numbers","field-id":3,"type":{"type":"array","items":"long","element-id":4}}]}`
	path := writeAvroOCF(t, schema, map[string]any{"profile": map[string]any{"score": int64(7)}, "numbers": []int64{1, 2}})
	reader := openAvroFile(t, path, 1)
	arrowSchema, err := reader.Schema()
	require.NoError(t, err)
	fileSchema, err := table.ArrowSchemaToIceberg(arrowSchema, false, nil)
	require.NoError(t, err)
	field, ok := fileSchema.FindFieldByID(2)
	require.True(t, ok)
	require.Equal(t, "score", field.Name)
	_, ok = fileSchema.FindFieldByID(4)
	require.True(t, ok)
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	require.Equal(t, int64(1), records.RecordBatch().NumRows())
}

func TestAvroRejectsInvalidAndDuplicateFieldIDs(t *testing.T) {
	for _, tt := range []struct{ name, schema string }{
		{"duplicate", `{"type":"record","name":"row","fields":[{"name":"a","type":"long","field-id":1},{"name":"b","type":"long","field-id":1}]}`},
		{"fractional", `{"type":"record","name":"row","fields":[{"name":"a","type":"long","field-id":1.25}]}`},
		{"non-positive", `{"type":"record","name":"row","fields":[{"name":"a","type":"long","field-id":-1}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := writeAvroOCF(t, tt.schema, map[string]any{"a": int64(1), "b": int64(2)})
			stat, err := os.Stat(path)
			require.NoError(t, err)
			df, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
				iceberg.EntryContentData, path, iceberg.AvroFile,
				nil, nil, nil, 1, stat.Size())
			require.NoError(t, err)
			src, err := internal.GetFile(context.Background(), iceio.LocalFS{}, df.Build(), false)
			require.NoError(t, err)
			_, err = src.GetReader(context.Background())
			require.Error(t, err)
		})
	}
}

func TestAvroRecordReaderStopsOnContextCancellation(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"id","type":"long","field-id":1}]}`
	path := writeAvroOCF(t, schema, map[string]any{"id": int64(42)})
	reader := openAvroFile(t, path, 1)
	ctx, cancel := context.WithCancel(context.Background())
	records, err := reader.GetRecords(ctx, nil, nil)
	require.NoError(t, err)
	defer records.Release()

	cancel()
	require.False(t, records.Next())
	require.ErrorIs(t, records.Err(), context.Canceled)
}

func TestAvroRecordReaderSingleUse(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"id","type":"long","field-id":1}]}`
	path := writeAvroOCF(t, schema, map[string]any{"id": int64(42)})
	reader := openAvroFile(t, path, 1)
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()

	_, err = reader.GetRecords(context.Background(), nil, nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.True(t, records.Next())
	require.Equal(t, int64(42), records.RecordBatch().Column(0).(*array.Int64).Value(0))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}

func TestAvroRejectsExplicitColumnSelectionWithoutConsumingStream(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"id","type":"long","field-id":1}]}`
	path := writeAvroOCF(t, schema, map[string]any{"id": int64(42)})
	reader := openAvroFile(t, path, 1)

	for _, cols := range [][]int{{0}, {}} {
		_, err := reader.GetRecords(context.Background(), cols, nil)
		require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	}
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	require.Equal(t, int64(42), records.RecordBatch().Column(0).(*array.Int64).Value(0))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}

func TestAvroReadTablePreservesFieldIDs(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"id","type":"long","field-id":1}]}`
	path := writeAvroOCF(t, schema, map[string]any{"id": int64(7)}, map[string]any{"id": int64(9)})
	reader := openAvroFile(t, path, 2)

	tbl, err := reader.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()
	require.Equal(t, int64(2), tbl.NumRows())
	require.Equal(t, 1, tbl.NumCols())
	fieldID, ok := tbl.Schema().Field(0).Metadata.GetValue("PARQUET:field_id")
	require.True(t, ok)
	require.Equal(t, "1", fieldID)
}
