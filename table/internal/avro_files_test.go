// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro/ocf"
)

func TestAvroSchemaPreservesIcebergIDs(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 7, Name: "before_rename", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 12, Name: "tags", Type: &iceberg.ListType{ElementID: 13, Element: iceberg.StringType{}, ElementRequired: false}},
	)
	value, err := avroRecordSchema(sc.Fields(), "iceberg_record")
	require.NoError(t, err)
	b, err := json.Marshal(value)
	require.NoError(t, err)
	var schema struct {
		Fields []struct {
			Name string `json:"name"`
			ID   int    `json:"field-id"`
			Type any    `json:"type"`
		} `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(b, &schema))
	require.Len(t, schema.Fields, 2)
	require.Equal(t, 7, schema.Fields[0].ID)
	require.Equal(t, 12, schema.Fields[1].ID)
	listField := schema.Fields[1].Type.([]any)[1].(map[string]any)
	require.Equal(t, float64(13), listField["element-id"])
}

func TestAvroWriterProducesReadableOCF(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}, Required: false},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "", "c"}, []bool{true, false, true})
	batch := builder.NewRecordBatch()
	defer batch.Release()
	fileName := filepath.Join(t.TempDir(), "data.avro")
	format := avroFormat{}
	w, err := format.NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: fileName,
		WriteProps: format.GetWriteProperties(nil), Content: iceberg.EntryContentData,
	}, arrowSchema)
	require.NoError(t, err)
	require.NoError(t, w.Write(batch))
	dataFile, err := w.Close()
	require.NoError(t, err)
	require.Equal(t, iceberg.AvroFile, dataFile.FileFormat())
	require.Equal(t, int64(3), dataFile.Count())
	require.Greater(t, dataFile.FileSizeBytes(), int64(0))
	file, err := os.Open(fileName)
	require.NoError(t, err)
	defer file.Close()
	reader, err := ocf.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()
	var rows []map[string]any
	for {
		var rec map[string]any
		err = reader.Decode(&rec)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rows = append(rows, rec)
	}
	require.Len(t, rows, 3)
	require.Equal(t, int64(1), rows[0]["id"])
	require.Equal(t, "a", rows[0]["name"])
	require.Nil(t, rows[1]["name"])
	require.Equal(t, "c", rows[2]["name"])
}

func TestAvroWriterRejectsUnsupportedTypeBeforeCreate(t *testing.T) {
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "variant", Type: iceberg.VariantType{}})
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "variant", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	name := filepath.Join(t.TempDir(), "invalid.avro")
	format := avroFormat{}
	_, err := format.NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: name, Content: iceberg.EntryContentData,
		WriteProps: format.GetWriteProperties(nil),
	}, arrowSchema)
	require.Error(t, err)
	_, err = os.Stat(name)
	require.True(t, os.IsNotExist(err))
}

func TestAvroTimestampSchema(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  iceberg.Type
		utc  bool
	}{
		{"timestamp", iceberg.TimestampType{}, false},
		{"timestamptz", iceberg.TimestampTzType{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ, err := icebergAvroType(tc.typ, 1)
			require.NoError(t, err)
			schema := typ.(map[string]any)
			require.Equal(t, "timestamp-micros", schema["logicalType"])
			require.Equal(t, tc.utc, schema["adjust-to-utc"])
		})
	}
}

func TestAvroUnsupportedLogicalTypesBeforeCreate(t *testing.T) {
	decimal, err := iceberg.DecimalTypeOf(10, 2)
	require.NoError(t, err)
	for _, typ := range []iceberg.Type{iceberg.UUIDType{}, decimal} {
		t.Run(typ.String(), func(t *testing.T) {
			sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: typ})
			arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
			name := filepath.Join(t.TempDir(), "invalid.avro")
			_, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
				FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: name, Content: iceberg.EntryContentData,
			}, arrowSchema)
			require.ErrorIs(t, err, iceberg.ErrNotImplemented)
			_, err = os.Stat(name)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestAvroWriteTimestampValues(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "local", Type: iceberg.TimestampType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "utc", Type: iceberg.TimestampTzType{}, Required: true},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "utc", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1000000))
	b.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(2000000))
	batch := b.NewRecordBatch()
	defer batch.Release()
	name := filepath.Join(t.TempDir(), "data.avro")
	w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: name, Content: iceberg.EntryContentData}, as)
	require.NoError(t, err)
	require.NoError(t, w.Write(batch))
	_, err = w.Close()
	require.NoError(t, err)
	f, err := os.Open(name)
	require.NoError(t, err)
	defer f.Close()
	r, err := ocf.NewReader(f)
	require.NoError(t, err)
	defer r.Close()
	schema := string(r.Metadata()["avro.schema"])
	require.Contains(t, schema, `"adjust-to-utc":false`)
	require.Contains(t, schema, `"adjust-to-utc":true`)
	var row map[string]any
	require.NoError(t, r.Decode(&row))
	require.NotNil(t, row["local"])
	require.NotNil(t, row["utc"])
}
