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
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro/ocf"
)

func TestAvroFormatNotRegisteredForTableWrites(t *testing.T) {
	require.Nil(t, GetFileFormat(iceberg.AvroFile))
	require.NotNil(t, GetFileFormat(iceberg.ParquetFile))
}

func TestAvroDefaultCompressionMatchesJava(t *testing.T) {
	format := avroFormat{}
	require.Equal(t, avroWriteProps{codec: "gzip"}, format.GetWriteProperties(nil))
	require.Equal(t, avroWriteProps{codec: "snappy"}, format.GetWriteProperties(iceberg.Properties{
		"write.avro.compression-codec": "snappy",
	}))
}

func TestAvroFieldNamesMatchJava(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"normal_name", "normal_name"},
		{"customer-id", "customer_x2Did"},
		{"1st-value", "_1st_x2Dvalue"},
		{"first name", "first_x20name"},
		{"a.b", "a_x2Eb"},
		{"emoji🚀", "emoji_xD83D_xDE80"},
	} {
		require.Equal(t, tc.want, avroFieldName(tc.name))
	}
}

func TestAvroSchemaRejectsSanitizedNameCollisions(t *testing.T) {
	_, err := avroRecordSchema([]iceberg.NestedField{
		{ID: 1, Name: "a-b", Type: iceberg.Int32Type{}},
		{ID: 2, Name: "a_x2Db", Type: iceberg.Int32Type{}},
	}, "iceberg_record")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "same encoded name")
}

func TestAvroWriterSanitizesTopLevelAndNestedFields(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "1st-value", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "person", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "first name", Type: iceberg.StringType{}, Required: true},
			},
		}, Required: true},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "1st-value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "person", Type: arrow.StructOf(
			arrow.Field{Name: "first name", Type: arrow.BinaryTypes.String, Nullable: false},
		), Nullable: false},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(42)
	nested := builder.Field(1).(*array.StructBuilder)
	nested.Append(true)
	nested.FieldBuilder(0).(*array.StringBuilder).Append("Ada")
	batch := builder.NewRecordBatch()
	defer batch.Release()

	path := filepath.Join(t.TempDir(), "sanitized.avro")
	w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: path,
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	require.NoError(t, w.Write(batch))
	_, err = w.Close()
	require.NoError(t, err)

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	reader, err := ocf.NewReader(file)
	require.NoError(t, err)
	defer reader.Close()
	avroSchema := string(reader.Metadata()["avro.schema"])
	require.Contains(t, avroSchema, `"name":"_1st_x2Dvalue"`)
	require.Contains(t, avroSchema, `"iceberg-field-name":"1st-value"`)
	require.Contains(t, avroSchema, `"name":"first_x20name"`)
	require.Contains(t, avroSchema, `"iceberg-field-name":"first name"`)

	var record map[string]any
	require.NoError(t, reader.Decode(&record))
	require.Equal(t, int64(42), record["_1st_x2Dvalue"])
	decodedStruct, ok := record["person"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Ada", decodedStruct["first_x20name"])
	require.ErrorIs(t, reader.Decode(&record), io.EOF)
}

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
	require.Equal(t, "deflate", string(reader.Metadata()["avro.codec"]))
	icebergSchemaJSON, err := json.Marshal(sc)
	require.NoError(t, err)
	require.JSONEq(t, string(icebergSchemaJSON), string(reader.Metadata()["iceberg.schema"]))
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

func TestAvroWriterAcceptsJavaCompressionNames(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.Int32Type{}, Required: true},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).Append(7)
	batch := builder.NewRecordBatch()
	defer batch.Release()

	for _, tc := range []struct {
		name      string
		props     any
		wantCodec string
	}{
		{name: "default", wantCodec: "deflate"},
		{name: "uncompressed", props: avroWriteProps{codec: "UNCOMPRESSED"}},
		{name: "gzip", props: avroWriteProps{codec: "GZIP"}, wantCodec: "deflate"},
		{name: "snappy", props: avroWriteProps{codec: "SnApPy"}, wantCodec: "snappy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.avro")
			w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
				FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: path,
				Content: iceberg.EntryContentData, WriteProps: tc.props,
			}, as)
			require.NoError(t, err)
			require.NoError(t, w.Write(batch))
			_, err = w.Close()
			require.NoError(t, err)

			file, err := os.Open(path)
			require.NoError(t, err)
			defer file.Close()
			reader, err := ocf.NewReader(file)
			require.NoError(t, err)
			defer reader.Close()
			if tc.wantCodec != "" {
				require.Equal(t, tc.wantCodec, string(reader.Metadata()["avro.codec"]))
			}
			var record map[string]any
			require.NoError(t, reader.Decode(&record))
			require.Equal(t, int32(7), record["value"])
			require.ErrorIs(t, reader.Decode(&record), io.EOF)
		})
	}
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

func TestAvroUUIDAndDecimalSchemas(t *testing.T) {
	uuidSchema, err := icebergAvroType(iceberg.UUIDType{}, 1)
	require.NoError(t, err)
	uuidFixed := uuidSchema.(map[string]any)
	require.Equal(t, "fixed", uuidFixed["type"])
	require.Equal(t, "uuid", uuidFixed["logicalType"])
	require.Equal(t, 16, uuidFixed["size"])

	decimalType := iceberg.MustDecimalTypeOf(9, 2)
	decimalSchema, err := icebergAvroType(decimalType, 2)
	require.NoError(t, err)
	fixed := decimalSchema.(map[string]any)
	require.Equal(t, "fixed", fixed["type"])
	require.Equal(t, "decimal", fixed["logicalType"])
	require.Equal(t, 4, fixed["size"])
	require.Equal(t, 9, fixed["precision"])
	require.Equal(t, 2, fixed["scale"])
}

func TestAvroSignedDecimalFixedBytes(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  []byte
	}{
		{"12.34", []byte{0, 0, 4, 210}},
		{"-12.34", []byte{255, 255, 251, 46}},
		{"0.00", []byte{0, 0, 0, 0}},
	} {
		t.Run(tc.value, func(t *testing.T) {
			number, err := decimal128.FromString(tc.value, 9, 2)
			require.NoError(t, err)
			got, err := avroSignedFixed(number.BigInt(), 4)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	_, err := avroSignedFixed(nil, 4)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = avroSignedFixed(decimal128.FromI64(200).BigInt(), 1)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestAvroWriterUUIDAndDecimal(t *testing.T) {
	id := uuid.MustParse("f79c3e09-677c-4bbd-a479-3f349cb785e7")
	decimalType := iceberg.MustDecimalTypeOf(9, 2)
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.UUIDType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "amount", Type: decimalType, Required: true},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: extensions.NewUUIDType()},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 9, Scale: 2}},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*extensions.UUIDBuilder).Append(id)
	number, err := decimal128.FromString("-12.34", 9, 2)
	require.NoError(t, err)
	builder.Field(1).(*array.Decimal128Builder).Append(number)
	batch := builder.NewRecordBatch()
	defer batch.Release()

	path := filepath.Join(t.TempDir(), "data.avro")
	w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: icebergSchema, Spec: *iceberg.UnpartitionedSpec, FileName: path,
		Content: iceberg.EntryContentData,
	}, arrowSchema)
	require.NoError(t, err)
	require.NoError(t, w.Write(batch))
	file, err := w.Close()
	require.NoError(t, err)
	require.EqualValues(t, 1, file.Count())

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	r, err := ocf.NewReader(f)
	require.NoError(t, err)
	defer r.Close()
	schema := string(r.Metadata()["avro.schema"])
	require.Contains(t, schema, `"logicalType":"uuid"`)
	require.Contains(t, schema, `"logicalType":"decimal"`)
	var row map[string]any
	require.NoError(t, r.Decode(&row))
	require.Equal(t, [16]byte(id), row["id"])
	require.Equal(t, big.NewRat(-617, 50), row["amount"])
}

func TestAvroWriterDecimal38RoundTrip(t *testing.T) {
	decimalType := iceberg.MustDecimalTypeOf(38, 0)
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "amount", Type: decimalType, Required: true},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer builder.Release()
	max := strings.Repeat("9", 38)
	values := []string{max, "-" + max, "0"}
	for _, value := range values {
		number, err := decimal128.FromString(value, 38, 0)
		require.NoError(t, err)
		builder.Field(0).(*array.Decimal128Builder).Append(number)
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()

	path := filepath.Join(t.TempDir(), "decimal38.avro")
	w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: path,
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	require.NoError(t, w.Write(batch))
	dataFile, err := w.Close()
	require.NoError(t, err)
	require.EqualValues(t, len(values), dataFile.Count())

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	r, err := ocf.NewReader(f)
	require.NoError(t, err)
	defer r.Close()
	for _, value := range values {
		var row map[string]any
		require.NoError(t, r.Decode(&row))
		unscaled, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		require.Equal(t, new(big.Rat).SetInt(unscaled), row["amount"])
	}
	var row map[string]any
	require.ErrorIs(t, r.Decode(&row), io.EOF)
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

func TestAvroWriterRejectsIncompatibleArrowTypesBeforeCreate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		iceType   iceberg.Type
		arrowType arrow.DataType
	}{
		{"int64_from_float64", iceberg.Int64Type{}, arrow.PrimitiveTypes.Float64},
		{"time_nanoseconds", iceberg.TimeType{}, arrow.FixedWidthTypes.Time64ns},
		{"timestamp_nanoseconds", iceberg.TimestampType{}, &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{"timestamp_utc_for_local", iceberg.TimestampType{}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"timestamp_missing_utc", iceberg.TimestampTzType{}, &arrow.TimestampType{Unit: arrow.Microsecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: tc.iceType})
			as := arrow.NewSchema([]arrow.Field{{Name: "value", Type: tc.arrowType}}, nil)
			name := filepath.Join(t.TempDir(), "invalid.avro")
			_, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
				FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: name, Content: iceberg.EntryContentData,
			}, as)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			_, err = os.Stat(name)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestAvroWriterRejectsReorderedNestedFields(t *testing.T) {
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "record", Type: &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 2, Name: "first", Type: iceberg.Int32Type{}},
			{ID: 3, Name: "second", Type: iceberg.Int32Type{}},
		},
	}})
	as := arrow.NewSchema([]arrow.Field{{
		Name: "record",
		Type: arrow.StructOf(
			arrow.Field{Name: "second", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "first", Type: arrow.PrimitiveTypes.Int32},
		),
	}}, nil)
	name := filepath.Join(t.TempDir(), "invalid.avro")
	_, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: name, Content: iceberg.EntryContentData,
	}, as)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = os.Stat(name)
	require.True(t, os.IsNotExist(err))
}

func TestAvroWriterRejectsChangedBatchSchema(t *testing.T) {
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.Int64Type{}})
	as := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "data.avro"),
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	defer w.Abort()
	changed := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, changed)
	defer builder.Release()
	builder.Field(0).(*array.Float64Builder).Append(1)
	batch := builder.NewRecordBatch()
	defer batch.Release()
	require.ErrorIs(t, w.Write(batch), iceberg.ErrInvalidArgument)
}

func TestAvroWriterRejectsTimeOutsideDay(t *testing.T) {
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "time", Type: iceberg.TimeType{}})
	as := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Time64us}}, nil)
	for _, value := range []arrow.Time64{-1, 86400000000} {
		w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
			FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "data.avro"),
			Content: iceberg.EntryContentData,
		}, as)
		require.NoError(t, err)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, as)
		builder.Field(0).(*array.Time64Builder).Append(value)
		batch := builder.NewRecordBatch()
		require.ErrorIs(t, w.Write(batch), iceberg.ErrInvalidArgument)
		batch.Release()
		builder.Release()
		require.NoError(t, w.Abort())
	}
}

func TestAvroWriterRejectsMissingFileIO(t *testing.T) {
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.Int64Type{}})
	as := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	_, err := (avroFormat{}).NewFileWriter(t.Context(), nil, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: "data.avro",
		Content: iceberg.EntryContentData,
	}, as)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestAvroWriterAbortsAfterPartialBatchFailure(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0}, []bool{true, false})
	batch := b.NewRecordBatch()
	defer batch.Release()

	path := filepath.Join(t.TempDir(), "partial.avro")
	w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: sc, Spec: *iceberg.UnpartitionedSpec, FileName: path,
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	// Row one is encoded before row two violates the required field.
	require.ErrorIs(t, w.Write(batch), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, w.Write(batch), iceberg.ErrInvalidArgument)
	_, err = w.Close()
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestAvroWriterRejectsDuplicateStringMapKeys(t *testing.T) {
	builder := array.NewMapBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32, false)
	defer builder.Release()
	builder.Append(true)
	builder.KeyBuilder().(*array.StringBuilder).AppendValues([]string{"same", "same"}, nil)
	builder.ItemBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	values := builder.NewMapArray()
	defer values.Release()

	_, err := avroValue(values, 0, &iceberg.MapType{
		KeyID: 2, KeyType: iceberg.StringType{},
		ValueID: 3, ValueType: iceberg.Int32Type{}, ValueRequired: true,
	})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "duplicate Avro map key")
}

func TestAvroWriteNanosecondTimestamps(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "local", Type: iceberg.TimestampNsType{}},
		iceberg.NestedField{ID: 2, Name: "utc", Type: iceberg.TimestampTzNsType{}},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
		{Name: "utc", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	vals := []arrow.Timestamp{-9223372036854775808, -1, 0, 1, 9223372036854775807}
	for _, v := range vals {
		b.Field(0).(*array.TimestampBuilder).Append(v)
		b.Field(1).(*array.TimestampBuilder).Append(v)
	}
	b.Field(0).AppendNull()
	b.Field(1).AppendNull()
	batch := b.NewRecordBatch()
	defer batch.Release()
	name := filepath.Join(t.TempDir(), "nanos.avro")
	w, err := (avroFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: name, Content: iceberg.EntryContentData}, as)
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
	text := string(r.Metadata()["avro.schema"])
	require.Contains(t, text, `"logicalType":"timestamp-nanos"`)
	require.Contains(t, text, `"adjust-to-utc":false`)
	require.Contains(t, text, `"adjust-to-utc":true`)
	for _, want := range vals {
		var row struct {
			Local int64 `avro:"local"`
			UTC   int64 `avro:"utc"`
		}
		require.NoError(t, r.Decode(&row))
		require.Equal(t, int64(want), row.Local)
		require.Equal(t, int64(want), row.UTC)
	}
	var row map[string]any
	require.NoError(t, r.Decode(&row))
	require.Nil(t, row["local"])
	require.Nil(t, row["utc"])
	require.ErrorIs(t, r.Decode(&row), io.EOF)
}

func TestAvroNanosecondTimestampRejectsIncompatibleTypes(t *testing.T) {
	for _, tc := range []struct {
		ice iceberg.Type
		arr arrow.DataType
	}{
		{iceberg.TimestampNsType{}, &arrow.TimestampType{Unit: arrow.Microsecond}},
		{iceberg.TimestampNsType{}, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{iceberg.TimestampTzNsType{}, &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{iceberg.TimestampTzNsType{}, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Europe/Paris"}},
	} {
		require.ErrorIs(t, validateAvroArrowType(tc.ice, tc.arr), iceberg.ErrInvalidArgument)
	}
}

func TestAvroWriterRejectsNilBatches(t *testing.T) {
	_, err := (avroFormat{}).WriteDataFile(context.Background(), nil, nil, WriteFileInfo{}, []arrow.RecordBatch{nil})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	writer := &avroFileWriter{}
	err = writer.Write(nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorIs(t, writer.failed, iceberg.ErrInvalidArgument)
}

func TestAvroWriterRespectsMetricsPlan(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}})
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 0}, []bool{true, false})
	batch := builder.NewRecordBatch()
	defer batch.Release()
	for _, mode := range []MetricModeType{MetricModeNone, MetricModeCounts, MetricModeFull} {
		t.Run(string(mode), func(t *testing.T) {
			plan := map[int]StatisticsCollector{1: {FieldID: 1, Mode: MetricsMode{Typ: mode}, IcebergTyp: iceberg.Int64Type{}}}
			format := avroFormat{}
			writer, err := format.NewFileWriter(t.Context(), iceio.NewMemFS(), nil, WriteFileInfo{
				FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: "metrics.avro", Content: iceberg.EntryContentData,
				WriteProps: format.GetWriteProperties(nil), StatsCols: plan,
			}, arrowSchema)
			require.NoError(t, err)
			require.NoError(t, writer.Write(batch))
			data, err := writer.Close()
			require.NoError(t, err)
			require.EqualValues(t, 2, data.Count())
			if mode == MetricModeNone {
				require.Empty(t, data.ValueCounts())
				require.Empty(t, data.NullValueCounts())
			} else {
				require.EqualValues(t, 2, data.ValueCounts()[1])
				require.EqualValues(t, 1, data.NullValueCounts()[1])
			}
		})
	}
}
