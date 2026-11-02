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
package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func orcTestFieldBytes(t *testing.T, b []byte, target protowire.Number) [][]byte {
	t.Helper()
	var results [][]byte
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		require.Positive(t, n)
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			_, n = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			value, size := protowire.ConsumeBytes(b)
			n = size
			if num == target {
				results = append(results, value)
			}
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		require.Positive(t, n)
		b = b[n:]
	}

	return results
}

func orcTestVarint(t *testing.T, b []byte, target protowire.Number) uint64 {
	t.Helper()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		require.Positive(t, n)
		b = b[n:]
		if typ == protowire.VarintType {
			v, size := protowire.ConsumeVarint(b)
			require.Positive(t, size)
			if num == target {
				return v
			}
			b = b[size:]

			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		require.Positive(t, n)
		b = b[n:]
	}
	t.Fatalf("missing protobuf field %d", target)

	return 0
}

func TestORCWriterWritesIcebergIDsAndNullableStreams(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 8, Name: "id", Required: true, Type: iceberg.Int32Type{}},
		iceberg.NestedField{ID: 21, Name: "value", Required: false, Type: iceberg.StringType{}},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{4, 7, 9}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "", "long"}, []bool{true, false, true})
	batch := builder.NewRecordBatch()
	defer batch.Release()
	name := filepath.Join(t.TempDir(), "data.orc")
	format := orcFormat{}
	writer, err := format.NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: name,
		Content: iceberg.EntryContentData,
	}, arrowSchema)
	require.NoError(t, err)
	require.NoError(t, writer.Write(batch))
	dataFile, err := writer.Close()
	require.NoError(t, err)
	require.Equal(t, iceberg.OrcFile, dataFile.FileFormat())
	require.Equal(t, int64(3), dataFile.Count())
	require.Equal(t, int64(1), dataFile.NullValueCounts()[21])
	raw, err := os.ReadFile(name)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(raw, []byte("ORC")))
	require.Greater(t, len(raw), 50)
	psLen := int(raw[len(raw)-1])
	require.Greater(t, psLen, 0)
	postscript := raw[len(raw)-1-psLen : len(raw)-1]
	require.Equal(t, []byte("ORC"), orcTestFieldBytes(t, postscript, 8000)[0])
	footerLen := orcTestVarint(t, postscript, 1)
	footer := raw[len(raw)-1-psLen-int(footerLen) : len(raw)-1-psLen]
	require.Equal(t, uint64(3), orcTestVarint(t, footer, 6))
	types := orcTestFieldBytes(t, footer, 4)
	require.Len(t, types, 3)
	require.Equal(t, uint64(12), orcTestVarint(t, types[0], 1))
	require.Equal(t, uint64(3), orcTestVarint(t, types[1], 1))
	require.Equal(t, uint64(7), orcTestVarint(t, types[2], 1))
	require.Contains(t, string(types[1]), "iceberg.id")
	require.Contains(t, string(types[1]), "8")
	require.Contains(t, string(types[2]), "iceberg.id")
	require.Contains(t, string(types[2]), "21")
	stripes := orcTestFieldBytes(t, footer, 3)
	require.Len(t, stripes, 1)
	require.Equal(t, uint64(3), orcTestVarint(t, stripes[0], 5))
	stripeFooterOffset := int(orcTestVarint(t, stripes[0], 1) + orcTestVarint(t, stripes[0], 3))
	stripeFooterLength := int(orcTestVarint(t, stripes[0], 4))
	stripeFooter := raw[stripeFooterOffset : stripeFooterOffset+stripeFooterLength]
	require.Len(t, orcTestFieldBytes(t, stripeFooter, 2), 3)
	require.Len(t, orcTestFieldBytes(t, stripeFooter, 1), 4)
}

func TestORCWriterRejectsUnsupportedWithoutCreatingFile(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "uuid", Required: true, Type: iceberg.UUIDType{}})
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "uuid", Type: arrow.BinaryTypes.String}}, nil)
	name := filepath.Join(t.TempDir(), "unsupported.orc")
	f := orcFormat{}
	_, err := f.NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec,
		FileName: name, Content: iceberg.EntryContentData,
	}, arrowSchema)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	_, err = os.Stat(name)
	require.True(t, os.IsNotExist(err))
}

func TestORCRLELiteralEncoding(t *testing.T) {
	values := []uint64{orcZigZag(-2), orcZigZag(0), orcZigZag(17)}
	encoded := orcEncodeIntegers(values)
	require.Equal(t, byte(253), encoded[0])
	pos := 1
	for _, want := range values {
		got, n := binary.Uvarint(encoded[pos:])
		require.Positive(t, n)
		require.Equal(t, want, got)
		pos += n
	}
	require.Len(t, encoded, pos)
	require.Equal(t, []byte{254, 0x80, 0x40}, orcEncodeByteRLE([]byte{0x80, 0x40}))
	require.Equal(t, []byte{0xA0}, orcPackBits([]bool{true, false, true}))
}

func TestORCWriterRejectsMismatchedArrowTypes(t *testing.T) {
	for _, tc := range []struct {
		iceType   iceberg.Type
		arrowType arrow.DataType
	}{
		{iceberg.TimeType{}, arrow.FixedWidthTypes.Time64ns},
		{iceberg.TimeType{}, arrow.PrimitiveTypes.Int64},
		{iceberg.Int64Type{}, arrow.FixedWidthTypes.Time64us},
	} {
		field := iceberg.NestedField{ID: 1, Name: "x", Type: tc.iceType}
		_, err := newORCColumn(field, arrow.Field{Name: "x", Type: tc.arrowType})
		require.Error(t, err)
	}
}

func TestORCWriterRejectsChangedBatchSchema(t *testing.T) {
	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.Int64Type{}})
	firstSchema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	writer, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: iceSchema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "file.orc")}, firstSchema)
	require.NoError(t, err)
	defer writer.Abort()
	secondSchema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, secondSchema)
	defer builder.Release()
	builder.Field(0).(*array.Float64Builder).Append(1)
	batch := builder.NewRecordBatch()
	defer batch.Release()
	require.ErrorIs(t, writer.Write(batch), iceberg.ErrInvalidArgument)
}

func TestORCWriterRejectsTimeOutsideDay(t *testing.T) {
	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.TimeType{}})
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.FixedWidthTypes.Time64us}}, nil)
	for _, v := range []arrow.Time64{-1, 86400000000} {
		writer, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: iceSchema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "file.orc")}, arrowSchema)
		require.NoError(t, err)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		builder.Field(0).(*array.Time64Builder).Append(v)
		batch := builder.NewRecordBatch()
		require.ErrorIs(t, writer.Write(batch), iceberg.ErrInvalidArgument)
		batch.Release()
		builder.Release()
		require.NoError(t, writer.Abort())
	}
}

// Capture the write context while retaining real filesystem behavior.
type orcTraceKey struct{}

type orcContextIO struct {
	iceio.LocalFS
	createContext context.Context
}

func (f *orcContextIO) Create(ctx context.Context, name string) (iceio.FileWriter, error) {
	f.createContext = ctx

	return f.LocalFS.Create(ctx, name)
}

func TestORCWriterHonorsCanceledContextOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &orcContextIO{}
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}})
	as := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	fileName := filepath.Join(t.TempDir(), "data.orc")
	w, err := (orcFormat{}).NewFileWriter(ctx, fs, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: fileName,
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	cancel()
	_, err = w.Close()
	require.ErrorIs(t, err, context.Canceled)
	// No destination file is created after the context is canceled.
	require.Nil(t, fs.createContext)
	_, err = os.Stat(fileName)
	require.True(t, os.IsNotExist(err))
}

func TestORCWriterForwardsContextToCreate(t *testing.T) {
	ctx := context.WithValue(context.Background(), orcTraceKey{}, "trace")
	fs := &orcContextIO{}
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}})
	as := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	w, err := (orcFormat{}).NewFileWriter(ctx, fs, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "data.orc"),
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	_, err = w.Close()
	require.NoError(t, err)
	require.Same(t, ctx, fs.createContext)
}

func TestORCBytesWrittenDoesNotAssumeTenBytesPerInteger(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true})
	as := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4}, nil)
	batch := builder.NewRecordBatch()
	defer batch.Release()
	w, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "short.orc"),
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	defer w.Abort()
	require.NoError(t, w.Write(batch))
	require.Equal(t, int64(5), w.BytesWritten()) // Four one-byte zigzags and the RLE literal header.
}

func TestORCWriterDoesNotPublishAfterPartialBatchFailure(t *testing.T) {
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "time", Type: iceberg.TimeType{}, Required: true},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "time", Type: arrow.FixedWidthTypes.Time64us},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	b.Field(1).(*array.Time64Builder).AppendValues([]arrow.Time64{123, -1}, nil)
	batch := b.NewRecordBatch()
	defer batch.Release()

	path := filepath.Join(t.TempDir(), "partial.orc")
	w, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: iceSchema, Spec: *iceberg.UnpartitionedSpec, FileName: path,
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	require.ErrorIs(t, w.Write(batch), iceberg.ErrInvalidArgument)
	// The earlier column and first time value have already been buffered.
	// Retrying or closing must not produce a corrupt file with mismatched rows.
	require.ErrorIs(t, w.Write(batch), iceberg.ErrInvalidArgument)
	_, err = w.Close()
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestORCNestedStructListAndMap(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "record", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "inner", Type: iceberg.Int32Type{}},
		}}},
		iceberg.NestedField{ID: 3, Name: "nums", Type: &iceberg.ListType{
			ElementID: 4, Element: iceberg.Int64Type{},
		}},
		iceberg.NestedField{ID: 5, Name: "labels", Type: &iceberg.MapType{
			KeyID: 6, KeyType: iceberg.StringType{}, ValueID: 7, ValueType: iceberg.Int64Type{},
		}},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "record", Type: arrow.StructOf(arrow.Field{Name: "inner", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), Nullable: true},
		{Name: "nums", Type: arrow.ListOfField(arrow.Field{Name: "element", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), Nullable: true},
		{Name: "labels", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	st := b.Field(0).(*array.StructBuilder)
	st.Append(true)
	st.FieldBuilder(0).(*array.Int32Builder).Append(42)
	li := b.Field(1).(*array.ListBuilder)
	li.Append(true)
	li.ValueBuilder().(*array.Int64Builder).Append(2)
	li.ValueBuilder().(*array.Int64Builder).AppendNull()
	mp := b.Field(2).(*array.MapBuilder)
	mp.Append(true)
	mp.KeyBuilder().(*array.StringBuilder).Append("k")
	mp.ItemBuilder().(*array.Int64Builder).Append(9)
	rec := b.NewRecordBatch()
	defer rec.Release()

	path := filepath.Join(t.TempDir(), "nested.orc")
	w, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: path,
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	file, err := w.Close()
	require.NoError(t, err)
	require.Equal(t, int64(1), file.Count())
	require.Equal(t, int64(1), file.ValueCounts()[1])
	require.Equal(t, int64(1), file.ValueCounts()[2])
	require.Equal(t, int64(1), file.ValueCounts()[3])
	require.NotContains(t, file.ValueCounts(), 4) // Repeated list element.
	require.NotContains(t, file.ValueCounts(), 6) // Repeated map key.

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	ps := raw[len(raw)-1-int(raw[len(raw)-1]) : len(raw)-1]
	footerSize := int(orcTestVarint(t, ps, 1))
	footer := raw[len(raw)-1-len(ps)-footerSize : len(raw)-1-len(ps)]
	types := orcTestFieldBytes(t, footer, 4)
	require.Len(t, types, 8)
	require.Equal(t, uint64(12), orcTestVarint(t, types[1], 1))
	require.Equal(t, uint64(10), orcTestVarint(t, types[3], 1))
	require.Equal(t, uint64(11), orcTestVarint(t, types[5], 1))
	require.Equal(t, []byte{2}, orcTestFieldBytes(t, types[1], 2)[0])
	require.Equal(t, []byte{4}, orcTestFieldBytes(t, types[3], 2)[0])
	require.Equal(t, []byte{6, 7}, orcTestFieldBytes(t, types[5], 2)[0])
	for i := 1; i <= 7; i++ {
		require.Contains(t, string(types[i]), "iceberg.id")
	}
	mapping, err := (orcFormat{}).PathToIDMapping(schema)
	require.NoError(t, err)
	require.Equal(t, 2, mapping["record.inner"])
	require.Equal(t, 4, mapping["nums.element"])
	require.Equal(t, 7, mapping["labels.value"])
}

func TestORCTimestampUnitAndTimezone(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "local", Type: iceberg.TimestampType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "utc", Type: iceberg.TimestampTzType{}, Required: true},
		iceberg.NestedField{ID: 3, Name: "nano", Type: iceberg.TimestampNsType{}, Required: true},
		iceberg.NestedField{ID: 4, Name: "utc_nano", Type: iceberg.TimestampTzNsType{}, Required: true},
	)
	as := arrow.NewSchema([]arrow.Field{
		{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "utc", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "nano", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
		{Name: "utc_nano", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	for i, timestamp := range []arrow.Timestamp{-1, 1_420_070_400_000_000, -1, 1_420_070_400_123_456_789} {
		b.Field(i).(*array.TimestampBuilder).Append(timestamp)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	name := filepath.Join(t.TempDir(), "ts.orc")
	w, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: name,
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	file, err := w.Close()
	require.NoError(t, err)
	require.Equal(t, int64(1), file.Count())
	raw, err := os.ReadFile(name)
	require.NoError(t, err)
	ps := raw[len(raw)-1-int(raw[len(raw)-1]) : len(raw)-1]
	footerSize := int(orcTestVarint(t, ps, 1))
	footer := raw[len(raw)-1-len(ps)-footerSize : len(raw)-1-len(ps)]
	types := orcTestFieldBytes(t, footer, 4)
	require.Len(t, types, 5)
	require.Equal(t, uint64(9), orcTestVarint(t, types[1], 1))
	require.Equal(t, uint64(18), orcTestVarint(t, types[2], 1))
	require.Equal(t, uint64(9), orcTestVarint(t, types[3], 1))
	require.Equal(t, uint64(18), orcTestVarint(t, types[4], 1))
	require.Contains(t, string(types[1]), "MICROS")
	require.Contains(t, string(types[2]), "MICROS")
	require.Contains(t, string(types[3]), "NANOS")
	require.Contains(t, string(types[4]), "NANOS")
	stripes := orcTestFieldBytes(t, footer, 3)
	require.Len(t, stripes, 1)
	stripe := stripes[0]
	offset := int(orcTestVarint(t, stripe, 1) + orcTestVarint(t, stripe, 3))
	size := int(orcTestVarint(t, stripe, 4))
	sf := raw[offset:offset+size]
	require.Equal(t, [][]byte{[]byte("UTC")}, orcTestFieldBytes(t, sf, 3))
	streams := orcTestFieldBytes(t, sf, 1)
	require.Len(t, streams, 8)
	for i := 1; i < len(streams); i += 2 {
		require.Equal(t, uint64(5), orcTestVarint(t, streams[i], 1))
	}
}

func TestORCTimestampsRejectWrongTimezones(t *testing.T) {
	for _, tc := range []struct {
		ice iceberg.Type
		arrow arrow.DataType
	}{
		{iceberg.TimestampType{}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{iceberg.TimestampTzType{}, &arrow.TimestampType{Unit: arrow.Microsecond}},
		{iceberg.TimestampNsType{}, &arrow.TimestampType{Unit: arrow.Microsecond}},
		{iceberg.TimestampTzNsType{}, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Europe/Berlin"}},
	} {
		_, err := newORCColumn(iceberg.NestedField{ID: 1, Name: "ts", Type: tc.ice}, arrow.Field{Name: "ts", Type: tc.arrow})
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
}

func TestORCPositionAndEqualityDeletes(t *testing.T) {
	t.Run("position", func(t *testing.T) {
		as := arrow.NewSchema([]arrow.Field{
			{Name: "file_path", Type: arrow.BinaryTypes.String},
			{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
		}, nil)
		b := array.NewRecordBuilder(memory.DefaultAllocator, as)
		defer b.Release()
		b.Field(0).(*array.StringBuilder).AppendValues([]string{"file.parquet", "file.parquet"}, nil)
		b.Field(1).(*array.Int64Builder).AppendValues([]int64{2, 7}, nil)
		rec := b.NewRecordBatch()
		defer rec.Release()
		path := filepath.Join(t.TempDir(), "positions.orc")
		w, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
			FileSchema: iceberg.PositionalDeleteSchema, Spec: *iceberg.UnpartitionedSpec,
			FileName: path, Content: iceberg.EntryContentPosDeletes,
		}, as)
		require.NoError(t, err)
		require.NoError(t, w.Write(rec))
		df, err := w.Close()
		require.NoError(t, err)
		require.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType())
		require.Equal(t, int64(2), df.Count())
		require.NotNil(t, df.ReferencedDataFile())
		require.Equal(t, "file.parquet", *df.ReferencedDataFile())
		require.Equal(t, []byte("file.parquet"), df.LowerBoundValues()[iceberg.DeleteFilePathFieldID])
		require.Equal(t, []byte("file.parquet"), df.UpperBoundValues()[iceberg.DeleteFilePathFieldID])
	})
	t.Run("equality", func(t *testing.T) {
		schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 5, Name: "id", Type: iceberg.Int32Type{}, Required: true})
		as := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
		b := array.NewRecordBuilder(memory.DefaultAllocator, as)
		defer b.Release()
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 4}, nil)
		rec := b.NewRecordBatch()
		defer rec.Release()
		w, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
			FileSchema: schema, Spec: *iceberg.UnpartitionedSpec,
			FileName: filepath.Join(t.TempDir(), "equal.orc"),
			Content: iceberg.EntryContentEqDeletes, EqualityFieldIDs: []int{5},
		}, as)
		require.NoError(t, err)
		require.NoError(t, w.Write(rec))
		df, err := w.Close()
		require.NoError(t, err)
		require.Equal(t, iceberg.EntryContentEqDeletes, df.ContentType())
		require.Equal(t, []int{5}, df.EqualityFieldIDs())
	})
}

func TestORCEqualityDeletesRejectInvalidKeyTypes(t *testing.T) {
	cases := []struct {
		name   string
		schema *iceberg.Schema
		arrow  *arrow.Schema
		keyID  int
	}{
		{
			name: "float",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "score", Type: iceberg.Float32Type{}}),
			arrow: arrow.NewSchema([]arrow.Field{
				{Name: "score", Type: arrow.PrimitiveTypes.Float32},
			}, nil),
			keyID: 1,
		},
		{
			name: "list element",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "numbers", Type: &iceberg.ListType{
					ElementID: 2, Element: iceberg.Int32Type{},
				}}),
			arrow: arrow.NewSchema([]arrow.Field{
				{Name: "numbers", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
			}, nil),
			keyID: 2,
		},
		{
			name: "map value",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "lookup", Type: &iceberg.MapType{
					KeyID: 2, KeyType: iceberg.StringType{}, ValueID: 3, ValueType: iceberg.Int32Type{},
				}}),
			arrow: arrow.NewSchema([]arrow.Field{
				{Name: "lookup", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32)},
			}, nil),
			keyID: 3,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
				FileSchema: tc.schema, Spec: *iceberg.UnpartitionedSpec,
				FileName: filepath.Join(t.TempDir(), "invalid-key.orc"),
				Content: iceberg.EntryContentEqDeletes, EqualityFieldIDs: []int{tc.keyID},
			}, tc.arrow)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		})
	}
}

func TestORCPositionDeletesRejectOutOfOrder(t *testing.T) {
	as := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"file.parquet", "file.parquet"}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{9, 1}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	path := filepath.Join(t.TempDir(), "invalid.orc")
	w, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: iceberg.PositionalDeleteSchema, Spec: *iceberg.UnpartitionedSpec,
		FileName: path, Content: iceberg.EntryContentPosDeletes,
	}, as)
	require.NoError(t, err)
	require.ErrorIs(t, w.Write(rec), iceberg.ErrInvalidArgument)
	_, err = w.Close()
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestORCStructNullsPropagateIntoChildMetrics(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "record", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "inner", Type: iceberg.Int32Type{}},
		}},
	})
	as := arrow.NewSchema([]arrow.Field{
		{Name: "record", Type: arrow.StructOf(arrow.Field{Name: "inner", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	st := b.Field(0).(*array.StructBuilder)
	st.Append(false)
	st.FieldBuilder(0).(*array.Int32Builder).AppendNull()
	st.Append(true)
	st.FieldBuilder(0).(*array.Int32Builder).Append(7)
	rec := b.NewRecordBatch()
	defer rec.Release()

	w, err := (orcFormat{}).NewFileWriter(t.Context(), iceio.LocalFS{}, nil, WriteFileInfo{
		FileSchema: schema, Spec: *iceberg.UnpartitionedSpec,
		FileName: filepath.Join(t.TempDir(), "nullable-struct.orc"),
		Content: iceberg.EntryContentData,
	}, as)
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	df, err := w.Close()
	require.NoError(t, err)
	require.Equal(t, int64(2), df.Count())
	require.Equal(t, int64(1), df.NullValueCounts()[1])
	require.Equal(t, int64(1), df.NullValueCounts()[2])
	require.Equal(t, int64(2), df.ValueCounts()[1])
	require.Equal(t, int64(2), df.ValueCounts()[2])
}
