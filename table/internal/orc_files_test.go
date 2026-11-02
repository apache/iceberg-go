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
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "event_time", Required: true, Type: iceberg.TimestampType{}})
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "event_time", Type: arrow.FixedWidthTypes.Timestamp_us}}, nil)
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
	ctx := context.WithValue(context.Background(), struct{}{}, "trace")
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
