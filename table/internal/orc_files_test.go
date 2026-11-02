// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/scritchley/orc"
	orcpb "github.com/scritchley/orc/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

func writeORCFixture(t *testing.T, schemaText string, attributes map[int]map[string]string, rows ...[]any) string {
	t.Helper()
	sc, err := orc.ParseSchema(schemaText)
	require.NoError(t, err)
	var out bytes.Buffer
	writer, err := orc.NewWriter(&out, orc.SetSchema(sc), orc.SetCompression(orc.CompressionNone{}))
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, writer.Write(row...))
	}
	require.NoError(t, writer.Close())

	// Re-encode an uncompressed footer with iceberg.id attributes. The
	// scritchley writer predates ORC's type attribute support.
	raw := out.Bytes()
	require.GreaterOrEqual(t, len(raw), 4)
	psLen := int(raw[len(raw)-1])
	psStart := len(raw) - 1 - psLen
	require.GreaterOrEqual(t, psStart, 0)
	var ps orcpb.PostScript
	require.NoError(t, proto.Unmarshal(raw[psStart:len(raw)-1], protoadapt.MessageV2Of(&ps)))
	require.Equal(t, orcpb.CompressionKind_NONE, ps.GetCompression())
	footerStart := psStart - int(ps.GetFooterLength())
	require.GreaterOrEqual(t, footerStart, 0)
	var footer orcpb.Footer
	require.NoError(t, proto.Unmarshal(raw[footerStart:psStart], protoadapt.MessageV2Of(&footer)))
	for idx, kv := range attributes {
		require.Less(t, idx, len(footer.Types))
		for k, v := range kv {
			footer.Types[idx].XXX_unrecognized = append(footer.Types[idx].XXX_unrecognized, encodeORCAttribute(k, v)...)
		}
	}
	updatedFooter, err := proto.Marshal(protoadapt.MessageV2Of(&footer))
	require.NoError(t, err)
	ps.FooterLength = new(uint64(len(updatedFooter)))
	updatedPS, err := proto.Marshal(protoadapt.MessageV2Of(&ps))
	require.NoError(t, err)
	require.LessOrEqual(t, len(updatedPS), 255)
	updated := append([]byte{}, raw[:footerStart]...)
	updated = append(updated, updatedFooter...)
	updated = append(updated, updatedPS...)
	updated = append(updated, byte(len(updatedPS)))
	path := filepath.Join(t.TempDir(), "data.orc")
	require.NoError(t, os.WriteFile(path, updated, 0o644))

	return path
}

func openORCFixture(t *testing.T, path string, rows int64) FileReader {
	t.Helper()
	stat, err := os.Stat(path)
	require.NoError(t, err)
	df, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData, path, iceberg.OrcFile,
		nil, nil, nil, rows, stat.Size())
	require.NoError(t, err)
	src, err := GetFile(context.Background(), iceio.LocalFS{}, df.Build(), false)
	require.NoError(t, err)
	rd, err := src.GetReader(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rd.Close()) })

	return rd
}

func TestORCIcebergFieldIDsAndValues(t *testing.T) {
	path := writeORCFixture(t, "struct<old_name:bigint,num:int,label:string,active:boolean>",
		map[int]map[string]string{
			1: {"iceberg.id": "1", "iceberg.required": "true"},
			2: {"iceberg.id": "2"},
			3: {"iceberg.id": "3"},
			4: {"iceberg.id": "4"},
		},
		[]any{int64(42), int64(3), "hello", true},
		[]any{int64(71), int64(4), "world", false},
	)
	rd := openORCFixture(t, path, 2)
	schema, _, err := rd.PrunedSchema(map[int]struct{}{1: {}, 3: {}}, nil)
	require.NoError(t, err)
	require.Equal(t, 4, schema.NumFields())
	id, ok := schema.Field(0).Metadata.GetValue("PARQUET:field_id")
	require.True(t, ok)
	require.Equal(t, "1", id)
	require.False(t, schema.Field(0).Nullable)

	records, err := rd.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	batch := records.RecordBatch()
	require.Equal(t, int64(2), batch.NumRows())
	require.Equal(t, int64(42), batch.Column(0).(*array.Int64).Value(0))
	require.Equal(t, int64(71), batch.Column(0).(*array.Int64).Value(1))
	require.Equal(t, int32(3), batch.Column(1).(*array.Int32).Value(0))
	require.Equal(t, "world", batch.Column(2).(*array.String).Value(1))
	require.True(t, batch.Column(3).(*array.Boolean).Value(0))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
	_, err = rd.GetRecords(context.Background(), nil, nil)
	require.Error(t, err)
}

func TestORCNestedStructAndListIDs(t *testing.T) {
	path := writeORCFixture(t, "struct<profile:struct<score:bigint>,nums:array<int>>",
		map[int]map[string]string{
			1: {"iceberg.id": "1"},
			2: {"iceberg.id": "2"},
			3: {"iceberg.id": "3"},
			4: {"iceberg.id": "4"},
		},
		[]any{[]any{int64(7)}, []int64{1, 2}},
	)
	rd := openORCFixture(t, path, 1)
	schema, err := rd.Schema()
	require.NoError(t, err)
	nested, ok := schema.Field(0).Type.(*arrow.StructType)
	require.True(t, ok)
	id, ok := nested.Field(0).Metadata.GetValue("PARQUET:field_id")
	require.True(t, ok)
	require.Equal(t, "2", id)
	list, ok := schema.Field(1).Type.(*arrow.ListType)
	require.True(t, ok)
	elementID, ok := list.ElemField().Metadata.GetValue("PARQUET:field_id")
	require.True(t, ok)
	require.Equal(t, "4", elementID)
	records, err := rd.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	require.Equal(t, int64(1), records.RecordBatch().NumRows())
}

func TestORCRejectsDuplicateFieldID(t *testing.T) {
	path := writeORCFixture(t, "struct<a:bigint,b:bigint>",
		map[int]map[string]string{1: {"iceberg.id": "1"}, 2: {"iceberg.id": "1"}},
		[]any{int64(1), int64(2)})
	f, err := iceio.LocalFS{}.Open(context.Background(), path)
	require.NoError(t, err)
	defer f.Close()
	_, _, err = readORCFileSchema(f)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
}

func TestORCRejectsReservedFieldIDs(t *testing.T) {
	for _, tc := range []struct {
		fieldID string
		valid   bool
	}{
		{"2147483447", true},
		{"2147483448", false},
		{"2147483647", false},
	} {
		t.Run(tc.fieldID, func(t *testing.T) {
			types := []*orcpb.Type{{
				Kind:             orcpb.Type_LONG.Enum(),
				XXX_unrecognized: encodeORCAttribute("iceberg.id", tc.fieldID),
			}}
			field, err := orcSchemaField(types, 0, "value", make(map[int]struct{}))
			if tc.valid {
				require.NoError(t, err)
				id, ok := field.Metadata.GetValue("PARQUET:field_id")
				require.True(t, ok)
				require.Equal(t, tc.fieldID, id)
			} else {
				require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			}
		})
	}
}

func TestORCAttributesRejectMalformedFields(t *testing.T) {
	attrs, err := orcTypeAttributes(&orcpb.Type{
		XXX_unrecognized: append(encodeORCAttribute("iceberg.id", "42"),
			encodeORCAttribute("iceberg.required", "true")...),
	})
	require.NoError(t, err)
	require.Equal(t, "42", attrs["iceberg.id"])

	tpe := &orcpb.Type{XXX_unrecognized: append(encodeORCAttribute("iceberg.id", "1"), encodeORCAttribute("iceberg.id", "2")...)}
	_, err = orcTypeAttributes(tpe)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)

	// A malformed unknown field must not cause an endless parse loop.
	_, err = orcTypeAttributes(&orcpb.Type{XXX_unrecognized: []byte{0x3a, 0xff}})
	require.Error(t, err)
}

func TestORCAttributePairsRejectAmbiguousAndIncompleteFields(t *testing.T) {
	stringField := func(number uint64, value string) []byte {
		out := binary.AppendUvarint(nil, number<<3|2)
		out = binary.AppendUvarint(out, uint64(len(value)))
		return append(out, value...)
	}
	wrapPair := func(fields ...[]byte) *orcpb.Type {
		body := bytes.Join(fields, nil)
		data := binary.AppendUvarint(nil, 7<<3|2)
		data = binary.AppendUvarint(data, uint64(len(body)))
		data = append(data, body...)
		return &orcpb.Type{XXX_unrecognized: data}
	}
	for _, tc := range []struct {
		name   string
		fields [][]byte
	}{
		{"missing-value", [][]byte{stringField(1, "iceberg.id")}},
		{"missing-key", [][]byte{stringField(2, "1")}},
		{"duplicate-key", [][]byte{stringField(1, "iceberg.id"), stringField(1, "iceberg.required"), stringField(2, "1")}},
		{"duplicate-value", [][]byte{stringField(1, "iceberg.id"), stringField(2, "1"), stringField(2, "2")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := orcTypeAttributes(wrapPair(tc.fields...))
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		})
	}
	// Unknown StringPair fields are ignored as required by protobuf's
	// forward-compatibility rules, not mistaken for the key or value.
	unknown := binary.AppendUvarint(nil, 3<<3)
	unknown = binary.AppendUvarint(unknown, 123)
	attrs, err := orcTypeAttributes(wrapPair(stringField(1, "iceberg.id"), unknown, stringField(2, "7")))
	require.NoError(t, err)
	require.Equal(t, "7", attrs["iceberg.id"])
}

func TestORCCompressedAndInvalidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.orc")
	require.NoError(t, os.WriteFile(path, []byte("not-orc"), 0o644))
	f, err := iceio.LocalFS{}.Open(context.Background(), path)
	require.NoError(t, err)
	defer f.Close()
	require.Error(t, orcValidFooterSignature(f))
	buf := binary.AppendUvarint(nil, 5<<3)
	buf = binary.AppendUvarint(buf, 123)
	_, _, _, _, err = consumeORCProtoField(buf)
	require.NoError(t, err)
}

func TestORCPyArrowWriterCompatibility(t *testing.T) {
	for _, codec := range []string{"uncompressed", "zlib", "snappy"} {
		t.Run(codec, func(t *testing.T) {
			reader := openORCFixture(t, filepath.Join("testdata", "orc", "pyarrow_"+codec+".orc"), 3)
			records, err := reader.GetRecords(t.Context(), nil, nil)
			require.NoError(t, err)
			defer records.Release()
			require.True(t, records.Next())
			rec := records.RecordBatch()
			require.EqualValues(t, 3, rec.NumRows())
			i := rec.Column(0).(*array.Int32)
			require.Equal(t, int32(1), i.Value(0))
			require.True(t, i.IsNull(1))
			require.Equal(t, int32(3), i.Value(2))
			l := rec.Column(1).(*array.Int64)
			require.Equal(t, int64(9000000001), l.Value(0))
			require.Equal(t, int64(-5), l.Value(1))
			require.True(t, l.IsNull(2))
			f := rec.Column(2).(*array.Float32)
			require.Equal(t, float32(1.5), f.Value(0))
			require.True(t, f.IsNull(1))
			require.Equal(t, float32(-4.5), f.Value(2))
			d := rec.Column(3).(*array.Float64)
			require.Equal(t, 2.25, d.Value(0))
			require.Equal(t, -1.25, d.Value(1))
			require.True(t, d.IsNull(2))
			s := rec.Column(4).(*array.String)
			require.Equal(t, "a", s.Value(0))
			require.Equal(t, "", s.Value(1))
			require.True(t, s.IsNull(2))
			binary := rec.Column(5).(*array.Binary)
			require.Equal(t, []byte{0, 255}, binary.Value(0))
			require.Empty(t, binary.Value(1))
			require.True(t, binary.IsNull(2))
			booleans := rec.Column(6).(*array.Boolean)
			require.True(t, booleans.Value(0))
			require.False(t, booleans.Value(1))
			require.True(t, booleans.IsNull(2))
			lists := rec.Column(7).(*array.List)
			require.EqualValues(t, []int32{0, 3, 3, 3}, lists.Offsets())
			require.True(t, lists.IsNull(2))
			values := lists.ListValues().(*array.Int32)
			require.Equal(t, int32(1), values.Value(0))
			require.True(t, values.IsNull(1))
			require.Equal(t, int32(3), values.Value(2))
			st := rec.Column(8).(*array.Struct)
			require.True(t, st.IsNull(2))
			x := st.Field(0).(*array.Int64)
			require.Equal(t, int64(42), x.Value(0))
			require.True(t, x.IsNull(1))
			str := st.Field(1).(*array.String)
			require.Equal(t, "hi", str.Value(0))
			require.Equal(t, "b", str.Value(1))
			require.False(t, records.Next())
			require.NoError(t, records.Err())
		})
	}
}

func TestORCPyArrowMultipleBatches(t *testing.T) {
	reader := openORCFixture(t, filepath.Join("testdata", "orc", "pyarrow_multibatch.orc"), 5000)
	records, err := reader.GetRecords(t.Context(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	var row int
	var batches int
	for records.Next() {
		batches++
		rec := records.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		strings := rec.Column(1).(*array.String)
		lists := rec.Column(2).(*array.List)
		values := lists.ListValues().(*array.Int32)
		for i := range int(rec.NumRows()) {
			require.Equal(t, int64(row), ids.Value(i))
			if row%17 == 0 {
				require.True(t, strings.IsNull(i))
			} else {
				require.Equal(t, strconv.Itoa(row), strings.Value(i))
			}
			start, end := lists.ValueOffsets(i)
			if row%5 == 0 {
				require.Equal(t, start, end)
			} else {
				require.EqualValues(t, 2, end-start)
				require.Equal(t, int32(row), values.Value(int(start)))
				require.True(t, values.IsNull(int(start+1)))
			}
			row++
		}
	}
	require.NoError(t, records.Err())
	require.Equal(t, 5000, row)
	require.Greater(t, batches, 1)
}

func TestORCStructDoesNotTreatMissingMembersAsNull(t *testing.T) {
	typ := arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	appendStruct := func(value orc.Struct) error {
		builder := array.NewBuilder(memory.DefaultAllocator, typ)
		defer builder.Release()
		return appendORCValue(builder, typ, value)
	}

	// A nullable child is allowed to be null, but its physical column must exist.
	require.NoError(t, appendStruct(orc.Struct{"id": int64(7), "label": nil}))
	require.ErrorIs(t, appendStruct(orc.Struct{"id": int64(7)}), iceberg.ErrInvalidSchema)
}
