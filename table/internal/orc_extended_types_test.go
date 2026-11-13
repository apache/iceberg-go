// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
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
	"bytes"
	"context"
	"math/big"
	"os"
	"path/filepath"
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
)

func TestORCWriterDecimalUUIDFixed(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 11, Name: "amount", Type: iceberg.MustDecimalTypeOf(38, 4)},
		iceberg.NestedField{ID: 12, Name: "id", Type: iceberg.UUIDType{}},
		iceberg.NestedField{ID: 13, Name: "token", Type: iceberg.MustFixedTypeOf(3)},
	)
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 4}, Nullable: true},
		{Name: "id", Type: extensions.NewUUIDType(), Nullable: true},
		{Name: "token", Type: &arrow.FixedSizeBinaryType{ByteWidth: 3}, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrSchema)
	defer b.Release()
	dec := b.Field(0).(*array.Decimal128Builder)
	huge, ok := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	require.True(t, ok)
	dec.Append(decimal128.FromBigInt(huge))
	dec.Append(decimal128.FromBigInt(new(big.Int).Neg(huge)))
	dec.AppendNull()
	dec.Append(decimal128.FromI64(0))
	ids := b.Field(1).(*extensions.UUIDBuilder)
	ids.Append(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"))
	ids.Append(uuid.Nil)
	ids.AppendNull()
	ids.Append(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))
	fixed := b.Field(2).(*array.FixedSizeBinaryBuilder)
	fixed.Append([]byte{0, 1, 255})
	fixed.Append([]byte{255, 0, 2})
	fixed.AppendNull()
	fixed.Append([]byte{3, 4, 5})
	batch := b.NewRecordBatch()
	defer batch.Release()
	name := filepath.Join(t.TempDir(), "extended.orc")
	w, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: name, Content: iceberg.EntryContentData}, arrSchema)
	require.NoError(t, err)
	require.NoError(t, w.Write(batch))
	df, err := w.Close()
	require.NoError(t, err)
	require.Equal(t, int64(4), df.Count())
	for _, id := range []int{11, 12, 13} {
		require.Equal(t, int64(1), df.NullValueCounts()[id])
	}
	raw, err := os.ReadFile(name)
	require.NoError(t, err)
	psLen := int(raw[len(raw)-1])
	ps := raw[len(raw)-1-psLen : len(raw)-1]
	footerLen := int(orcTestVarint(t, ps, 1))
	footer := raw[len(raw)-1-psLen-footerLen : len(raw)-1-psLen]
	types := orcTestFieldBytes(t, footer, 4)
	require.Len(t, types, 4)
	require.Equal(t, uint64(14), orcTestVarint(t, types[1], 1))
	require.Equal(t, uint64(38), orcTestVarint(t, types[1], 5))
	require.Equal(t, uint64(4), orcTestVarint(t, types[1], 6))
	require.Equal(t, uint64(8), orcTestVarint(t, types[2], 1))
	require.Contains(t, string(types[2]), "iceberg.binary-type")
	require.Contains(t, string(types[2]), "UUID")
	require.Contains(t, string(types[3]), "FIXED")
	require.Contains(t, string(types[3]), "iceberg.length")
}

func TestORCDecimalVarint(t *testing.T) {
	for _, v := range []string{"0", "1", "-1", "127", "-128", "99999999999999999999999999999999999999", "-99999999999999999999999999999999999999"} {
		n, ok := new(big.Int).SetString(v, 10)
		require.True(t, ok)
		var out bytes.Buffer
		orcWriteDecimal(&out, n)
		// Independently decode arbitrary-width base-128 zigzag values.
		decoded := new(big.Int)
		for i, b := range out.Bytes() {
			part := new(big.Int).SetUint64(uint64(b & 0x7f))
			decoded.Or(decoded, part.Lsh(part, uint(7*i)))
			require.Equal(t, i < out.Len()-1, b&0x80 != 0)
		}
		negative := decoded.Bit(0) == 1
		decoded.Rsh(decoded, 1)
		if negative {
			decoded.Neg(decoded.Add(decoded, big.NewInt(1)))
		}
		require.Equal(t, v, decoded.String())
	}
}

func TestORCWriterExtendedTypeValidation(t *testing.T) {
	for _, tc := range []struct {
		typ iceberg.Type
		arr arrow.DataType
	}{
		{iceberg.UUIDType{}, &arrow.FixedSizeBinaryType{ByteWidth: 16}},
		{iceberg.MustFixedTypeOf(3), &arrow.FixedSizeBinaryType{ByteWidth: 4}},
		{iceberg.MustDecimalTypeOf(19, 3), &arrow.Decimal128Type{Precision: 19, Scale: 4}},
		{iceberg.MustDecimalTypeOf(19, 3), &arrow.Decimal128Type{Precision: 18, Scale: 3}},
	} {
		_, err := newORCColumn(iceberg.NestedField{ID: 1, Name: "value", Type: tc.typ}, arrow.Field{Name: "value", Type: tc.arr})
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
}

func TestORCWriterNegativeFractionalTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     iceberg.Type
		unit    arrow.TimeUnit
		zone    string
		values  []arrow.Timestamp
		want    []int64
		invalid arrow.Timestamp
	}{
		{"micros", iceberg.TimestampType{}, arrow.Microsecond, "", []arrow.Timestamp{-2500000, -1500000, -1000001, -1000000, 0, 500000}, []int64{-2, -1, -1, -1, 0, 0}, -1},
		{"nanos", iceberg.TimestampNsType{}, arrow.Nanosecond, "", []arrow.Timestamp{-2500000000, -1500000000, -1000000001, -1000000000, 0, 500000000}, []int64{-2, -1, -1, -1, 0, 0}, -1},
		{"micros_utc", iceberg.TimestampTzType{}, arrow.Microsecond, "UTC", []arrow.Timestamp{-2500000, -1500000, -1000001, -1000000, 0, 500000}, []int64{-2, -1, -1, -1, 0, 0}, -1},
		{"nanos_utc", iceberg.TimestampTzNsType{}, arrow.Nanosecond, "UTC", []arrow.Timestamp{-2500000000, -1500000000, -1000000001, -1000000000, 0, 500000000}, []int64{-2, -1, -1, -1, 0, 0}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "time", Type: tc.typ})
			arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "time", Type: &arrow.TimestampType{Unit: tc.unit, TimeZone: tc.zone}}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer b.Release()
			b.Field(0).(*array.TimestampBuilder).AppendValues(tc.values, nil)
			batch := b.NewRecordBatch()
			defer batch.Release()
			w, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "timestamps.orc"), Content: iceberg.EntryContentData}, arrowSchema)
			require.NoError(t, err)
			require.NoError(t, w.Write(batch))
			column := &w.(*orcFileWriter).columns[0]
			require.Len(t, column.integers, len(tc.values))
			for i, seconds := range tc.want {
				require.Equal(t, orcZigZag(seconds-1420070400), column.integers[i])
			}
			_, err = w.Close()
			require.NoError(t, err)
			b.Field(0).(*array.TimestampBuilder).Append(tc.invalid)
			invalid := b.NewRecordBatch()
			defer invalid.Release()
			bad, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "invalid.orc"), Content: iceberg.EntryContentData}, arrowSchema)
			require.NoError(t, err)
			require.ErrorIs(t, bad.Write(invalid), iceberg.ErrNotImplemented)
			_, err = bad.Close()
			require.Error(t, err)
		})
	}
}

func TestORCWriterExtendedTypesNestedAndEqualityDeletes(t *testing.T) {
	t.Run("nested", func(t *testing.T) {
		schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "record", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "amount", Type: iceberg.MustDecimalTypeOf(19, 2)},
			{ID: 3, Name: "id", Type: iceberg.UUIDType{}},
			{ID: 4, Name: "token", Type: iceberg.MustFixedTypeOf(1)},
		}}})
		as := arrow.NewSchema([]arrow.Field{{Name: "record", Nullable: true, Type: arrow.StructOf(
			arrow.Field{Name: "amount", Type: &arrow.Decimal128Type{Precision: 19, Scale: 2}, Nullable: true},
			arrow.Field{Name: "id", Type: extensions.NewUUIDType(), Nullable: true},
			arrow.Field{Name: "token", Type: &arrow.FixedSizeBinaryType{ByteWidth: 1}, Nullable: true},
		)}}, nil)
		b := array.NewRecordBuilder(memory.DefaultAllocator, as)
		defer b.Release()
		st := b.Field(0).(*array.StructBuilder)
		st.AppendNull()
		st.Append(true)
		st.FieldBuilder(0).(*array.Decimal128Builder).Append(decimal128.FromI64(-123))
		st.FieldBuilder(1).(*extensions.UUIDBuilder).Append(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff"))
		st.FieldBuilder(2).(*array.FixedSizeBinaryBuilder).Append([]byte{255})
		batch := b.NewRecordBatch()
		defer batch.Release()
		w, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "nested.orc"), Content: iceberg.EntryContentData}, as)
		require.NoError(t, err)
		require.NoError(t, w.Write(batch))
		df, err := w.Close()
		require.NoError(t, err)
		for _, id := range []int{1, 2, 3, 4} {
			require.Equal(t, int64(1), df.NullValueCounts()[id])
		}
	})
	t.Run("equality deletes", func(t *testing.T) {
		schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 20, Name: "id", Type: iceberg.UUIDType{}})
		as := arrow.NewSchema([]arrow.Field{{Name: "id", Type: extensions.NewUUIDType(), Nullable: true}}, nil)
		b := array.NewRecordBuilder(memory.DefaultAllocator, as)
		defer b.Release()
		b.Field(0).(*extensions.UUIDBuilder).Append(uuid.Nil)
		batch := b.NewRecordBatch()
		defer batch.Release()
		w, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: filepath.Join(t.TempDir(), "delete.orc"), Content: iceberg.EntryContentEqDeletes, EqualityFieldIDs: []int{20}}, as)
		require.NoError(t, err)
		require.NoError(t, w.Write(batch))
		df, err := w.Close()
		require.NoError(t, err)
		require.Equal(t, []int{20}, df.EqualityFieldIDs())
	})
}

func TestORCWriterRejectsDecimalBeyondPrecision(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.MustDecimalTypeOf(3, 0)})
	as := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: &arrow.Decimal128Type{Precision: 3, Scale: 0}}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer b.Release()
	b.Field(0).(*array.Decimal128Builder).Append(decimal128.FromI64(1000))
	batch := b.NewRecordBatch()
	defer batch.Release()
	path := filepath.Join(t.TempDir(), "invalid.orc")
	w, err := (orcFormat{}).NewFileWriter(context.Background(), iceio.LocalFS{}, nil, WriteFileInfo{FileSchema: schema, Spec: *iceberg.UnpartitionedSpec, FileName: path, Content: iceberg.EntryContentData}, as)
	require.NoError(t, err)
	require.ErrorIs(t, w.Write(batch), iceberg.ErrInvalidArgument)
	_, err = w.Close()
	require.Error(t, err)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}
