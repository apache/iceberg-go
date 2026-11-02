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

package internal

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
	avro "github.com/twmb/avro"
)

func TestAvroRetagRejectsMissingNestedChildren(t *testing.T) {
	source := array.NewData(arrow.PrimitiveTypes.Int32, 0, nil, nil, 0, 0)
	defer source.Release()

	target := arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32})
	converted, err := avroRetagData(source, target)
	require.Nil(t, converted)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
}

func TestAvroRetagRejectsIncompatiblePhysicalTypes(t *testing.T) {
	stringData := array.NewData(arrow.BinaryTypes.String, 0, nil, nil, 0, 0)
	defer stringData.Release()
	got, err := avroRetagData(stringData, &arrow.TimestampType{Unit: arrow.Microsecond})
	require.Nil(t, got)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)

	microData := array.NewData(&arrow.TimestampType{Unit: arrow.Microsecond}, 0, nil, nil, 0, 0)
	defer microData.Release()
	got, err = avroRetagData(microData, &arrow.TimestampType{Unit: arrow.Nanosecond})
	require.Nil(t, got)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)

	intData := array.NewData(arrow.PrimitiveTypes.Int64, 0, nil, nil, 0, 0)
	defer intData.Release()
	got, err = avroRetagData(intData, &arrow.TimestampType{Unit: arrow.Nanosecond})
	require.NoError(t, err)
	got.Release()
}

func TestAvroRejectsIncorrectTimestampPhysicalType(t *testing.T) {
	for _, test := range []struct {
		name  string
		field arrow.Field
		node  avro.SchemaNode
	}{
		{
			name:  "string timestamp",
			field: arrow.Field{Name: "ts", Type: arrow.BinaryTypes.String},
			node:  avro.SchemaNode{Type: "string", LogicalType: "timestamp-micros"},
		},
		{
			name:  "mismatched timestamp units",
			field: arrow.Field{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
			node:  avro.SchemaNode{Type: "long", LogicalType: "timestamp-micros"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := decorateAvroField(test.field, test.node, nil, "field-id", make(map[int]struct{}))
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		})
	}
}

func TestAvroRetagRejectsIncompatibleBinaryAndDecimalTypes(t *testing.T) {
	fixed := array.NewData(&arrow.FixedSizeBinaryType{ByteWidth: 8}, 0, nil, nil, 0, 0)
	defer fixed.Release()
	got, err := avroRetagData(fixed, &arrow.FixedSizeBinaryType{ByteWidth: 16})
	require.Nil(t, got)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)

	decimal := array.NewData(&arrow.Decimal128Type{Precision: 9, Scale: 2}, 0, nil, nil, 0, 0)
	defer decimal.Release()
	got, err = avroRetagData(decimal, &arrow.Decimal128Type{Precision: 9, Scale: 3})
	require.Nil(t, got)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
}

func TestAvroRejectsReservedFieldIDs(t *testing.T) {
	for _, tc := range []struct {
		id    float64
		valid bool
	}{
		{2147483447, true},
		{2147483448, false},
		{2147483647, false},
	} {
		field, err := addAvroFieldID(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64},
			map[string]any{"field-id": tc.id}, "field-id", make(map[int]struct{}))
		if tc.valid {
			require.NoError(t, err)
			require.NotEmpty(t, field.Metadata.Keys())
		} else {
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		}
	}
}

func TestAvroReaderErrorIsTerminal(t *testing.T) {
	reader := &avroRecordReader{readErr: iceberg.ErrInvalidSchema}
	require.False(t, reader.Next())
	require.ErrorIs(t, reader.Err(), iceberg.ErrInvalidSchema)
}

func TestAvroRetagPreservesValidPrimitiveData(t *testing.T) {
	source := array.NewData(arrow.PrimitiveTypes.Int64, 0, nil, nil, 0, 0)
	defer source.Release()

	converted, err := avroRetagData(source, arrow.PrimitiveTypes.Int64)
	require.NoError(t, err)
	require.NotNil(t, converted)
	converted.Release()
}
