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

package internal_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestAvroNullablePrimitiveValues(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"value","type":["null","long"],"default":null,"field-id":1}]}`
	path := writeAvroOCF(t, schema, map[string]any{"value": int64(42)}, map[string]any{"value": nil}, map[string]any{"value": int64(-8)})
	reader := openAvroFile(t, path, 3)
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	values := records.RecordBatch().Column(0).(*array.Int64)
	require.Equal(t, 3, values.Len())
	require.Equal(t, int64(42), values.Value(0))
	require.True(t, values.IsNull(1))
	require.Equal(t, int64(-8), values.Value(2))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}

func TestAvroDecimalValues(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"value","type":["null",{"type":"fixed","name":"decimal4","size":2,"logicalType":"decimal","precision":4,"scale":2}],"default":null,"field-id":1}]}`
	path := writeAvroOCF(t, schema, map[string]any{"value": big.NewRat(1234, 100)}, map[string]any{"value": nil}, map[string]any{"value": big.NewRat(-123, 100)})
	reader := openAvroFile(t, path, 3)
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	values := records.RecordBatch().Column(0).(*array.Decimal128)
	require.Equal(t, "12.34", values.Value(0).ToString(2))
	require.True(t, values.IsNull(1))
	require.Equal(t, "-1.23", values.Value(2).ToString(2))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}

func TestAvroUUIDValues(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"value","type":["null",{"type":"fixed","name":"uuid_fixed","size":16,"logicalType":"uuid"}],"default":null,"field-id":1}]}`
	first := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	second := [16]byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	path := writeAvroOCF(t, schema, map[string]any{"value": first}, map[string]any{"value": nil}, map[string]any{"value": second})
	reader := openAvroFile(t, path, 3)
	arrowSchema, err := reader.Schema()
	require.NoError(t, err)
	iceSchema, err := table.ArrowSchemaToIceberg(arrowSchema, false, nil)
	require.NoError(t, err)
	field, found := iceSchema.FindFieldByID(1)
	require.True(t, found)
	require.Equal(t, iceberg.PrimitiveTypes.UUID, field.Type)
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	values := records.RecordBatch().Column(0)
	switch arr := values.(type) {
	case *array.FixedSizeBinary:
		require.Equal(t, first[:], arr.Value(0))
		require.True(t, arr.IsNull(1))
		require.Equal(t, second[:], arr.Value(2))
	case array.ExtensionArray:
		storage := arr.Storage().(*array.FixedSizeBinary)
		require.Equal(t, first[:], storage.Value(0))
		require.True(t, storage.IsNull(1))
		require.Equal(t, second[:], storage.Value(2))
	default:
		t.Fatalf("unexpected UUID array %T", values)
	}
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}

func TestAvroListAndStringMapValues(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"items","type":{"type":"array","items":["null","long"],"element-id":2},"field-id":1},{"name":"tags","type":{"type":"map","values":["null","string"],"key-id":4,"value-id":5},"field-id":3}]}`
	path := writeAvroOCF(t, schema,
		map[string]any{"items": []any{int64(1), nil, int64(3)}, "tags": map[string]any{"a": "x", "b": nil}},
		map[string]any{"items": []any{}, "tags": map[string]any{}},
	)
	reader := openAvroFile(t, path, 2)
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	lists := records.RecordBatch().Column(0).(*array.List)
	start, end := lists.ValueOffsets(0)
	require.EqualValues(t, 0, start)
	require.EqualValues(t, 3, end)
	start, end = lists.ValueOffsets(1)
	require.Equal(t, start, end)
	listValues := lists.ListValues().(*array.Int64)
	require.Equal(t, int64(1), listValues.Value(0))
	require.True(t, listValues.IsNull(1))
	require.Equal(t, int64(3), listValues.Value(2))
	maps := records.RecordBatch().Column(1).(*array.Map)
	require.Equal(t, 2, maps.Len())
	start, end = maps.ValueOffsets(1)
	require.Equal(t, start, end)
	keys := maps.Keys().(*array.String)
	values := maps.Items().(*array.String)
	got := make(map[string]*string)
	for i := range keys.Len() {
		if values.IsNull(i) {
			got[keys.Value(i)] = nil
		} else {
			value := values.Value(i)
			got[keys.Value(i)] = &value
		}
	}
	x := "x"
	require.Equal(t, map[string]*string{"a": &x, "b": nil}, got)
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}
