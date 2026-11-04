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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestAvroReadsNullableStructAndListOfStruct(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[
		{"name":"profile","field-id":1,"type":["null",{"type":"record","name":"Profile","fields":[{"name":"score","type":"long","field-id":2}]}]},
		{"name":"profiles","field-id":3,"type":{"type":"array","element-id":4,"items":["null",{"type":"record","name":"ListProfile","fields":[{"name":"label","type":"string","field-id":5}]}]}}
	]}`
	path := writeAvroOCF(t, schema,
		map[string]any{"profile": map[string]any{"score": int64(42)}, "profiles": []any{map[string]any{"label": "x"}, nil}},
		map[string]any{"profile": nil, "profiles": []any{}},
	)
	reader := openAvroFile(t, path, 2)
	sc, err := reader.Schema()
	require.NoError(t, err)
	iceSchema, err := table.ArrowSchemaToIceberg(sc, false, nil)
	require.NoError(t, err)
	for _, id := range []int{1, 2, 3, 4, 5} {
		_, found := iceSchema.FindFieldByID(id)
		require.True(t, found, "missing field id %d", id)
	}
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	profiles := records.RecordBatch().Column(0).(*array.Struct)
	require.False(t, profiles.IsNull(0))
	require.True(t, profiles.IsNull(1))
	require.Equal(t, int64(42), profiles.Field(0).(*array.Int64).Value(0))
	list := records.RecordBatch().Column(1).(*array.List)
	start, end := list.ValueOffsets(0)
	require.EqualValues(t, 0, start)
	require.EqualValues(t, 2, end)
	items := list.ListValues().(*array.Struct)
	require.True(t, items.IsNull(1))
	require.Equal(t, "x", items.Field(0).(*array.String).Value(0))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}

func TestAvroReadsArrayBackedMapWithNonStringKeys(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"by_id","field-id":1,
		"type":{"type":"array","logicalType":"map","items":{"type":"record","name":"KeyValue","fields":[
			{"name":"key","type":"int","field-id":2},
			{"name":"value","type":["null","string"],"field-id":3}
		]}}}]}`
	path := writeAvroOCF(t, schema, map[string]any{"by_id": []any{
		map[string]any{"key": int32(4), "value": "four"},
		map[string]any{"key": int32(8), "value": nil},
	}})
	reader := openAvroFile(t, path, 1)
	sc, err := reader.Schema()
	require.NoError(t, err)
	iceSchema, err := table.ArrowSchemaToIceberg(sc, false, nil)
	require.NoError(t, err)
	for _, id := range []int{1, 2, 3} {
		_, found := iceSchema.FindFieldByID(id)
		require.True(t, found, "missing field id %d", id)
	}
	records, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	got := records.RecordBatch().Column(0).(*array.Map)
	keys := got.Keys().(*array.Int32)
	vals := got.Items().(*array.String)
	require.Equal(t, []int32{4, 8}, []int32{keys.Value(0), keys.Value(1)})
	require.Equal(t, "four", vals.Value(0))
	require.True(t, vals.IsNull(1))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}
