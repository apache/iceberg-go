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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestAvroTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name, logical, property, tz string
		unit                        arrow.TimeUnit
		iceType                     iceberg.Type
	}{
		{"micros-default", "timestamp-micros", "", "", arrow.Microsecond, iceberg.PrimitiveTypes.Timestamp},
		{"micros-local", "timestamp-micros", `,"adjust-to-utc":false`, "", arrow.Microsecond, iceberg.PrimitiveTypes.Timestamp},
		{"micros-utc", "timestamp-micros", `,"adjust-to-utc":true`, "UTC", arrow.Microsecond, iceberg.PrimitiveTypes.TimestampTz},
		{"nanos-local", "timestamp-nanos", `,"adjust-to-utc":false`, "", arrow.Nanosecond, iceberg.PrimitiveTypes.TimestampNs},
		{"nanos-utc", "timestamp-nanos", `,"adjust-to-utc":true`, "UTC", arrow.Nanosecond, iceberg.PrimitiveTypes.TimestampTzNs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := fmt.Sprintf(`{"type":"long","logicalType":%q%s}`, tc.logical, tc.property)
			schema := fmt.Sprintf(`{"type":"record","name":"row","fields":[{"name":"ts","type":%s,"field-id":1},{"name":"nested","field-id":2,"type":{"type":"record","name":"nested_record","fields":[{"name":"inner","type":%s,"field-id":3}]}}]}`, typ, typ)
			path := writeAvroOCF(t, schema, map[string]any{"ts": int64(123), "nested": map[string]any{"inner": int64(456)}})
			reader := openAvroFile(t, path, 1)
			sc, err := reader.Schema()
			require.NoError(t, err)
			got, err := table.ArrowSchemaToIceberg(sc, false, nil)
			require.NoError(t, err)
			for _, id := range []int{1, 3} {
				f, ok := got.FindFieldByID(id)
				require.True(t, ok)
				require.Equal(t, tc.iceType, f.Type)
			}
			rr, err := reader.GetRecords(context.Background(), nil, nil)
			require.NoError(t, err)
			defer rr.Release()
			require.True(t, rr.Next())
			batch := rr.RecordBatch()
			require.True(t, rr.Schema().Equal(sc))
			require.True(t, batch.Schema().Equal(sc))
			top := batch.Column(0).(*array.Timestamp)
			nested := batch.Column(1).(*array.Struct).Field(0).(*array.Timestamp)
			want := &arrow.TimestampType{Unit: tc.unit, TimeZone: tc.tz}
			require.True(t, arrow.TypeEqual(want, top.DataType()))
			require.True(t, arrow.TypeEqual(want, nested.DataType()))
			require.Equal(t, arrow.Timestamp(123), top.Value(0))
			require.Equal(t, arrow.Timestamp(456), nested.Value(0))
			projected, err := table.ToRequestedSchema(context.Background(), got, got, batch, table.SchemaOptions{})
			require.NoError(t, err)
			projected.Release()
			require.False(t, rr.Next())
			require.NoError(t, rr.Err())
		})
	}
}

func TestAvroTimestampCollections(t *testing.T) {
	const schema = `{"type":"record","name":"row","fields":[{"name":"timestamps","field-id":1,"type":{"type":"array","element-id":2,"items":{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":false}}},{"name":"by_name","field-id":3,"type":{"type":"map","key-id":4,"value-id":5,"values":{"type":"long","logicalType":"timestamp-nanos","adjust-to-utc":false}}}]}`
	path := writeAvroOCF(t, schema, map[string]any{"timestamps": []int64{123}, "by_name": map[string]int64{"a": 456}})
	reader := openAvroFile(t, path, 1)
	sc, err := reader.Schema()
	require.NoError(t, err)
	rr, err := reader.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer rr.Release()
	require.True(t, rr.Next())
	batch := rr.RecordBatch()
	require.True(t, batch.Schema().Equal(sc))
	list := batch.Column(0).(*array.List).ListValues().(*array.Timestamp)
	values := batch.Column(1).(*array.Map).Items().(*array.Timestamp)
	require.True(t, arrow.TypeEqual(&arrow.TimestampType{Unit: arrow.Microsecond}, list.DataType()))
	require.True(t, arrow.TypeEqual(&arrow.TimestampType{Unit: arrow.Nanosecond}, values.DataType()))
	require.Equal(t, arrow.Timestamp(123), list.Value(0))
	require.Equal(t, arrow.Timestamp(456), values.Value(0))
	require.False(t, rr.Next())
	require.NoError(t, rr.Err())
}
