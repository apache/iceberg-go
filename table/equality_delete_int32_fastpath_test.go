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

package table

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func singleInt32EqualityDeleteTestSet(values ...int32) *equalityDeleteSet {
	keys := make(set[string], len(values))
	var buf bytes.Buffer
	for _, value := range values {
		buf.Reset()
		buf.WriteByte(1)
		bufPutUint32(&buf, uint32(value))
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{keys: keys, fieldIDs: []int{1}, colNames: []string{"id"}}
}

func TestProcessEqualityDeletesSingleInt32MatchesGeneric(t *testing.T) {
	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
	)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	values := []int32{math.MinInt32, math.MaxInt32, -1, 0, 1, 7, 0, 42}
	valid := []bool{true, true, true, false, true, true, false, true}

	tests := []struct {
		name      string
		valid     []bool
		deletes   []int32
		hasNull   bool
		wantRows  int64
		wantNulls int
	}{
		{name: "integer extremes", deletes: []int32{math.MinInt32, math.MaxInt32, -1, 0}, wantRows: 3},
		{name: "nulls retained", valid: valid, deletes: []int32{math.MinInt32, math.MaxInt32, -1, 0}, wantRows: 5, wantNulls: 2},
		{name: "nulls deleted", valid: valid, deletes: []int32{math.MinInt32, math.MaxInt32, -1, 0}, hasNull: true, wantRows: 3},
		{name: "no matches", valid: valid, deletes: []int32{99}, wantRows: 8, wantNulls: 2},
		{name: "empty set", valid: valid, wantRows: 8, wantNulls: 2},
	}
	for _, tt := range tests {
		for _, offset := range []int{0, 3, 9} {
			t.Run(fmt.Sprintf("%s/offset=%d", tt.name, offset), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				ctx := compute.WithAllocator(t.Context(), mem)

				builder := array.NewInt32Builder(mem)
				builder.AppendValues(make([]int32, offset), nil)
				builder.AppendValues(values, tt.valid)
				full := builder.NewArray()
				builder.Release()
				sliced := array.NewSlice(full, int64(offset), int64(offset+len(values)))
				full.Release()
				record := array.NewRecordBatch(schema, []arrow.Array{sliced}, int64(len(values)))
				sliced.Release()
				defer record.Release()

				deleteSet := singleInt32EqualityDeleteTestSet(tt.deletes...)
				if tt.hasNull {
					deleteSet.keys["\x00"] = struct{}{}
				}

				genericSet := &equalityDeleteSet{
					keys: deleteSet.keys, fieldIDs: deleteSet.fieldIDs, colNames: deleteSet.colNames,
				}
				genericSet.singleInt32Once.Do(func() {})
				generic, err := processEqualityDeletesColumnarForFile(ctx,
					[]*equalityDeleteSet{genericSet}, fileSchema, "data.parquet")
				require.NoError(t, err)
				require.Nil(t, genericSet.singleInt32)
				record.Retain()
				want, err := generic(record)
				require.NoError(t, err)
				defer want.Release()

				fast, err := processEqualityDeletesColumnarForFile(ctx,
					[]*equalityDeleteSet{deleteSet}, fileSchema, "data.parquet")
				require.NoError(t, err)
				require.NotNil(t, deleteSet.singleInt32, "typed index must be exercised")
				record.Retain()
				got, err := fast(record)
				require.NoError(t, err)
				defer got.Release()

				assert.True(t, array.RecordEqual(want, got), "generic: %s; fast: %s", want, got)
				assert.Equal(t, tt.wantRows, got.NumRows())
				assert.Equal(t, tt.wantNulls, got.Column(0).NullN())
				if tt.wantRows == record.NumRows() {
					assert.Same(t, record, want)
					assert.Same(t, record, got)
				}
			})
		}
	}
}

func TestProcessEqualityDeletesSingleInt32MalformedKeysFallback(t *testing.T) {
	for _, key := range []string{"", "\x01\x00", "\x02\x00\x00\x00\x00", "\x01\x00\x00\x00\x00\x00"} {
		t.Run(fmt.Sprintf("key=%x", key), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			builder := array.NewInt32Builder(mem)
			builder.AppendValues([]int32{-1, 7, 42}, nil)
			values := builder.NewArray()
			builder.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
			values.Release()
			defer record.Release()

			deleteSet := singleInt32EqualityDeleteTestSet(-1)
			deleteSet.keys[key] = struct{}{}
			fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32,
			})
			process, err := processEqualityDeletesColumnarForFile(ctx,
				[]*equalityDeleteSet{deleteSet}, fileSchema, "data.parquet")
			require.NoError(t, err)
			require.Nil(t, deleteSet.singleInt32, "malformed key must use generic encoding")
			record.Retain()
			result, err := process(record)
			require.NoError(t, err)
			defer result.Release()
			assert.Equal(t, []int32{7, 42}, result.Column(0).(*array.Int32).Int32Values())
		})
	}
}

func TestProcessEqualityDeletesSingleInt32AndInt64Sets(t *testing.T) {
	for _, int32First := range []bool{true, false} {
		t.Run(fmt.Sprintf("int32_first=%t", int32First), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int32},
				{Name: "category", Type: arrow.PrimitiveTypes.Int64},
			}, nil)
			builder := array.NewRecordBuilder(mem, schema)
			builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5}, nil)
			builder.Field(1).(*array.Int64Builder).AppendValues([]int64{9, 8, 7, 6, 5}, nil)
			record := builder.NewRecordBatch()
			builder.Release()
			defer record.Release()

			fileSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
				iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.Int64},
			)
			int32Deletes := singleInt32EqualityDeleteTestSet(2, 4)
			int64Deletes := int64EqualityDeleteSet(7)
			int64Deletes.fieldIDs[0] = 2
			int64Deletes.colNames[0] = "category"

			sets := []*equalityDeleteSet{int32Deletes, int64Deletes}
			if !int32First {
				sets[0], sets[1] = sets[1], sets[0]
			}
			process, err := processEqualityDeletesColumnarForFile(
				ctx, sets, fileSchema, "data.parquet")
			require.NoError(t, err)
			require.NotNil(t, int32Deletes.singleInt32)
			require.NotNil(t, int64Deletes.singleInt64)

			record.Retain()
			result, err := process(record)
			require.NoError(t, err)
			defer result.Release()
			assert.Equal(t, []int32{1, 5}, result.Column(0).(*array.Int32).Int32Values())
		})
	}
}

func TestProcessEqualityDeletesSingleInt32SkipsNestedField(t *testing.T) {
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 2, Name: "person", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		}},
	})
	del := singleInt32EqualityDeleteTestSet(7)
	del.colNames[0] = "person.id"
	del.keys["\x00"] = struct{}{}

	_, err := processEqualityDeletesColumnarForFile(t.Context(),
		[]*equalityDeleteSet{del}, fileSchema, "data.parquet")
	require.NoError(t, err)
	require.Nil(t, del.singleInt32, "nested fields need the generic parent validity check")
}

func buildBenchRecordSingleInt32(mem memory.Allocator, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	ids := builder.Field(0).(*array.Int32Builder)
	for i := range numRows {
		ids.Append(int32(i))
	}

	return builder.NewRecordBatch()
}

func buildBenchDeleteSetSingleInt32(numDeletes int) *equalityDeleteSet {
	keys := make(set[string], numDeletes)
	var buf bytes.Buffer
	for i := range numDeletes {
		buf.Reset()
		buf.WriteByte(1)
		bufPutUint32(&buf, uint32(i*3))
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{keys: keys, fieldIDs: []int{1}, colNames: []string{"id"}}
}

func BenchmarkProcessEqualityDeletesSingleInt32(b *testing.B) {
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32,
	})
	for _, tt := range []struct {
		name string
		fast bool
	}{
		{name: "generic"},
		{name: "fast", fast: true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			benchEqDeletesForFile(b, buildBenchRecordSingleInt32, func(numDeletes int) *equalityDeleteSet {
				deleteSet := buildBenchDeleteSetSingleInt32(numDeletes)
				if !tt.fast {
					deleteSet.singleInt32Once.Do(func() {})
				}

				return deleteSet
			}, fileSchema)
		})
	}
}
