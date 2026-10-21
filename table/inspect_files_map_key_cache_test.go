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

package table

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestSortedInspectMapIDsRevalidatesKeys(t *testing.T) {
	var cache inspectSortedMapIDs
	cases := []struct {
		name   string
		values map[int]int64
		want   []int
	}{
		{"first", map[int]int64{8: 0, 2: 0, 4: 0}, []int{2, 4, 8}},
		{"same", map[int]int64{8: 1, 2: 1, 4: 1}, []int{2, 4, 8}},
		{"equal length different keys", map[int]int64{8: 0, 2: 0, 5: 0}, []int{2, 5, 8}},
		{"shrinking", map[int]int64{2: 0}, []int{2}},
		{"empty", map[int]int64{}, []int{}},
		{"nil", nil, []int{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sortedInspectMapIDs(tc.values, &cache))
		})
	}

	large := make(map[int][]byte, 32)
	for id := 32; id > 0; id-- {
		large[id] = []byte{byte(id)}
	}
	want := make([]int, 32)
	for i := range want {
		want[i] = i + 1
	}
	require.Equal(t, want, sortedInspectMapIDs(large, &cache))
	require.Equal(t, want, sortedInspectMapIDs(large, &cache))

	delete(large, 19)
	large[40] = []byte{40}
	want[18] = 20
	for i := 19; i < 31; i++ {
		want[i] = i + 2
	}
	want[31] = 40
	require.Equal(t, want, sortedInspectMapIDs(large, &cache))
}

func TestInspectContentFileAppenderRevalidatesStatKeys(t *testing.T) {
	fields := make([]iceberg.NestedField, 96)
	for i := range fields {
		fields[i] = iceberg.NestedField{
			ID: i + 1, Name: fmt.Sprintf("field_%d", i+1),
			Type: iceberg.PrimitiveTypes.Int32, Required: true,
		}
	}
	schema := iceberg.NewSchema(0, fields...)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part",
		Transform: iceberg.IdentityTransform{},
	})
	partitionType := spec.PartitionType(schema)
	arrowSchema, err := SchemaToArrowSchema(DataFilesSchema(partitionType), nil, true, false)
	require.NoError(t, err)

	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	appendFile := newInspectContentFileAppender(partitionType)

	for row, firstID := range []int{1, 1, 33, 1, 65, 65} {
		counts := make(map[int]int64, 32)
		bounds := make(map[int][]byte, 32)
		for id := firstID; id < firstID+32; id++ {
			counts[id] = int64(id * 3)
			bounds[id] = []byte{byte(id)}
		}
		fileBuilder, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, fmt.Sprintf("file-%d.parquet", row),
			iceberg.ParquetFile, map[int]any{1000: int32(row)}, nil, nil, 1, 100,
		)
		require.NoError(t, err)
		file := fileBuilder.
			ColumnSizes(counts).
			ValueCounts(counts).
			NullValueCounts(counts).
			NaNValueCounts(counts).
			LowerBoundValues(bounds).
			UpperBoundValues(bounds).
			Build()
		require.NoError(t, appendFile(builder, file))
	}

	record := builder.NewRecordBatch()
	defer record.Release()
	for _, column := range []int{7, 8, 9, 10} {
		m := record.Column(column).(*array.Map)
		keys := m.Keys().(*array.Int32)
		values := m.Items().(*array.Int64)
		for row, firstID := range []int{1, 1, 33, 1, 65, 65} {
			start, end := m.ValueOffsets(row)
			require.EqualValues(t, 32, end-start)
			for j := int64(0); j < 32; j++ {
				id := firstID + int(j)
				require.EqualValues(t, id, keys.Value(int(start+j)))
				require.EqualValues(t, id*3, values.Value(int(start+j)))
			}
		}
	}
	for _, column := range []int{11, 12} {
		m := record.Column(column).(*array.Map)
		keys := m.Keys().(*array.Int32)
		values := m.Items().(*array.Binary)
		for row, firstID := range []int{1, 1, 33, 1, 65, 65} {
			start, end := m.ValueOffsets(row)
			require.EqualValues(t, 32, end-start)
			for j := int64(0); j < 32; j++ {
				id := firstID + int(j)
				require.EqualValues(t, id, keys.Value(int(start+j)))
				require.Equal(t, []byte{byte(id)}, values.Value(int(start+j)))
			}
		}
	}
}
