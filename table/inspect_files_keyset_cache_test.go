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
	"slices"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

func TestCachedInspectFieldIDsRechecksKeys(t *testing.T) {
	var ids []int
	for _, values := range []map[int]int64{
		{9: 90, 1: 10, 5: 50},
		{9: 91, 1: 11, 5: 51}, // Same keys, different values.
		{9: 92, 2: 22, 5: 52}, // Same length, different keys.
		{},
		{7: 70},
		{3: 30, 1: 10},
		{3: 31, 1: 11},
	} {
		expected := make([]int, 0, len(values))
		for id := range values {
			expected = append(expected, id)
		}
		sort.Ints(expected)
		if got := cachedInspectFieldIDs(values, &ids); !slices.Equal(got, expected) {
			t.Fatalf("sorted keys = %v, want %v", got, expected)
		}
	}

	binaryValues := map[int][]byte{12: {12}, 3: {3}, 8: {8}}
	var binaryIDs []int
	if got := cachedInspectFieldIDs(binaryValues, &binaryIDs); !slices.Equal(got, []int{3, 8, 12}) {
		t.Fatalf("binary map keys = %v", got)
	}
}

func TestInspectFileAppenderRechecksChangedStatsMapKeys(t *testing.T) {
	schema, err := SchemaToArrowSchema(DataFilesSchema(&iceberg.StructType{}), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	appendFile := newInspectContentFileAppender(&iceberg.StructType{})

	values := []map[int]int64{
		{1: 11, 3: 33},
		{2: 22, 3: 34},
		{1: 14, 2: 24},
		{},
		nil,
		{2: 26, 3: 36},
	}
	for i, counts := range values {
		file, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec,
			iceberg.EntryContentData,
			fmt.Sprintf("file-%d.parquet", i),
			iceberg.ParquetFile,
			nil, nil, nil, 1, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		bounds := make(map[int][]byte, len(counts))
		for id := range counts {
			bounds[id] = []byte{byte(id), byte(i)}
		}
		if counts == nil {
			bounds = nil
		}
		file.ColumnSizes(counts).ValueCounts(counts).LowerBoundValues(bounds)
		if err := appendFile(builder, file.Build()); err != nil {
			t.Fatal(err)
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	for _, colIndex := range []int{6, 7, 10} {
		column := rec.Column(colIndex).(*array.Map)
		keys := column.Keys().(*array.Int32)
		for row, counts := range values {
			if column.IsNull(row) != (counts == nil) {
				t.Fatalf("column %d row %d null = %v", colIndex, row, column.IsNull(row))
			}
			expected := make([]int32, 0, len(counts))
			for id := range counts {
				expected = append(expected, int32(id))
			}
			slices.Sort(expected)
			start, end := column.ValueOffsets(row)
			got := make([]int32, 0, end-start)
			for idx := start; idx < end; idx++ {
				got = append(got, keys.Value(int(idx)))
			}
			if !slices.Equal(got, expected) {
				t.Fatalf("column %d row %d sorted field IDs = %v, want %v", colIndex, row, got, expected)
			}
		}
	}
	// Verify the cached ordering does not cache map values.
	column := rec.Column(7).(*array.Map)
	items := column.Items().(*array.Int64)
	start, end := column.ValueOffsets(1)
	if end-start != 2 || items.Value(int(start)) != 22 || items.Value(int(start+1)) != 34 {
		t.Fatalf("wrong values on changed key set: %v", items)
	}
}

var benchmarkInspectFieldIDKeysetSink int

func BenchmarkInspectFieldIDKeyset(b *testing.B) {
	for _, width := range []int{8, 32, 128, 512} {
		first := make(map[int]int64, width)
		second := make(map[int]int64, width)
		for id := range width {
			first[id*2+1] = int64(id)
			second[id*2+2] = int64(id)
		}
		for _, changing := range []bool{false, true} {
			mode := "stable"
			if changing {
				mode = "changing"
			}
			for _, implementation := range []string{"baseline", "cached"} {
				b.Run(fmt.Sprintf("fields=%d/%s/%s", width, mode, implementation), func(b *testing.B) {
					var scratch []int
					b.ReportAllocs()
					b.ResetTimer()
					for i := range b.N {
						values := first
						if changing && i%2 != 0 {
							values = second
						}
						var ids []int
						if implementation == "cached" {
							ids = cachedInspectFieldIDs(values, &scratch)
						} else {
							ids = scratch[:0]
							for id := range values {
								ids = append(ids, id)
							}
							sort.Ints(ids)
							scratch = ids
						}
						benchmarkInspectFieldIDKeysetSink = len(ids)
					}
				})
			}
		}
	}
}
