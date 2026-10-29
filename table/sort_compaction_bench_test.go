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
	"iter"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

const (
	benchMergeStreams   = 2
	benchMergeRows      = 131072
	benchMergeCols      = 32
	benchMergeBatchRows = 8192
)

// benchMergeInputs builds benchMergeStreams sorted streams of int64 rows,
// either interleaved key by key or covering disjoint key ranges.
func benchMergeInputs(b *testing.B, interleaved bool) (rowComparator, [][]arrow.RecordBatch) {
	const (
		streams   = benchMergeStreams
		rows      = benchMergeRows
		cols      = benchMergeCols
		batchRows = benchMergeBatchRows
	)
	fields := make([]arrow.Field, cols)
	ifields := make([]iceberg.NestedField, cols)
	for c := range cols {
		fields[c] = arrow.Field{Name: "c" + strconv.Itoa(c), Type: arrow.PrimitiveTypes.Int64}
		ifields[c] = iceberg.NestedField{ID: c + 1, Name: fields[c].Name, Type: iceberg.PrimitiveTypes.Int64, Required: true}
	}
	sc := arrow.NewSchema(fields, nil)
	order, err := NewSortOrder(1, []SortField{{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst}})
	if err != nil {
		b.Fatal(err)
	}
	cmp, err := newRowComparator(order, iceberg.NewSchema(0, ifields...), sc)
	if err != nil {
		b.Fatal(err)
	}

	out := make([][]arrow.RecordBatch, streams)
	for s := range streams {
		for off := 0; off < rows; off += batchRows {
			bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
			for r := off; r < off+batchRows; r++ {
				key := int64(s*rows + r)
				if interleaved {
					key = int64(r*streams + s)
				}
				for c := range cols {
					bldr.Field(c).(*array.Int64Builder).Append(key)
				}
			}
			out[s] = append(out[s], bldr.NewRecordBatch())
			bldr.Release()
		}
	}

	return cmp, out
}

func benchMerge(b *testing.B, interleaved bool) {
	cmp, inputs := benchMergeInputs(b, interleaved)
	defer func() {
		for _, batches := range inputs {
			for _, rec := range batches {
				rec.Release()
			}
		}
	}()
	paths := make([]string, len(inputs))
	b.ResetTimer()
	for range b.N {
		streams := make([]iter.Seq2[arrow.RecordBatch, error], len(inputs))
		for i, batches := range inputs {
			streams[i] = func(yield func(arrow.RecordBatch, error) bool) {
				for _, rec := range batches {
					rec.Retain()
					if !yield(rec, nil) {
						return
					}
				}
			}
		}
		var n int64
		mergeSortedStreams(b.Context(), cmp, paths, streams, benchMergeBatchRows, func(rec arrow.RecordBatch, err error) bool {
			if err != nil {
				b.Fatal(err)
			}
			n += rec.NumRows()
			rec.Release()

			return true
		})
		if n != benchMergeStreams*benchMergeRows {
			b.Fatalf("got %d rows", n)
		}
	}
}

func BenchmarkMergeInterleaved(b *testing.B) { benchMerge(b, true) }
func BenchmarkMergeDisjoint(b *testing.B)    { benchMerge(b, false) }
