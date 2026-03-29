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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func benchEqDeletes(b *testing.B, buildRec func(memory.Allocator, int) arrow.RecordBatch, buildDel func(int) *equalityDeleteSet) {
	b.Helper()

	dataRows := []int{1_000, 100_000, 1_000_000}
	deleteRows := []int{10, 100, 10_000}

	for _, nData := range dataRows {
		for _, nDel := range deleteRows {
			if nDel > nData {
				continue
			}

			b.Run(fmt.Sprintf("rows=%d/deletes=%d", nData, nDel), func(b *testing.B) {
				mem := memory.NewGoAllocator()
				ctx := compute.WithAllocator(context.Background(), mem)
				rec := buildRec(mem, nData)
				defer rec.Release()

				delSet := buildDel(nDel)
				filterFn, err := processEqualityDeletes(ctx, []*equalityDeleteSet{delSet})
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					rec.Retain()
					result, err := filterFn(rec)
					if err != nil {
						b.Fatal(err)
					}

					result.Release()
				}
			})
		}
	}
}

func buildBenchRecordInt(mem memory.Allocator, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	catBldr := bldr.Field(1).(*array.Int64Builder)

	for i := 0; i < numRows; i++ {
		idBldr.Append(int64(i))
		catBldr.Append(int64(i % 100))
	}

	return bldr.NewRecordBatch()
}

func buildBenchDeleteSetInt(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := 0; i < numDeletes; i++ {
		buf.Reset()
		buf.WriteByte(1)
		binary.Write(&buf, binary.BigEndian, int64(i*3))
		buf.WriteByte(1)
		binary.Write(&buf, binary.BigEndian, int64((i*3)%100))
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{
		keys:     keys,
		fieldIDs: []int{1, 2},
		colNames: []string{"id", "category"},
	}
}

func buildBenchRecordString(mem memory.Allocator, numRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	nameBldr := bldr.Field(1).(*array.StringBuilder)

	for i := 0; i < numRows; i++ {
		idBldr.Append(int64(i))
		nameBldr.Append(fmt.Sprintf("user-%08d", i))
	}

	return bldr.NewRecordBatch()
}

func buildBenchDeleteSetString(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := 0; i < numDeletes; i++ {
		buf.Reset()
		buf.WriteByte(1)
		binary.Write(&buf, binary.BigEndian, int64(i*3))
		buf.WriteByte(1)
		s := fmt.Sprintf("user-%08d", i*3)
		binary.Write(&buf, binary.BigEndian, int32(len(s)))
		buf.WriteString(s)
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{
		keys:     keys,
		fieldIDs: []int{1, 2},
		colNames: []string{"id", "name"},
	}
}

func BenchmarkProcessEqualityDeletesInt(b *testing.B) {
	benchEqDeletes(b, buildBenchRecordInt, buildBenchDeleteSetInt)
}

func BenchmarkProcessEqualityDeletesString(b *testing.B) {
	benchEqDeletes(b, buildBenchRecordString, buildBenchDeleteSetString)
}
