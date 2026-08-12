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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func BenchmarkReadEqualityDeleteFile(b *testing.B) {
	for _, numRows := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			tableSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
				iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String},
			)
			arrowSchema, err := SchemaToArrowSchema(tableSchema, nil, true, false)
			if err != nil {
				b.Fatal(err)
			}

			path := filepath.Join(b.TempDir(), "equality-delete.parquet")
			mem := memory.DefaultAllocator
			builder := array.NewRecordBuilder(mem, arrowSchema)
			idBuilder := builder.Field(0).(*array.Int64Builder)
			nameBuilder := builder.Field(1).(*array.StringBuilder)
			payloadBuilder := builder.Field(2).(*array.StringBuilder)
			payload := strings.Repeat("payload-", 64)
			for i := 0; i < numRows; i++ {
				idBuilder.Append(int64(i))
				nameBuilder.Append(fmt.Sprintf("user-%08d", i))
				payloadBuilder.Append(payload)
			}
			rec := builder.NewRecordBatch()
			builder.Release()
			defer rec.Release()

			tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
			defer tbl.Release()
			file, err := (iceio.LocalFS{}).Create(path)
			if err != nil {
				b.Fatal(err)
			}
			if err := pqarrow.WriteTable(tbl, file, 16_384,
				parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}

			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			dataFileBuilder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
				path, iceberg.ParquetFile, nil, nil, nil, int64(numRows), info.Size())
			if err != nil {
				b.Fatal(err)
			}
			dataFileBuilder.EqualityFieldIDs([]int{1, 2})
			dataFile := dataFileBuilder.Build()

			for _, benchmark := range []struct {
				name string
				read func(context.Context, iceio.IO, *iceberg.Schema, iceberg.NameMapping, iceberg.DataFile, []int) (set[string], []string, error)
			}{
				{name: "streamed-projection", read: readEqualityDeleteFile},
				{name: "materialized-table", read: readEqualityDeleteFileMaterialized},
			} {
				b.Run(benchmark.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						keys, _, err := benchmark.read(context.Background(), iceio.LocalFS{}, tableSchema, nil, dataFile, []int{1, 2})
						if err != nil {
							b.Fatal(err)
						}
						if len(keys) != numRows {
							b.Fatalf("got %d keys, want %d", len(keys), numRows)
						}
					}
				})
			}
		})
	}
}

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
				fileSchema := iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Int64},
					iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.Int64},
				)
				filterFn, err := processEqualityDeletesColumnarForFile(ctx, []*equalityDeleteSet{delSet}, fileSchema, "data.parquet")
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				b.ReportAllocs()

				for range b.N {
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

	for i := range numRows {
		idBldr.Append(int64(i))
		catBldr.Append(int64(i % 100))
	}

	return bldr.NewRecordBatch()
}

func buildBenchDeleteSetInt(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := range numDeletes {
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

	for i := range numRows {
		idBldr.Append(int64(i))
		nameBldr.Append(fmt.Sprintf("user-%08d", i))
	}

	return bldr.NewRecordBatch()
}

func buildBenchDeleteSetString(numDeletes int) *equalityDeleteSet {
	keys := make(set[string])
	var buf bytes.Buffer

	for i := range numDeletes {
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
