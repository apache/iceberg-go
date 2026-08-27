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
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
)

type benchmarkNoopDataFileFormat struct {
	tblutils.FileFormat
}

func (benchmarkNoopDataFileFormat) WriteDataFile(context.Context, iceio.WriteFileIO, map[int]any, tblutils.WriteFileInfo, []arrow.RecordBatch) (iceberg.DataFile, error) {
	return nil, nil
}

func BenchmarkDefaultDataFileWriter(b *testing.B) {
	const (
		fieldID = 1
		rows    = 128
	)

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: fieldID, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	spec := iceberg.NewPartitionSpec()
	metadata, err := NewMetadata(schema, &spec, UnsortedSortOrder, b.TempDir(), iceberg.Properties{})
	if err != nil {
		b.Fatal(err)
	}
	metaBuilder, err := MetadataBuilderFromBase(metadata, "")
	if err != nil {
		b.Fatal(err)
	}
	arrowSchema, err := SchemaToArrowSchemaWithOptions(schema, ArrowSchemaOptions{IncludeFieldIDs: true})
	if err != nil {
		b.Fatal(err)
	}

	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	for range rows {
		builder.Field(0).(*array.Int64Builder).Append(1)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	writer, err := newDataFileWriter(b.TempDir(), iceio.LocalFS{}, metaBuilder, iceberg.Properties{},
		withFormat(benchmarkNoopDataFileFormat{FileFormat: tblutils.GetFileFormat(iceberg.ParquetFile)}))
	if err != nil {
		b.Fatal(err)
	}

	writeUUID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		record.Retain()
		_, err := writer.writeFile(b.Context(), nil, WriteTask{
			Uuid: writeUUID, ID: i, FileCount: 1, Schema: schema,
			Batches: []arrow.RecordBatch{record},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(rows), "rows/op")
}

func BenchmarkToRequestedSchemaWriteFastPath(b *testing.B) {
	requested := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64,
	})
	requestedArrowSchema, err := SchemaToArrowSchemaWithOptions(requested, ArrowSchemaOptions{IncludeFieldIDs: true})
	if err != nil {
		b.Fatal(err)
	}

	provided := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32,
	})
	providedArrowSchema, err := SchemaToArrowSchemaWithOptions(provided, ArrowSchemaOptions{IncludeFieldIDs: true})
	if err != nil {
		b.Fatal(err)
	}

	opts := SchemaOptions{
		DowncastTimestamp: true,
		IncludeFieldIDs:   true,
		UseWriteDefault:   true,
	}
	for _, rows := range []int{0, 1, 16, 1024, 65536} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			exact := benchmarkIntRecord(b, requestedArrowSchema, rows, true)
			defer exact.Release()
			conversion := benchmarkIntRecord(b, providedArrowSchema, rows, false)
			defer conversion.Release()

			b.Run("projection", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := ToRequestedSchema(b.Context(), requested, requested, exact, opts)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
				b.ReportMetric(float64(rows), "rows/op")
			})

			b.Run("exact_fast_path", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := toRequestedSchema(b.Context(), requested, requested, exact, opts, requestedArrowSchema)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
				b.ReportMetric(float64(rows), "rows/op")
			})

			b.Run("conversion", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := toRequestedSchema(b.Context(), requested, provided, conversion, opts, requestedArrowSchema)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
				b.ReportMetric(float64(rows), "rows/op")
			})
		})
	}
}

func benchmarkIntRecord(b *testing.B, schema *arrow.Schema, rows int, int64Values bool) arrow.RecordBatch {
	b.Helper()

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	if int64Values {
		field := builder.Field(0).(*array.Int64Builder)
		for range rows {
			field.Append(1)
		}
	} else {
		field := builder.Field(0).(*array.Int32Builder)
		for range rows {
			field.Append(1)
		}
	}
	record := builder.NewRecordBatch()
	builder.Release()

	return record
}
