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
