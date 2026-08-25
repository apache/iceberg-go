// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func benchmarkScanSchema(fieldCount int) *iceberg.Schema {
	fields := make([]iceberg.NestedField, fieldCount)
	for i := range fields {
		baseID := i*3 + 1
		fields[i] = iceberg.NestedField{
			ID:   baseID,
			Name: fmt.Sprintf("field_%d", i),
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: baseID + 1, Name: "value", Type: iceberg.PrimitiveTypes.String},
				{ID: baseID + 2, Name: "count", Type: iceberg.PrimitiveTypes.Int64},
			}},
		}
	}

	return iceberg.NewSchema(1, fields...)
}

func benchmarkScanMetadata(b *testing.B, fieldCount, propertyCount int) Metadata {
	b.Helper()

	props := make(iceberg.Properties, propertyCount)
	for i := range propertyCount {
		props[fmt.Sprintf("property_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	metadata, err := NewMetadata(
		benchmarkScanSchema(fieldCount),
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"mem://benchmark/table",
		props,
	)
	if err != nil {
		b.Fatal(err)
	}

	return metadata
}

func benchmarkScanJSON(rowCount, fieldCount int) string {
	var result strings.Builder
	result.WriteByte('[')
	for row := range rowCount {
		if row > 0 {
			result.WriteByte(',')
		}
		result.WriteByte('{')
		for field := range fieldCount {
			if field > 0 {
				result.WriteByte(',')
			}
			fmt.Fprintf(&result, `"field_%d":{"value":"value_%d","count":%d}`,
				field, field, row)
		}
		result.WriteByte('}')
	}
	result.WriteByte(']')

	return result.String()
}

func BenchmarkArrowScanManyFilesAndBatches(b *testing.B) {
	const (
		fieldCount = 16
		fileCount  = 32
		batchCount = 32
	)

	for _, propertyCount := range []int{0, 16, 64} {
		b.Run(fmt.Sprintf("extra_properties_%d", propertyCount), func(b *testing.B) {
			metadata := benchmarkScanMetadata(b, fieldCount, propertyCount)
			properties := metadata.Properties()
			properties[ParquetBatchSizeKey] = "1"
			metadata, err := NewMetadata(
				metadata.CurrentSchema(),
				iceberg.UnpartitionedSpec,
				UnsortedSortOrder,
				"mem://benchmark/table",
				properties,
			)
			if err != nil {
				b.Fatal(err)
			}

			projectedSchema := metadata.CurrentSchema()
			arrowSchema, err := SchemaToArrowSchemaWithOptions(projectedSchema, ArrowSchemaOptions{IncludeFieldIDs: true})
			if err != nil {
				b.Fatal(err)
			}
			record := mustLoadRecordBatchFromJSON(arrowSchema, benchmarkScanJSON(batchCount, fieldCount))
			defer record.Release()
			table := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
			defer table.Release()

			const dataPath = "mem://benchmark/table/data.parquet"
			fs := iceio.NewMemFS()
			writer, err := fs.Create(dataPath)
			if err != nil {
				b.Fatal(err)
			}
			if err := pqarrow.WriteTable(table, writer, record.NumRows(),
				parquet.NewWriterProperties(parquet.WithStats(true)),
				pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}

			dataFile, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec,
				iceberg.EntryContentData,
				dataPath,
				iceberg.ParquetFile,
				nil,
				nil,
				nil,
				record.NumRows(),
				1,
			)
			if err != nil {
				b.Fatal(err)
			}
			tasks := make([]FileScanTask, fileCount)
			for i := range tasks {
				tasks[i].File = dataFile.Build()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				scan := &arrowScan{
					metadata:        metadata,
					fs:              fs,
					projectedSchema: projectedSchema,
					boundRowFilter:  iceberg.AlwaysTrue{},
					rowLimit:        -1,
					concurrency:     4,
				}
				_, records, err := scan.GetRecords(b.Context(), tasks)
				if err != nil {
					b.Fatal(err)
				}
				batches := 0
				for record, err := range records {
					if err != nil {
						b.Fatal(err)
					}
					record.Release()
					batches++
				}
				if batches != fileCount*batchCount {
					b.Fatalf("unexpected batch count: %d", batches)
				}
			}
			b.ReportMetric(fileCount, "files/op")
			b.ReportMetric(fileCount*batchCount, "batches/op")
		})
	}
}
