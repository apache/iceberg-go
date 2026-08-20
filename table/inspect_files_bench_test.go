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
)

func BenchmarkInspectContentFileAppender(b *testing.B) {
	for _, fieldCount := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("fields=%d/files=4096", fieldCount), func(b *testing.B) {
			partitionType, files := benchmarkInspectContentFiles(b, fieldCount, 4096)
			arrowSchema, err := SchemaToArrowSchema(DataFilesSchema(partitionType), nil, true, false)
			if err != nil {
				b.Fatal(err)
			}
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer builder.Release()
			appendFile := newInspectContentFileAppender(partitionType)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for _, file := range files {
					if err := appendFile(builder, file); err != nil {
						b.Fatal(err)
					}
				}
				record := builder.NewRecordBatch()
				record.Release()
			}
			b.StopTimer()
		})
	}
}

func benchmarkInspectContentFiles(
	b *testing.B,
	fieldCount int,
	fileCount int,
) (*iceberg.StructType, []iceberg.DataFile) {
	b.Helper()

	schemaFields := make([]iceberg.NestedField, fieldCount)
	partitionFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fieldCount {
		sourceID := i + 1
		fieldID := 1000 + i
		schemaFields[i] = iceberg.NestedField{
			ID: sourceID, Name: fmt.Sprintf("source_%d", sourceID),
			Type: iceberg.PrimitiveTypes.Int32, Required: true,
		}
		partitionFields[i] = iceberg.PartitionField{
			SourceIDs: []int{sourceID}, FieldID: fieldID,
			Name: fmt.Sprintf("partition_%d", fieldID), Transform: iceberg.IdentityTransform{},
		}
	}

	schema := iceberg.NewSchema(0, schemaFields...)
	spec := iceberg.NewPartitionSpec(partitionFields...)
	partitionType := spec.PartitionType(schema)
	files := make([]iceberg.DataFile, fileCount)
	for i := range files {
		values := make(map[int]any, fieldCount)
		for field := range partitionFields {
			values[partitionFields[field].FieldID] = int32(i + field)
		}
		dataFile, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, fmt.Sprintf("file-%d.parquet", i),
			iceberg.ParquetFile, values, nil, nil, 1, 1,
		)
		if err != nil {
			b.Fatal(err)
		}
		files[i] = dataFile.Build()
	}

	return partitionType, files
}
