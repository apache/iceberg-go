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

type inspectPublicStatsDataFile struct {
	iceberg.DataFile
}

func (f *inspectPublicStatsDataFile) Partition() map[int]any {
	return f.DataFile.Partition()
}

func (f *inspectPublicStatsDataFile) ValueCounts() map[int]int64 {
	return f.DataFile.ValueCounts()
}

func (f *inspectPublicStatsDataFile) NullValueCounts() map[int]int64 {
	return f.DataFile.NullValueCounts()
}

func (f *inspectPublicStatsDataFile) NaNValueCounts() map[int]int64 {
	return f.DataFile.NaNValueCounts()
}

func (f *inspectPublicStatsDataFile) LowerBoundValues() map[int][]byte {
	return f.DataFile.LowerBoundValues()
}

func (f *inspectPublicStatsDataFile) UpperBoundValues() map[int][]byte {
	return f.DataFile.UpperBoundValues()
}

func BenchmarkInspectContentFileAppenderDataFileStats(b *testing.B) {
	for _, fileCount := range []int{4096, 16384} {
		for _, statsWidth := range []int{0, 8, 32, 128} {
			b.Run(fmt.Sprintf("files=%d/stats=%d", fileCount, statsWidth), func(b *testing.B) {
				partitionType, files := benchmarkInspectContentFilesWithStats(b, statsWidth, fileCount)
				publicFiles := make([]iceberg.DataFile, len(files))
				for i, file := range files {
					publicFiles[i] = &inspectPublicStatsDataFile{DataFile: file}
				}

				b.Run("borrowed", func(b *testing.B) {
					benchmarkAppendInspectContentFiles(b, partitionType, files)
				})
				b.Run("public", func(b *testing.B) {
					benchmarkAppendInspectContentFiles(b, partitionType, publicFiles)
				})
			})
		}
	}
}

func benchmarkAppendInspectContentFiles(
	b *testing.B,
	partitionType *iceberg.StructType,
	files []iceberg.DataFile,
) {
	b.Helper()

	arrowSchema, err := SchemaToArrowSchema(DataFilesSchema(partitionType), nil, true, false)
	if err != nil {
		b.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	appendFile := newInspectContentFileAppender(partitionType)

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(files)), "files/op")
	for range b.N {
		for _, file := range files {
			if err := appendFile(builder, file); err != nil {
				b.Fatal(err)
			}
		}
		record := builder.NewRecordBatch()
		record.Release()
	}
}

func benchmarkInspectContentFilesWithStats(
	b *testing.B,
	statsWidth int,
	fileCount int,
) (*iceberg.StructType, []iceberg.DataFile) {
	b.Helper()

	schemaWidth := max(statsWidth, 1)
	schemaFields := make([]iceberg.NestedField, schemaWidth)
	for i := range schemaFields {
		fieldID := i + 1
		schemaFields[i] = iceberg.NestedField{
			ID: fieldID, Name: fmt.Sprintf("field_%d", fieldID),
			Type: iceberg.PrimitiveTypes.Int32, Required: true,
		}
	}
	schema := iceberg.NewSchema(0, schemaFields...)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: iceberg.IdentityTransform{},
	})
	partitionType := spec.PartitionType(schema)

	files := make([]iceberg.DataFile, fileCount)
	for i := range files {
		builder, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, fmt.Sprintf("file-%d.parquet", i),
			iceberg.ParquetFile, map[int]any{1000: int32(i)}, nil, nil, 1, 1,
		)
		if err != nil {
			b.Fatal(err)
		}
		if statsWidth > 0 {
			columnSizes := make(map[int]int64, statsWidth)
			valueCounts := make(map[int]int64, statsWidth)
			nullCounts := make(map[int]int64, statsWidth)
			nanCounts := make(map[int]int64, statsWidth)
			lowerBounds := make(map[int][]byte, statsWidth)
			upperBounds := make(map[int][]byte, statsWidth)
			for field := range statsWidth {
				fieldID := field + 1
				columnSizes[fieldID] = int64(field + 1)
				valueCounts[fieldID] = int64(i + 1)
				nullCounts[fieldID] = int64(field)
				nanCounts[fieldID] = 0
				lowerBounds[fieldID] = []byte{byte(field), byte(i)}
				upperBounds[fieldID] = []byte{byte(field + 1), byte(i)}
			}
			builder = builder.
				ColumnSizes(columnSizes).
				ValueCounts(valueCounts).
				NullValueCounts(nullCounts).
				NaNValueCounts(nanCounts).
				LowerBoundValues(lowerBounds).
				UpperBoundValues(upperBounds)
		}
		files[i] = builder.Build()
	}

	return partitionType, files
}
