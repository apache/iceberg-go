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

	"github.com/apache/iceberg-go"
)

var inspectPartitionAggregationBenchmarkSink int

func BenchmarkInspectPartitionAggregation(b *testing.B) {
	for _, benchmark := range []struct {
		name           string
		fileCount      int
		partitionCount int
		fieldCount     int
		binary         bool
	}{
		{name: "int32", fileCount: 10_000, partitionCount: 100, fieldCount: 1},
		{name: "int32", fileCount: 100_000, partitionCount: 100, fieldCount: 8},
		{name: "binary", fileCount: 100_000, partitionCount: 100, fieldCount: 32, binary: true},
	} {
		b.Run(fmt.Sprintf("%s/files=%d/partitions=%d/fields=%d",
			benchmark.name, benchmark.fileCount, benchmark.partitionCount, benchmark.fieldCount), func(b *testing.B) {
			partitionType, files := benchmarkInspectPartitionFiles(b, benchmark)
			b.Run("before", func(b *testing.B) {
				benchmarkInspectPartitionAggregation(b, partitionType, files, true)
			})
			b.Run("after", func(b *testing.B) {
				benchmarkInspectPartitionAggregation(b, partitionType, files, false)
			})
		})
	}
}

func benchmarkInspectPartitionAggregation(
	b *testing.B,
	partitionType *iceberg.StructType,
	files []iceberg.DataFile,
	materialize bool,
) {
	b.Helper()
	b.ReportAllocs()
	b.ReportMetric(float64(len(files)), "files/op")
	b.ResetTimer()
	for b.Loop() {
		tree := newInspectPartitionAggregateTree()
		aggregateCount := 0
		for _, file := range files {
			var aggregate *inspectPartitionAggregate
			if materialize {
				partition := inspectCoercePartition(file.Partition(), partitionType)
				record := newPartitionRecord(partition, partitionType)
				aggregate = tree.lookup(record)
				if aggregate == nil {
					aggregate = &inspectPartitionAggregate{
						partition:       cloneInspectPartition(partition),
						partitionRecord: record,
						orderingKey:     inspectPartitionKey(partition),
					}
					tree.insert(record, aggregate)
					aggregateCount++
				}
			} else {
				partitionValues := dataFilePartition(file)
				aggregate = tree.lookupPartition(partitionValues, partitionType)
				if aggregate == nil {
					partition := inspectCoercePartition(partitionValues, partitionType)
					record := newPartitionRecord(partition, partitionType)
					aggregate = &inspectPartitionAggregate{
						partition:       cloneInspectPartition(partition),
						partitionRecord: record,
						orderingKey:     inspectPartitionKey(partition),
					}
					tree.insert(record, aggregate)
					aggregateCount++
				}
			}

			aggregate.dataFileCount++
		}
		inspectPartitionAggregationBenchmarkSink = aggregateCount
	}
}

func benchmarkInspectPartitionFiles(
	b *testing.B,
	benchmark struct {
		name           string
		fileCount      int
		partitionCount int
		fieldCount     int
		binary         bool
	},
) (*iceberg.StructType, []iceberg.DataFile) {
	b.Helper()

	schemaFields := make([]iceberg.NestedField, benchmark.fieldCount)
	partitionFields := make([]iceberg.PartitionField, benchmark.fieldCount)
	for i := range benchmark.fieldCount {
		sourceID := i + 1
		fieldID := 1000 + i
		fieldType := iceberg.PrimitiveTypes.Int32
		if benchmark.binary {
			fieldType = iceberg.PrimitiveTypes.Binary
		}
		schemaFields[i] = iceberg.NestedField{
			ID: sourceID, Name: fmt.Sprintf("source_%d", sourceID), Type: fieldType, Required: true,
		}
		partitionFields[i] = iceberg.PartitionField{
			SourceIDs: []int{sourceID}, FieldID: fieldID,
			Name: fmt.Sprintf("partition_%d", fieldID), Transform: iceberg.IdentityTransform{},
		}
	}

	schema := iceberg.NewSchema(0, schemaFields...)
	spec := iceberg.NewPartitionSpec(partitionFields...)
	partitionType := spec.PartitionType(schema)
	files := make([]iceberg.DataFile, benchmark.fileCount)
	for i := range benchmark.fileCount {
		partitionID := i % benchmark.partitionCount
		partition := make(map[int]any, benchmark.fieldCount)
		for field, partitionField := range partitionFields {
			if benchmark.binary {
				partition[partitionField.FieldID] = []byte(fmt.Sprintf("partition-%d-field-%d", partitionID, field))
			} else {
				partition[partitionField.FieldID] = int32(partitionID + field)
			}
		}

		file, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, fmt.Sprintf("file-%d.parquet", i),
			iceberg.ParquetFile, partition, nil, nil, 1, 1,
		)
		if err != nil {
			b.Fatal(err)
		}
		files[i] = file.Build()
	}

	return partitionType, files
}
