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

package internal

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var dataFileFinalizationBenchmarkSink int64

type dataFileFinalizationBenchmarkAgg struct {
	value iceberg.Literal
}

func (a *dataFileFinalizationBenchmarkAgg) Min() iceberg.Literal                 { return a.value }
func (a *dataFileFinalizationBenchmarkAgg) Max() iceberg.Literal                 { return a.value }
func (a *dataFileFinalizationBenchmarkAgg) Update(interface{ HasMinMax() bool }) {}
func (a *dataFileFinalizationBenchmarkAgg) MinAsBytes() ([]byte, error)          { return nil, nil }
func (a *dataFileFinalizationBenchmarkAgg) MaxAsBytes() ([]byte, error)          { return nil, nil }

func BenchmarkDataFileStatisticsToDataFile(b *testing.B) {
	for _, fieldCount := range []int{1, 8, 32} {
		schema, spec, stats := dataFileFinalizationBenchmarkData(fieldCount)
		opts := DataFileOpts{
			Schema:   schema,
			Spec:     spec,
			Path:     "s3://bucket/data/file.parquet",
			Format:   iceberg.ParquetFile,
			Content:  iceberg.EntryContentData,
			FileSize: 1024,
		}
		if df := stats.ToDataFile(opts); df.Count() != int64(fieldCount*100) {
			b.Fatalf("unexpected record count: %d", df.Count())
		}

		b.Run(fmt.Sprintf("partition_fields_%d", fieldCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				df := stats.ToDataFile(opts)
				dataFileFinalizationBenchmarkSink = df.Count()
			}
		})
	}
}

func dataFileFinalizationBenchmarkData(fieldCount int) (*iceberg.Schema, iceberg.PartitionSpec, *DataFileStatistics) {
	schemaFields := make([]iceberg.NestedField, fieldCount)
	specFields := make([]iceberg.PartitionField, fieldCount)
	colAggs := make(map[int]StatsAgg, fieldCount)
	for i := range fieldCount {
		sourceID := i + 1
		defaultValue := make([]byte, 32)
		for j := range defaultValue {
			defaultValue[j] = byte(i + j)
		}
		schemaFields[i] = iceberg.NestedField{
			ID:             sourceID,
			Name:           fmt.Sprintf("field_%02d", i),
			Type:           iceberg.PrimitiveTypes.Binary,
			InitialDefault: defaultValue,
			WriteDefault:   defaultValue,
		}
		specFields[i] = iceberg.PartitionField{
			SourceIDs: []int{sourceID},
			FieldID:   1000 + i,
			Name:      fmt.Sprintf("field_%02d", i),
			Transform: iceberg.IdentityTransform{},
		}
		colAggs[sourceID] = &dataFileFinalizationBenchmarkAgg{
			value: iceberg.NewLiteral([]byte{byte(i), byte(i + 1)}),
		}
	}

	return iceberg.NewSchema(0, schemaFields...), iceberg.NewPartitionSpec(specFields...), &DataFileStatistics{
		RecordCount: int64(fieldCount * 100),
		ColAggs:     colAggs,
	}
}
