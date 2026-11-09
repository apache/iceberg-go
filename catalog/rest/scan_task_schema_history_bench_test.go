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

package rest

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

func historicalSchemaBenchmarkFixture(schemaCount, fieldCount, sourceCount int) (*cloningSchemaHistoryMetadata, []int) {
	history := make([]*iceberg.Schema, schemaCount)
	for version := range history {
		fields := make([]iceberg.NestedField, fieldCount)
		for index := range fields {
			fields[index] = iceberg.NestedField{
				ID:   1 + version*fieldCount + index,
				Name: fmt.Sprintf("field_%d", index),
				Type: iceberg.PrimitiveTypes.Int64,
			}
		}
		history[version] = iceberg.NewSchema(version, fields...)
	}
	sources := make([]int, sourceCount)
	for index := range sources {
		version := index * schemaCount / sourceCount
		sources[index] = 1 + version*fieldCount + index%fieldCount
	}
	metadata := &cloningSchemaHistoryMetadata{
		scanTaskDecoderMetadata: &scanTaskDecoderMetadata{
			schema: iceberg.NewSchema(schemaCount+1, iceberg.NestedField{
				ID: 900000, Name: "current", Type: iceberg.PrimitiveTypes.String,
			}),
		},
		history: history,
	}

	return metadata, sources
}

var benchmarkPartitionSourceTypeSink int

// BenchmarkPartitionSourceTypeSchemaHistory measures source-type lookups
// with defensive schema copies; it excludes JSON decoding and REST scans.
func BenchmarkPartitionSourceTypeSchemaHistory(b *testing.B) {
	for _, tc := range []struct {
		schemas, fields, sources int
	}{
		{schemas: 16, fields: 32, sources: 4},
		{schemas: 64, fields: 64, sources: 16},
		{schemas: 128, fields: 64, sources: 32},
	} {
		name := fmt.Sprintf("%d_schemas_%d_fields_%d_sources", tc.schemas, tc.fields, tc.sources)
		metadata, sources := historicalSchemaBenchmarkFixture(tc.schemas, tc.fields, tc.sources)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			hits := 0
			for b.Loop() {
				var lookup partitionSourceTypeLookup
				for _, id := range sources {
					if typ, ok := lookup.find(id, metadata); !ok || typ == nil {
						b.Fatal("failed historical source lookup", id)
					}
					hits++
				}
			}
			benchmarkPartitionSourceTypeSink = hits
		})
	}
}
