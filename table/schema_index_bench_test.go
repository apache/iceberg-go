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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
)

var schemaLookupBenchmarkSink int

func BenchmarkCommonMetadataSchemaByID(b *testing.B) {
	for _, schemaCount := range []int{1, 16, 128, 1_024, 8_192} {
		schemas := schemaIndexBenchmarkSchemas(schemaCount)
		metadata := commonMetadata{
			SchemaList:  schemas,
			schemaIndex: buildSchemaIndex(schemas),
		}

		for _, tt := range []struct {
			name string
			id   int
		}{
			{name: "first", id: schemas[0].ID},
			{name: "middle", id: schemas[schemaCount/2].ID},
			{name: "last", id: schemas[schemaCount-1].ID},
			{name: "miss", id: -1},
		} {
			b.Run(schemaCountName(schemaCount)+"/"+tt.name+"/indexed", func(b *testing.B) {
				benchmarkSchemaLookup(b, metadata.schemaByID, tt.id, schemaCount)
			})
			b.Run(schemaCountName(schemaCount)+"/"+tt.name+"/linear", func(b *testing.B) {
				benchmarkSchemaLookup(b, func(id int) *iceberg.Schema {
					return linearMetadataSchemaByID(schemas, id)
				}, tt.id, schemaCount)
			})
		}
	}
}

func BenchmarkMetadataBuilderGetSchemaByID(b *testing.B) {
	for _, schemaCount := range []int{1, 16, 128, 1_024, 8_192} {
		schemas := schemaIndexBenchmarkSchemas(schemaCount)
		builder := MetadataBuilder{
			schemaList:  schemas,
			schemaIndex: buildSchemaIndex(schemas),
		}

		for _, tt := range []struct {
			name string
			id   int
		}{
			{name: "first", id: schemas[0].ID},
			{name: "middle", id: schemas[schemaCount/2].ID},
			{name: "last", id: schemas[schemaCount-1].ID},
			{name: "miss", id: -1},
		} {
			b.Run(schemaCountName(schemaCount)+"/"+tt.name+"/indexed", func(b *testing.B) {
				benchmarkBuilderSchemaLookup(b, builder.GetSchemaByID, tt.id, schemaCount)
			})
			b.Run(schemaCountName(schemaCount)+"/"+tt.name+"/linear", func(b *testing.B) {
				benchmarkBuilderSchemaLookup(b, func(id int) (*iceberg.Schema, error) {
					return linearBuilderSchemaByID(schemas, id)
				}, tt.id, schemaCount)
			})
		}
	}
}

func benchmarkSchemaLookup(b *testing.B, lookup func(int) *iceberg.Schema, id, schemaCount int) {
	b.Helper()
	b.ReportAllocs()
	b.ReportMetric(float64(schemaCount), "schemas")
	b.ResetTimer()
	for range b.N {
		schema := lookup(id)
		if schema == nil {
			schemaLookupBenchmarkSink = -1
		} else {
			schemaLookupBenchmarkSink = schema.ID
		}
	}
}

func benchmarkBuilderSchemaLookup(b *testing.B, lookup func(int) (*iceberg.Schema, error), id, schemaCount int) {
	b.Helper()
	b.ReportAllocs()
	b.ReportMetric(float64(schemaCount), "schemas")
	b.ResetTimer()
	for range b.N {
		schema, err := lookup(id)
		if err != nil {
			schemaLookupBenchmarkSink = -1
		} else {
			schemaLookupBenchmarkSink = schema.ID
		}
	}
}

func linearMetadataSchemaByID(schemas []*iceberg.Schema, id int) *iceberg.Schema {
	schema := linearSchemaByID(schemas, id)
	if schema == nil {
		return nil
	}

	return cloneSchema(schema)
}

func linearSchemaByID(schemas []*iceberg.Schema, id int) *iceberg.Schema {
	for _, schema := range schemas {
		if schema != nil && schema.ID == id {
			return schema
		}
	}

	return nil
}

func linearBuilderSchemaByID(schemas []*iceberg.Schema, id int) (*iceberg.Schema, error) {
	if schema := linearSchemaByID(schemas, id); schema != nil {
		return schema, nil
	}

	return nil, fmt.Errorf("%w: schema with id %d not found", iceberg.ErrInvalidArgument, id)
}

func schemaIndexBenchmarkSchemas(count int) []*iceberg.Schema {
	schemas := make([]*iceberg.Schema, count)
	for i := range count {
		schemas[i] = iceberg.NewSchema(i)
	}

	return schemas
}

func schemaCountName(count int) string {
	return "schemas=" + strconv.Itoa(count)
}
