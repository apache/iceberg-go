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

package glue

import (
	"fmt"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

var schemasToGlueColumnsBenchmarkSink []types.Column

func BenchmarkSchemasToGlueColumns(b *testing.B) {
	for _, tc := range []struct {
		schemaCount int
		fieldCount  int
	}{
		{schemaCount: 1, fieldCount: 16},
		{schemaCount: 10, fieldCount: 128},
		{schemaCount: 100, fieldCount: 128},
	} {
		b.Run(fmt.Sprintf("schemas=%d/fields=%d", tc.schemaCount, tc.fieldCount), func(b *testing.B) {
			metadata := schemaHistoryBenchmarkMetadata(b, tc.schemaCount, tc.fieldCount)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				schemasToGlueColumnsBenchmarkSink = schemasToGlueColumns(metadata, nil)
			}
		})
	}
}

func schemaHistoryBenchmarkMetadata(b *testing.B, schemaCount, fieldCount int) table.Metadata {
	b.Helper()

	fields := make([]iceberg.NestedField, fieldCount)
	for i := range fields {
		fields[i] = iceberg.NestedField{
			ID:   i + 1,
			Name: fmt.Sprintf("field_%d", i+1),
			Type: iceberg.PrimitiveTypes.String,
		}
	}

	base, err := table.NewMetadata(iceberg.NewSchema(0, fields...), nil,
		table.SortOrder{}, "s3://glue-schema-benchmark", nil)
	if err != nil {
		b.Fatal(err)
	}
	builder, err := table.MetadataBuilderFromBase(base, "")
	if err != nil {
		b.Fatal(err)
	}

	for schemaID := 1; schemaID < schemaCount; schemaID++ {
		schemaFields := slices.Clone(fields)
		if schemaID == schemaCount-1 {
			schemaFields[0].Name = "current_field"
		} else {
			schemaFields[0].Name = fmt.Sprintf("historical_field_%d", schemaID)
		}
		if err := builder.AddSchema(iceberg.NewSchema(schemaID, schemaFields...)); err != nil {
			b.Fatal(err)
		}
	}
	if err := builder.SetCurrentSchemaID(schemaCount - 1); err != nil {
		b.Fatal(err)
	}

	metadata, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}

	return metadata
}
