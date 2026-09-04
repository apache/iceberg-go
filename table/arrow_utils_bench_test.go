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

func BenchmarkComputeStatsPlan(b *testing.B) {
	for _, fieldCount := range []int{100, 1000, 10000} {
		for _, benchmarkCase := range []struct {
			name                string
			defaultMode         string
			overrideStride      int
			unrelatedProperties int
		}{
			{name: "default", defaultMode: "truncate(16)"},
			{name: "one_percent_overrides", defaultMode: "truncate(16)", overrideStride: 100},
			{name: "one_percent_overrides_many_properties", defaultMode: "truncate(16)", overrideStride: 100, unrelatedProperties: 1000},
		} {
			b.Run(fmt.Sprintf("fields=%d/%s", fieldCount, benchmarkCase.name), func(b *testing.B) {
				schema := benchmarkMetricsSchema(fieldCount)
				props := iceberg.Properties{DefaultWriteMetricsModeKey: benchmarkCase.defaultMode}
				for i := 0; benchmarkCase.overrideStride > 0 && i < fieldCount; i += benchmarkCase.overrideStride {
					props[MetricsModeColumnConfPrefix+fmt.Sprintf(".field_%d", i)] = "counts"
				}
				for i := range benchmarkCase.unrelatedProperties {
					props[fmt.Sprintf("unrelated.property.%d", i)] = "value"
				}

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					plan, err := computeStatsPlan(schema, props)
					if err != nil {
						b.Fatal(err)
					}
					if len(plan) != fieldCount {
						b.Fatalf("expected %d stats columns, got %d", fieldCount, len(plan))
					}
				}
			})
		}
	}
}

func benchmarkMetricsSchema(fieldCount int) *iceberg.Schema {
	fields := make([]iceberg.NestedField, fieldCount)
	for i := range fields {
		fields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     fmt.Sprintf("field_%d", i),
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		}
	}

	return iceberg.NewSchema(0, fields...)
}
