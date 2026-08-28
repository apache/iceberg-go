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

var updateSpecBuildBenchmarkSink int

// BenchmarkUpdateSpecBuildUpdates measures schema resolution while validating
// existing and newly added partition fields. BuildUpdates consumes the queued
// operations, so each iteration creates a fresh UpdateSpec.
func BenchmarkUpdateSpecBuildUpdates(b *testing.B) {
	for _, tc := range []struct {
		schemaFields   int
		existingFields int
		addedFields    int
		nested         bool
	}{
		{schemaFields: 100, existingFields: 8, addedFields: 4},
		{schemaFields: 1_000, existingFields: 32, addedFields: 16},
		{schemaFields: 1_000, existingFields: 32, addedFields: 16, nested: true},
	} {
		name := fmt.Sprintf("schema=%d/partitions=%d/additions=%d/nested=%t",
			tc.schemaFields, tc.existingFields, tc.addedFields, tc.nested)
		b.Run(name, func(b *testing.B) {
			schema, sourceNames, sourceIDs := benchmarkUpdateSpecSchema(tc.schemaFields, tc.nested)
			partitionFields := make([]iceberg.PartitionField, tc.existingFields)
			for i := range tc.existingFields {
				partitionFields[i] = iceberg.PartitionField{
					SourceIDs: []int{sourceIDs[i]},
					FieldID:   iceberg.PartitionDataIDStart + i,
					Name:      fmt.Sprintf("partition_%04d", i),
					Transform: iceberg.IdentityTransform{},
				}
			}
			partitionSpec := iceberg.NewPartitionSpec(partitionFields...)
			metadata, err := NewMetadata(schema, &partitionSpec, UnsortedSortOrder,
				"s3://bucket/table", nil)
			if err != nil {
				b.Fatal(err)
			}
			tbl := New([]string{"benchmark"}, metadata, "", nil, nil)
			txn := tbl.NewTransaction()

			b.ReportAllocs()
			b.ReportMetric(float64(tc.schemaFields), "schema_fields")
			b.ReportMetric(float64(tc.existingFields), "existing_partitions")
			b.ReportMetric(float64(tc.addedFields), "added_partitions")
			b.ResetTimer()

			for range b.N {
				update := NewUpdateSpec(txn, false)
				for i := tc.existingFields; i < tc.existingFields+tc.addedFields; i++ {
					update.AddField(sourceNames[i], iceberg.IdentityTransform{},
						fmt.Sprintf("added_%04d", i))
				}
				updates, requirements, err := update.BuildUpdates()
				if err != nil {
					b.Fatal(err)
				}
				updateSpecBuildBenchmarkSink = len(updates) + len(requirements)
			}
		})
	}
}

func benchmarkUpdateSpecSchema(fieldCount int, nested bool) (*iceberg.Schema, []string, []int) {
	fields := make([]iceberg.NestedField, fieldCount)
	names := make([]string, fieldCount)
	sourceIDs := make([]int, fieldCount)
	nextID := fieldCount + 1
	for i := range fieldCount {
		name := fmt.Sprintf("field_%04d", i)
		names[i] = name
		if !nested {
			fields[i] = iceberg.NestedField{
				ID:       i + 1,
				Name:     name,
				Required: true,
				Type:     iceberg.PrimitiveTypes.Int64,
			}
			sourceIDs[i] = i + 1
			continue
		}

		nestedFields := []iceberg.NestedField{
			{ID: nextID, Name: "value", Required: true, Type: iceberg.PrimitiveTypes.Int64},
			{ID: nextID + 1, Name: "label", Required: false, Type: iceberg.PrimitiveTypes.String},
		}
		sourceIDs[i] = nextID
		nextID += len(nestedFields)
		fields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     name,
			Required: true,
			Type:     &iceberg.StructType{FieldList: nestedFields},
		}
		names[i] += ".value"
	}

	return iceberg.NewSchema(1, fields...), names, sourceIDs
}
