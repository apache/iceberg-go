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

package iceberg_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var partitionTypeBenchmarkResult *iceberg.StructType

func BenchmarkPartitionTypeFieldCount(b *testing.B) {
	for _, numFields := range []int{0, 1, 3, 20, 100} {
		b.Run(fmt.Sprintf("fields=%d", numFields), func(b *testing.B) {
			fields := make([]iceberg.NestedField, numFields)
			partitionFields := make([]iceberg.PartitionField, numFields)
			for i := range fields {
				name := fmt.Sprintf("field_%d", i)
				fields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: iceberg.StringType{}}
				partitionFields[i] = iceberg.PartitionField{
					SourceIDs: []int{i + 1}, FieldID: 1000 + i, Name: name,
					Transform: iceberg.IdentityTransform{},
				}
			}
			schema := iceberg.NewSchema(0, fields...)
			spec := iceberg.NewPartitionSpec(partitionFields...)
			partitionTypeBenchmarkResult = spec.PartitionType(schema)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				partitionTypeBenchmarkResult = spec.PartitionType(schema)
			}
		})
	}
}
