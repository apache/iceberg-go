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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
)

var clonePartitionSpecsBenchmarkSink []iceberg.PartitionSpec

func BenchmarkClonePartitionSpecs(b *testing.B) {
	for _, fieldCount := range []int{1, 8, 32} {
		b.Run("fields="+strconv.Itoa(fieldCount), func(b *testing.B) {
			specs := []iceberg.PartitionSpec{
				iceberg.NewPartitionSpecID(1, partitionSpecCloneBenchmarkFields(fieldCount)...),
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				clonePartitionSpecsBenchmarkSink = clonePartitionSpecs(specs)
			}
		})
	}
}

func partitionSpecCloneBenchmarkFields(count int) []iceberg.PartitionField {
	fields := make([]iceberg.PartitionField, count)
	for i := range fields {
		fields[i] = iceberg.PartitionField{
			SourceIDs: []int{i + 1}, FieldID: i + 1000,
			Name: "field", Transform: iceberg.IdentityTransform{},
		}
	}

	return fields
}
