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
	"testing"

	"github.com/apache/iceberg-go"
)

func BenchmarkDecodeScanTasksPrimitivePartitions(b *testing.B) {
	metadata := newScanTaskDecoderMetadata()
	metadata.spec = iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "category_part", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{5}, FieldID: 1002, Name: "code_part", Transform: iceberg.IdentityTransform{}},
	)
	base := validScanTasksWire().FileScanTasks[0].DataFile
	b.Run("1024_tasks", func(b *testing.B) {
		wire := ScanTasks{FileScanTasks: make([]RESTFileScanTask, 1024)}
		for i := range wire.FileScanTasks {
			dataFile := *base
			wire.FileScanTasks[i] = RESTFileScanTask{DataFile: &dataFile}
		}
		b.ReportAllocs()
		for b.Loop() {
			tasks, err := DecodeScanTasks(wire, metadata, metadata.schema, nil)
			if err != nil {
				b.Fatal(err)
			}
			decodeScanTasksBenchmarkSink = len(tasks)
		}
	})
}
