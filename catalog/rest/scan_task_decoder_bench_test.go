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
)

var decodeScanTasksBenchmarkSink int

func BenchmarkDecodeScanTasksDeletionVectors(b *testing.B) {
	metadata := newScanTaskDecoderMetadata()

	for _, tc := range []struct {
		name          string
		taskCount     int
		explicitOwner bool
	}{
		{name: "explicit-owner/tasks=100", taskCount: 100, explicitOwner: true},
		{name: "derived-owner/tasks=100", taskCount: 100},
		{name: "explicit-owner/tasks=1000", taskCount: 1000, explicitOwner: true},
		{name: "derived-owner/tasks=1000", taskCount: 1000},
		{name: "explicit-owner/tasks=10000", taskCount: 10000, explicitOwner: true},
		{name: "derived-owner/tasks=10000", taskCount: 10000},
	} {
		wire := deletionVectorScanTasksWire(tc.taskCount, tc.explicitOwner)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				tasks, err := DecodeScanTasks(wire, metadata, metadata.schema, nil)
				if err != nil {
					b.Fatal(err)
				}
				decodeScanTasksBenchmarkSink = len(tasks)
			}
		})
	}
}

func deletionVectorScanTasksWire(taskCount int, explicitOwner bool) ScanTasks {
	base := validScanTasksWire()
	dataFile := *base.FileScanTasks[0].DataFile
	deleteFile := base.DeleteFiles[0]
	deleteFile.FileFormat = "puffin"
	deleteFile.ContentOffset = int64Ptr(10)
	deleteFile.ContentSizeInBytes = int64Ptr(20)

	wire := ScanTasks{
		FileScanTasks: make([]RESTFileScanTask, taskCount),
		DeleteFiles:   make([]RESTDeleteFile, taskCount),
	}
	for i := range taskCount {
		data := dataFile
		data.FilePath = fmt.Sprintf("s3://bucket/table/data-%d.parquet", i)
		delete := deleteFile
		delete.FilePath = fmt.Sprintf("s3://bucket/table/delete-%d.puffin", i)
		if explicitOwner {
			delete.ReferencedDataFile = stringPtr(data.FilePath)
		}

		wire.FileScanTasks[i] = RESTFileScanTask{
			DataFile:             &data,
			DeleteFileReferences: []int{i},
		}
		wire.DeleteFiles[i] = delete
	}

	return wire
}
