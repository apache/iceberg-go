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

func BenchmarkDecodeScanTasks(b *testing.B) {
	metadata := newScanTaskDecoderMetadata()
	base := validScanTasksWire().FileScanTasks[0].DataFile

	for _, taskCount := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("%d_tasks", taskCount), func(b *testing.B) {
			wire := ScanTasks{FileScanTasks: make([]RESTFileScanTask, taskCount)}
			for i := range wire.FileScanTasks {
				dataFile := *base
				wire.FileScanTasks[i] = RESTFileScanTask{
					DataFile: &dataFile,
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := DecodeScanTasks(wire, metadata, metadata.schema, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
