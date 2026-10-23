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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var residualReuseBenchmarkSink iceberg.BooleanExpression

func BenchmarkScanTaskResidualReuse(b *testing.B) {
	metadata := newScanTaskDecoderMetadata()
	for _, taskCount := range []int{64, 1024} {
		for _, distribution := range []string{"identical", "runs-of-16", "unique", "missing"} {
			residuals := make([]json.RawMessage, taskCount)
			for i := range residuals {
				if distribution == "missing" {
					continue
				}
				value := 34
				switch distribution {
				case "runs-of-16":
					value += i / 16
				case "unique":
					value += i
				}
				// Give each task its own JSON buffer, as in real decoded wire payloads.
				residuals[i] = json.RawMessage(fmt.Sprintf(`{"type":"eq","term":"id","value":%d}`, value))
			}
			for _, variant := range []struct {
				name   string
				cached bool
			}{{"uncached", false}, {"cached", true}} {
				b.Run(fmt.Sprintf("tasks=%d/%s/%s", taskCount, distribution, variant.name), func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						var cache taskResidualDecoder
						for _, raw := range residuals {
							var (
								value iceberg.BooleanExpression
								err   error
							)
							if variant.cached {
								value, err = cache.decode(raw, metadata.schema, nil)
							} else {
								value, err = decodeTaskResidual(raw, metadata.schema, nil)
							}
							if err != nil {
								b.Fatal(err)
							}
							residualReuseBenchmarkSink = value
						}
					}
				})
			}
		}
	}
}

func BenchmarkDecodeScanTasksWithResiduals(b *testing.B) {
	metadata := newScanTaskDecoderMetadata()
	base := validScanTasksWire().FileScanTasks[0].DataFile
	for _, distribution := range []string{"identical", "unique", "missing"} {
		const taskCount = 1024
		wire := ScanTasks{FileScanTasks: make([]RESTFileScanTask, taskCount)}
		for i := range wire.FileScanTasks {
			data := *base
			var raw json.RawMessage
			if distribution != "missing" {
				value := 34
				if distribution == "unique" {
					value += i
				}
				raw = json.RawMessage(fmt.Sprintf(`{"type":"eq","term":"id","value":%d}`, value))
			}
			wire.FileScanTasks[i] = RESTFileScanTask{DataFile: &data, ResidualFilter: raw}
		}
		b.Run(distribution, func(b *testing.B) {
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
