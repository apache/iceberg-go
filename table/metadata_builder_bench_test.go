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
	"slices"
	"testing"
)

var removeSnapshotsBenchmarkSink int

func BenchmarkRemoveSnapshots(b *testing.B) {
	for _, snapshotCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("snapshots=%d", snapshotCount), func(b *testing.B) {
			log := make([]SnapshotLogEntry, snapshotCount)
			for i := range log {
				log[i] = SnapshotLogEntry{SnapshotID: int64(i), TimestampMs: int64(i)}
			}

			removed := make([]int64, snapshotCount/2)
			for i := range removed {
				removed[i] = int64(i * 2)
			}

			builder := &MetadataBuilder{}
			b.ReportAllocs()
			b.ReportMetric(float64(snapshotCount), "snapshot_entries")
			b.ReportMetric(float64(len(removed)), "removed_snapshots")
			b.ResetTimer()

			for range b.N {
				b.StopTimer()
				builder.snapshotLog = slices.Clone(log)
				builder.updates = nil
				b.StartTimer()

				if err := builder.RemoveSnapshots(removed, false); err != nil {
					b.Fatal(err)
				}
				removeSnapshotsBenchmarkSink = len(builder.snapshotLog)
			}
		})
	}
}
