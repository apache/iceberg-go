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

import "testing"

var snapshotLookupBenchmarkSink int64

func BenchmarkSnapshotByID(b *testing.B) {
	const snapshotCount = 10_000

	snapshots := make([]Snapshot, snapshotCount)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i)
	}

	metadata := commonMetadata{SnapshotList: snapshots}
	metadata.SnapshotByID(int64(snapshotCount - 1))

	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(snapshotCount, "snapshots")
		b.ResetTimer()
		for range b.N {
			snapshotLookupBenchmarkSink = metadata.SnapshotByID(int64(snapshotCount - 1)).SnapshotID
		}
	})

	b.Run("linear", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(snapshotCount, "snapshots")
		b.ResetTimer()
		for range b.N {
			snapshotLookupBenchmarkSink = linearSnapshotByID(snapshots, int64(snapshotCount-1)).SnapshotID
		}
	})
}

func linearSnapshotByID(snapshots []Snapshot, id int64) *Snapshot {
	for i := range snapshots {
		if snapshots[i].SnapshotID == id {
			return cloneSnapshotPtr(&snapshots[i])
		}
	}

	return nil
}
