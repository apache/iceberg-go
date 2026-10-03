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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

type countingPartitionSnapshotsMetadata struct {
	Metadata
	calls int
}

func (m *countingPartitionSnapshotsMetadata) Snapshots() []Snapshot {
	m.calls++
	return m.Metadata.Snapshots()
}

func TestInspectPartitionsReadsSnapshotHistoryOnce(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		count, calls int
	}{
		{"no snapshot", 0, 0},
		{"one snapshot", 1, 1},
		{"historical snapshot", 4, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tbl := inspectPartitionSnapshotHistoryTable(t, tt.count)
			before := tbl.metadata.Snapshots()
			counted := &countingPartitionSnapshotsMetadata{Metadata: tbl.metadata}
			tbl.metadata = counted
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			rr, err := tbl.Inspect(WithInspectAllocator(mem)).Partitions(t.Context())
			require.NoError(t, err)
			defer rr.Release()
			record := collectRecord(t, rr)
			defer record.Release()
			require.Equal(t, tt.calls, counted.calls)
			if tt.count == 0 {
				require.Zero(t, record.NumRows())
			} else {
				require.EqualValues(t, 1, record.NumRows())
				entryID := max(int64(1), int64(tt.count)/2)
				require.EqualValues(t, entryID*1000*1000, record.Column(9).(*array.Timestamp).Value(0))
				require.Equal(t, entryID, record.Column(10).(*array.Int64).Value(0))
			}
			require.Equal(t, before, counted.Metadata.Snapshots())
		})
	}
}
