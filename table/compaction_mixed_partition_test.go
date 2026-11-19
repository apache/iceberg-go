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

package table_test

import (
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// A distributed coordinator can provide groups without calling the local
// compaction planner. Reject mixed partitions before opening any data files.
func TestExecuteCompactionGroupRejectsMixedPartitions(t *testing.T) {
	field := iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition",
		Transform: iceberg.IdentityTransform{},
	}
	specA := iceberg.NewPartitionSpecID(3, field)
	specB := iceberg.NewPartitionSpecID(4, field)

	build := func(path string, spec iceberg.PartitionSpec, part map[int]any) iceberg.DataFile {
		t.Helper()
		b, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
			part, nil, nil, 1, 128,
		)
		require.NoError(t, err)

		return b.Build()
	}
	for _, tt := range []struct {
		name      string
		otherSpec iceberg.PartitionSpec
		a, b      map[int]any
	}{
		{name: "different partition values", otherSpec: specA,
			a: map[int]any{1000: "a;1000=b"}, b: map[int]any{1000: "a"}},
		{name: "different specs even with same values", otherSpec: specB,
			a: map[int]any{1000: "a"}, b: map[int]any{1000: "a"}},
		{name: "positive and negative zero", otherSpec: specA,
			a: map[int]any{1000: math.Copysign(0, -1)}, b: map[int]any{1000: float64(0)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := build("/a.parquet", specA, tt.a)
			b := build("/b.parquet", tt.otherSpec, tt.b)
			_, err := table.ExecuteCompactionGroup(t.Context(), nil, table.CompactionTaskGroup{
				Tasks: []table.FileScanTask{
					{File: a, Length: a.FileSizeBytes()},
					{File: b, Length: b.FileSizeBytes()},
				},
			})
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			require.ErrorContains(t, err, "different partitions")
		})
	}
}
