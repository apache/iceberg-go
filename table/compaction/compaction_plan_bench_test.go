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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compaction_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// Keep the entire plan reachable so the benchmark measures construction of
// the groups, not just candidate classification.
var compactionPlanBenchmarkSink compaction.Plan

func TestPlanCompaction_SkippedFilesDoNotChangeCandidateGroups(t *testing.T) {
	cfg := compaction.DefaultConfig()

	candidateTasks := make([]table.FileScanTask, 0, 12)
	for i := range 6 {
		candidateTasks = append(candidateTasks,
			makeTask(newPartitionedDataFile(fmt.Sprintf("a-%d.parquet", i), 10, map[int]any{1000: "a"}), 0, 0))
		candidateTasks = append(candidateTasks,
			makeTask(newPartitionedDataFile(fmt.Sprintf("b-%d.parquet", i), 10, map[int]any{1000: "b"}), 0, 0))
	}

	mixedTasks := make([]table.FileScanTask, 0, len(candidateTasks)*2)
	for i, task := range candidateTasks {
		// Mix skipped files in existing partitions and in distinct
		// partitions that do not contain any candidate files.
		skipPartition := map[int]any{1000: fmt.Sprintf("unrelated-%d", i)}
		if i%3 == 0 {
			skipPartition = map[int]any{1000: "a"}
		}
		mixedTasks = append(mixedTasks,
			makeTask(newPartitionedDataFile(fmt.Sprintf("skip-%d.parquet", i), 500, skipPartition), 0, 0),
			task,
		)
	}

	want, err := cfg.PlanCompaction(candidateTasks)
	require.NoError(t, err)
	got, err := cfg.PlanCompaction(mixedTasks)
	require.NoError(t, err)

	require.Equal(t, want.Groups, got.Groups)
	require.Equal(t, want.EstOutputFiles, got.EstOutputFiles)
	require.Equal(t, want.EstOutputBytes, got.EstOutputBytes)
	require.Equal(t, want.SkippedFiles+len(candidateTasks), got.SkippedFiles)
	require.Equal(t, len(mixedTasks), got.TotalInputFiles)
	require.Equal(t, want.TotalInputBytes+int64(len(candidateTasks))*500*1024*1024, got.TotalInputBytes)
}

func TestPlanCompaction_AllSkippedWithManyPartitions(t *testing.T) {
	cfg := compaction.DefaultConfig()
	tasks := make([]table.FileScanTask, 128)
	for i := range tasks {
		tasks[i] = makeTask(newPartitionedDataFile(fmt.Sprintf("optimal-%d", i), 500,
			map[int]any{1: i, 2: "region", 3: true}), 0, 0)
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.Empty(t, plan.Groups)
	require.Equal(t, len(tasks), plan.TotalInputFiles)
	require.Equal(t, len(tasks), plan.SkippedFiles)
}

func BenchmarkPlanCompaction(b *testing.B) {
	for _, tc := range []struct {
		files      int
		partitions int
		candidates int
	}{
		{files: 20_000, partitions: 256, candidates: 0},
		{files: 20_000, partitions: 256, candidates: 10},
		{files: 20_000, partitions: 256, candidates: 50},
		{files: 20_000, partitions: 256, candidates: 100},
		{files: 100_000, partitions: 1_024, candidates: 10},
		{files: 100_000, partitions: 1_024, candidates: 100},
	} {
		b.Run(fmt.Sprintf("files=%d/partitions=%d/candidates=%d%%", tc.files, tc.partitions, tc.candidates), func(b *testing.B) {
			// Reuse immutable partition maps as real table metadata does. All
			// setup is outside the timer; only the planner is measured.
			parts := make([]map[int]any, tc.partitions)
			for i := range parts {
				parts[i] = map[int]any{1000: int32(i), 1001: "us-east", 1002: true, 1003: int64(i) * 60}
			}

			tasks := make([]table.FileScanTask, tc.files)
			for i := range tasks {
				sizeMB := int64(500) // right-sized under the default 512-MB target
				if (i*37+i/97)%100 < tc.candidates {
					sizeMB = 10 // undersized, eligible for compaction
				}
				part := parts[(i*977+i/97)%len(parts)]
				file := newPartitionedDataFile(fmt.Sprintf("file-%d.parquet", i), sizeMB, part)
				tasks[i] = makeTask(file, 0, 0)
			}

			cfg := compaction.DefaultConfig()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				plan, err := cfg.PlanCompaction(tasks)
				if err != nil {
					b.Fatal(err)
				}
				compactionPlanBenchmarkSink = plan
			}
		})
	}
}
