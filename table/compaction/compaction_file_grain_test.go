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

package compaction_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

func TestPlanCompactionCountsWholeFiles(t *testing.T) {
	file := newDataFile("split.parquet", 10)
	firstRowID, sequence := int64(20), int64(3)
	first := makeTask(file, 1, 1)
	first.DeletionVectorFiles = []iceberg.DataFile{newScopedDeleteFile("delete.puffin", file.path, 1)}
	first.FirstRowID, first.DataSequenceNumber = &firstRowID, &sequence
	first.Residual = iceberg.AlwaysTrue{}
	first.Length = file.size / 2
	second := first
	second.Start, second.Length = first.Length, file.size-first.Length
	duplicate := *file
	first.File = &duplicate
	tasks := []table.FileScanTask{second, first, makeTask(newDataFile("other.parquet", 10), 0, 0)}
	original := append([]table.FileScanTask(nil), tasks...)

	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 1
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.Equal(t, 2, plan.TotalInputFiles)
	require.Equal(t, 2*file.size, plan.TotalInputBytes)
	require.Len(t, plan.Groups, 1)
	require.Len(t, plan.Groups[0].Tasks, 2)
	require.Equal(t, 2*file.size, plan.Groups[0].TotalSizeBytes)
	require.Equal(t, 3, plan.Groups[0].DeleteFileCount)
	want := second
	want.Start, want.Length = 0, file.size
	require.Contains(t, plan.Groups[0].Tasks, want)
	require.Equal(t, original, tasks, "normalization must not mutate caller tasks")
}

func TestPlanCompactionDoesNotCountRangesTowardMinimumFiles(t *testing.T) {
	file := newDataFile("split.parquet", 10)
	tasks := make([]table.FileScanTask, 5)
	for i := range tasks {
		tasks[i] = table.FileScanTask{File: file, Start: int64(i) * file.size / 5, Length: file.size / 5}
	}
	plan, err := compaction.DefaultConfig().PlanCompaction(tasks)
	require.NoError(t, err)
	require.Equal(t, 1, plan.TotalInputFiles)
	require.Equal(t, file.size, plan.TotalInputBytes)
	require.Equal(t, 1, plan.SkippedFiles)
	require.Empty(t, plan.Groups, "five ranges are not five source files")
}

func TestPlanCompactionCountsSkippedFileOnce(t *testing.T) {
	file := newDataFile("optimal.parquet", 512)
	plan, err := compaction.DefaultConfig().PlanCompaction([]table.FileScanTask{
		{File: file, Length: file.size / 2},
		{File: file, Start: file.size / 2, Length: file.size / 2},
	})
	require.NoError(t, err)
	require.Equal(t, 1, plan.TotalInputFiles)
	require.Equal(t, file.size, plan.TotalInputBytes)
	require.Equal(t, 1, plan.SkippedFiles)
	require.Empty(t, plan.Groups)
}

func TestPlanCompactionRejectsNilFile(t *testing.T) {
	_, err := compaction.DefaultConfig().PlanCompaction([]table.FileScanTask{{}})
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}
