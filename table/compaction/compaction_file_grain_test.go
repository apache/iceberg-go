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

type offsetDataFile struct {
	iceberg.DataFile
	offset int64
}

func (f offsetDataFile) ContentOffset() *int64 { return &f.offset }

func TestPlanCompactionCountsWholeFiles(t *testing.T) {
	file := newDataFile("split.parquet", 10)
	firstRowID, sequence := int64(20), int64(3)
	first := makeTask(file, 1, 1)
	first.DeletionVectorFiles = []iceberg.DataFile{newScopedDeleteFile("delete.puffin", file.path, 1)}
	first.FirstRowID, first.DataSequenceNumber = &firstRowID, &sequence
	first.Residual = iceberg.EqualTo(iceberg.Reference("id"), int64(1))
	first.Length = file.size / 2
	second := first
	second.Start, second.Length = first.Length, file.size-first.Length
	second.FirstRowID = new(int64)
	*second.FirstRowID = firstRowID
	second.DataSequenceNumber = new(int64)
	*second.DataSequenceNumber = sequence
	second.DeletionVectorFiles = []iceberg.DataFile{newScopedDeleteFile("delete.puffin", file.path, 1)}
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
	got := plan.Groups[0].Tasks[0]
	require.Same(t, second.File, got.File)
	require.Same(t, second.FirstRowID, got.FirstRowID)
	require.Same(t, second.DataSequenceNumber, got.DataSequenceNumber)
	require.Same(t, second.DeletionVectorFiles[0], got.DeletionVectorFiles[0])
	require.Equal(t, int64(0), got.Start)
	require.Equal(t, file.size, got.Length)
	require.Nil(t, got.Residual, "compaction rewrites all rows in the selected file")
	require.Equal(t, original, tasks, "normalization must not mutate caller tasks")
}

func TestPlanCompactionRejectsConflictingSplitMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*table.FileScanTask)
	}{
		{"first row ID", func(task *table.FileScanTask) { value := int64(21); task.FirstRowID = &value }},
		{"missing first row ID", func(task *table.FileScanTask) { task.FirstRowID = nil }},
		{"data sequence number", func(task *table.FileScanTask) { value := int64(4); task.DataSequenceNumber = &value }},
		{"missing data sequence number", func(task *table.FileScanTask) { task.DataSequenceNumber = nil }},
		{"positional deletes", func(task *table.FileScanTask) { task.DeleteFiles = []iceberg.DataFile{newDeleteFile("other.pos")} }},
		{"same delete path, different metadata", func(task *table.FileScanTask) {
			task.DeleteFiles = []iceberg.DataFile{newDeleteFile("same.pos")}
		}},
		{"delete content offset", func(task *table.FileScanTask) {
			base := newDeleteFile("same.puffin")
			task.DeleteFiles = []iceberg.DataFile{offsetDataFile{DataFile: base, offset: 2}}
		}},
		{"equality deletes", func(task *table.FileScanTask) {
			task.EqualityDeleteFiles = []iceberg.DataFile{newDeleteFile("other.eq")}
		}},
		{"deletion vectors", func(task *table.FileScanTask) {
			task.DeletionVectorFiles = []iceberg.DataFile{newScopedDeleteFile("other.puffin", "split.parquet", 1)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := newDataFile("split.parquet", 10)
			first := makeTask(file, 0, 0)
			first.DeleteFiles = nil
			if tc.name == "same delete path, different metadata" {
				first.DeleteFiles = []iceberg.DataFile{newDeleteFile("same.pos")}
				first.DeleteFiles[0] = &testDataFile{path: "same.pos", size: 1024, records: 5, content: iceberg.EntryContentPosDeletes}
			}
			first.FirstRowID, first.DataSequenceNumber = new(int64), new(int64)
			if tc.name == "delete content offset" {
				first.DeleteFiles = []iceberg.DataFile{offsetDataFile{DataFile: newDeleteFile("same.puffin"), offset: 1}}
			}
			second := first
			second.Start, second.Length = file.size/2, file.size/2
			tc.change(&second)
			_, err := compaction.DefaultConfig().PlanCompaction([]table.FileScanTask{first, second})
			require.ErrorIs(t, err, table.ErrInvalidOperation)
		})
	}
}

func TestPlanCompactionTreatsNilAndEmptyDeleteListsEqually(t *testing.T) {
	file := newDataFile("split.parquet", 10)
	first := makeTask(file, 0, 0)
	first.DeleteFiles = nil
	second := first
	second.Start, second.Length = file.size/2, file.size/2
	second.DeleteFiles = []iceberg.DataFile{}
	_, err := compaction.DefaultConfig().PlanCompaction([]table.FileScanTask{first, second})
	require.NoError(t, err)
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
