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
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDataFile implements iceberg.DataFile for testing.
type testDataFile struct {
	path      string
	size      int64
	partition map[int]any
	specID    int32
	content   iceberg.ManifestEntryContent
}

func (f *testDataFile) ContentType() iceberg.ManifestEntryContent { return f.content }
func (f *testDataFile) FilePath() string                          { return f.path }
func (f *testDataFile) FileFormat() iceberg.FileFormat            { return iceberg.ParquetFile }
func (f *testDataFile) Partition() map[int]any                    { return f.partition }
func (f *testDataFile) Count() int64                              { return 100 }
func (f *testDataFile) FileSizeBytes() int64                      { return f.size }
func (f *testDataFile) ColumnSizes() map[int]int64                { return nil }
func (f *testDataFile) ValueCounts() map[int]int64                { return nil }
func (f *testDataFile) NullValueCounts() map[int]int64            { return nil }
func (f *testDataFile) NaNValueCounts() map[int]int64             { return nil }
func (f *testDataFile) DistinctValueCounts() map[int]int64        { return nil }
func (f *testDataFile) LowerBoundValues() map[int][]byte          { return nil }
func (f *testDataFile) UpperBoundValues() map[int][]byte          { return nil }
func (f *testDataFile) KeyMetadata() []byte                       { return nil }
func (f *testDataFile) SplitOffsets() []int64                     { return nil }
func (f *testDataFile) EqualityFieldIDs() []int                   { return nil }
func (f *testDataFile) SortOrderID() *int                         { return nil }
func (f *testDataFile) SpecID() int32                             { return f.specID }
func (f *testDataFile) FirstRowID() *int64                        { return nil }
func (f *testDataFile) ContentOffset() *int64                     { return nil }
func (f *testDataFile) ContentSizeInBytes() *int64                { return nil }
func (f *testDataFile) ReferencedDataFile() *string               { return nil }

func newDataFile(path string, sizeMB int64) *testDataFile {
	return &testDataFile{
		path:    path,
		size:    sizeMB * 1024 * 1024,
		content: iceberg.EntryContentData,
	}
}

func newPartitionedDataFile(path string, sizeMB int64, partition map[int]any) *testDataFile {
	return &testDataFile{
		path:      path,
		size:      sizeMB * 1024 * 1024,
		partition: partition,
		content:   iceberg.EntryContentData,
	}
}

func newDeleteFile(path string) *testDataFile {
	return &testDataFile{
		path:    path,
		size:    1024,
		content: iceberg.EntryContentPosDeletes,
	}
}

func makeTask(file *testDataFile, numPosDeletes, numEqDeletes int) table.FileScanTask {
	task := table.FileScanTask{
		File:   file,
		Start:  0,
		Length: file.size,
	}
	for i := range numPosDeletes {
		task.DeleteFiles = append(task.DeleteFiles, newDeleteFile(
			file.path+".pos."+strconv.Itoa(i),
		))
	}
	for i := range numEqDeletes {
		task.EqualityDeleteFiles = append(task.EqualityDeleteFiles, newDeleteFile(
			file.path+".eq."+strconv.Itoa(i),
		))
	}

	return task
}

func TestDefaultConfig(t *testing.T) {
	cfg := compaction.DefaultConfig()
	assert.Equal(t, int64(table.WriteTargetFileSizeBytesDefault), cfg.TargetFileSizeBytes)
	assert.Equal(t, cfg.TargetFileSizeBytes*3/4, cfg.MinFileSizeBytes)
	assert.Equal(t, cfg.TargetFileSizeBytes*9/5, cfg.MaxFileSizeBytes)
	assert.Equal(t, 5, cfg.MinInputFiles)
	assert.Equal(t, 5, cfg.DeleteFileThreshold)
	assert.NoError(t, cfg.Validate())
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name string
		cfg  compaction.Config
		err  string
	}{
		{
			name: "zero target",
			cfg:  compaction.Config{TargetFileSizeBytes: 0, MinFileSizeBytes: 1, MaxFileSizeBytes: 2, MinInputFiles: 1, DeleteFileThreshold: 1},
			err:  "target file size must be positive",
		},
		{
			name: "min >= max",
			cfg:  compaction.Config{TargetFileSizeBytes: 100, MinFileSizeBytes: 200, MaxFileSizeBytes: 100, MinInputFiles: 1, DeleteFileThreshold: 1},
			err:  "min file size (200) must be less than max (100)",
		},
		{
			name: "target below min",
			cfg:  compaction.Config{TargetFileSizeBytes: 5, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 1, DeleteFileThreshold: 1},
			err:  "target file size (5) must be between min (10) and max (200)",
		},
		{
			name: "target above max",
			cfg:  compaction.Config{TargetFileSizeBytes: 500, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 1, DeleteFileThreshold: 1},
			err:  "target file size (500) must be between min (10) and max (200)",
		},
		{
			name: "zero min input files",
			cfg:  compaction.Config{TargetFileSizeBytes: 100, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 0, DeleteFileThreshold: 1},
			err:  "min input files must be >= 1",
		},
		{
			name: "zero delete threshold",
			cfg:  compaction.Config{TargetFileSizeBytes: 100, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 1, DeleteFileThreshold: 0},
			err:  "delete file threshold must be >= 1",
		},
		{
			name: "valid",
			cfg:  compaction.DefaultConfig(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPlanCompaction_EmptyInput(t *testing.T) {
	cfg := compaction.DefaultConfig()
	plan, err := cfg.PlanCompaction(nil)
	require.NoError(t, err)

	assert.Empty(t, plan.Groups)
	assert.Equal(t, 0, plan.TotalInputFiles)
	assert.Equal(t, int64(0), plan.TotalInputBytes)
	assert.Equal(t, 0, plan.SkippedFiles)
}

func TestPlanCompaction_AllFilesOptimal(t *testing.T) {
	cfg := compaction.DefaultConfig()
	var tasks []table.FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("file-%d.parquet", i), 500),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	assert.Empty(t, plan.Groups)
	assert.Equal(t, 5, plan.SkippedFiles)
	assert.Equal(t, 5, plan.TotalInputFiles)
}

func TestPlanCompaction_SmallFiles(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 10 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("small-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	require.NotEmpty(t, plan.Groups)
	assert.Equal(t, 0, plan.SkippedFiles)

	totalTasksInGroups := 0
	for _, g := range plan.Groups {
		totalTasksInGroups += len(g.Tasks)
		assert.GreaterOrEqual(t, len(g.Tasks), cfg.MinInputFiles)
	}
	assert.Equal(t, 10, totalTasksInGroups)
}

func TestPlanCompaction_DeleteFilesForcesCompaction(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 2600 * 1024 * 1024,
		MinFileSizeBytes:    384 * 1024 * 1024,
		MaxFileSizeBytes:    3000 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 3,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("file-%d.parquet", i), 500),
			3, 2,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	assert.Equal(t, 0, plan.SkippedFiles)

	totalInGroups := 0
	totalDeletes := 0
	for _, g := range plan.Groups {
		totalInGroups += len(g.Tasks)
		totalDeletes += g.DeleteFileCount
	}
	assert.Equal(t, 5, totalInGroups)
	assert.Equal(t, 25, totalDeletes)
}

func TestPlanCompaction_OversizedFilesSkipped(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("big-%d.parquet", i), 200),
			0, 0,
		))
	}
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("small-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	assert.Equal(t, 3, plan.SkippedFiles)
	require.NotEmpty(t, plan.Groups)

	totalInGroups := 0
	for _, g := range plan.Groups {
		totalInGroups += len(g.Tasks)
	}
	assert.Equal(t, 5, totalInGroups)
}

func TestPlanCompaction_OversizedWithDeletesCompacted(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 512 * 1024 * 1024,
		MinFileSizeBytes:    384 * 1024 * 1024,
		MaxFileSizeBytes:    600 * 1024 * 1024,
		MinInputFiles:       1,
		DeleteFileThreshold: 3,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	tasks = append(tasks, makeTask(newDataFile("big-with-deletes.parquet", 700), 3, 2))

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	assert.Equal(t, 0, plan.SkippedFiles)
	require.Len(t, plan.Groups, 1)
	assert.Equal(t, 5, plan.Groups[0].DeleteFileCount)
}

func TestPlanCompaction_MultiplePartitions(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newPartitionedDataFile(fmt.Sprintf("a-%d.parquet", i), 10,
				map[int]any{1000: "2024-01-15"}),
			0, 0,
		))
	}
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newPartitionedDataFile(fmt.Sprintf("b-%d.parquet", i), 10,
				map[int]any{1000: "2024-01-16"}),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	assert.Equal(t, 8, plan.TotalInputFiles)

	partitionKeys := make(map[string]bool)
	for _, g := range plan.Groups {
		partitionKeys[g.PartitionKey] = true
	}
	assert.Len(t, partitionKeys, 2)
}

func TestPlanCompaction_MultiFieldPartition(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	partition := map[int]any{1000: "2024-01-15", 1001: int32(42)}
	var tasks []table.FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newPartitionedDataFile(fmt.Sprintf("file-%d.parquet", i), 10, partition),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	require.NotEmpty(t, plan.Groups)
	totalInGroups := 0
	for _, g := range plan.Groups {
		totalInGroups += len(g.Tasks)
	}
	assert.Equal(t, 5, totalInGroups)
}

func TestPlanCompaction_BelowMinInputFiles(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       5,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("small-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	assert.Empty(t, plan.Groups)
	assert.Equal(t, 3, plan.SkippedFiles)
}

func TestPlanCompaction_UnpartitionedTable(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("file-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	require.NotEmpty(t, plan.Groups)
	for _, g := range plan.Groups {
		assert.Contains(t, g.PartitionKey, "0:")
	}
}

func TestPlanCompaction_MixedOptimalAndCandidates(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("optimal-%d.parquet", i), 100),
			0, 0,
		))
	}
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("small-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	assert.Equal(t, 8, plan.TotalInputFiles)
	assert.Equal(t, 3, plan.SkippedFiles)
	require.NotEmpty(t, plan.Groups)

	totalInGroups := 0
	for _, g := range plan.Groups {
		totalInGroups += len(g.Tasks)
	}
	assert.Equal(t, 5, totalInGroups)
}

func TestPlanCompaction_EstOutputFiles(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     128,
	}

	var tasks []table.FileScanTask
	for i := range 10 {
		tasks = append(tasks, makeTask(
			newDataFile(fmt.Sprintf("file-%d.parquet", i), 50),
			0, 0,
		))
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	assert.Greater(t, plan.EstOutputFiles, 0)
	assert.Greater(t, plan.EstOutputBytes, int64(0))
	assert.Less(t, plan.EstOutputFiles, plan.TotalInputFiles)
}

func TestPlanCompaction_InvalidConfig(t *testing.T) {
	cfg := compaction.Config{TargetFileSizeBytes: 0}
	_, err := cfg.PlanCompaction(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid compaction config")
}
