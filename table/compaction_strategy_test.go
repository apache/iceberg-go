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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compactDataFile implements iceberg.DataFile for testing compaction strategy.
type compactDataFile struct {
	path      string
	size      int64
	partition map[int]any
	specID    int32
	content   iceberg.ManifestEntryContent
}

func (f *compactDataFile) ContentType() iceberg.ManifestEntryContent { return f.content }
func (f *compactDataFile) FilePath() string                          { return f.path }
func (f *compactDataFile) FileFormat() iceberg.FileFormat            { return iceberg.ParquetFile }
func (f *compactDataFile) Partition() map[int]any                    { return f.partition }
func (f *compactDataFile) Count() int64                              { return 100 }
func (f *compactDataFile) FileSizeBytes() int64                      { return f.size }
func (f *compactDataFile) ColumnSizes() map[int]int64                { return nil }
func (f *compactDataFile) ValueCounts() map[int]int64                { return nil }
func (f *compactDataFile) NullValueCounts() map[int]int64            { return nil }
func (f *compactDataFile) NaNValueCounts() map[int]int64             { return nil }
func (f *compactDataFile) DistinctValueCounts() map[int]int64        { return nil }
func (f *compactDataFile) LowerBoundValues() map[int][]byte          { return nil }
func (f *compactDataFile) UpperBoundValues() map[int][]byte          { return nil }
func (f *compactDataFile) KeyMetadata() []byte                       { return nil }
func (f *compactDataFile) SplitOffsets() []int64                     { return nil }
func (f *compactDataFile) EqualityFieldIDs() []int                   { return nil }
func (f *compactDataFile) SortOrderID() *int                         { return nil }
func (f *compactDataFile) SpecID() int32                             { return f.specID }
func (f *compactDataFile) FirstRowID() *int64                        { return nil }
func (f *compactDataFile) ContentOffset() *int64                     { return nil }
func (f *compactDataFile) ContentSizeInBytes() *int64                { return nil }
func (f *compactDataFile) ReferencedDataFile() *string               { return nil }

func newCompactDataFile(path string, sizeMB int64) *compactDataFile {
	return &compactDataFile{
		path:    path,
		size:    sizeMB * 1024 * 1024,
		content: iceberg.EntryContentData,
	}
}

func newCompactPartitionedDataFile(path string, sizeMB int64, partition map[int]any) *compactDataFile {
	return &compactDataFile{
		path:      path,
		size:      sizeMB * 1024 * 1024,
		partition: partition,
		content:   iceberg.EntryContentData,
	}
}

func newCompactDeleteFile(path string) *compactDataFile {
	return &compactDataFile{
		path:    path,
		size:    1024,
		content: iceberg.EntryContentPosDeletes,
	}
}

func makeTask(file *compactDataFile, numPosDeletes, numEqDeletes int) FileScanTask {
	task := FileScanTask{
		File:   file,
		Start:  0,
		Length: file.size,
	}
	for i := range numPosDeletes {
		task.DeleteFiles = append(task.DeleteFiles, newCompactDeleteFile(
			file.path+".pos."+strconv.Itoa(i),
		))
	}
	for i := range numEqDeletes {
		task.EqualityDeleteFiles = append(task.EqualityDeleteFiles, newCompactDeleteFile(
			file.path+".eq."+strconv.Itoa(i),
		))
	}

	return task
}

func TestDefaultCompactionConfig(t *testing.T) {
	cfg := DefaultCompactionConfig()
	assert.Equal(t, int64(WriteTargetFileSizeBytesDefault), cfg.TargetFileSizeBytes)
	assert.Equal(t, cfg.TargetFileSizeBytes*3/4, cfg.MinFileSizeBytes)
	assert.Equal(t, cfg.TargetFileSizeBytes*9/5, cfg.MaxFileSizeBytes)
	assert.Equal(t, 5, cfg.MinInputFiles)
	assert.Equal(t, 5, cfg.DeleteFileThreshold)
	assert.NoError(t, cfg.Validate())
}

func TestCompactionConfig_Validate(t *testing.T) {
	tests := []struct {
		name string
		cfg  CompactionConfig
		err  string
	}{
		{
			name: "zero target",
			cfg:  CompactionConfig{TargetFileSizeBytes: 0, MinFileSizeBytes: 1, MaxFileSizeBytes: 2, MinInputFiles: 1},
			err:  "target file size must be positive",
		},
		{
			name: "min >= max",
			cfg:  CompactionConfig{TargetFileSizeBytes: 100, MinFileSizeBytes: 200, MaxFileSizeBytes: 100, MinInputFiles: 1},
			err:  "min file size (200) must be less than max (100)",
		},
		{
			name: "target below min",
			cfg:  CompactionConfig{TargetFileSizeBytes: 5, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 1, DeleteFileThreshold: 1},
			err:  "target file size (5) must be between min (10) and max (200)",
		},
		{
			name: "target above max",
			cfg:  CompactionConfig{TargetFileSizeBytes: 500, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 1, DeleteFileThreshold: 1},
			err:  "target file size (500) must be between min (10) and max (200)",
		},
		{
			name: "zero min input files",
			cfg:  CompactionConfig{TargetFileSizeBytes: 100, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 0, DeleteFileThreshold: 1},
			err:  "min input files must be >= 1",
		},
		{
			name: "zero delete threshold",
			cfg:  CompactionConfig{TargetFileSizeBytes: 100, MinFileSizeBytes: 10, MaxFileSizeBytes: 200, MinInputFiles: 1, DeleteFileThreshold: 0},
			err:  "delete file threshold must be >= 1",
		},
		{
			name: "valid",
			cfg:  DefaultCompactionConfig(),
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

func TestCompactionPlan_EmptyInput(t *testing.T) {
	cfg := DefaultCompactionConfig()
	plan, err := cfg.Plan(nil)
	require.NoError(t, err)

	assert.Empty(t, plan.Groups)
	assert.Equal(t, 0, plan.TotalInputFiles)
	assert.Equal(t, int64(0), plan.TotalInputBytes)
	assert.Equal(t, 0, plan.SkippedFiles)
}

func TestCompactionPlan_AllFilesOptimal(t *testing.T) {
	cfg := DefaultCompactionConfig()
	var tasks []FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("file-%d.parquet", i), 500), // 500MB — within [384MB, 921MB]
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	assert.Empty(t, plan.Groups)
	assert.Equal(t, 5, plan.SkippedFiles)
	assert.Equal(t, 5, plan.TotalInputFiles)
}

func TestCompactionPlan_SmallFiles(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024, // 100MB
		MinFileSizeBytes:    75 * 1024 * 1024,  // 75MB
		MaxFileSizeBytes:    180 * 1024 * 1024, // 180MB
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	var tasks []FileScanTask
	for i := range 10 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("small-%d.parquet", i), 10), // 10MB — below 75MB threshold
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
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

func TestCompactionPlan_DeleteFilesForcesCompaction(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 2600 * 1024 * 1024, // 2600MB — fits all 5 × 500MB files in one bin
		MinFileSizeBytes:    384 * 1024 * 1024,
		MaxFileSizeBytes:    3000 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 3,
	}

	var tasks []FileScanTask
	for i := range 5 {
		// 500MB files — within size range but with 5 delete files each (>= threshold of 3)
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("file-%d.parquet", i), 500),
			3, 2, // 3 pos + 2 eq = 5 total, >= threshold of 3
		))
	}

	plan, err := cfg.Plan(tasks)
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
	assert.Equal(t, 25, totalDeletes) // 5 files × 5 deletes each
}

func TestCompactionPlan_OversizedFilesSkipped(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024, // 100MB
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	var tasks []FileScanTask
	// 3 oversized files (200MB > 180MB max) with no deletes — should be skipped
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("big-%d.parquet", i), 200),
			0, 0,
		))
	}
	// 5 small files — candidates
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("small-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	assert.Equal(t, 3, plan.SkippedFiles) // only the oversized ones
	require.NotEmpty(t, plan.Groups)

	totalInGroups := 0
	for _, g := range plan.Groups {
		totalInGroups += len(g.Tasks)
	}
	assert.Equal(t, 5, totalInGroups) // only small files compacted
}

func TestCompactionPlan_OversizedWithDeletesCompacted(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 512 * 1024 * 1024,
		MinFileSizeBytes:    384 * 1024 * 1024,
		MaxFileSizeBytes:    600 * 1024 * 1024,
		MinInputFiles:       1,
		DeleteFileThreshold: 3,
	}

	var tasks []FileScanTask
	// 700MB file (oversized) but with 5 deletes (>= threshold) — should still be compacted
	tasks = append(tasks, makeTask(
		newCompactDataFile("big-with-deletes.parquet", 700),
		3, 2,
	))

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	assert.Equal(t, 0, plan.SkippedFiles)
	require.Len(t, plan.Groups, 1)
	assert.Equal(t, 5, plan.Groups[0].DeleteFileCount)
}

func TestCompactionPlan_MultiplePartitions(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	var tasks []FileScanTask
	// Partition A: 5 small files
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newCompactPartitionedDataFile(fmt.Sprintf("a-%d.parquet", i), 10,
				map[int]any{1000: "2024-01-15"}),
			0, 0,
		))
	}
	// Partition B: 3 small files
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newCompactPartitionedDataFile(fmt.Sprintf("b-%d.parquet", i), 10,
				map[int]any{1000: "2024-01-16"}),
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	assert.Equal(t, 8, plan.TotalInputFiles)

	partitionKeys := make(map[string]bool)
	for _, g := range plan.Groups {
		partitionKeys[g.PartitionKey] = true
	}
	assert.Len(t, partitionKeys, 2, "should have groups in 2 different partitions")
}

func TestCompactionPlan_MultiFieldPartition(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	partition := map[int]any{1000: "2024-01-15", 1001: int32(42)}

	var tasks []FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newCompactPartitionedDataFile(fmt.Sprintf("file-%d.parquet", i), 10, partition),
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	require.NotEmpty(t, plan.Groups)
	// All files should be in the same group — deterministic partition key.
	totalInGroups := 0
	for _, g := range plan.Groups {
		totalInGroups += len(g.Tasks)
	}
	assert.Equal(t, 5, totalInGroups)
}

func TestCompactionPlan_BelowMinInputFiles(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       5,
		DeleteFileThreshold: 5,
	}

	var tasks []FileScanTask
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("small-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	assert.Empty(t, plan.Groups)
	assert.Equal(t, 3, plan.SkippedFiles)
}

func TestCompactionPlan_UnpartitionedTable(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	var tasks []FileScanTask
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("file-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	require.NotEmpty(t, plan.Groups)
	for _, g := range plan.Groups {
		assert.Equal(t, "", g.PartitionKey)
	}
}

func TestCompactionPlan_MixedOptimalAndCandidates(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	var tasks []FileScanTask
	// 3 optimal files
	for i := range 3 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("optimal-%d.parquet", i), 100),
			0, 0,
		))
	}
	// 5 small files
	for i := range 5 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("small-%d.parquet", i), 10),
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
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

func TestCompactionPlan_EstOutputFiles(t *testing.T) {
	cfg := CompactionConfig{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
	}

	var tasks []FileScanTask
	// 10 files at 50MB each = 500MB total → should produce ~5 output files at 100MB target
	for i := range 10 {
		tasks = append(tasks, makeTask(
			newCompactDataFile(fmt.Sprintf("file-%d.parquet", i), 50),
			0, 0,
		))
	}

	plan, err := cfg.Plan(tasks)
	require.NoError(t, err)

	assert.Greater(t, plan.EstOutputFiles, 0)
	assert.Greater(t, plan.EstOutputBytes, int64(0))
	assert.Less(t, plan.EstOutputFiles, plan.TotalInputFiles)
}

func TestCompactionPlan_InvalidConfig(t *testing.T) {
	cfg := CompactionConfig{TargetFileSizeBytes: 0}
	_, err := cfg.Plan(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid compaction config")
}
