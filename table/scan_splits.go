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
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
)

// splittableParquetOffsets validates the source task before any output is
// appended. In particular, malformed split offsets must not leave a partial
// result behind when a different task in the batch has already been split.
func splittableParquetOffsets(task FileScanTask, targetSize int64) ([]int64, int64, bool) {
	file := task.File
	if file == nil || file.FileFormat() != iceberg.ParquetFile || targetSize <= 0 {
		return nil, 0, false
	}

	fileSize := file.FileSizeBytes()
	if fileSize <= targetSize || task.Start != 0 || task.Length != fileSize {
		return nil, 0, false
	}

	offsets := iceinternal.BorrowedDataFileSplitOffsets(file)
	if len(offsets) < 2 {
		return nil, 0, false
	}
	for i, offset := range offsets {
		if offset < 0 || offset >= fileSize || (i > 0 && offset <= offsets[i-1]) {
			return nil, 0, false
		}
	}

	return offsets, fileSize, true
}

// parquetSplitCapacity bounds the reserved output size for coalesced groups.
// Compare before multiplying to avoid overflow for very large files.
func parquetSplitCapacity(offsetCount int, fileSize, targetSize int64) int {
	capacity := offsetCount
	if groups := fileSize / targetSize; groups < int64(capacity/2) {
		capacity = int(2*groups + 1)
	}

	return capacity
}

// appendParquetTaskSplits appends the ranges directly to the destination.
// The offsets have already been checked as strictly increasing, non-negative,
// and below the file size, so every emitted range has positive length.
func appendParquetTaskSplits(result []FileScanTask, task FileScanTask, offsets []int64, fileSize, targetSize int64) []FileScanTask {
	var taskStart, taskEnd int64
	for i := range offsets {
		chunkStart := int64(0)
		if i > 0 {
			chunkStart = offsets[i]
		}

		chunkEnd := fileSize
		if i+1 < len(offsets) {
			chunkEnd = offsets[i+1]
		}

		if taskEnd > taskStart && chunkEnd-taskStart > targetSize {
			split := task
			split.Start = taskStart
			split.Length = taskEnd - taskStart
			result = append(result, split)
			taskStart = chunkStart
		}
		taskEnd = chunkEnd
	}

	split := task
	split.Start = taskStart
	split.Length = taskEnd - taskStart

	return append(result, split)
}

// splitParquetScanTask returns coalesced byte-range tasks when a complete,
// large-file task can be split safely. The boolean is false when the original
// task should be used unchanged. Split offsets are absolute byte positions in
// the data file and each safe range ends at the next supplied offset, or at the
// end of the file. The first range starts at byte zero so sparse split-offset
// lists cannot leave leading row groups unassigned.
//
// Tasks that are already partial, small, non-Parquet, or missing usable split
// offsets stay unchanged. Keeping the original task in those cases preserves
// the existing behavior for remote tasks and older files.
func splitParquetScanTask(task FileScanTask, targetSize int64) ([]FileScanTask, bool) {
	offsets, fileSize, ok := splittableParquetOffsets(task, targetSize)
	if !ok {
		return nil, false
	}

	result := make([]FileScanTask, 0, parquetSplitCapacity(len(offsets), fileSize, targetSize))

	return appendParquetTaskSplits(result, task, offsets, fileSize, targetSize), true
}

// splitRemoteScanTasks allocates a new task slice only if a file needs splitting.
// All split ranges are written directly into that slice instead of allocating
// and copying one temporary slice for each large Parquet file.
func splitRemoteScanTasks(tasks []FileScanTask, targetSize int64) []FileScanTask {
	var result []FileScanTask
	for i, task := range tasks {
		offsets, fileSize, ok := splittableParquetOffsets(task, targetSize)
		if !ok {
			if result != nil {
				result = append(result, task)
			}

			continue
		}

		if result == nil {
			capacity := len(tasks)
			extra := parquetSplitCapacity(len(offsets), fileSize, targetSize) - 1
			if extra <= int(^uint(0)>>1)-capacity {
				capacity += extra
			}
			result = append(make([]FileScanTask, 0, capacity), tasks[:i]...)
		}
		result = appendParquetTaskSplits(result, task, offsets, fileSize, targetSize)
	}
	if result == nil {
		return tasks
	}

	return result
}
