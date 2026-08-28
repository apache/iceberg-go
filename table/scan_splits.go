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

import "github.com/apache/iceberg-go"

// splitParquetScanTask returns one task per valid Parquet split offset when a
// complete, large-file task can be split safely. The boolean is false when the
// original task should be used unchanged. Split offsets are absolute byte
// positions in the data file and each resulting range ends at the next offset,
// or at the end of the file.
//
// Tasks that are already partial, small, non-Parquet, or missing usable split
// offsets stay unchanged. Keeping the original task in those cases preserves
// the existing behavior for remote tasks and older files.
func splitParquetScanTask(task FileScanTask, targetSize int64) ([]FileScanTask, bool) {
	file := task.File
	if file == nil || file.FileFormat() != iceberg.ParquetFile || targetSize <= 0 {
		return nil, false
	}

	fileSize := file.FileSizeBytes()
	if fileSize <= targetSize || task.Start != 0 || task.Length != fileSize {
		return nil, false
	}

	offsets := file.SplitOffsets()
	if len(offsets) < 2 {
		return nil, false
	}

	for i, offset := range offsets {
		if offset < 0 || offset >= fileSize || (i > 0 && offset <= offsets[i-1]) {
			return nil, false
		}
	}

	result := make([]FileScanTask, 0, len(offsets))
	for i, start := range offsets {
		end := fileSize
		if i+1 < len(offsets) {
			end = offsets[i+1]
		}
		if end <= start {
			return nil, false
		}

		split := task
		split.Start = start
		split.Length = end - start
		result = append(result, split)
	}

	return result, true
}
