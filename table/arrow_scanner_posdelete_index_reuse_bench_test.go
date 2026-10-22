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
	"testing"

	"github.com/apache/iceberg-go"
)

var benchmarkPositionDeleteIndexBuildSink *lazyPositionDeleteLoader

// benchmarkPositionDeleteIndexBuildBefore keeps the previous key-generation
// loop so the same benchmark invocation can measure before and after.
func benchmarkPositionDeleteIndexBuildBefore(tasks []FileScanTask) *lazyPositionDeleteLoader {
	uniqueDeletes, targetsByDelete := collectPositionDeleteFilesAndTargets(tasks)
	if len(uniqueDeletes) == 0 {
		return nil
	}
	loader := &lazyPositionDeleteLoader{
		files:   make(map[string]*lazyPositionDeleteFile, len(uniqueDeletes)),
		indexes: make([]positionDeleteTaskIndex, len(tasks)),
	}
	for path, deleteFile := range uniqueDeletes {
		loader.files[path] = &lazyPositionDeleteFile{
			dataFile: deleteFile,
			targets:  targetsByDelete[path],
		}
	}
	indexesByKey := make(map[string]*lazyPositionDeleteIndex)
	for i, task := range tasks {
		key, targetPath, deletePaths := positionDeleteIndexKey(task)
		loader.indexes[i].targetPath = targetPath
		if key == "" {
			continue
		}
		index := indexesByKey[key]
		if index == nil {
			index = &lazyPositionDeleteIndex{
				targetPath:  targetPath,
				deletePaths: deletePaths,
			}
			indexesByKey[key] = index
		}
		loader.indexes[i].index = index
	}

	return loader
}

func benchmarkPositionDeleteIndexTasks(deleteCount int, interleaved bool) []FileScanTask {
	const fileCount, splitsPerFile = 1000, 10
	dataFiles := make([]iceberg.DataFile, fileCount)
	deleteFiles := make([][]iceberg.DataFile, fileCount)
	for i := range dataFiles {
		dataFiles[i] = &mockDataFile{
			path: fmt.Sprintf("s3://bucket/catalog/namespace/table/data/%016d-%032d.parquet", i, i),
		}
		deleteFiles[i] = make([]iceberg.DataFile, deleteCount)
		for j := range deleteFiles[i] {
			deleteFiles[i][j] = &mockDataFile{
				path:        fmt.Sprintf("s3://bucket/catalog/namespace/table/deletes/%016d-%012d-%032d.parquet", i, j, j),
				contentType: iceberg.EntryContentPosDeletes,
			}
		}
	}
	tasks := make([]FileScanTask, fileCount*splitsPerFile)
	for i := range tasks {
		fileIndex := i / splitsPerFile
		if interleaved {
			fileIndex = i % fileCount
		}
		tasks[i] = FileScanTask{
			File:        dataFiles[fileIndex],
			DeleteFiles: deleteFiles[fileIndex],
			Start:       int64(i * 128),
			Length:      128,
		}
	}

	return tasks
}

// BenchmarkPositionDeleteIndexConstruction measures building the complete
// scan-scoped index for 10,000 split tasks and 1,000 data files. The interleaved
// control prevents a best-case-only result. It excludes Parquet IO.
func BenchmarkPositionDeleteIndexConstruction(b *testing.B) {
	for _, deleteCount := range []int{1, 4, 16} {
		for _, layout := range []struct {
			name        string
			interleaved bool
		}{
			{name: "contiguous"},
			{name: "interleaved", interleaved: true},
		} {
			tasks := benchmarkPositionDeleteIndexTasks(deleteCount, layout.interleaved)
			b.Run(fmt.Sprintf("%s/deletes=%d", layout.name, deleteCount), func(b *testing.B) {
				for _, implementation := range []struct {
					name  string
					build func() *lazyPositionDeleteLoader
				}{
					{name: "before", build: func() *lazyPositionDeleteLoader {
						return benchmarkPositionDeleteIndexBuildBefore(tasks)
					}},
					{name: "after", build: func() *lazyPositionDeleteLoader {
						return newLazyPositionDeleteLoader(nil, tasks)
					}},
				} {
					b.Run(implementation.name, func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							benchmarkPositionDeleteIndexBuildSink = implementation.build()
						}
					})
				}
			})
		}
	}
}
