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

var equalityDeleteAssemblyBenchmarkSink map[int][]*equalityDeleteSet

func BenchmarkEqualityDeleteSetAssembly(b *testing.B) {
	benchmarks := []struct {
		name                string
		combinations        int
		tasksPerCombination int
		filesPerCombination int
		keysPerFile         int
	}{
		{
			name:                "shared-single-file",
			combinations:        1,
			tasksPerCombination: 1_000,
			filesPerCombination: 1,
			keysPerFile:         1_000,
		},
		{
			name:                "shared-four-file-union",
			combinations:        1,
			tasksPerCombination: 1_000,
			filesPerCombination: 4,
			keysPerFile:         250,
		},
		{
			name:                "partitioned-four-file-unions",
			combinations:        100,
			tasksPerCombination: 10,
			filesPerCombination: 4,
			keysPerFile:         250,
		},
		{
			name:                "unique-two-file-unions",
			combinations:        1_000,
			tasksPerCombination: 1,
			filesPerCombination: 2,
			keysPerFile:         100,
		},
	}

	for _, benchmark := range benchmarks {
		tasks, perFile := equalityDeleteAssemblyBenchmarkInput(
			b,
			benchmark.combinations,
			benchmark.tasksPerCombination,
			benchmark.filesPerCombination,
			benchmark.keysPerFile,
		)

		b.Run(benchmark.name, func(b *testing.B) {
			b.Run("shared", func(b *testing.B) {
				benchmarkEqualityDeleteSetAssembly(b, tasks, func() map[int][]*equalityDeleteSet {
					return buildEqualityDeleteSetsPerTask(tasks, perFile)
				})
			})
			b.Run("copy-per-task", func(b *testing.B) {
				benchmarkEqualityDeleteSetAssembly(b, tasks, func() map[int][]*equalityDeleteSet {
					return buildEqualityDeleteSetsPerTaskWithCopies(tasks, perFile)
				})
			})
		})
	}
}

func benchmarkEqualityDeleteSetAssembly(
	b *testing.B,
	tasks []FileScanTask,
	build func() map[int][]*equalityDeleteSet,
) {
	b.Helper()
	b.ReportAllocs()
	b.ReportMetric(float64(len(tasks)), "tasks")
	b.ResetTimer()
	for range b.N {
		equalityDeleteAssemblyBenchmarkSink = build()
	}
	if len(equalityDeleteAssemblyBenchmarkSink) != len(tasks) {
		b.Fatalf("expected sets for %d tasks, got %d", len(tasks), len(equalityDeleteAssemblyBenchmarkSink))
	}
}

func buildEqualityDeleteSetsPerTaskWithCopies(
	tasks []FileScanTask,
	perFile map[string]*equalityDeleteFileSet,
) map[int][]*equalityDeleteSet {
	perTask := make(map[int][]*equalityDeleteSet)
	for taskIndex, task := range tasks {
		groups := make(map[string]*equalityDeleteSet)
		for _, deleteFile := range task.EqualityDeleteFiles {
			fileSet, ok := perFile[deleteFile.FilePath()]
			if !ok {
				continue
			}

			groupKey := fmt.Sprint(fileSet.fieldIDs)
			deleteSet, exists := groups[groupKey]
			if !exists {
				deleteSet = &equalityDeleteSet{
					keys:     make(set[string]),
					fieldIDs: fileSet.fieldIDs,
					colNames: fileSet.colNames,
				}
				groups[groupKey] = deleteSet
			}

			for key := range fileSet.keys {
				deleteSet.keys[key] = struct{}{}
			}
		}

		sets := make([]*equalityDeleteSet, 0, len(groups))
		for _, deleteSet := range groups {
			if len(deleteSet.keys) > 0 {
				sets = append(sets, deleteSet)
			}
		}
		if len(sets) > 0 {
			perTask[taskIndex] = sets
		}
	}

	return perTask
}

func equalityDeleteAssemblyBenchmarkInput(
	b *testing.B,
	combinations, tasksPerCombination, filesPerCombination, keysPerFile int,
) ([]FileScanTask, map[string]*equalityDeleteFileSet) {
	b.Helper()

	tasks := make([]FileScanTask, 0, combinations*tasksPerCombination)
	perFile := make(map[string]*equalityDeleteFileSet, combinations*filesPerCombination)
	fileID := 0

	for combination := range combinations {
		deleteFiles := make([]iceberg.DataFile, filesPerCombination)
		for fileIndex := range filesPerCombination {
			path := fmt.Sprintf("delete-%d-%d.parquet", combination, fileIndex)
			builder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec,
				iceberg.EntryContentEqDeletes,
				path,
				iceberg.ParquetFile,
				nil,
				nil,
				nil,
				int64(keysPerFile),
				128,
			)
			if err != nil {
				b.Fatal(err)
			}
			deleteFiles[fileIndex] = builder.EqualityFieldIDs([]int{1}).Build()

			keys := make(set[string], keysPerFile)
			for keyIndex := range keysPerFile {
				keys[fmt.Sprintf("key-%d-%d", fileID, keyIndex)] = struct{}{}
			}
			perFile[path] = &equalityDeleteFileSet{
				id: fileID,
				equalityDeleteSet: &equalityDeleteSet{
					keys:     keys,
					fieldIDs: []int{1},
					colNames: []string{"id"},
				},
			}
			fileID++
		}

		for range tasksPerCombination {
			tasks = append(tasks, FileScanTask{EqualityDeleteFiles: deleteFiles})
		}
	}

	return tasks, perFile
}
