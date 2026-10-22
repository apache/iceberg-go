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
	"runtime"
	"sync"

	"github.com/apache/iceberg-go"
)

// Conservative worker threshold heuristic; the actual crossover depends on the
// fraction of residual and split work. BenchmarkFinalizePlannedTasks sweeps it.
const scanTaskFinalizationMinTasksPerWorker = 256

type scanTaskFinalizationResult struct {
	metrics  scanMetricsAccumulator
	tasks    []FileScanTask
	expanded bool
	err      error
}

// finalizePlannedTasks applies residuals, records result metrics, and expands
// Parquet byte-range splits. The caller transfers ownership of plannedTasks:
// workers mutate residuals in that slice, even when an error is returned.
// Callers must not reuse it after this call. If no task splits, the returned
// slice aliases the original backing array.
func (scan *Scan) finalizePlannedTasks(
	plannedTasks []FileScanTask,
	schema *iceberg.Schema,
	acc *scanMetricsAccumulator,
) ([]FileScanTask, error) {
	var boundRowFilter iceberg.BooleanExpression
	if scan.rowFilter != nil && !scan.rowFilter.Equals(iceberg.AlwaysTrue{}) {
		var err error
		boundRowFilter, err = iceberg.BindExpr(schema, scan.rowFilter, scan.caseSensitive)
		if err != nil {
			return nil, err
		}
	}

	var residualEvaluators *keyDefaultMapErr[int, *partitionResidualEvaluator]
	if boundRowFilter != nil {
		residualEvaluators = newKeyDefaultMapWrapErr(func(specID int) (*partitionResidualEvaluator, error) {
			return newPartitionResidualEvaluator(
				schema, scan.metadata.PartitionSpecByID(specID), boundRowFilter, scan.caseSensitive)
		})
	}

	splitTargetSize := scan.metadata.Properties().GetInt64(
		ReadSplitTargetSizeKey, ReadSplitTargetSizeDefault)
	acc.resultDataFiles = int64(len(plannedTasks))
	if len(plannedTasks) == 0 {
		return []FileScanTask{}, nil
	}

	workerCount := min(max(scan.concurrency, 1), runtime.GOMAXPROCS(0),
		max(1, len(plannedTasks)/scanTaskFinalizationMinTasksPerWorker))
	results := make([]scanTaskFinalizationResult, workerCount)

	if workerCount == 1 {
		// Preserve the serial caller's ability to recover a panic and inspect
		// its stack. Only spawned goroutines need panic-to-error conversion.
		results[0] = scan.finalizeTaskRange(
			plannedTasks, 0, len(plannedTasks), residualEvaluators, splitTargetSize)
	} else {
		var wg sync.WaitGroup
		chunkSize, remainder := len(plannedTasks)/workerCount, len(plannedTasks)%workerCount
		start := 0
		for worker := range workerCount {
			// Distribute the remainder instead of rounding every range up:
			// even large worker counts must stay within the input slice.
			end := start + chunkSize
			if worker < remainder {
				end++
			}

			wg.Add(1)
			go func(worker, start, end int) {
				defer wg.Done()
				defer func() {
					if recovered := recover(); recovered != nil {
						results[worker].err = fmt.Errorf("finalize scan tasks [%d:%d]: panic: %v",
							start, end, recovered)
					}
				}()
				results[worker] = scan.finalizeTaskRange(
					plannedTasks, start, end, residualEvaluators, splitTargetSize)
			}(worker, start, end)
			start = end
		}
		wg.Wait()
	}

	// Ranges follow task order, regardless of the order workers completed.
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
	}

	totalTasks := 0
	anySplit := false
	for i := range results {
		mergeTaskFinalizationMetrics(acc, &results[i].metrics)
		anySplit = anySplit || results[i].expanded
		totalTasks += len(results[i].tasks)
	}
	if !anySplit {
		return plannedTasks, nil
	}
	if workerCount == 1 {
		return results[0].tasks, nil
	}

	// Workers that split files emit their ranges in task order. Unsplit
	// workers reuse their input ranges without temporary replacement lists.
	// Concatenate once; do not hold an allocation per individual split.
	finalized := make([]FileScanTask, 0, totalTasks)
	for i := range results {
		finalized = append(finalized, results[i].tasks...)
	}

	return finalized, nil
}

func (scan *Scan) finalizeTaskRange(
	plannedTasks []FileScanTask,
	start, end int,
	residualEvaluators *keyDefaultMapErr[int, *partitionResidualEvaluator],
	splitTargetSize int64,
) scanTaskFinalizationResult {
	var result scanTaskFinalizationResult
	var residualEvaluator *partitionResidualEvaluator
	var cachedSpecID int
	var hasCachedSpec bool
	for index := start; index < end; index++ {
		task := &plannedTasks[index]
		if residualEvaluators != nil {
			specID := int(task.File.SpecID())
			var err error
			if !hasCachedSpec || specID != cachedSpecID {
				residualEvaluator, err = residualEvaluators.Get(specID)
				if err != nil {
					result.err = fmt.Errorf(
						"build partition residual evaluator for spec %d: %w", specID, err)

					return result
				}
				// Manifest-order runs normally share one spec. Evaluators are
				// immutable; a worker caches the last lookup for a file run.
				cachedSpecID, hasCachedSpec = specID, true
			}
			if residualEvaluator != nil {
				var simplified bool
				task.Residual, simplified, err = residualEvaluator.residual(dataFilePartition(task.File))
				if err != nil {
					result.err = fmt.Errorf(
						"evaluate partition residual for %s: %w", task.File.FilePath(), err)

					return result
				}
				if !simplified {
					task.Residual = nil
				}
			}
		}

		result.metrics.addResultDeleteMetrics(*task)
		result.metrics.totalFileSize += task.File.FileSizeBytes()
		if offsets, fileSize, split := splittableParquetOffsets(*task, splitTargetSize); split {
			if !result.expanded {
				result.expanded = true
				// A mixed range keeps the unsplit prefix. Reserve space once
				// without allocating a temporary slice for each split file.
				result.tasks = make([]FileScanTask, 0, max((end-start)*2,
					index-start+parquetSplitCapacity(len(offsets), fileSize, splitTargetSize)))
				result.tasks = append(result.tasks, plannedTasks[start:index]...)
			}
			result.tasks = appendParquetTaskSplits(result.tasks, *task, offsets, fileSize, splitTargetSize)
		} else if result.expanded {
			result.tasks = append(result.tasks, *task)
		}
	}
	if !result.expanded {
		result.tasks = plannedTasks[start:end]
	}

	return result
}

func mergeTaskFinalizationMetrics(acc, local *scanMetricsAccumulator) {
	acc.totalFileSize += local.totalFileSize
	acc.totalDeleteFileSize += local.totalDeleteFileSize
	acc.positionalDeleteFiles += local.positionalDeleteFiles
	acc.equalityDeleteFiles += local.equalityDeleteFiles
	acc.dvs += local.dvs
	acc.resultDeleteFiles = acc.positionalDeleteFiles + acc.equalityDeleteFiles + acc.dvs
}
