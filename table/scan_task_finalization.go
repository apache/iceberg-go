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
	"sync"

	"github.com/apache/iceberg-go"
)

// Keep enough work in each finalization worker to amortize goroutine and
// result-merging overhead on small scans.
const scanTaskFinalizationMinTasksPerWorker = 64

type scanTaskSplitReplacement struct {
	index int
	tasks []FileScanTask
}

type scanTaskFinalizationResult struct {
	metrics      scanMetricsAccumulator
	replacements []scanTaskSplitReplacement
	err          error
}

// finalizePlannedTasks applies task residuals, records result metrics, and
// expands Parquet byte-range splits. Workers own disjoint task ranges, so base
// tasks can be updated in place. Only tasks that actually split need replacement
// slices; unsplit scans return plannedTasks directly without another O(files)
// copy.
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

	workerCount := min(max(scan.concurrency, 1), max(1, len(plannedTasks)/scanTaskFinalizationMinTasksPerWorker))
	results := make([]scanTaskFinalizationResult, workerCount)
	chunkSize := (len(plannedTasks) + workerCount - 1) / workerCount

	finalizeRange := func(worker, start, end int) {
		results[worker] = scan.finalizeTaskRange(
			plannedTasks, start, end, residualEvaluators, splitTargetSize)
	}

	if workerCount == 1 {
		finalizeRange(0, 0, len(plannedTasks))
	} else {
		var wg sync.WaitGroup
		for worker := range workerCount {
			start := worker * chunkSize
			end := min(start+chunkSize, len(plannedTasks))

			wg.Add(1)
			go func(worker, start, end int) {
				defer wg.Done()
				finalizeRange(worker, start, end)
			}(worker, start, end)
		}
		wg.Wait()
	}

	// Each worker stops at its first error, and results follow contiguous task
	// ranges. The first error in result order is therefore also the first error
	// in task order, regardless of which worker finished first.
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
	}

	totalTasks := len(plannedTasks)
	for i := range results {
		mergeTaskFinalizationMetrics(acc, &results[i].metrics)
		for _, replacement := range results[i].replacements {
			totalTasks += len(replacement.tasks) - 1
		}
	}
	if totalTasks == len(plannedTasks) {
		return plannedTasks, nil
	}

	finalized := make([]FileScanTask, 0, totalTasks)
	next := 0
	for i := range results {
		for _, replacement := range results[i].replacements {
			finalized = append(finalized, plannedTasks[next:replacement.index]...)
			finalized = append(finalized, replacement.tasks...)
			next = replacement.index + 1
		}
	}
	finalized = append(finalized, plannedTasks[next:]...)

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
				// immutable, so each worker can reuse its last lookup without
				// acquiring the shared cache lock for every file.
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
		if splitTasks, split := splitParquetScanTask(*task, splitTargetSize); split {
			result.replacements = append(result.replacements, scanTaskSplitReplacement{
				index: index,
				tasks: splitTasks,
			})
		}
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
