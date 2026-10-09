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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFinalizePlannedTasksParallelMatchesSerial(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"mem://table",
		iceberg.Properties{ReadSplitTargetSizeKey: "25"},
	)
	require.NoError(t, err)

	tasks := make([]FileScanTask, 1024)
	for i := range tasks {
		file := finalizationTestDataFile(
			t, *iceberg.UnpartitionedSpec,
			fmt.Sprintf("mem://table/data/file-%d.parquet", i), nil,
			100, []int64{8, 25, 50, 75})
		tasks[i] = FileScanTask{File: file, Start: 0, Length: file.FileSizeBytes()}
		if i%3 == 0 {
			tasks[i].DeleteFiles = []iceberg.DataFile{file}
		}
		if i%5 == 0 {
			tasks[i].EqualityDeleteFiles = []iceberg.DataFile{file}
		}
	}

	scan := &Scan{
		metadata: metadata, rowFilter: iceberg.AlwaysTrue{}, caseSensitive: true, concurrency: 8,
	}
	serial, serialMetrics, err := serialFinalizationReference(
		scan, append([]FileScanTask(nil), tasks...), schema)
	require.NoError(t, err)
	var parallelMetrics scanMetricsAccumulator
	parallel, err := scan.finalizePlannedTasks(
		append([]FileScanTask(nil), tasks...), schema, &parallelMetrics)
	require.NoError(t, err)

	assert.Equal(t, serial, parallel)
	assert.Equal(t, serialMetrics, parallelMetrics)
	require.Len(t, parallel, len(tasks)*4)
	for i := range tasks {
		for split, start := range []int64{0, 25, 50, 75} {
			task := parallel[i*4+split]
			assert.Equal(t, fmt.Sprintf("mem://table/data/file-%d.parquet", i), task.File.FilePath())
			assert.Equal(t, start, task.Start)
			assert.Equal(t, int64(25), task.Length)
		}
	}
	assert.Equal(t, int64(len(tasks)), parallelMetrics.resultDataFiles)
	assert.Equal(t, int64(len(tasks)*100), parallelMetrics.totalFileSize)
	assert.Equal(t, int64(342), parallelMetrics.positionalDeleteFiles)
	assert.Equal(t, int64(205), parallelMetrics.equalityDeleteFiles)
	assert.Equal(t, int64(547), parallelMetrics.resultDeleteFiles)
	assert.Equal(t, int64(54_700), parallelMetrics.totalDeleteFileSize)
}

func TestFinalizePlannedTasksParallelResidualsMatchSerial(t *testing.T) {
	schema := simpleSchema()
	spec := partitionedSpec()
	unpartitioned := iceberg.NewPartitionSpecID(1)
	metadata, err := NewMetadata(
		schema,
		&spec,
		UnsortedSortOrder,
		"mem://table",
		nil,
	)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddPartitionSpec(&unpartitioned, false))
	metadata, err = builder.Build()
	require.NoError(t, err)

	tasks := make([]FileScanTask, 1024)
	for i := range tasks {
		partitionValue := int32(7)
		if i%2 != 0 {
			partitionValue = 8
		}
		fileSpec := spec
		if i%4 < 2 {
			fileSpec = unpartitioned
		}
		file := finalizationTestDataFile(
			t, fileSpec,
			fmt.Sprintf("mem://table/data/file-%d.parquet", i),
			map[int]any{1000: partitionValue}, 100, nil)
		tasks[i] = FileScanTask{File: file, Start: 0, Length: file.FileSizeBytes()}
	}

	filter := iceberg.EqualTo(iceberg.Reference("id"), int32(7))
	scan := &Scan{metadata: metadata, rowFilter: filter, caseSensitive: true, concurrency: 8}
	serial, serialMetrics, err := serialFinalizationReference(
		scan, append([]FileScanTask(nil), tasks...), schema)
	require.NoError(t, err)
	var parallelMetrics scanMetricsAccumulator
	parallel, err := scan.finalizePlannedTasks(
		append([]FileScanTask(nil), tasks...), schema, &parallelMetrics)
	require.NoError(t, err)

	assert.Equal(t, serial, parallel)
	assert.Equal(t, serialMetrics, parallelMetrics)
	for i, task := range parallel {
		if i%4 < 2 {
			assert.Nil(t, task.Residual, "task %d has no partition projection", i)
		} else if i%2 == 0 {
			_, ok := task.Residual.(iceberg.AlwaysTrue)
			assert.True(t, ok, "task %d should simplify to always true", i)
		} else {
			_, ok := task.Residual.(iceberg.AlwaysFalse)
			assert.True(t, ok, "task %d should simplify to always false", i)
		}
	}
}

func TestFinalizePlannedTasksReturnsFirstErrorInTaskOrder(t *testing.T) {
	schema := simpleSchema()
	spec := partitionedSpec()
	metadata, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://table", nil)
	require.NoError(t, err)

	tasks := make([]FileScanTask, 1024)
	for i := range tasks {
		var partitionValue any = int32(7)
		if i == 255 || i == 256 {
			// The earlier failing task is at the end of worker range 0;
			// worker 1 can fail first at the start of its own range.
			partitionValue = "invalid integer partition"
		}
		file := finalizationTestDataFile(t, spec,
			fmt.Sprintf("mem://table/data/file-%d.parquet", i),
			map[int]any{1000: partitionValue}, 100, nil)
		tasks[i] = FileScanTask{File: file, Length: file.FileSizeBytes()}
	}

	for _, concurrency := range []int{1, 8} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			scan := &Scan{
				metadata: metadata, concurrency: concurrency, caseSensitive: true,
				rowFilter: iceberg.EqualTo(iceberg.Reference("id"), int32(7)),
			}
			input := append([]FileScanTask(nil), tasks...)
			if concurrency > 1 {
				// With four CPU workers and 1024 tasks, worker 0 owns
				// [0:256] and worker 1 owns [256:512].
				previous := runtime.GOMAXPROCS(4)
				defer runtime.GOMAXPROCS(previous)

				laterErrorReached := make(chan struct{})
				input[255].File = finalizationOrderFile{
					DataFile: input[255].File, wait: laterErrorReached,
				}
				input[256].File = finalizationOrderFile{
					DataFile: input[256].File, signal: laterErrorReached,
				}
			}
			result, planErr := scan.finalizePlannedTasks(input, schema, &scanMetricsAccumulator{})
			require.ErrorContains(t, planErr, "evaluate partition residual for mem://table/data/file-255.parquet")
			assert.Nil(t, result)
		})
	}
}

// Controls which worker reaches its partition error first, without relying on
// scheduler timing. The wrapper intentionally does not forward the built-in
// borrowed-partition interface, so residual evaluation calls Partition.
type finalizationOrderFile struct {
	iceberg.DataFile
	wait   <-chan struct{}
	signal chan struct{}
}

func (f finalizationOrderFile) Partition() map[int]any {
	if f.signal != nil {
		close(f.signal)
	}
	if f.wait != nil {
		<-f.wait
	}

	return f.DataFile.Partition()
}

func TestFinalizePlannedTasksUnsplitReusesInput(t *testing.T) {
	schema := simpleSchema()
	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"mem://table",
		nil,
	)
	require.NoError(t, err)
	file := finalizationTestDataFile(
		t, *iceberg.UnpartitionedSpec, "mem://table/data/file.parquet", nil, 100, nil)
	tasks := make([]FileScanTask, 512)
	for i := range tasks {
		tasks[i] = FileScanTask{File: file, Start: 0, Length: file.FileSizeBytes()}
	}
	scan := &Scan{metadata: metadata, rowFilter: iceberg.AlwaysTrue{}, caseSensitive: true, concurrency: 8}

	got, err := scan.finalizePlannedTasks(tasks, schema, &scanMetricsAccumulator{})
	require.NoError(t, err)
	require.Len(t, got, len(tasks))
	assert.Same(t, &tasks[0], &got[0])
	assert.Same(t, &tasks[len(tasks)-1], &got[len(got)-1])
	assert.Nil(t, got[0].Residual)
}

// serialFinalizationReference reproduces the pre-parallelization loop. It is
// deliberately independent of finalizeTaskRange and the worker merge logic.
func serialFinalizationReference(
	scan *Scan,
	tasks []FileScanTask,
	schema *iceberg.Schema,
) ([]FileScanTask, scanMetricsAccumulator, error) {
	var acc scanMetricsAccumulator
	var filter iceberg.BooleanExpression
	if scan.rowFilter != nil && !scan.rowFilter.Equals(iceberg.AlwaysTrue{}) {
		var err error
		filter, err = iceberg.BindExpr(schema, scan.rowFilter, scan.caseSensitive)
		if err != nil {
			return nil, acc, err
		}
	}
	evaluators := make(map[int]*partitionResidualEvaluator)
	acc.resultDataFiles = int64(len(tasks))
	result := make([]FileScanTask, 0, len(tasks))
	target := scan.metadata.Properties().GetInt64(ReadSplitTargetSizeKey, ReadSplitTargetSizeDefault)
	for _, task := range tasks {
		if filter != nil {
			specID := int(task.File.SpecID())
			evaluator, ok := evaluators[specID]
			if !ok {
				var err error
				evaluator, err = newPartitionResidualEvaluator(
					schema, scan.metadata.PartitionSpecByID(specID), filter, scan.caseSensitive)
				if err != nil {
					return nil, acc, fmt.Errorf("build partition residual evaluator for spec %d: %w", specID, err)
				}
				evaluators[specID] = evaluator
			}
			if evaluator != nil {
				var err error
				var simplified bool
				task.Residual, simplified, err = evaluator.residual(dataFilePartition(task.File))
				if err != nil {
					return nil, acc, fmt.Errorf(
						"evaluate partition residual for %s: %w", task.File.FilePath(), err)
				}
				if !simplified {
					task.Residual = nil
				}
			}
		}
		acc.addResultDeleteMetrics(task)
		acc.totalFileSize += task.File.FileSizeBytes()
		if splits, ok := splitParquetScanTask(task, target); ok {
			result = append(result, splits...)
		} else {
			result = append(result, task)
		}
	}

	return result, acc, nil
}

func TestFinalizePlannedTasksMixedSplitsAgainstIndependentReference(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://table", iceberg.Properties{ReadSplitTargetSizeKey: "25"})
	require.NoError(t, err)

	dvBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		"mem://table/deletes.dv.puffin", iceberg.PuffinFile, nil, nil, nil, 1, 2048)
	require.NoError(t, err)
	dv := dvBuilder.ContentSizeInBytes(17).ContentOffset(100).Build()

	for _, count := range []int{0, 1, 255, 256, 257, 512, 1024} {
		t.Run(fmt.Sprintf("files=%d", count), func(t *testing.T) {
			tasks := make([]FileScanTask, count)
			splits := 0
			for i := range tasks {
				offsets := []int64(nil)
				if i%7 < 2 || i == count-1 || i == 255 || i == 256 {
					offsets = []int64{8, 25, 50, 75}
					splits++
				}
				file := finalizationTestDataFile(t, *iceberg.UnpartitionedSpec,
					fmt.Sprintf("mem://table/data/file-%d.parquet", i), nil, 100, offsets)
				tasks[i] = FileScanTask{File: file, Length: 100}
				if i%9 == 0 {
					tasks[i].DeleteFiles = []iceberg.DataFile{file}
				}
				if i%11 == 0 {
					tasks[i].EqualityDeleteFiles = []iceberg.DataFile{file}
				}
				if i%13 == 0 {
					tasks[i].DeletionVectorFiles = []iceberg.DataFile{dv}
				}
			}
			scan := &Scan{
				metadata: metadata, rowFilter: iceberg.AlwaysTrue{},
				caseSensitive: true, concurrency: 8,
			}
			expected, expectedMetrics, err := serialFinalizationReference(
				scan, append([]FileScanTask(nil), tasks...), schema)
			require.NoError(t, err)
			var actualMetrics scanMetricsAccumulator
			actual, err := scan.finalizePlannedTasks(
				append([]FileScanTask(nil), tasks...), schema, &actualMetrics)
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
			assert.Equal(t, expectedMetrics, actualMetrics)
			assert.Len(t, actual, count+3*splits)
			assert.Equal(t, int64((count+12)/13), actualMetrics.dvs)
			assert.Equal(t, int64((count+8)/9*100+(count+10)/11*100+(count+12)/13*17),
				actualMetrics.totalDeleteFileSize)
		})
	}
}

type panicSizeFinalizationFile struct {
	iceberg.DataFile
}

func (panicSizeFinalizationFile) FileSizeBytes() int64 {
	panic("injected file size panic")
}

func TestFinalizePlannedTasksWorkerPanicReturnsError(t *testing.T) {
	schema := simpleSchema()
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://table", nil)
	require.NoError(t, err)
	file := finalizationTestDataFile(t, *iceberg.UnpartitionedSpec,
		"mem://table/data/file.parquet", nil, 100, nil)
	tasks := make([]FileScanTask, 512)
	for i := range tasks {
		tasks[i] = FileScanTask{File: file, Length: 100}
	}
	tasks[256].File = panicSizeFinalizationFile{DataFile: file}

	scan := &Scan{metadata: meta, rowFilter: iceberg.AlwaysTrue{}, caseSensitive: true, concurrency: 8}
	actual, err := scan.finalizePlannedTasks(tasks, schema, &scanMetricsAccumulator{})
	require.ErrorContains(t, err, "injected file size panic")
	require.Nil(t, actual)
}

func finalizationTestDataFile(
	t testing.TB,
	spec iceberg.PartitionSpec,
	path string,
	partition map[int]any,
	size int64,
	offsets []int64,
) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		100,
		size,
	)
	require.NoError(t, err)
	if offsets != nil {
		builder.SplitOffsets(offsets)
	}

	return builder.Build()
}

var scanTaskFinalizationBenchmarkSink int

func BenchmarkFinalizePlannedTasks(b *testing.B) {
	schema := simpleSchema()
	spec := partitionedSpec()

	for _, tc := range []struct {
		name       string
		rowFilter  iceberg.BooleanExpression
		offsets    []int64
		targetSize string
		fileCount  int
	}{
		{
			name:      "plain",
			rowFilter: iceberg.AlwaysTrue{},
			fileCount: 100_000,
		},
		{
			name:      "identity_residual",
			rowFilter: iceberg.EqualTo(iceberg.Reference("id"), int32(7)),
			fileCount: 100_000,
		},
		{
			name:       "split_offsets",
			rowFilter:  iceberg.AlwaysTrue{},
			offsets:    benchmarkFinalizationOffsets(32, 32),
			targetSize: "128",
			fileCount:  20_000,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			properties := iceberg.Properties{}
			if tc.targetSize != "" {
				properties[ReadSplitTargetSizeKey] = tc.targetSize
			}
			metadata, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://benchmark", properties)
			if err != nil {
				b.Fatal(err)
			}
			tasks := make([]FileScanTask, tc.fileCount)
			for i := range tasks {
				file := finalizationTestDataFile(
					b,
					spec,
					fmt.Sprintf("mem://benchmark/data/file-%d.parquet", i),
					map[int]any{1000: int32(7)},
					2048,
					tc.offsets,
				)
				tasks[i] = FileScanTask{File: file, Start: 0, Length: file.FileSizeBytes()}
			}

			b.Run("old_serial_loop", func(b *testing.B) {
				scan := &Scan{
					metadata: metadata, rowFilter: tc.rowFilter, caseSensitive: true, concurrency: 1,
				}
				b.ReportAllocs()
				for b.Loop() {
					result, _, err := serialFinalizationReference(scan, tasks, schema)
					if err != nil {
						b.Fatal(err)
					}
					scanTaskFinalizationBenchmarkSink += len(result)
				}
			})

			for _, concurrency := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
					scan := &Scan{
						metadata: metadata, rowFilter: tc.rowFilter,
						caseSensitive: true, concurrency: concurrency,
					}
					b.ReportAllocs()
					b.ReportMetric(float64(tc.fileCount), "files/op")
					for b.Loop() {
						var acc scanMetricsAccumulator
						finalized, err := scan.finalizePlannedTasks(tasks, schema, &acc)
						if err != nil {
							b.Fatal(err)
						}
						scanTaskFinalizationBenchmarkSink += len(finalized)
					}
				})
			}
		})
	}
}

func benchmarkFinalizationOffsets(count int, spacing int64) []int64 {
	offsets := make([]int64, count)
	for i := range offsets {
		offsets[i] = int64(i+1) * spacing
	}

	return offsets
}
