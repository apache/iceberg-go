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

	tasks := make([]FileScanTask, 256)
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

	serialScan := &Scan{
		metadata: metadata, rowFilter: iceberg.AlwaysTrue{}, caseSensitive: true, concurrency: 1,
	}
	parallelScan := &Scan{
		metadata: metadata, rowFilter: iceberg.AlwaysTrue{}, caseSensitive: true, concurrency: 8,
	}
	serialInput := append([]FileScanTask(nil), tasks...)
	parallelInput := append([]FileScanTask(nil), tasks...)
	var serialMetrics, parallelMetrics scanMetricsAccumulator

	serial, err := serialScan.finalizePlannedTasks(serialInput, schema, &serialMetrics)
	require.NoError(t, err)
	parallel, err := parallelScan.finalizePlannedTasks(parallelInput, schema, &parallelMetrics)
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
	assert.Equal(t, int64(86), parallelMetrics.positionalDeleteFiles)
	assert.Equal(t, int64(52), parallelMetrics.equalityDeleteFiles)
	assert.Equal(t, int64(138), parallelMetrics.resultDeleteFiles)
	assert.Equal(t, int64(13_800), parallelMetrics.totalDeleteFileSize)
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

	tasks := make([]FileScanTask, 256)
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
	serialScan := &Scan{metadata: metadata, rowFilter: filter, caseSensitive: true, concurrency: 1}
	parallelScan := &Scan{metadata: metadata, rowFilter: filter, caseSensitive: true, concurrency: 8}
	serialInput := append([]FileScanTask(nil), tasks...)
	parallelInput := append([]FileScanTask(nil), tasks...)
	var serialMetrics, parallelMetrics scanMetricsAccumulator

	serial, err := serialScan.finalizePlannedTasks(serialInput, schema, &serialMetrics)
	require.NoError(t, err)
	parallel, err := parallelScan.finalizePlannedTasks(parallelInput, schema, &parallelMetrics)
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

	tasks := make([]FileScanTask, 256)
	for i := range tasks {
		var partitionValue any = int32(7)
		if i == 1 || i == 128 {
			// Put failures in separate worker ranges. Both must retain the
			// first file's diagnostic even if a later range finishes first.
			partitionValue = "invalid integer partition"
		}
		file := finalizationTestDataFile(t, spec,
			fmt.Sprintf("mem://table/data/file-%d.parquet", i),
			map[int]any{1000: partitionValue}, 100, nil)
		tasks[i] = FileScanTask{File: file, Length: file.FileSizeBytes()}
	}

	for _, concurrency := range []int{1, 8} {
		scan := &Scan{
			metadata: metadata, concurrency: concurrency, caseSensitive: true,
			rowFilter: iceberg.EqualTo(iceberg.Reference("id"), int32(7)),
		}
		result, err := scan.finalizePlannedTasks(tasks, schema, &scanMetricsAccumulator{})
		require.ErrorContains(t, err, "evaluate partition residual for mem://table/data/file-1.parquet")
		assert.Nil(t, result)
	}
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
	tasks := []FileScanTask{{File: file, Start: 0, Length: file.FileSizeBytes()}}
	scan := &Scan{metadata: metadata, rowFilter: iceberg.AlwaysTrue{}, caseSensitive: true, concurrency: 8}

	got, err := scan.finalizePlannedTasks(tasks, schema, &scanMetricsAccumulator{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Same(t, &tasks[0], &got[0])
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

			for _, concurrency := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
					scan := &Scan{
						metadata: metadata, rowFilter: tc.rowFilter,
						caseSensitive: true, concurrency: concurrency,
					}
					b.ReportAllocs()
					b.ReportMetric(float64(tc.fileCount), "files/op")
					b.ResetTimer()
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
