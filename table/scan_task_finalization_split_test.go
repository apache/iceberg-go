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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFinalizeTaskRangeMixedParquetSplitOffsets(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	valid := finalizationTestDataFile(t, spec, "mem://valid.parquet", nil, 100, []int64{8, 40, 70})
	invalid := finalizationTestDataFile(t, spec, "mem://invalid.parquet", nil, 100, []int64{8, 40, 40})
	firstRowID := int64(23)

	tasks := []FileScanTask{
		{File: invalid, Length: 100},
		{File: valid, Length: 100, FirstRowID: &firstRowID},
		{File: valid, Start: 40, Length: 60},
		{File: invalid, Length: 100},
		{File: valid, Length: 100, Residual: iceberg.AlwaysTrue{}},
	}
	before := append([]FileScanTask(nil), tasks...)
	result := (&Scan{}).finalizeTaskRange(tasks, 0, len(tasks), nil, 50)

	require.NoError(t, result.err)
	assert.True(t, result.expanded)
	require.Len(t, result.tasks, 9)
	assert.Equal(t, before, tasks, "splitting must not mutate the source tasks")
	assert.Equal(t, before[0], result.tasks[0])
	assert.Equal(t, before[2], result.tasks[4])
	assert.Equal(t, before[3], result.tasks[5])
	for _, first := range []int{1, 6} {
		for splitIndex, start := range []int64{0, 40, 70} {
			split := result.tasks[first+splitIndex]
			assert.Equal(t, start, split.Start)
			assert.Equal(t, []int64{40, 30, 30}[splitIndex], split.Length)
			assert.Equal(t, valid, split.File)
		}
	}
	for _, task := range result.tasks[1:4] {
		assert.Equal(t, &firstRowID, task.FirstRowID)
	}
	for _, task := range result.tasks[6:9] {
		assert.Equal(t, iceberg.AlwaysTrue{}, task.Residual)
	}
}
