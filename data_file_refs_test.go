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

package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/require"
)

func TestBorrowedSplitOffsetsDoNotMaterializeColumnStats(t *testing.T) {
	builder, err := NewDataFileBuilder(
		*UnpartitionedSpec,
		EntryContentData,
		"file.parquet",
		ParquetFile,
		nil,
		nil,
		nil,
		1,
		128,
	)
	require.NoError(t, err)

	file := builder.
		ColumnSizes(map[int]int64{1: 8}).
		ValueCounts(map[int]int64{1: 1}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {1}}).
		UpperBoundValues(map[int][]byte{1: {2}}).
		SplitOffsets([]int64{0, 64}).
		Build().(*dataFile)

	require.Nil(t, file.colSizeMap)
	require.Nil(t, file.valCntMap)
	require.Nil(t, file.nullCntMap)
	require.Nil(t, file.nanCntMap)
	require.Nil(t, file.lowerBoundMap)
	require.Nil(t, file.upperBoundMap)

	offsets := internal.BorrowedDataFileSplitOffsets(file)
	require.Equal(t, []int64{0, 64}, offsets)

	require.Nil(t, file.colSizeMap)
	require.Nil(t, file.valCntMap)
	require.Nil(t, file.nullCntMap)
	require.Nil(t, file.nanCntMap)
	require.Nil(t, file.lowerBoundMap)
	require.Nil(t, file.upperBoundMap)
}
