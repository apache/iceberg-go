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

	iceberg "github.com/apache/iceberg-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type publicStatsDataFile struct {
	iceberg.DataFile
	getterCalls int
}

func (f *publicStatsDataFile) ValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.ValueCounts()
}

func (f *publicStatsDataFile) NullValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.NullValueCounts()
}

func (f *publicStatsDataFile) NaNValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.NaNValueCounts()
}

func (f *publicStatsDataFile) LowerBoundValues() map[int][]byte {
	f.getterCalls++

	return f.DataFile.LowerBoundValues()
}

func (f *publicStatsDataFile) UpperBoundValues() map[int][]byte {
	f.getterCalls++

	return f.DataFile.UpperBoundValues()
}

func testDataFileWithStats(t *testing.T) iceberg.DataFile {
	t.Helper()

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "part",
		Transform: iceberg.IdentityTransform{},
	})
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"s3://bucket/file.parquet",
		iceberg.ParquetFile,
		map[int]any{1000: "partition"},
		nil,
		nil,
		2,
		10,
	)
	require.NoError(t, err)

	return builder.
		ValueCounts(map[int]int64{1: 2}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {1, 2}}).
		UpperBoundValues(map[int][]byte{1: {3, 4}}).
		Build()
}

func TestDataFileStatsUsesBorrowedView(t *testing.T) {
	file := testDataFileWithStats(t)
	require.Implements(t, (*dataFileStatsRefer)(nil), file)

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(file)
	assert.Equal(t, map[int]int64{1: 2}, valueCounts)
	assert.Equal(t, map[int]int64{1: 0}, nullCounts)
	assert.Equal(t, map[int]int64{1: 0}, nanCounts)
	assert.Equal(t, map[int][]byte{1: {1, 2}}, lowerBounds)
	assert.Equal(t, map[int][]byte{1: {3, 4}}, upperBounds)

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		dataFileStats(file)
	}))
}

func TestDataFileStatsFallsBackToPublicGetters(t *testing.T) {
	file := &publicStatsDataFile{DataFile: testDataFileWithStats(t)}

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(file)
	assert.Equal(t, map[int]int64{1: 2}, valueCounts)
	assert.Equal(t, map[int]int64{1: 0}, nullCounts)
	assert.Equal(t, map[int]int64{1: 0}, nanCounts)
	assert.Equal(t, map[int][]byte{1: {1, 2}}, lowerBounds)
	assert.Equal(t, map[int][]byte{1: {3, 4}}, upperBounds)
	assert.Equal(t, 5, file.getterCalls)
}
