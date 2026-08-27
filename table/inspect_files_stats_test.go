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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/require"
)

type borrowedInspectDataFile struct {
	iceberg.DataFile
}

func (f *borrowedInspectDataFile) Partition() map[int]any {
	panic("metadata appender called the public Partition getter")
}

func (f *borrowedInspectDataFile) ValueCounts() map[int]int64 {
	panic("metadata appender called the public ValueCounts getter")
}

func (f *borrowedInspectDataFile) NullValueCounts() map[int]int64 {
	panic("metadata appender called the public NullValueCounts getter")
}

func (f *borrowedInspectDataFile) NaNValueCounts() map[int]int64 {
	panic("metadata appender called the public NaNValueCounts getter")
}

func (f *borrowedInspectDataFile) LowerBoundValues() map[int][]byte {
	panic("metadata appender called the public LowerBoundValues getter")
}

func (f *borrowedInspectDataFile) UpperBoundValues() map[int][]byte {
	panic("metadata appender called the public UpperBoundValues getter")
}

func (f *borrowedInspectDataFile) ColumnSizes() map[int]int64 {
	panic("metadata appender called the public ColumnSizes getter")
}

func (f *borrowedInspectDataFile) KeyMetadata() []byte {
	panic("metadata appender called the public KeyMetadata getter")
}

func (f *borrowedInspectDataFile) SplitOffsets() []int64 {
	panic("metadata appender called the public SplitOffsets getter")
}

func (f *borrowedInspectDataFile) EqualityFieldIDs() []int {
	panic("metadata appender called the public EqualityFieldIDs getter")
}

func (f *borrowedInspectDataFile) DataFileStatsRef(_ internal.DataFileRef) (
	map[int]int64, map[int]int64, map[int]int64, map[int][]byte, map[int][]byte,
) {
	return internal.BorrowedDataFileStats(f.DataFile)
}

func (f *borrowedInspectDataFile) DataFileCollectionsRef(_ internal.DataFileRef) (
	map[int]int64, []byte, []int64, []int,
) {
	return internal.BorrowedDataFileCollections(f.DataFile)
}

func (f *borrowedInspectDataFile) DataFilePartitionRef(_ internal.DataFileRef) map[int]any {
	return internal.BorrowedDataFilePartition(f.DataFile)
}

func TestInspectContentFileBuilderUsesBorrowedDataFileMetadata(t *testing.T) {
	file := &borrowedInspectDataFile{DataFile: testDataFileWithStats(t)}
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "part", Type: iceberg.PrimitiveTypes.String},
	}}

	withInspectContentFileRecord(t, partitionType, file, func(record arrow.RecordBatch) {
		require.EqualValues(t, 1, record.NumRows())
		partition := record.Column(4).(*array.Struct)
		require.Equal(t, "partition", partition.Field(0).(*array.String).Value(0))

		valueCounts := record.Column(8).(*array.Map)
		require.False(t, valueCounts.IsNull(0))
		start, end := valueCounts.ValueOffsets(0)
		require.EqualValues(t, 1, end-start)
		require.EqualValues(t, 2, valueCounts.Items().(*array.Int64).Value(int(start)))

		columnSizes := record.Column(7).(*array.Map)
		require.False(t, columnSizes.IsNull(0))
		start, end = columnSizes.ValueOffsets(0)
		require.EqualValues(t, 1, end-start)
		require.EqualValues(t, 10, columnSizes.Items().(*array.Int64).Value(int(start)))

		require.Equal(t, []byte{5, 6}, record.Column(13).(*array.Binary).Value(0))

		splitOffsets := record.Column(14).(*array.List)
		start, end = splitOffsets.ValueOffsets(0)
		require.EqualValues(t, 2, end-start)
		require.EqualValues(t, 0, splitOffsets.ListValues().(*array.Int64).Value(int(start)))
		require.EqualValues(t, 64, splitOffsets.ListValues().(*array.Int64).Value(int(start+1)))

		equalityIDs := record.Column(15).(*array.List)
		start, end = equalityIDs.ValueOffsets(0)
		require.EqualValues(t, 1, end-start)
		require.EqualValues(t, 1, equalityIDs.ListValues().(*array.Int32).Value(int(start)))
	})
}

func TestInspectContentFileBuilderFallsBackToPublicDataFileMetadata(t *testing.T) {
	file := &publicStatsDataFile{DataFile: testDataFileWithStats(t)}
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "part", Type: iceberg.PrimitiveTypes.String},
	}}

	withInspectContentFileRecord(t, partitionType, file, func(record arrow.RecordBatch) {
		require.EqualValues(t, 1, record.NumRows())
	})
	require.Equal(t, 9, file.getterCalls)
}

func withInspectContentFileRecord(
	t *testing.T,
	partitionType *iceberg.StructType,
	file iceberg.DataFile,
	check func(arrow.RecordBatch),
) {
	t.Helper()

	arrowSchema, err := SchemaToArrowSchema(DataFilesSchema(partitionType), nil, true, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, appendContentFileRecord(builder, partitionType, file))

	record := builder.NewRecordBatch()
	defer record.Release()
	check(record)
}
