// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

type countingPositionDeleteDataFile struct {
	iceberg.DataFile
	partitionCalls     int
	specIDCalls        int
	filePathCalls      int
	contentOffsetCalls int
	contentSizeCalls   int
}

func (f *countingPositionDeleteDataFile) Partition() map[int]any {
	f.partitionCalls++

	return f.DataFile.Partition()
}

func (f *countingPositionDeleteDataFile) SpecID() int32 {
	f.specIDCalls++

	return f.DataFile.SpecID()
}

func (f *countingPositionDeleteDataFile) FilePath() string {
	f.filePathCalls++

	return f.DataFile.FilePath()
}

func (f *countingPositionDeleteDataFile) ContentOffset() *int64 {
	f.contentOffsetCalls++

	return f.DataFile.ContentOffset()
}

func (f *countingPositionDeleteDataFile) ContentSizeInBytes() *int64 {
	f.contentSizeCalls++

	return f.DataFile.ContentSizeInBytes()
}

func TestPositionDeleteAppenderReusesFileMetadata(t *testing.T) {
	schema := simpleSchema()
	spec := partitionedSpec()
	baseBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentPosDeletes,
		"mem://position-deletes/table/data/delete.puffin",
		iceberg.PuffinFile,
		map[int]any{1000: int32(7)},
		nil,
		nil,
		3,
		128,
	)
	require.NoError(t, err)
	baseFile := baseBuilder.ContentOffset(16).ContentSizeInBytes(64).Build()
	file := &countingPositionDeleteDataFile{DataFile: baseFile}

	partitionType := spec.PartitionType(schema)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(schema, partitionType, 3), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(
		bldr, partitionType, map[int]int{1000: 1000}, 3)
	require.NoError(t, err)

	meta := appender.prepareFile(file)
	require.Equal(t, map[int]any{1000: int32(7)}, meta.partition)
	require.EqualValues(t, 0, meta.specID)
	require.Equal(t, baseFile.FilePath(), meta.deleteFilePath)
	require.Equal(t, int64(16), *meta.contentOffset)
	require.Equal(t, int64(64), *meta.contentSize)

	for pos := range 100 {
		require.NoError(t, appender.appendPrepared(
			meta, "mem://position-deletes/table/data/data.parquet", int64(pos), nil, false))
	}

	record := bldr.NewRecordBatch()
	defer record.Release()
	require.EqualValues(t, 100, record.NumRows())
	partition := record.Column(3).(*array.Struct)
	require.EqualValues(t, 7, partition.Field(0).(*array.Int32).Value(0))
	require.EqualValues(t, 0, record.Column(4).(*array.Int32).Value(0))
	require.Equal(t, baseFile.FilePath(), record.Column(5).(*array.String).Value(0))
	require.EqualValues(t, 16, record.Column(6).(*array.Int64).Value(0))
	require.EqualValues(t, 64, record.Column(7).(*array.Int64).Value(0))

	require.Equal(t, 1, file.partitionCalls)
	require.Equal(t, 1, file.specIDCalls)
	require.Equal(t, 1, file.filePathCalls)
	require.Equal(t, 1, file.contentOffsetCalls)
	require.Equal(t, 1, file.contentSizeCalls)
}

func TestPositionDeleteAppenderCopiesBinaryPartitionValueWhenPreparingMetadata(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "part", Type: iceberg.PrimitiveTypes.Binary, Required: false},
	}}
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(simpleSchema(), partitionType, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(
		bldr, partitionType, map[int]int{1000: 1000}, 2)
	require.NoError(t, err)

	partitionValue := []byte("before")
	file := &mockDataFile{
		path:      "mem://position-deletes/table/data/delete.parquet",
		format:    iceberg.ParquetFile,
		partition: map[int]any{1000: partitionValue},
	}
	meta := appender.prepareFile(file)
	partitionValue[0] = 'a'

	require.Equal(t, []byte("before"), meta.partition[1000])
}

func TestPositionDeleteAppenderDoesNotReadUnpartitionedPartition(t *testing.T) {
	file := &countingPositionDeleteDataFile{
		DataFile: newPosDeleteFile(t,
			"mem://position-deletes/table/data/delete.parquet", 1, 128),
	}
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(simpleSchema(), &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(
		bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	meta := appender.prepareFile(file)

	require.Nil(t, meta.partition)
	require.Zero(t, file.partitionCalls)
}

var positionDeletesPartitionTypeBenchmarkSink *iceberg.StructType

func BenchmarkPositionDeletesPartitionType(b *testing.B) {
	for _, schemaCount := range []int{1, 16, 128, 1024} {
		b.Run(fmt.Sprintf("schemas=%d/fields=64", schemaCount), func(b *testing.B) {
			metadata := benchmarkPositionDeletesMetadata(b, schemaCount, 64)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				partitionType, _, err := positionDeletesPartitionType(metadata)
				if err != nil {
					b.Fatal(err)
				}
				positionDeletesPartitionTypeBenchmarkSink = partitionType
			}
		})
	}
}

func benchmarkPositionDeletesMetadata(b *testing.B, schemaCount, fieldsPerSchema int) Metadata {
	b.Helper()

	schemas := make([]*iceberg.Schema, schemaCount)
	lastColumnID := 0
	for schemaID := range schemaCount {
		fields := make([]iceberg.NestedField, fieldsPerSchema)
		for fieldIndex := range fieldsPerSchema {
			fieldID := schemaID*fieldsPerSchema + fieldIndex + 1
			fields[fieldIndex] = iceberg.NestedField{
				ID: fieldID, Name: fmt.Sprintf("field_%d", fieldID),
				Type: iceberg.PrimitiveTypes.Int32, Required: true,
			}
			lastColumnID = fieldID
		}
		schemas[schemaID] = iceberg.NewSchema(schemaID, fields...)
	}

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	lastPartitionID := 1000

	return &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:   2,
		LastColumnId:    lastColumnID,
		SchemaList:      schemas,
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{spec},
		DefaultSpecID:   spec.ID(),
		LastPartitionID: &lastPartitionID,
	}}
}

func BenchmarkPositionDeleteAppender(b *testing.B) {
	for _, fieldCount := range []int{1, 5} {
		for _, rowCount := range []int{1_000, 100_000} {
			for _, reuse := range []struct {
				name  string
				reuse bool
			}{
				{name: "prepare_per_row", reuse: false},
				{name: "prepare_once", reuse: true},
			} {
				b.Run(fmt.Sprintf("fields=%d/rows=%d/%s", fieldCount, rowCount, reuse.name),
					func(b *testing.B) {
						benchmarkPositionDeleteAppender(b, fieldCount, rowCount, reuse.reuse)
					})
			}
		}
	}
}

func benchmarkPositionDeleteAppender(b *testing.B, fieldCount, rowCount int, reuse bool) {
	b.Helper()

	schemaFields := make([]iceberg.NestedField, fieldCount)
	partitionFields := make([]iceberg.PartitionField, fieldCount)
	partitionValues := make(map[int]any, fieldCount)
	partitionIDs := make(map[int]int, fieldCount)
	for i := range fieldCount {
		sourceID := i + 1
		fieldID := 1000 + i
		schemaFields[i] = iceberg.NestedField{
			ID: sourceID, Name: fmt.Sprintf("source_%d", sourceID),
			Type: iceberg.PrimitiveTypes.Int32, Required: true,
		}
		partitionFields[i] = iceberg.PartitionField{
			SourceIDs: []int{sourceID}, FieldID: fieldID,
			Name: fmt.Sprintf("partition_%d", fieldID), Transform: iceberg.IdentityTransform{},
		}
		partitionValues[fieldID] = int32(i)
		partitionIDs[fieldID] = fieldID
	}

	tableSchema := iceberg.NewSchema(0, schemaFields...)
	spec := iceberg.NewPartitionSpec(partitionFields...)
	partitionType := spec.PartitionType(tableSchema)
	fileBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, "delete.puffin", iceberg.PuffinFile,
		partitionValues, nil, nil, int64(rowCount), 128)
	if err != nil {
		b.Fatal(err)
	}
	file := fileBuilder.ContentOffset(16).ContentSizeInBytes(64).Build()

	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, partitionType, 3), nil, true, false)
	if err != nil {
		b.Fatal(err)
	}
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, partitionType, partitionIDs, 3)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ReportMetric(float64(rowCount), "rows/op")
	b.ResetTimer()
	for range b.N {
		var meta positionDeleteFileMeta
		if reuse {
			meta = appender.prepareFile(file)
		}
		for pos := range rowCount {
			var err error
			if reuse {
				err = appender.appendPrepared(
					meta, "data.parquet", int64(pos), nil, false)
			} else {
				err = appender.append(file, "data.parquet", int64(pos), nil, false)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		record := bldr.NewRecordBatch()
		record.Release()
	}
	b.StopTimer()
}
