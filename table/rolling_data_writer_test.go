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
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type RollingDataWriterTestSuite struct {
	suite.Suite

	mem memory.Allocator
	ctx context.Context
}

func (s *RollingDataWriterTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func TestRollingDataWriter(t *testing.T) {
	suite.Run(t, new(RollingDataWriterTestSuite))
}

func (s *RollingDataWriterTestSuite) createWriterFactory(loc string, arrSchema *arrow.Schema, targetFileSize int64) (*writerFactory, *iceberg.Schema) {
	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	spec := iceberg.NewPartitionSpec()
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)

	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	s.Require().NoError(err)

	writeUUID := uuid.New()
	args := recordWritingArgs{
		sc:        arrSchema,
		fs:        iceio.LocalFS{},
		writeUUID: &writeUUID,
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					break
				}
			}
		},
	}

	factory, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, targetFileSize)
	s.Require().NoError(err)

	return factory, icebergSchema
}

func (s *RollingDataWriterTestSuite) buildRecord(arrSchema *arrow.Schema, numRows int) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(s.mem, arrSchema)
	defer bldr.Release()

	for i := range numRows {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		bldr.Field(1).(*array.StringBuilder).Append("value")
	}

	return bldr.NewRecordBatch()
}

func (s *RollingDataWriterTestSuite) TestSingleFileUnderTarget() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, arrSchema, 1024*1024)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(s.ctx, nil, "", nil, outputCh)

	record := s.buildRecord(arrSchema, 5)
	defer record.Release()

	s.Require().NoError(writer.Add(record))
	s.Require().NoError(writer.closeAndWait())
	close(outputCh)

	var dataFiles []iceberg.DataFile
	for df := range outputCh {
		dataFiles = append(dataFiles, df)
	}

	s.Require().Len(dataFiles, 1)
	s.Equal(int64(5), dataFiles[0].Count())
}

func (s *RollingDataWriterTestSuite) TestRollsMultipleFiles() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, arrSchema, 1)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 100)
	writer := factory.newRollingDataWriter(s.ctx, nil, "", nil, outputCh)

	totalRows := int64(0)
	for range 10 {
		record := s.buildRecord(arrSchema, 50)
		s.Require().NoError(writer.Add(record))
		totalRows += record.NumRows()
		record.Release()
	}

	s.Require().NoError(writer.closeAndWait())
	close(outputCh)

	var dataFiles []iceberg.DataFile
	for df := range outputCh {
		dataFiles = append(dataFiles, df)
	}

	s.Greater(len(dataFiles), 1, "expected multiple files when targetFileSize is 1 byte")

	var actualRows int64
	for _, df := range dataFiles {
		actualRows += df.Count()
	}
	s.Equal(totalRows, actualRows)
}

func (s *RollingDataWriterTestSuite) TestBytesWrittenReflectsCompressedSize() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	spec := iceberg.NewPartitionSpec()
	format := tblutils.GetFileFormat(iceberg.ParquetFile)
	writeProps := format.GetWriteProperties(iceberg.Properties{})

	statsCols, err := computeStatsPlan(icebergSchema, iceberg.Properties{})
	s.Require().NoError(err)

	filePath := filepath.Join(loc, "test-bytes-written.parquet")
	fw, err := format.NewFileWriter(s.ctx, iceio.LocalFS{}, nil, tblutils.WriteFileInfo{
		FileSchema: icebergSchema,
		FileName:   filePath,
		StatsCols:  statsCols,
		WriteProps: writeProps,
		Spec:       spec,
	}, arrSchema)
	s.Require().NoError(err)

	record := s.buildRecord(arrSchema, 100)
	defer record.Release()

	s.Require().NoError(fw.Write(record))

	bytesWritten := fw.BytesWritten()
	s.Greater(bytesWritten, int64(0), "BytesWritten should be positive after writing data")

	arrowBytes := int64(record.NumCols()) * record.NumRows() * 4
	s.Less(bytesWritten, arrowBytes, "compressed parquet should be smaller than raw arrow estimate")

	df, err := fw.Close()
	s.Require().NoError(err)
	s.Equal(int64(100), df.Count())
}
