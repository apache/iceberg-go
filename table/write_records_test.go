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

package table_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type WriteRecordsTestSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator
	ctx context.Context
}

func TestWriteRecords(t *testing.T) {
	suite.Run(t, new(WriteRecordsTestSuite))
}

func (s *WriteRecordsTestSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.ctx = compute.WithAllocator(context.Background(), s.mem)
}

func (s *WriteRecordsTestSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *WriteRecordsTestSuite) newTable(loc string) *table.Table {
	iceSch := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	spec := iceberg.NewPartitionSpec()
	meta, err := table.NewMetadata(iceSch, &spec, table.UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)

	return table.New(
		table.Identifier{"test", "write_records"},
		meta,
		filepath.Join(loc, "metadata", "v1.metadata.json"),
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		nil,
	)
}

func (s *WriteRecordsTestSuite) arrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func (s *WriteRecordsTestSuite) buildRecords(schema *arrow.Schema, numRows int) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(s.mem, schema)
	defer bldr.Release()

	for i := range numRows {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		bldr.Field(1).(*array.StringBuilder).Append("value")
	}

	return bldr.NewRecordBatch()
}

func (s *WriteRecordsTestSuite) TestBasicWrite() {
	loc := filepath.ToSlash(s.T().TempDir())
	tbl := s.newTable(loc)
	schema := s.arrowSchema()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		rec := s.buildRecords(schema, 10)
		yield(rec, nil)
	}

	var dataFiles []iceberg.DataFile
	for df, err := range table.WriteRecords(s.ctx, tbl, schema, records) {
		s.Require().NoError(err)
		dataFiles = append(dataFiles, df)
	}

	s.Require().Len(dataFiles, 1)
	s.Equal(int64(10), dataFiles[0].Count())
	s.Equal(iceberg.ParquetFile, dataFiles[0].FileFormat())
	s.Greater(dataFiles[0].FileSizeBytes(), int64(0))
	s.Contains(dataFiles[0].FilePath(), loc)
}

func (s *WriteRecordsTestSuite) TestSmallTargetFileSizeProducesMultipleFiles() {
	loc := filepath.ToSlash(s.T().TempDir())
	tbl := s.newTable(loc)
	schema := s.arrowSchema()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		for range 10 {
			rec := s.buildRecords(schema, 100)
			if !yield(rec, nil) {
				return
			}
		}
	}

	var totalRows int64
	var dataFiles []iceberg.DataFile
	for df, err := range table.WriteRecords(s.ctx, tbl, schema, records, table.WithTargetFileSize(1)) {
		s.Require().NoError(err)
		dataFiles = append(dataFiles, df)
		totalRows += df.Count()
	}

	s.Greater(len(dataFiles), 1)
	s.Equal(int64(1000), totalRows)
}

func (s *WriteRecordsTestSuite) TestWithWriteUUID() {
	loc := filepath.ToSlash(s.T().TempDir())
	tbl := s.newTable(loc)
	schema := s.arrowSchema()

	writeID := uuid.New()
	records := func(yield func(arrow.RecordBatch, error) bool) {
		rec := s.buildRecords(schema, 5)
		yield(rec, nil)
	}

	for df, err := range table.WriteRecords(s.ctx, tbl, schema, records, table.WithWriteUUID(writeID)) {
		s.Require().NoError(err)
		s.Contains(df.FilePath(), writeID.String())
	}
}

func (s *WriteRecordsTestSuite) TestClusteredWithMaxWriteWorkersIsRejected() {
	loc := filepath.ToSlash(s.T().TempDir())
	tbl := s.newTable(loc)
	schema := s.arrowSchema()

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	var (
		dataFiles []iceberg.DataFile
		writeErr  error
	)
	for df, err := range table.WriteRecords(s.ctx, tbl, schema, records,
		table.WithClusteredWrite(), table.WithMaxWriteWorkers(4)) {
		if err != nil {
			writeErr = err

			break
		}
		dataFiles = append(dataFiles, df)
	}

	s.Require().Error(writeErr)
	s.Contains(writeErr.Error(), "WithClusteredWrite")
	s.Contains(writeErr.Error(), "WithMaxWriteWorkers")
	s.Empty(dataFiles)
}

func (s *WriteRecordsTestSuite) TestFSNotWritable() {
	iceSch := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)
	spec := iceberg.NewPartitionSpec()
	meta, err := table.NewMetadata(iceSch, &spec, table.UnsortedSortOrder, "/tmp/noop", iceberg.Properties{})
	s.Require().NoError(err)

	tbl := table.New(
		table.Identifier{"test", "readonly"},
		meta,
		"/tmp/noop/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) { return readOnlyFS{}, nil },
		nil,
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	records := func(yield func(arrow.RecordBatch, error) bool) {}

	for _, err := range table.WriteRecords(s.ctx, tbl, schema, records) {
		s.Require().ErrorIs(err, iceberg.ErrNotImplemented)
	}
}

func (s *WriteRecordsTestSuite) TestFSError() {
	iceSch := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)
	spec := iceberg.NewPartitionSpec()
	meta, err := table.NewMetadata(iceSch, &spec, table.UnsortedSortOrder, "/tmp/noop", iceberg.Properties{})
	s.Require().NoError(err)

	fsErr := errors.New("connection refused")
	tbl := table.New(
		table.Identifier{"test", "fs_error"},
		meta,
		"/tmp/noop/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) { return nil, fsErr },
		nil,
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	records := func(yield func(arrow.RecordBatch, error) bool) {}

	for _, err := range table.WriteRecords(s.ctx, tbl, schema, records) {
		s.Require().ErrorIs(err, fsErr)
	}
}

func (s *WriteRecordsTestSuite) TestEmptyInput() {
	loc := filepath.ToSlash(s.T().TempDir())
	tbl := s.newTable(loc)
	schema := s.arrowSchema()

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	count := 0
	for _, err := range table.WriteRecords(s.ctx, tbl, schema, records) {
		s.Require().NoError(err)
		count++
	}

	s.Equal(0, count)
}

type readOnlyFS struct{}

func (readOnlyFS) Open(name string) (iceio.File, error) {
	return nil, errors.New("not supported")
}

func (readOnlyFS) Remove(name string) error {
	return errors.New("not supported")
}
