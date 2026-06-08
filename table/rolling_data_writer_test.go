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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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

func (s *RollingDataWriterTestSuite) createFileWriter(loc string, arrSchema *arrow.Schema, fileName string) tblutils.FileWriter {
	s.T().Helper()

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	spec := iceberg.NewPartitionSpec()
	format := tblutils.GetFileFormat(iceberg.ParquetFile)
	writeProps := format.GetWriteProperties(iceberg.Properties{})

	statsCols, err := computeStatsPlan(icebergSchema, iceberg.Properties{})
	s.Require().NoError(err)

	fw, err := format.NewFileWriter(s.ctx, iceio.LocalFS{}, nil, tblutils.WriteFileInfo{
		FileSchema: icebergSchema,
		FileName:   filepath.Join(loc, fileName),
		StatsCols:  statsCols,
		WriteProps: writeProps,
		Spec:       spec,
	}, arrSchema)
	s.Require().NoError(err)

	return fw
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
	writer := factory.newRollingDataWriter(s.ctx, "", nil, outputCh)

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
	writer := factory.newRollingDataWriter(s.ctx, "", nil, outputCh)

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

func (s *RollingDataWriterTestSuite) TestBytesWrittenNoDoubleCountAcrossRowGroups() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	spec := iceberg.NewPartitionSpec()
	format := tblutils.GetFileFormat(iceberg.ParquetFile)
	writeProps := format.GetWriteProperties(iceberg.Properties{
		tblutils.ParquetRowGroupLimitKey: "10",
	})

	statsCols, err := computeStatsPlan(icebergSchema, iceberg.Properties{})
	s.Require().NoError(err)

	filePath := filepath.Join(loc, "test-no-double-count.parquet")
	fw, err := format.NewFileWriter(s.ctx, iceio.LocalFS{}, nil, tblutils.WriteFileInfo{
		FileSchema: icebergSchema,
		FileName:   filePath,
		StatsCols:  statsCols,
		WriteProps: writeProps,
		Spec:       spec,
	}, arrSchema)
	s.Require().NoError(err)

	var prevBytes int64
	for batch := range 5 {
		record := s.buildRecord(arrSchema, 100)
		s.Require().NoError(fw.Write(record))
		record.Release()

		bytesWritten := fw.BytesWritten()
		s.Greater(bytesWritten, prevBytes,
			"BytesWritten must monotonically increase after batch %d", batch)
		prevBytes = bytesWritten
	}

	bytesBeforeClose := fw.BytesWritten()

	df, err := fw.Close()
	s.Require().NoError(err)
	s.Equal(int64(500), df.Count())

	s.GreaterOrEqual(df.FileSizeBytes(), bytesBeforeClose,
		"final file size should be >= BytesWritten before close (close flushes the last row group)")

	stat, err := os.Stat(filePath)
	s.Require().NoError(err)
	s.Equal(df.FileSizeBytes(), stat.Size(),
		"DataFile.FileSizeBytes should match actual file size on disk")
}

// TestWriterFactoryPropagatesSortOrderID covers the standard data write path:
// the rolling data writer must stamp each emitted DataFile with the table's
// default sort order id, mirroring how the partitioned fanout writer uses
// writerFactory under the hood.
func (s *RollingDataWriterTestSuite) TestWriterFactoryPropagatesSortOrderID() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	spec := iceberg.NewPartitionSpec()
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{icebergSchema.Fields()[0].ID},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	s.Require().NoError(err)

	loc := filepath.ToSlash(s.T().TempDir())
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	s.Require().NoError(err)
	s.Require().NoError(metaBuilder.AddSortOrder(&sortOrder))
	// -1 means "use the sort order that was just added". AddSortOrder
	// re-maps the caller-supplied ID, so we can't refer to it by literal.
	s.Require().NoError(metaBuilder.SetDefaultSortOrderID(-1))
	builtMeta, err := metaBuilder.Build()
	s.Require().NoError(err)
	expectedSortOrderID := builtMeta.DefaultSortOrder()
	s.Require().NotEqual(UnsortedSortOrderID, expectedSortOrderID, "sanity: sort order id should be non-zero")

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

	factory, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, 1024*1024)
	s.Require().NoError(err)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(s.ctx, "", nil, outputCh)

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
	s.Require().NotNil(dataFiles[0].SortOrderID(), "SortOrderID must be set on the emitted DataFile")
	s.Equal(expectedSortOrderID, *dataFiles[0].SortOrderID())
}

func (s *RollingDataWriterTestSuite) TestAbortWithZeroRowsWritten() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	fw := s.createFileWriter(loc, arrSchema, "test-abort-zero-rows.parquet")

	// Abort without writing any rows must not panic.
	s.Require().NoError(fw.Abort())
}

func (s *RollingDataWriterTestSuite) TestStreamErrorPathUsesAbort() {
	writerSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, writerSchema, 1024*1024)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(s.ctx, "", nil, outputCh)

	// Send a record with an incompatible schema to trigger a
	// ToRequestedSchema error, exercising the deferred Abort() path.
	badSchema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(s.mem, badSchema)
	bldr.Field(0).(*array.Float64Builder).Append(1.0)
	badRecord := bldr.NewRecordBatch()
	bldr.Release()
	defer badRecord.Release()

	s.Require().NoError(writer.Add(badRecord))
	s.Require().Error(writer.closeAndWait())
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

// TestPartitionLocProviderPreservesObjectStorageProps reproduces #1155: when
// write.object-storage.enabled is true on a partitioned table, partition data
// file paths must include entropy hash dirs from objectStoreLocationProvider.
// Previously partitionLocProvider rebuilt props with only WriteDataPathKey, so
// LoadLocationProvider fell back to simpleLocationProvider for partitions.
func (s *RollingDataWriterTestSuite) TestPartitionLocProviderPreservesObjectStorageProps() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	spec := iceberg.NewPartitionSpec()
	loc := "table_location"
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{
		ObjectStoreEnabledKey: "true",
	})
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
	factory, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, 1024*1024)
	s.Require().NoError(err)
	defer factory.closeAll()

	provider, err := factory.partitionLocProvider("dt=2026-06-04")
	s.Require().NoError(err)
	_, ok := provider.(*objectStoreLocationProvider)
	s.True(ok, "expected objectStoreLocationProvider for partitioned writes when write.object-storage.enabled=true")

	// Hashed prefix should appear before the file name for object storage layout.
	dataLoc := provider.NewDataLocation("a")
	s.Contains(dataLoc, "table_location/data/dt=2026-06-04/")
	s.NotEqual("table_location/data/dt=2026-06-04/a", dataLoc,
		"object storage layout should inject an entropy hash prefix between partition dir and file")
}

// TestSortsRowsBeforeWriting exercises the sort-on-write path: the rolling
// data writer must reorder rows according to the table's default sort order
// before flushing them to the data file. We use a multi-key order with mixed
// directions and null placement to cover the full SortField surface.
func (s *RollingDataWriterTestSuite) TestSortsRowsBeforeWriting() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)
	idFieldID := icebergSchema.Fields()[0].ID
	nameFieldID := icebergSchema.Fields()[1].ID

	// id ASC NULLS LAST, name DESC NULLS FIRST.
	sortOrder, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{idFieldID}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsLast},
		{SourceIDs: []int{nameFieldID}, Transform: iceberg.IdentityTransform{}, Direction: SortDESC, NullOrder: NullsFirst},
	})
	s.Require().NoError(err)

	loc := filepath.ToSlash(s.T().TempDir())
	spec := iceberg.NewPartitionSpec()
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	s.Require().NoError(err)
	s.Require().NoError(metaBuilder.AddSortOrder(&sortOrder))
	s.Require().NoError(metaBuilder.SetDefaultSortOrderID(-1))

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

	factory, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, 1024*1024)
	s.Require().NoError(err)
	defer factory.closeAll()

	bldr := array.NewRecordBuilder(s.mem, arrSchema)
	defer bldr.Release()
	idBldr := bldr.Field(0).(*array.Int32Builder)
	nameBldr := bldr.Field(1).(*array.StringBuilder)
	type row struct {
		id   int32
		idOK bool
		name string
		nmOK bool
	}
	rows := []row{
		{id: 3, idOK: true, name: "c", nmOK: true},
		{id: 1, idOK: true, name: "b", nmOK: true},
		{id: 1, idOK: true, name: "a", nmOK: true},
		{idOK: false, name: "x", nmOK: true},
		{id: 2, idOK: true, nmOK: false},
		{id: 1, idOK: true, name: "z", nmOK: true},
	}
	for _, r := range rows {
		if r.idOK {
			idBldr.Append(r.id)
		} else {
			idBldr.AppendNull()
		}
		if r.nmOK {
			nameBldr.Append(r.name)
		} else {
			nameBldr.AppendNull()
		}
	}
	record := bldr.NewRecordBatch()
	defer record.Release()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(s.ctx, "", nil, outputCh)
	s.Require().NoError(writer.Add(record))
	s.Require().NoError(writer.closeAndWait())
	close(outputCh)

	var dataFiles []iceberg.DataFile
	for df := range outputCh {
		dataFiles = append(dataFiles, df)
	}
	s.Require().Len(dataFiles, 1)

	rdr, err := file.OpenParquetFile(dataFiles[0].FilePath(), false)
	s.Require().NoError(err)
	defer rdr.Close()
	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	s.Require().NoError(err)
	tbl, err := arrowRdr.ReadTable(s.ctx)
	s.Require().NoError(err)
	defer tbl.Release()

	s.Equal(int64(len(rows)), tbl.NumRows())

	type observed struct {
		id   int32
		idOK bool
		name string
		nmOK bool
	}
	var got []observed
	idChunks := tbl.Column(0).Data().Chunks()
	nameChunks := tbl.Column(1).Data().Chunks()
	for c := range idChunks {
		idArr := idChunks[c].(*array.Int32)
		nameArr := nameChunks[c].(*array.String)
		for i := 0; i < idArr.Len(); i++ {
			o := observed{}
			if idArr.IsValid(i) {
				o.id = idArr.Value(i)
				o.idOK = true
			}
			if nameArr.IsValid(i) {
				o.name = nameArr.Value(i)
				o.nmOK = true
			}
			got = append(got, o)
		}
	}

	// Expected order: id ASC NULLS LAST, then name DESC NULLS FIRST.
	want := []observed{
		{id: 1, idOK: true, name: "z", nmOK: true},
		{id: 1, idOK: true, name: "b", nmOK: true},
		{id: 1, idOK: true, name: "a", nmOK: true},
		{id: 2, idOK: true, nmOK: false},
		{id: 3, idOK: true, name: "c", nmOK: true},
		{idOK: false, name: "x", nmOK: true},
	}
	s.Equal(want, got)
}

// TestUnsortedOrderIsNoOp confirms that when the table has no sort order
// (id 0), batches are written in arrival order — i.e. the sort path costs
// nothing for unsorted tables.
func (s *RollingDataWriterTestSuite) TestUnsortedOrderIsNoOp() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, arrSchema, 1024*1024)
	defer factory.closeAll()
	s.Require().Nil(factory.sortKeys, "unsorted tables must skip sort key resolution")

	bldr := array.NewRecordBuilder(s.mem, arrSchema)
	defer bldr.Release()
	for _, v := range []int32{3, 1, 2} {
		bldr.Field(0).(*array.Int32Builder).Append(v)
		bldr.Field(1).(*array.StringBuilder).Append("x")
	}
	record := bldr.NewRecordBatch()
	defer record.Release()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(s.ctx, "", nil, outputCh)
	s.Require().NoError(writer.Add(record))
	s.Require().NoError(writer.closeAndWait())
	close(outputCh)

	var dataFiles []iceberg.DataFile
	for df := range outputCh {
		dataFiles = append(dataFiles, df)
	}
	s.Require().Len(dataFiles, 1)

	rdr, err := file.OpenParquetFile(dataFiles[0].FilePath(), false)
	s.Require().NoError(err)
	defer rdr.Close()
	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	s.Require().NoError(err)
	tbl, err := arrowRdr.ReadTable(s.ctx)
	s.Require().NoError(err)
	defer tbl.Release()

	var ids []int32
	for _, chunk := range tbl.Column(0).Data().Chunks() {
		arr := chunk.(*array.Int32)
		for i := 0; i < arr.Len(); i++ {
			ids = append(ids, arr.Value(i))
		}
	}
	s.Equal([]int32{3, 1, 2}, ids, "unsorted order must preserve arrival order")
}
