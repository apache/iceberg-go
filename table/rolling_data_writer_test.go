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
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
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

type unsupportedType struct{}

func (unsupportedType) Type() string { return "unsupported" }
func (unsupportedType) String() string {
	return "unsupported"
}

func (unsupportedType) Equals(other iceberg.Type) bool {
	_, ok := other.(unsupportedType)

	return ok
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

func (s *RollingDataWriterTestSuite) TestNewWriterFactoryReturnsErrorForInvalidFileSchema() {
	loc := filepath.ToSlash(s.T().TempDir())
	spec := iceberg.NewPartitionSpec()
	taskSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	meta, err := NewMetadata(taskSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	s.Require().NoError(err)

	args := recordWritingArgs{
		fs:      iceio.LocalFS{},
		counter: internal.Counter(0),
	}

	invalidSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "bad", Type: unsupportedType{}, Required: true,
	})

	factory, err := newWriterFactory(
		loc,
		args,
		metaBuilder,
		taskSchema,
		1024*1024,
		withFactoryFileSchema(invalidSchema),
	)
	s.Require().Error(err)
	s.Nil(factory)
	s.ErrorContains(err, "withFactoryFileSchema")
}

func (s *RollingDataWriterTestSuite) TestCloseAllFinishesQueuedRecordsWithoutCancellingContext() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, arrSchema, 1024*1024)

	outputCh := make(chan iceberg.DataFile, 10)
	writerCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	writer, err := factory.getOrCreateRollingDataWriter(writerCtx, "", nil, outputCh)
	s.Require().NoError(err)
	const totalBatches = 4
	const rowsPerBatch = 25
	var expectedRows int64
	for range totalBatches {
		record := s.buildRecord(arrSchema, rowsPerBatch)
		expectedRows += record.NumRows()
		s.Require().NoError(writer.Add(record))
		record.Release()
	}

	s.Require().NoError(factory.closeAll())
	s.Require().Nil(writerCtx.Err(), "normal close should finish queued writes without aborting context")

	close(outputCh)
	var actualRows int64
	for df := range outputCh {
		actualRows += df.Count()
	}

	s.Equal(expectedRows, actualRows, "all queued records should be flushed when closeAll is used")
}

func (s *RollingDataWriterTestSuite) TestAbortAndWaitCancelsContext() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, arrSchema, 1024*1024)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writerCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	writer, err := factory.getOrCreateRollingDataWriter(writerCtx, "", nil, outputCh)
	s.Require().NoError(err)
	record := s.buildRecord(arrSchema, 5)
	s.Require().NoError(writer.Add(record))
	record.Release()

	writer.abortAndWait()
	s.Require().ErrorIs(writer.ctx.Err(), context.Canceled)
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

// The rolling path sorts per batch but takes no per-task claim, so the emitted
// DataFile must not claim the table's sort order even when one is set.
func (s *RollingDataWriterTestSuite) TestWriterFactoryDoesNotStampSortOrderID() {
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
	s.Require().NotEqual(UnsortedSortOrderID, builtMeta.DefaultSortOrder(),
		"sanity: sort order id should be non-zero")

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
	s.Nil(dataFiles[0].SortOrderID(),
		"emitted DataFile must not claim a sort order id: batches are only sorted individually")
}

func (s *RollingDataWriterTestSuite) TestAbortWithZeroRowsWritten() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	fileName := "test-abort-zero-rows.parquet"
	filePath := filepath.Join(loc, fileName)
	fw := s.createFileWriter(loc, arrSchema, fileName)

	// Abort without writing any rows must not panic.
	s.Require().NoError(fw.Abort())
	_, err := os.Stat(filePath)
	s.True(os.IsNotExist(err), "abort should remove the incomplete file")
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

	var files []string
	err := filepath.Walk(loc, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}

		return nil
	})
	s.Require().NoError(err)
	s.Empty(files, "deferred abort should remove the incomplete file")
}

func (s *RollingDataWriterTestSuite) TestAddReturnsErrorAfterStreamStopsCleanly() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	record := s.buildRecord(arrSchema, 1)
	defer record.Release()

	errorCh := make(chan error)
	close(errorCh)

	writer := &RollingDataWriter{
		recordCh: make(chan arrow.RecordBatch),
		errorCh:  errorCh,
		ctx:      s.ctx,
	}

	s.ErrorIs(writer.Add(record), ErrWriterClosed)
}

func (s *RollingDataWriterTestSuite) TestStreamErrorRemovesWriterFromFactory() {
	writerSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, writerSchema, 1024*1024)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writer, err := factory.getOrCreateRollingDataWriter(s.ctx, "", nil, outputCh)
	s.Require().NoError(err)

	badSchema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	bldr := array.NewRecordBuilder(s.mem, badSchema)
	bldr.Field(0).(*array.Float64Builder).Append(1.0)
	badRecord := bldr.NewRecordBatch()
	bldr.Release()
	defer badRecord.Release()

	s.Require().NoError(writer.Add(badRecord))
	writer.wg.Wait()

	_, ok := factory.writers.Load("")
	s.False(ok, "failed writer should be removed from partition writer cache")

	err, ok = <-writer.errorCh
	s.True(ok)
	s.Error(err)
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
		for i := range idArr.Len() {
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
		for i := range arr.Len() {
			ids = append(ids, arr.Value(i))
		}
	}
	s.Equal([]int32{3, 1, 2}, ids, "unsorted order must preserve arrival order")
}

// panicOnCloseFormat wraps a real FileFormat but hands out writers whose Close
// panics, mimicking stats.ToDataFile panicking when NewDataFileBuilder fails.
// bytesWritten is reported by every writer it creates: a value >= the target
// file size makes stream roll (and Close) mid-loop, 0 defers Close to the end
// of the stream.
type panicOnCloseFormat struct {
	tblutils.FileFormat
	bytesWritten int64
}

func (f panicOnCloseFormat) NewFileWriter(_ context.Context, _ iceio.WriteFileIO, _ map[int]any, _ tblutils.WriteFileInfo, _ *arrow.Schema) (tblutils.FileWriter, error) {
	return panicOnCloseWriter{bytesWritten: f.bytesWritten}, nil
}

// panicOnCloseWriter is a FileWriter whose Close panics with an error, like the
// real stats.ToDataFile -> NewDataFileBuilder failure path.
type panicOnCloseWriter struct {
	bytesWritten int64
}

func (panicOnCloseWriter) Write(arrow.RecordBatch) error { return nil }
func (w panicOnCloseWriter) BytesWritten() int64         { return w.bytesWritten }
func (panicOnCloseWriter) Close() (iceberg.DataFile, error) {
	panic(errors.New("simulated NewDataFileBuilder failure"))
}
func (panicOnCloseWriter) Abort() error { return nil }

// TestStreamRecoversWriterClosePanic drives the rolling writer so the file
// writer's Close panics inside the stream goroutine, and asserts the panic is
// recovered and surfaced as an error on errorCh rather than crashing the
// process. Both Close call sites are covered: the mid-loop roll (the common
// production path) and the end-of-stream close.
func (s *RollingDataWriterTestSuite) TestStreamRecoversWriterClosePanic() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	const targetFileSize = 1024 * 1024
	tests := []struct {
		name         string
		bytesWritten int64
	}{
		{"end-of-stream close", 0},
		{"mid-stream file roll", targetFileSize}, // >= target rolls (and closes) mid-loop
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			loc := filepath.ToSlash(s.T().TempDir())
			factory, _ := s.createWriterFactory(loc, arrSchema, targetFileSize)
			defer factory.closeAll()

			// Hand out a writer whose Close panics. Assigned before
			// newRollingDataWriter so it happens-before the stream goroutine
			// reads factory.format. Before the recover in stream, this panic
			// escaped the goroutine and crashed the process.
			factory.format = panicOnCloseFormat{FileFormat: factory.format, bytesWritten: tt.bytesWritten}

			outputCh := make(chan iceberg.DataFile, 1)
			writer := factory.newRollingDataWriter(s.ctx, "", nil, outputCh)

			record := s.buildRecord(arrSchema, 5)
			defer record.Release()
			s.Require().NoError(writer.Add(record))

			err := writer.closeAndWait()
			s.Require().Error(err)
			s.Contains(err.Error(), "panic during rolling data file writing")
			s.Contains(err.Error(), "simulated NewDataFileBuilder failure")
		})
	}
}

// failingOpenFormat always fails to open a file writer, simulating a
// mid-stream write error (e.g. an unwritable location) that makes stream
// return before it ever dequeues most of what's already buffered in recordCh.
type failingOpenFormat struct {
	tblutils.FileFormat
}

func (failingOpenFormat) NewFileWriter(context.Context, iceio.WriteFileIO, map[int]any, tblutils.WriteFileInfo, *arrow.Schema) (tblutils.FileWriter, error) {
	return nil, errors.New("simulated open failure")
}

// TestStreamErrorDrainsBufferedRecords reproduces the leak from #1825: when
// stream exits early on a write error, records already sitting in recordCh —
// queued by Add before stream's first dequeue attempt fails — must still be
// released. Before the fix, stream's deferred CompareAndDelete deregisters the
// writer before abortAll ever has a chance to run, so nothing drains what's
// left in recordCh. Records are queued directly, bypassing Add and the
// goroutine start in newRollingDataWriter, so all of them are buffered before
// stream ever runs — otherwise Add would race stream's error exit for the
// later records once errorCh closes.
func (s *RollingDataWriterTestSuite) TestStreamErrorDrainsBufferedRecords() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, arrSchema, 1024*1024)
	factory.format = failingOpenFormat{FileFormat: factory.format}

	outputCh := make(chan iceberg.DataFile, 10)
	ctx, cancel := context.WithCancel(s.ctx)
	writer := &RollingDataWriter{
		recordCh: make(chan arrow.RecordBatch, 64),
		errorCh:  make(chan error, 1),
		factory:  factory,
		ctx:      ctx,
		cancel:   cancel,
	}

	const numRecords = 5
	for range numRecords {
		record := s.buildRecord(arrSchema, 3)
		record.Retain()
		writer.recordCh <- record
		record.Release()
	}

	writer.wg.Add(1)
	go writer.stream(outputCh)
	writer.wg.Wait()

	checked := s.mem.(*memory.CheckedAllocator)
	s.Require().Zero(checked.CurrentAlloc(),
		"records still queued in recordCh when stream exited on the open error must still be released")
}

// gatedFailFormat blocks the first file open until openGate is closed, then
// fails it. Holding stream inside openFileWriter keeps ctx live and stops it
// consuming, so the drain can be triggered at a controlled moment. inOpen lets
// the test observe that stream has actually parked in the open.
type gatedFailFormat struct {
	tblutils.FileFormat
	openGate chan struct{}
	inOpen   *atomic.Bool
}

func (f gatedFailFormat) NewFileWriter(context.Context, iceio.WriteFileIO, map[int]any, tblutils.WriteFileInfo, *arrow.Schema) (tblutils.FileWriter, error) {
	f.inOpen.Store(true)
	<-f.openGate

	return nil, errors.New("simulated open failure")
}

// gatedRetainRecord blocks in Retain until retainGate is closed, signalling
// atRetain first. Add calls Retain between its closed check and its enqueue, so
// this parks a sender in exactly that window: past the check, not yet sent. It
// is the interleaving the reviewer hit with an instrumented Retain, made
// deterministic. Every other method (Release included) delegates to the
// embedded batch, so allocator accounting is unaffected.
type gatedRetainRecord struct {
	arrow.RecordBatch
	retainGate chan struct{}
	atRetain   *sync.WaitGroup
}

func (r gatedRetainRecord) Retain() {
	r.atRetain.Done()
	<-r.retainGate
	r.RecordBatch.Retain()
}

// TestConcurrentAddDuringStreamErrorLeaksNothing exercises the barrier between
// in-flight Add calls and stream's shutdown drain. Every record handed to Add
// must end owned by exactly one path: enqueued and released by the drain, or
// rejected/aborted and released by Add itself. Without a real barrier a send
// that lands in free buffer after the drain emptied the channel strands its
// record; the checked allocator must still return to zero.
//
// The interleaving is forced, not left to chance. A trigger record parks stream
// in the gated open with ctx still live. The real senders then pass their
// closed check and block in Retain (via gatedRetainRecord) — past the check,
// not yet enqueued. Closing openGate makes stream fail and run its drain; only
// then does retainGate release the senders to complete their sends. On a
// lockless drain those sends land in the drained-empty channel and strand; the
// write-lock barrier serializes them against the drain so none can.
func (s *RollingDataWriterTestSuite) TestConcurrentAddDuringStreamErrorLeaksNothing() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	loc := filepath.ToSlash(s.T().TempDir())
	factory, _ := s.createWriterFactory(loc, arrSchema, 1024*1024)
	openGate := make(chan struct{})
	var inOpen atomic.Bool
	factory.format = gatedFailFormat{FileFormat: factory.format, openGate: openGate, inOpen: &inOpen}

	outputCh := make(chan iceberg.DataFile, 64)
	ctx, cancel := context.WithCancel(s.ctx)
	writer := &RollingDataWriter{
		recordCh: make(chan arrow.RecordBatch, 64),
		errorCh:  make(chan error, 1),
		factory:  factory,
		ctx:      ctx,
		cancel:   cancel,
	}
	writer.wg.Add(1)
	go writer.stream(outputCh)

	// Trigger record: drives stream into the gated open so ctx stays live while
	// the real senders line up. Consumed and released by stream itself.
	trigger := s.buildRecord(arrSchema, 3)
	trigger.Retain()
	writer.recordCh <- trigger
	trigger.Release()
	for !inOpen.Load() {
		runtime.Gosched()
	}

	const senders = 24
	retainGate := make(chan struct{})
	var atRetain, done sync.WaitGroup
	atRetain.Add(senders)
	for range senders {
		done.Add(1)
		go func() {
			defer done.Done()
			record := s.buildRecord(arrSchema, 3)
			// Add retains (via gatedRetainRecord.Retain, which parks the sender
			// between the closed check and the enqueue) then enqueues or aborts;
			// the caller always drops its own reference afterward.
			_ = writer.Add(gatedRetainRecord{RecordBatch: record, retainGate: retainGate, atRetain: &atRetain})
			record.Release()
		}()
	}

	atRetain.Wait() // every sender is past its closed check, parked in Retain
	close(openGate) // stream's open fails -> cleanup runs its drain
	time.Sleep(20 * time.Millisecond)
	close(retainGate) // now release the parked sends into the just-drained channel

	done.Wait()      // every Add call has returned
	writer.wg.Wait() // stream has exited and finished its drain

	checked := s.mem.(*memory.CheckedAllocator)
	s.Require().Zero(checked.CurrentAlloc(),
		"no record may be stranded when a send completes after stream's shutdown drain")
}
