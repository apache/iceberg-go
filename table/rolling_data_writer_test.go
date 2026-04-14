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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	return s.createWriterFactoryWithIdleTimeout(loc, arrSchema, targetFileSize, 0)
}

func (s *RollingDataWriterTestSuite) createWriterFactoryWithIdleTimeout(loc string, arrSchema *arrow.Schema, targetFileSize int64, idleTimeout time.Duration, reaperTimeout ...time.Duration) (*writerFactory, *iceberg.Schema) {
	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	spec := iceberg.NewPartitionSpec()
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)

	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	s.Require().NoError(err)

	writeUUID := uuid.New()
	args := recordWritingArgs{
		sc:          arrSchema,
		fs:          iceio.LocalFS{},
		writeUUID:   &writeUUID,
		idleTimeout: idleTimeout,
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					break
				}
			}
		},
	}
	if len(reaperTimeout) > 0 {
		args.reaperTimeout = reaperTimeout[0]
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
	s.Require().NotNil(dataFiles[0].SortOrderID(), "SortOrderID must be set on the emitted DataFile")
	s.Equal(expectedSortOrderID, *dataFiles[0].SortOrderID())
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

// newTestWriterFactory creates a writerFactory with the given idle and reaper
// timeouts for use in standalone (non-suite) tests.
func newTestWriterFactory(t *testing.T, loc string, arrSchema *arrow.Schema, targetFileSize int64, idleTimeout, reaperTimeout time.Duration) *writerFactory {
	t.Helper()

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	require.NoError(t, err)

	spec := iceberg.NewPartitionSpec()
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	require.NoError(t, err)

	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	writeUUID := uuid.New()
	args := recordWritingArgs{
		sc:            arrSchema,
		fs:            iceio.LocalFS{},
		writeUUID:     &writeUUID,
		idleTimeout:   idleTimeout,
		reaperTimeout: reaperTimeout,
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					break
				}
			}
		},
	}

	factory, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, targetFileSize)
	require.NoError(t, err)

	return factory
}

func buildTestRecord(t *testing.T, mem memory.Allocator, arrSchema *arrow.Schema, numRows int) arrow.RecordBatch {
	t.Helper()

	bldr := array.NewRecordBuilder(mem, arrSchema)
	defer bldr.Release()

	for i := range numRows {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		bldr.Field(1).(*array.StringBuilder).Append("value")
	}

	return bldr.NewRecordBatch()
}

var testSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func TestIdleTimeoutFlushesFile(t *testing.T) {
	mem := memory.NewGoAllocator()
	loc := filepath.ToSlash(t.TempDir())
	// reaperTimeout=-1 disables the reaper for this test.
	factory := newTestWriterFactory(t, loc, testSchema, 1024*1024, 200*time.Millisecond, -1)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(context.Background(), nil, "", nil, outputCh)

	record := buildTestRecord(t, mem, testSchema, 5)
	require.NoError(t, writer.Add(record))
	record.Release()

	select {
	case df := <-outputCh:
		assert.Equal(t, int64(5), df.Count())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for idle flush")
	}
}

func TestIdleTimerResetsOnActivity(t *testing.T) {
	mem := memory.NewGoAllocator()
	loc := filepath.ToSlash(t.TempDir())
	factory := newTestWriterFactory(t, loc, testSchema, 1024*1024, 300*time.Millisecond, -1)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(context.Background(), nil, "", nil, outputCh)

	// Send records at intervals shorter than the idle timeout so
	// the timer keeps resetting and does not flush prematurely.
	for range 5 {
		record := buildTestRecord(t, mem, testSchema, 3)
		require.NoError(t, writer.Add(record))
		record.Release()
		time.Sleep(100 * time.Millisecond)
	}

	// No file should have been flushed yet (all 15 rows in one file).
	select {
	case <-outputCh:
		t.Fatal("idle flush should not have fired while records were being added")
	default:
	}

	// Now wait for the idle timeout to fire.
	select {
	case df := <-outputCh:
		assert.Equal(t, int64(15), df.Count())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for idle flush")
	}
}

func TestWriterReopensAfterIdleFlush(t *testing.T) {
	mem := memory.NewGoAllocator()
	loc := filepath.ToSlash(t.TempDir())
	factory := newTestWriterFactory(t, loc, testSchema, 1024*1024, 200*time.Millisecond, -1)
	defer factory.closeAll()

	outputCh := make(chan iceberg.DataFile, 10)
	writer := factory.newRollingDataWriter(context.Background(), nil, "", nil, outputCh)

	// First batch: write and let idle flush close the file.
	record1 := buildTestRecord(t, mem, testSchema, 5)
	require.NoError(t, writer.Add(record1))
	record1.Release()

	select {
	case df := <-outputCh:
		assert.Equal(t, int64(5), df.Count())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first idle flush")
	}

	// Second batch: same writer reopens a new file automatically.
	record2 := buildTestRecord(t, mem, testSchema, 7)
	require.NoError(t, writer.Add(record2))
	record2.Release()

	require.NoError(t, writer.closeAndWait())
	close(outputCh)

	var dataFiles []iceberg.DataFile
	for df := range outputCh {
		dataFiles = append(dataFiles, df)
	}

	require.Len(t, dataFiles, 1)
	assert.Equal(t, int64(7), dataFiles[0].Count())
}

func TestReaperCleansUpIdleWriters(t *testing.T) {
	mem := memory.NewGoAllocator()
	loc := filepath.ToSlash(t.TempDir())
	factory := newTestWriterFactory(t, loc, testSchema, 1024*1024, 200*time.Millisecond, 500*time.Millisecond)
	defer factory.closeAll()

	ctx := context.Background()
	outputCh := make(chan iceberg.DataFile, 10)

	writer, err := factory.getOrCreateRollingDataWriter(ctx, nil, "part=a", nil, outputCh)
	require.NoError(t, err)

	record := buildTestRecord(t, mem, testSchema, 5)
	require.NoError(t, writer.Add(record))
	record.Release()

	// Wait for the idle timer to flush the file.
	select {
	case df := <-outputCh:
		assert.Equal(t, int64(5), df.Count())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for idle flush")
	}

	// Writer should still be in the map right after flush.
	_, ok := factory.writers.Load("part=a")
	assert.True(t, ok, "writer should still be in map right after idle flush")

	// Wait for the reaper to sweep.
	assert.Eventually(t, func() bool {
		_, ok := factory.writers.Load("part=a")
		return !ok
	}, 5*time.Second, 50*time.Millisecond, "reaper should have removed the idle writer from the map")
}
