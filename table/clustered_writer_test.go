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
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// -- Functional tests --

type ClusteredWriterTestSuite struct {
	suite.Suite

	mem *memory.CheckedAllocator
	ctx context.Context
}

func (s *ClusteredWriterTestSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
	s.ctx = compute.WithAllocator(context.Background(), s.mem)
}

func (s *ClusteredWriterTestSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func TestClusteredWriter(t *testing.T) {
	suite.Run(t, new(ClusteredWriterTestSuite))
}

func (s *ClusteredWriterTestSuite) schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "partition_col", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func (s *ClusteredWriterTestSuite) buildBatch(schema *arrow.Schema, partition string, rows int) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(s.mem, schema)
	defer bldr.Release()

	for i := range rows {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		bldr.Field(1).(*array.StringBuilder).Append(partition)
	}

	return bldr.NewRecordBatch()
}

func (s *ClusteredWriterTestSuite) buildMixedBatch(schema *arrow.Schema, partitions []string, rowsPerPartition int) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(s.mem, schema)
	defer bldr.Release()

	row := 0
	for _, p := range partitions {
		for range rowsPerPartition {
			bldr.Field(0).(*array.Int64Builder).Append(int64(row))
			bldr.Field(1).(*array.StringBuilder).Append(p)
			row++
		}
	}

	return bldr.NewRecordBatch()
}

func (s *ClusteredWriterTestSuite) buildNullableBatch(schema *arrow.Schema, partitions []*string) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(s.mem, schema)
	defer bldr.Release()

	for i, p := range partitions {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		if p == nil {
			bldr.Field(1).(*array.StringBuilder).AppendNull()
		} else {
			bldr.Field(1).(*array.StringBuilder).Append(*p)
		}
	}

	return bldr.NewRecordBatch()
}

func (s *ClusteredWriterTestSuite) setupFactory(loc string, arrSchema *arrow.Schema, targetFileSize int64) (*writerFactory, iceberg.PartitionSpec, *iceberg.Schema) {
	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	sourceField, ok := icebergSchema.FindFieldByName("partition_col")
	s.Require().True(ok)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{sourceField.ID},
		FieldID:   1000,
		Transform: iceberg.IdentityTransform{},
		Name:      "partition_col",
	})

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

	return factory, spec, icebergSchema
}

func (s *ClusteredWriterTestSuite) writeAndCollect(spec iceberg.PartitionSpec, icebergSchema *iceberg.Schema, factory *writerFactory, records func(func(arrow.RecordBatch, error) bool)) []iceberg.DataFile {
	var dataFiles []iceberg.DataFile
	for df, err := range clusteredPartitionedWrite(s.ctx, spec, icebergSchema, factory, records) {
		s.Require().NoError(err)
		dataFiles = append(dataFiles, df)
	}

	return dataFiles
}

func (s *ClusteredWriterTestSuite) batchItr(batches ...arrow.RecordBatch) func(func(arrow.RecordBatch, error) bool) {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for _, b := range batches {
			b.Retain()
			if !yield(b, nil) {
				b.Release()

				return
			}
			b.Release()
		}
	}
}

func (s *ClusteredWriterTestSuite) TestSinglePartition() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	batch := s.buildBatch(arrSchema, "part_a", 100)
	defer batch.Release()

	dataFiles := s.writeAndCollect(spec, icebergSchema, factory, s.batchItr(batch))

	s.Require().Len(dataFiles, 1)
	s.Equal(int64(100), dataFiles[0].Count())
}

func (s *ClusteredWriterTestSuite) TestMultiplePartitionsClustered() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	batchA := s.buildBatch(arrSchema, "part_a", 50)
	defer batchA.Release()
	batchB := s.buildBatch(arrSchema, "part_b", 30)
	defer batchB.Release()
	batchC := s.buildBatch(arrSchema, "part_c", 20)
	defer batchC.Release()

	dataFiles := s.writeAndCollect(spec, icebergSchema, factory, s.batchItr(batchA, batchB, batchC))

	partitionRows := make(map[string]int64)
	for _, df := range dataFiles {
		rec := GetPartitionRecord(df, spec.PartitionType(icebergSchema))
		path := spec.PartitionToPath(rec, icebergSchema)
		partitionRows[path] += df.Count()
	}

	s.Len(partitionRows, 3)
	s.Equal(int64(50), partitionRows["partition_col=part_a"])
	s.Equal(int64(30), partitionRows["partition_col=part_b"])
	s.Equal(int64(20), partitionRows["partition_col=part_c"])
}

func (s *ClusteredWriterTestSuite) TestMixedBatchSpanningPartitions() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	batch := s.buildMixedBatch(arrSchema, []string{"part_a", "part_b"}, 25)
	defer batch.Release()

	dataFiles := s.writeAndCollect(spec, icebergSchema, factory, s.batchItr(batch))

	var totalRows int64
	for _, df := range dataFiles {
		totalRows += df.Count()
	}

	s.Equal(int64(50), totalRows)
	s.Len(dataFiles, 2)
}

func (s *ClusteredWriterTestSuite) TestRollsFilesOnTargetSize() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1) // 1 byte forces rolling

	var batches []arrow.RecordBatch
	for range 10 {
		b := s.buildBatch(arrSchema, "part_a", 50)
		batches = append(batches, b)
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	dataFiles := s.writeAndCollect(spec, icebergSchema, factory, s.batchItr(batches...))

	var totalRows int64
	for _, df := range dataFiles {
		totalRows += df.Count()
	}

	s.Equal(int64(500), totalRows)
	s.Greater(len(dataFiles), 1, "should produce multiple files with 1-byte target size")
}

func (s *ClusteredWriterTestSuite) TestPartitionRevisitReturnsError() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	batchA1 := s.buildBatch(arrSchema, "part_a", 10)
	defer batchA1.Release()
	batchB := s.buildBatch(arrSchema, "part_b", 20)
	defer batchB.Release()
	batchA2 := s.buildBatch(arrSchema, "part_a", 5)
	defer batchA2.Release()

	var (
		dataFiles []iceberg.DataFile
		writeErr  error
	)
	for df, err := range clusteredPartitionedWrite(s.ctx, spec, icebergSchema, factory, s.batchItr(batchA1, batchB, batchA2)) {
		if err != nil {
			writeErr = err

			break
		}
		dataFiles = append(dataFiles, df)
	}

	s.Require().Error(writeErr)
	s.Contains(writeErr.Error(), "partition_col=part_a")
	s.Len(dataFiles, 2, "part_a and part_b should have been flushed before the revisit was rejected")
}

func (s *ClusteredWriterTestSuite) TestNullVsLiteralStringWithinBatch() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	literalNull := "null"
	batch := s.buildNullableBatch(arrSchema, []*string{nil, &literalNull, nil})
	defer batch.Release()

	dataFiles := s.writeAndCollect(spec, icebergSchema, factory, s.batchItr(batch))

	s.Require().Len(dataFiles, 2, "SQL NULL and literal string \"null\" must produce two distinct files")

	partType := spec.PartitionType(icebergSchema)
	rowsByLiteralPartition := map[string]int64{}
	nilRows := int64(0)
	for _, df := range dataFiles {
		rec := GetPartitionRecord(df, partType)
		val := rec.Get(0)
		if val == nil {
			nilRows += df.Count()
		} else {
			rowsByLiteralPartition[fmt.Sprintf("%v", val)] += df.Count()
		}
	}

	s.Equal(int64(2), nilRows, "SQL NULL partition should contain rows 0 and 2")
	s.Equal(int64(1), rowsByLiteralPartition["null"], "literal \"null\" partition should contain row 1")
}

func (s *ClusteredWriterTestSuite) TestNullVsLiteralStringAcrossBatches() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	literalNull := "null"
	batchNil1 := s.buildNullableBatch(arrSchema, []*string{nil})
	defer batchNil1.Release()
	batchLiteral := s.buildNullableBatch(arrSchema, []*string{&literalNull})
	defer batchLiteral.Release()
	batchNil2 := s.buildNullableBatch(arrSchema, []*string{nil})
	defer batchNil2.Release()

	var (
		dataFiles []iceberg.DataFile
		writeErr  error
	)
	for df, err := range clusteredPartitionedWrite(s.ctx, spec, icebergSchema, factory, s.batchItr(batchNil1, batchLiteral, batchNil2)) {
		if err != nil {
			writeErr = err

			break
		}
		dataFiles = append(dataFiles, df)
	}

	s.Require().Error(writeErr)
	s.Contains(writeErr.Error(), "partition_col=null")
	s.Len(dataFiles, 2, "the SQL NULL and literal-\"null\" partitions should both have flushed before the revisit was rejected")
}

func (s *ClusteredWriterTestSuite) TestMixedBatchFollowedBySinglePartition() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	// Mixed batch with part_a's rows preceding part_b's, then a follow-up
	// batch for part_b only. The input is clustered by row order, so the
	// writer must process partitions within the mixed batch in row order
	// (part_a then part_b), leaving part_b open for the next batch.
	// Without that ordering, map iteration could surface part_b first,
	// close it on the transition to part_a, and then reject the next
	// batch as a clustering violation.
	batchMixed := s.buildMixedBatch(arrSchema, []string{"part_a", "part_b"}, 25)
	defer batchMixed.Release()
	batchB := s.buildBatch(arrSchema, "part_b", 30)
	defer batchB.Release()

	dataFiles := s.writeAndCollect(spec, icebergSchema, factory, s.batchItr(batchMixed, batchB))

	partitionRows := make(map[string]int64)
	for _, df := range dataFiles {
		rec := GetPartitionRecord(df, spec.PartitionType(icebergSchema))
		path := spec.PartitionToPath(rec, icebergSchema)
		partitionRows[path] += df.Count()
	}

	s.Len(partitionRows, 2)
	s.Equal(int64(25), partitionRows["partition_col=part_a"])
	s.Equal(int64(55), partitionRows["partition_col=part_b"])
}

func (s *ClusteredWriterTestSuite) TestWithinBatchInterleaveIsRecluster() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	// A single batch with rows interleaved [a, b, a, b, a, b]. The
	// clustered writer's contract is batch-granular: within a batch
	// the rows are reclustered into one writer per partition, so this
	// is accepted even though the row order is not contiguous.
	bldr := array.NewRecordBuilder(s.mem, arrSchema)
	parts := []string{"part_a", "part_b", "part_a", "part_b", "part_a", "part_b"}
	for i, p := range parts {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		bldr.Field(1).(*array.StringBuilder).Append(p)
	}
	batch := bldr.NewRecordBatch()
	bldr.Release()
	defer batch.Release()

	dataFiles := s.writeAndCollect(spec, icebergSchema, factory, s.batchItr(batch))

	partitionRows := make(map[string]int64)
	for _, df := range dataFiles {
		rec := GetPartitionRecord(df, spec.PartitionType(icebergSchema))
		path := spec.PartitionToPath(rec, icebergSchema)
		partitionRows[path] += df.Count()
	}

	s.Len(dataFiles, 2, "interleaved rows within a batch should produce one file per partition")
	s.Equal(int64(3), partitionRows["partition_col=part_a"])
	s.Equal(int64(3), partitionRows["partition_col=part_b"])
}

func (s *ClusteredWriterTestSuite) TestEarlyExitDoesNotDeadlock() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	parts := []string{"part_a", "part_b", "part_c", "part_d"}
	batches := make([]arrow.RecordBatch, len(parts))
	for i, p := range parts {
		batches[i] = s.buildBatch(arrSchema, p, 100)
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	var dataFiles []iceberg.DataFile
	for df, err := range clusteredPartitionedWrite(s.ctx, spec, icebergSchema, factory, s.batchItr(batches...)) {
		s.Require().NoError(err)
		dataFiles = append(dataFiles, df)
		if len(dataFiles) >= 1 {
			break
		}
	}

	s.GreaterOrEqual(len(dataFiles), 1)
}

func (s *ClusteredWriterTestSuite) TestPanickyIteratorSurfacesAsError() {
	arrSchema := s.schema()
	loc := filepath.ToSlash(s.T().TempDir())
	factory, spec, icebergSchema := s.setupFactory(loc, arrSchema, 1024*1024)

	batchA := s.buildBatch(arrSchema, "part_a", 50)
	defer batchA.Release()

	panickyItr := func(yield func(arrow.RecordBatch, error) bool) {
		batchA.Retain()
		if !yield(batchA, nil) {
			batchA.Release()

			return
		}
		batchA.Release()
		panic("test panic from iterator")
	}

	var (
		dataFiles []iceberg.DataFile
		writeErr  error
	)
	for df, err := range clusteredPartitionedWrite(s.ctx, spec, icebergSchema, factory, panickyItr) {
		if err != nil {
			writeErr = err

			break
		}
		dataFiles = append(dataFiles, df)
	}

	s.Require().Error(writeErr)
	s.Contains(writeErr.Error(), "panic")
}

// -- Peak memory and goroutine comparison --

func buildPartitionedBatch(mem memory.Allocator, schema *arrow.Schema, partition string, rows int) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	for i := range rows {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		bldr.Field(1).(*array.StringBuilder).Append(partition)
	}

	return bldr.NewRecordBatch()
}

type writeResult struct {
	goroutineDelta int
	heapInuseAtMid uint64
}

func writeWithMeasurement(t testing.TB, name string, numPartitions, rowsPerPartition int) writeResult {
	t.Helper()

	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "partition_col", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	mem := memory.NewGoAllocator()

	var batches []arrow.RecordBatch
	for p := range numPartitions {
		batches = append(batches, buildPartitionedBatch(mem, arrSchema, fmt.Sprintf("p_%04d", p), rowsPerPartition))
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	var (
		result      writeResult
		baselineSet bool
		baseline    int
	)
	halfway := numPartitions / 2
	count := 0

	itr := func(yield func(arrow.RecordBatch, error) bool) {
		for _, b := range batches {
			b.Retain()
			if !yield(b, nil) {
				b.Release()

				return
			}
			b.Release()
			count++
			if count == halfway && baselineSet {
				runtime.GC()
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				result.heapInuseAtMid = m.HeapInuse
				result.goroutineDelta = runtime.NumGoroutine() - baseline
			}
		}
	}

	// Captured immediately before the write starts so the delta isolates
	// goroutines spawned by the writer from anything else running in the
	// test process.
	captureBaseline := func() {
		baseline = runtime.NumGoroutine()
		baselineSet = true
	}

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	require.NoError(t, err)

	sourceField, ok := icebergSchema.FindFieldByName("partition_col")
	require.True(t, ok)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{sourceField.ID},
		FieldID:   1000,
		Transform: iceberg.IdentityTransform{},
		Name:      "partition_col",
	})

	loc := filepath.ToSlash(t.TempDir())
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	require.NoError(t, err)

	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	writeUUID := uuid.New()
	args := recordWritingArgs{
		sc:        arrSchema,
		itr:       itr,
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

	switch name {
	case "fanout":
		cw := newConcurrentDataFileWriter(func(rootLocation string, fs iceio.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
			return newDataFileWriter(rootLocation, fs, meta, props, opts...)
		})
		factory, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, 512*1024*1024)
		require.NoError(t, err)
		writer := newPartitionedFanoutWriter(spec, cw, icebergSchema, args.itr, factory)
		captureBaseline()
		for df, err := range writer.Write(context.Background(), config.EnvConfig.MaxWorkers) {
			require.NoError(t, err)
			_ = df
		}

	case "clustered":
		factory, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, 512*1024*1024)
		require.NoError(t, err)
		captureBaseline()
		for df, err := range clusteredPartitionedWrite(context.Background(), spec, icebergSchema, factory, args.itr) {
			require.NoError(t, err)
			_ = df
		}
	}

	return result
}

func TestClusteredWriterPeakMemory(t *testing.T) {
	const (
		numPartitions    = 200
		rowsPerPartition = 2000
	)

	// Run clustered first so its freed memory doesn't inflate the
	// fanout measurement.
	clustered := writeWithMeasurement(t, "clustered", numPartitions, rowsPerPartition)
	fanout := writeWithMeasurement(t, "fanout", numPartitions, rowsPerPartition)

	t.Logf("fanout    at midpoint: heap %d MB, goroutine delta %d",
		fanout.heapInuseAtMid/(1024*1024), fanout.goroutineDelta)
	t.Logf("clustered at midpoint: heap %d MB, goroutine delta %d",
		clustered.heapInuseAtMid/(1024*1024), clustered.goroutineDelta)

	// The goroutine delta is a proxy for the number of open partition
	// writers: each RollingDataWriter spawns a stream goroutine that
	// holds a parquet FileWriter with column buffers and compression
	// state, so at production scale (wide tables, zstd) the heap
	// difference scales with this count. Heap is logged but not
	// asserted because GC timing makes a tight bound flaky.
	require.Greater(t, fanout.goroutineDelta, clustered.goroutineDelta+50,
		"fanout should spawn many more concurrent goroutines than clustered (fanout=%d, clustered=%d)",
		fanout.goroutineDelta, clustered.goroutineDelta)
}

func BenchmarkPartitionedWrite(b *testing.B) {
	const (
		numPartitions    = 200
		rowsPerPartition = 5000
	)

	b.Run("fanout", func(b *testing.B) {
		for b.Loop() {
			writeWithMeasurement(b, "fanout", numPartitions, rowsPerPartition)
		}
	})

	b.Run("clustered", func(b *testing.B) {
		for b.Loop() {
			writeWithMeasurement(b, "clustered", numPartitions, rowsPerPartition)
		}
	})
}
