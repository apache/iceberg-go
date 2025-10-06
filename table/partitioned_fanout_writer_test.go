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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowdecimal "github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type FanoutWriterTestSuite struct {
	suite.Suite

	mem memory.Allocator
	ctx context.Context
}

func (s *FanoutWriterTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func TestFanoutWriter(t *testing.T) {
	suite.Run(t, new(FanoutWriterTestSuite))
}

func (s *FanoutWriterTestSuite) createCustomTestRecord(arrSchema *arrow.Schema, data [][]any) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(s.mem, arrSchema)
	defer bldr.Release()

	for _, row := range data {
		for i, val := range row {
			field := bldr.Field(i)

			if val == nil {
				field.AppendNull()

				continue
			}

			v := reflect.ValueOf(val)
			appendMethod := reflect.ValueOf(field).MethodByName("Append")

			switch t := val.(type) {
			case uuid.UUID:
				field.(*extensions.UUIDBuilder).Append(t)
			case []byte:
				field.(*array.BinaryBuilder).Append(t)
			default:
				appendMethod.Call([]reflect.Value{v})
			}
		}
	}

	return bldr.NewRecordBatch()
}

func (s *FanoutWriterTestSuite) testTransformPartition(transform iceberg.Transform, sourceFieldName string, transformName string, testRecord arrow.RecordBatch, expectedPartitionCount int) {
	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(testRecord.Schema(), false)
	s.Require().NoError(err, "Failed to convert Arrow Schema to Iceberg Schema")

	sourceField, ok := icebergSchema.FindFieldByName(sourceFieldName)
	s.Require().True(ok, "Source field %s not found in schema", sourceFieldName)

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID:  sourceField.ID,
			FieldID:   1000,
			Transform: transform,
			Name:      "test_%s" + transformName,
		},
	)

	loc := filepath.ToSlash(s.T().TempDir())
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)

	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	s.Require().NoError(err)

	args := recordWritingArgs{
		sc: testRecord.Schema(),
		itr: func(yield func(arrow.RecordBatch, error) bool) {
			testRecord.Retain()
			yield(testRecord, nil)
		},
		fs: iceio.LocalFS{},
		writeUUID: func() *uuid.UUID {
			u := uuid.New()

			return &u
		}(),
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					break
				}
			}
		},
	}

	nameMapping := icebergSchema.NameMapping()
	taskSchema, err := ArrowSchemaToIceberg(args.sc, false, nameMapping)
	s.Require().NoError(err)

	partitionWriter := newPartitionedFanoutWriter(spec, taskSchema, args.itr)
	rollingDataWriters := NewWriterFactory(loc, args, metaBuilder, icebergSchema, 1024*1024)

	partitionWriter.writers = &rollingDataWriters
	workers := config.EnvConfig.MaxWorkers

	dataFiles := partitionWriter.Write(s.ctx, workers)

	fileCount := 0
	totalRecords := int64(0)
	partitionPaths := make(map[string]int64)

	for dataFile, err := range dataFiles {
		s.Require().NoError(err, "Transform %s should work", transformName)
		s.NotNil(dataFile)
		fileCount++
		totalRecords += dataFile.Count()

		partitionRec := getPartitionRecord(dataFile, spec.PartitionType(icebergSchema))
		partitionPath := spec.PartitionToPath(partitionRec, icebergSchema)
		partitionPaths[partitionPath] += dataFile.Count()
	}

	s.Equal(expectedPartitionCount, fileCount, "Expected %d files, got %d", expectedPartitionCount, fileCount)
	s.Equal(totalRecords, testRecord.NumRows(), "Expected %d records, got %d", testRecord.NumRows(), totalRecords)

	s.T().Logf("Transform %s created %d partitions with distribution: %v", transformName, fileCount, partitionPaths)
}

func (s *FanoutWriterTestSuite) TestIdentityTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), "partition_a"},
		{int32(2), "partition_b"},
		{int32(3), "partition_a"},
		{int32(4), "partition_b"},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.IdentityTransform{}, "name", "identity", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestBucketTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), "partition_a"},
		{int32(2), "partition_b"},
		{int32(3), "partition_a"},
		{int32(4), "partition_b"},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.BucketTransform{NumBuckets: 3}, "id", "bucket", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestTruncateTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), "abcdef"},
		{int32(2), "abcxyz"},
		{int32(3), "abcuvw"},
		{int32(4), "defghi"},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.TruncateTransform{Width: 3}, "name", "truncate", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestYearTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "created_date", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), arrow.Date32(19358)},
		{int32(2), arrow.Date32(19723)},
		{int32(3), arrow.Date32(19400)},
		{int32(4), arrow.Date32(19800)},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.YearTransform{}, "created_date", "year", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestMonthTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "created_date", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), arrow.Date32(19358)},
		{int32(2), arrow.Date32(19386)},
		{int32(3), arrow.Date32(19389)},
		{int32(4), arrow.Date32(19416)},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.MonthTransform{}, "created_date", "month", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestDayTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "created_date", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), arrow.Date32(19358)},
		{int32(2), arrow.Date32(19358)},
		{int32(3), arrow.Date32(19359)},
		{int32(4), arrow.Date32(19359)},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.DayTransform{}, "created_date", "day", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestHourTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "created_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), arrow.Timestamp(1672531200000000)},
		{int32(2), arrow.Timestamp(1672531800000000)},
		{int32(3), arrow.Timestamp(1672534800000000)},
		{int32(4), arrow.Timestamp(1672535400000000)},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.HourTransform{}, "created_ts", "hour", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestVoidTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "nothing", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), int32(100)},
		{int32(2), int32(200)},
		{int32(3), int32(300)},
		{int32(4), int32(400)},
		{nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.VoidTransform{}, "nothing", "void", testRecord, 1)
}

func (s *FanoutWriterTestSuite) TestPartitionedLogicalTypesRequireIntFieldIDCase() {
	icebergSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "decimal_col", Type: iceberg.DecimalTypeOf(10, 6), Required: true},
		iceberg.NestedField{ID: 3, Name: "time_col", Type: iceberg.PrimitiveTypes.Time, Required: true},
		iceberg.NestedField{ID: 4, Name: "timestamp_col", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 5, Name: "timestamptz_col", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 6, Name: "uuid_col", Type: iceberg.PrimitiveTypes.UUID, Required: true},
		iceberg.NestedField{ID: 7, Name: "date_col", Type: iceberg.PrimitiveTypes.Date, Required: true},
	)

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 4008, Transform: iceberg.IdentityTransform{}, Name: "decimal_col"},
		iceberg.PartitionField{SourceID: 3, FieldID: 4009, Transform: iceberg.IdentityTransform{}, Name: "time_col"},
		iceberg.PartitionField{SourceID: 4, FieldID: 4010, Transform: iceberg.IdentityTransform{}, Name: "timestamp_col"},
		iceberg.PartitionField{SourceID: 5, FieldID: 4011, Transform: iceberg.IdentityTransform{}, Name: "timestamptz_col"},
		iceberg.PartitionField{SourceID: 6, FieldID: 4014, Transform: iceberg.IdentityTransform{}, Name: "uuid_col"},
		iceberg.PartitionField{SourceID: 7, FieldID: 4015, Transform: iceberg.IdentityTransform{}, Name: "date_col"},
	)

	loc := filepath.ToSlash(s.T().TempDir())
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)

	tbl := New(
		Identifier{"test", "table"},
		meta,
		filepath.Join(loc, "metadata", "v1.metadata.json"),
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		nil,
	)

	record := s.createComprehensiveTestRecord()
	defer record.Release()
	arrowTable := array.NewTableFromRecords(record.Schema(), []arrow.RecordBatch{record})
	defer arrowTable.Release()

	snapshotProps := iceberg.Properties{
		"operation":  "append",
		"source":     "iceberg-go-fanout-test",
		"timestamp":  strconv.FormatInt(time.Now().Unix(), 10),
		"rows-added": strconv.FormatInt(int64(arrowTable.NumRows()), 10),
	}

	batchSize := int64(record.NumRows())
	txn := tbl.NewTransaction()
	err = txn.AppendTable(s.ctx, arrowTable, batchSize, snapshotProps)
	s.Require().NoError(err, "AppendTable should succeed with all primitive types")
}

func (s *FanoutWriterTestSuite) createComprehensiveTestRecord() arrow.RecordBatch {
	pool := s.mem

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "decimal_col", Type: &arrow.Decimal128Type{Precision: 10, Scale: 6}},
		{Name: "time_col", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "timestamp_col", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "timestamptz_col", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "uuid_col", Type: extensions.NewUUIDType()},
		{Name: "date_col", Type: arrow.FixedWidthTypes.Date32},
	}
	arrSchema := arrow.NewSchema(fields, nil)

	idB := array.NewInt64Builder(pool)
	decB := array.NewDecimal128Builder(pool, &arrow.Decimal128Type{Precision: 10, Scale: 6})
	timeB := array.NewTime64Builder(pool, &arrow.Time64Type{Unit: arrow.Microsecond})
	tsB := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	tstzB := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	uuidB := extensions.NewUUIDBuilder(pool)
	dateB := array.NewDate32Builder(pool)

	for i := 0; i < 4; i++ {
		if i%2 == 0 {
			idB.Append(int64(i))
			val := fmt.Sprintf("%d.%06d", 123, i)
			arrowDec, _ := arrowdecimal.Decimal128FromString(val, 10, 6)
			decB.Append(arrowDec)
			timeB.Append(arrow.Time64(time.Duration(i * 1_000_000)))
			tsB.Append(arrow.Timestamp(1_600_000_000_000_000 + int64(i)*1_000_000))
			tstzB.Append(arrow.Timestamp(1_600_000_000_000_000 + int64(i)*1_000_000))
			uuidB.Append(uuid.New())
			dateB.Append(arrow.Date32(20000 + i))
		} else {
			idB.Append(int64(i))
			decB.AppendNull()
			timeB.AppendNull()
			tsB.AppendNull()
			tstzB.AppendNull()
			uuidB.AppendNull()
			dateB.AppendNull()
		}
	}

	cols := []arrow.Array{
		idB.NewArray(),
		decB.NewArray(),
		timeB.NewArray(),
		tsB.NewArray(),
		tstzB.NewArray(),
		uuidB.NewArray(),
		dateB.NewArray(),
	}

	record := array.NewRecordBatch(arrSchema, cols, int64(cols[0].Len()))

	return record
}
