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
	"iter"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
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

func (s *FanoutWriterTestSuite) createCustomTestRecord(arrSchema *arrow.Schema, data [][]any) arrow.Record {
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

	return bldr.NewRecord()
}

func (s *FanoutWriterTestSuite) testTransformPartition(transform iceberg.Transform, sourceFieldName string, transformName string, testRecord arrow.Record, expectedPartitionCount int, verifyRecordCount bool) {
	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(testRecord.Schema(), false)
	s.Require().NoError(err, "Failed to convert Arrow Schema to Iceberg Schema")

	sourceField, ok := icebergSchema.FindFieldByName(sourceFieldName)
	s.Require().True(ok, "Source field %s not found in schema", sourceFieldName)

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID:  sourceField.ID,
			FieldID:   1000,
			Transform: transform,
			Name:      fmt.Sprintf("test_%s", transformName),
		},
	)

	loc := filepath.ToSlash(strings.Replace(s.T().TempDir(), "#", "", -1))
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)

	metaBuilder, err := MetadataBuilderFromBase(meta)
	s.Require().NoError(err)

	args := recordWritingArgs{
		sc: testRecord.Schema(),
		itr: func(yield func(arrow.Record, error) bool) {
			testRecord.Retain()
			yield(testRecord, nil)
		},
		fs:        iceio.LocalFS{},
		writeUUID: func() *uuid.UUID { u := uuid.New(); return &u }(),
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

	nextCount, stopCount := iter.Pull(args.counter)
	partitionWriter := NewPartitionedFanoutWriter(spec, taskSchema, args.itr)
	rollingDataWriters := NewWriterFactory(loc, args, metaBuilder, icebergSchema, 1024*1024)
	rollingDataWriters.nextCount = nextCount
	rollingDataWriters.stopCount = stopCount

	partitionWriter.writers = &rollingDataWriters
	workers := metaBuilder.props.GetInt(FanoutWriterWorkersKey, FanoutWriterWorkersDefault)
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

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

	stopCount()

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

	s.testTransformPartition(iceberg.IdentityTransform{}, "name", "identity", testRecord, 3, true)
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

	s.testTransformPartition(iceberg.BucketTransform{NumBuckets: 3}, "id", "bucket", testRecord, 3, true)
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

	s.testTransformPartition(iceberg.TruncateTransform{Width: 3}, "name", "truncate", testRecord, 3, true)
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

	s.testTransformPartition(iceberg.YearTransform{}, "created_date", "year", testRecord, 3, true)
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

	s.testTransformPartition(iceberg.MonthTransform{}, "created_date", "month", testRecord, 3, true)
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

	s.testTransformPartition(iceberg.DayTransform{}, "created_date", "day", testRecord, 3, true)
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

	s.testTransformPartition(iceberg.HourTransform{}, "created_ts", "hour", testRecord, 3, true)
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

	s.testTransformPartition(iceberg.VoidTransform{}, "nothing", "void", testRecord, 1, true)
}
