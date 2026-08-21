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
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
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

	mem *memory.CheckedAllocator
	ctx context.Context
}

func (s *FanoutWriterTestSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.NewGoAllocator())
	s.ctx = compute.WithAllocator(context.Background(), s.mem)
}

func (s *FanoutWriterTestSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
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
				switch builder := field.(type) {
				case *array.BinaryBuilder:
					builder.Append(t)
				case *array.FixedSizeBinaryBuilder:
					builder.Append(t)
				default:
					s.FailNow("unsupported byte-slice builder", "%T", field)
				}
			default:
				appendMethod.Call([]reflect.Value{v})
			}
		}
	}

	return bldr.NewRecordBatch()
}

func (s *FanoutWriterTestSuite) createLargeTestRecord(arrSchema *arrow.Schema, rows int, idOffset int64, payloadSize int) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(s.mem, arrSchema)
	defer bldr.Release()

	payload := strings.Repeat("p", payloadSize)
	for i := range rows {
		bldr.Field(0).(*array.Int64Builder).Append(idOffset + int64(i))
		bldr.Field(1).(*array.StringBuilder).Append(payload)
	}

	return bldr.NewRecordBatch()
}

func (s *FanoutWriterTestSuite) TestCloseAllFlushesAfterFanoutSuccessContextCancel() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	s.Require().NoError(err)

	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{icebergSchema.Fields()[0].ID},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	s.Require().NoError(err)

	loc := filepath.ToSlash(s.T().TempDir())
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{icebergSchema.Fields()[0].ID},
		FieldID:   1000,
		Transform: iceberg.BucketTransform{NumBuckets: 1},
		Name:      "id_bucket",
	})
	meta, err := NewMetadata(icebergSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{})
	s.Require().NoError(err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	s.Require().NoError(err)
	s.Require().NoError(metaBuilder.AddSortOrder(&sortOrder))
	s.Require().NoError(metaBuilder.SetDefaultSortOrderID(-1))

	const totalRows = 10000
	record := s.createLargeTestRecord(arrSchema, totalRows, 0, 512)
	defer record.Release()

	itr := func(yield func(arrow.RecordBatch, error) bool) {
		yield(record, nil)
	}

	writeUUID := uuid.New()
	factory, err := newWriterFactory(loc, recordWritingArgs{
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
	}, metaBuilder, icebergSchema, 1024*1024*256)
	s.Require().NoError(err)
	defer factory.closeAll()

	partitionedWriter := newPartitionedFanoutWriter(spec, icebergSchema, itr, factory)

	dataFiles := partitionedWriter.Write(s.ctx, 1)
	type result struct {
		total int64
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		var total int64
		for dataFile, iterErr := range dataFiles {
			if iterErr != nil {
				resultCh <- result{err: iterErr}

				return
			}
			total += dataFile.Count()
		}
		resultCh <- result{total: total}
	}()

	sum := <-resultCh
	s.Require().NoError(sum.err)
	s.Equal(int64(totalRows), sum.total)
}

func (s *FanoutWriterTestSuite) testTransformPartition(transform iceberg.Transform, sourceFieldName string, transformName string, testRecord arrow.RecordBatch, expectedPartitionCount int) {
	icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(testRecord.Schema(), false)
	s.Require().NoError(err, "Failed to convert Arrow Schema to Iceberg Schema")

	sourceField, ok := icebergSchema.FindFieldByName(sourceFieldName)
	s.Require().True(ok, "Source field %s not found in schema", sourceFieldName)

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceIDs: []int{sourceField.ID},
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
			testRecord.Release()
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

	rollingDataWriters, err := newWriterFactory(loc, args, metaBuilder, icebergSchema, 1024*1024)
	s.Require().NoError(err)

	partitionWriter := newPartitionedFanoutWriter(spec, icebergSchema, args.itr, rollingDataWriters)
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

		partitionRec := GetPartitionRecord(dataFile, spec.PartitionType(icebergSchema))
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
		{Name: "large_name", Type: arrow.BinaryTypes.LargeString, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), "partition_a", "partition_a"},
		{int32(2), "partition_b", "partition_b"},
		{int32(3), "partition_a", "partition_c"},
		{int32(4), "partition_b", "partition_d"},
		{nil, nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.IdentityTransform{}, "name", "identity", testRecord, 3)
	s.testTransformPartition(iceberg.IdentityTransform{}, "large_name", "identity_large_string", testRecord, 5)
}

func (s *FanoutWriterTestSuite) TestBinaryPartitionValuesUseComparableKeys() {
	tests := []struct {
		name        string
		arrowType   arrow.DataType
		icebergType iceberg.Type
	}{
		{name: "binary", arrowType: arrow.BinaryTypes.Binary, icebergType: iceberg.PrimitiveTypes.Binary},
		{name: "fixed", arrowType: &arrow.FixedSizeBinaryType{ByteWidth: 4}, icebergType: iceberg.FixedTypeOf(4)},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: test.arrowType}}, nil)
			record := s.createCustomTestRecord(arrowSchema, [][]any{{[]byte{1, 2, 3, 4}}, {[]byte{1, 2, 3, 4}}, {[]byte{5, 6, 7, 8}}})
			defer record.Release()

			icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "part", Type: test.icebergType})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
			})

			partitions, err := getRecordPartitions(spec, icebergSchema, record)
			s.Require().NoError(err)
			s.Require().Len(partitions, 2)
			switch values := record.Column(0).(type) {
			case *array.Binary:
				values.Value(0)[0] = 9
			case *array.FixedSizeBinary:
				values.Value(0)[0] = 9
			}

			rowsByValue := make(map[string]int)
			for _, partition := range partitions {
				value, ok := partition.partitionRec[0].([]byte)
				s.Require().True(ok)
				rowsByValue[string(value)] = len(partition.rows)
			}
			s.Equal(2, rowsByValue[string([]byte{1, 2, 3, 4})])
			s.Equal(1, rowsByValue[string([]byte{5, 6, 7, 8})])
		})
	}
}

func (s *FanoutWriterTestSuite) TestNaNPartitionValuesUseStableKeys() {
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: arrow.PrimitiveTypes.Float64}}, nil)
	record := s.createCustomTestRecord(arrowSchema, [][]any{{math.NaN()}, {math.NaN()}})
	defer record.Release()

	icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Float64})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})

	partitions, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().NoError(err)
	s.Require().Len(partitions, 1)
	s.Len(partitions[0].rows, 2)
	s.True(math.IsNaN(partitions[0].partitionRec[0].(float64)))
}

func (s *FanoutWriterTestSuite) TestFloatPartitionValuesUseIcebergKeys() {
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: arrow.PrimitiveTypes.Float64}}, nil)
	record := s.createCustomTestRecord(arrowSchema, [][]any{
		{math.Copysign(0, -1)},
		{0.0},
		{math.Float64frombits(0x7ff8000000000001)},
		{math.Float64frombits(0x7ff8000000000002)},
	})
	defer record.Release()

	icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Float64})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})

	partitions, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().NoError(err)
	s.Require().Len(partitions, 3)

	rowsByBits := make(map[uint64]int)
	for _, partition := range partitions {
		value := partition.partitionRec[0].(float64)
		rowsByBits[math.Float64bits(value)] = len(partition.rows)
	}
	s.Equal(1, rowsByBits[math.Float64bits(math.Copysign(0, -1))])
	s.Equal(1, rowsByBits[math.Float64bits(0)])
	s.Equal(2, rowsByBits[math.Float64bits(math.Float64frombits(0x7ff8000000000001))])
}

func (s *FanoutWriterTestSuite) TestFloat32PartitionValuesUseIcebergKeys() {
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: arrow.PrimitiveTypes.Float32}}, nil)
	record := s.createCustomTestRecord(arrowSchema, [][]any{
		{math.Float32frombits(0x80000000)},
		{float32(0)},
		{math.Float32frombits(0xffc00001)},
		{math.Float32frombits(0x7fc00002)},
	})
	defer record.Release()

	icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Float32})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})

	partitions, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().NoError(err)
	s.Require().Len(partitions, 3)

	rowsByBits := make(map[uint32]int)
	for _, partition := range partitions {
		value := partition.partitionRec[0].(float32)
		rowsByBits[math.Float32bits(value)] = len(partition.rows)
	}
	s.Equal(1, rowsByBits[0x80000000])
	s.Equal(1, rowsByBits[0])
	s.Equal(2, rowsByBits[0xffc00001])
}

func (s *FanoutWriterTestSuite) TestPartitionMapUsesComparableKeysAtEveryLevel() {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "first", Type: arrow.PrimitiveTypes.Float64},
		{Name: "second", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	record := s.createCustomTestRecord(arrowSchema, [][]any{
		{math.Float64frombits(0x7ff8000000000001), math.Copysign(0, -1)},
		{math.Float64frombits(0xfff8000000000001), math.Copysign(0, -1)},
		{math.Float64frombits(0x7ff8000000000001), float64(0)},
		{float64(0), math.Float64frombits(0x7ff8000000000001)},
		{float64(0), math.Float64frombits(0xfff8000000000001)},
	})
	defer record.Release()

	icebergSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.Float64},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "first", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "second", Transform: iceberg.IdentityTransform{}},
	)

	partitions, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().NoError(err)
	s.Require().Len(partitions, 3)

	rowsByPartition := make([][]int64, 0, len(partitions))
	for _, partition := range partitions {
		rowsByPartition = append(rowsByPartition, partition.rows)
	}
	s.ElementsMatch([][]int64{{0, 1}, {2}, {3, 4}}, rowsByPartition)
}

func (s *FanoutWriterTestSuite) TestBucketTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "large_name", Type: arrow.BinaryTypes.LargeString, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), "partition_a", "partition_a"},
		{int32(2), "partition_b", "partition_b"},
		{int32(3), "partition_a", "partition_c"},
		{int32(4), "partition_b", "partition_d"},
		{nil, nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.BucketTransform{NumBuckets: 3}, "id", "bucket", testRecord, 3)
	s.testTransformPartition(iceberg.BucketTransform{NumBuckets: 3}, "large_name", "bucket_large_string", testRecord, 3)
}

func (s *FanoutWriterTestSuite) TestTruncateTransform() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "large_name", Type: arrow.BinaryTypes.LargeString, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), "abcdef", "abcdef"},
		{int32(2), "abcxyz", "abcxyz"},
		{int32(3), "abcuvw", "bcduvw"},
		{int32(4), "defghi", "defghi"},
		{nil, nil, nil},
	})
	defer testRecord.Release()

	s.testTransformPartition(iceberg.TruncateTransform{Width: 3}, "name", "truncate", testRecord, 3)
	s.testTransformPartition(iceberg.TruncateTransform{Width: 3}, "large_name", "truncate_large_string", testRecord, 4)
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

func (s *FanoutWriterTestSuite) TestTimestampPartitionUsesArrowUnit() {
	tests := []struct {
		name           string
		arrowType      *arrow.TimestampType
		value          arrow.Timestamp
		expectedSource iceberg.Type
	}{
		{
			name:           "second",
			arrowType:      &arrow.TimestampType{Unit: arrow.Second},
			value:          arrow.Timestamp(1_700_000_000),
			expectedSource: iceberg.PrimitiveTypes.Timestamp,
		},
		{
			name:           "millisecond",
			arrowType:      &arrow.TimestampType{Unit: arrow.Millisecond},
			value:          arrow.Timestamp(1_700_000_000_000),
			expectedSource: iceberg.PrimitiveTypes.Timestamp,
		},
		{
			name:           "microsecond",
			arrowType:      &arrow.TimestampType{Unit: arrow.Microsecond},
			value:          arrow.Timestamp(1_700_000_000_000_000),
			expectedSource: iceberg.PrimitiveTypes.Timestamp,
		},
		{
			name:           "millisecond UTC",
			arrowType:      &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"},
			value:          arrow.Timestamp(1_700_000_000_000),
			expectedSource: iceberg.PrimitiveTypes.TimestampTz,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			arrSchema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "created_ts", Type: tt.arrowType, Nullable: true},
			}, nil)

			testRecord := s.createCustomTestRecord(arrSchema, [][]any{
				{int32(1), tt.value},
			})
			defer testRecord.Release()

			icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(testRecord.Schema(), false)
			s.Require().NoError(err)

			sourceField, ok := icebergSchema.FindFieldByName("created_ts")
			s.Require().True(ok)
			s.True(tt.expectedSource.Equals(sourceField.Type))

			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{sourceField.ID},
				FieldID:   1000,
				Transform: iceberg.DayTransform{},
				Name:      "created_day",
			})

			partitions, err := getRecordPartitions(spec, icebergSchema, testRecord)
			s.Require().NoError(err)
			s.Require().Len(partitions, 1)

			partitionPath := spec.PartitionToPath(partitions[0].partitionRec, icebergSchema)
			s.Equal("created_day=2023-11-14", partitionPath)
		})
	}
}

func (s *FanoutWriterTestSuite) TestTimestampNsPartitionUsesArrowUnit() {
	tests := []struct {
		name           string
		arrowType      *arrow.TimestampType
		expectedSource iceberg.Type
	}{
		{
			name:           "nanosecond",
			arrowType:      &arrow.TimestampType{Unit: arrow.Nanosecond},
			expectedSource: iceberg.PrimitiveTypes.TimestampNs,
		},
		{
			name:           "nanosecond UTC",
			arrowType:      &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			expectedSource: iceberg.PrimitiveTypes.TimestampTzNs,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			arrSchema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "created_ts", Type: tt.arrowType, Nullable: true},
			}, nil)

			testRecord := s.createCustomTestRecord(arrSchema, [][]any{
				{int32(1), arrow.Timestamp(1_700_000_000_000_000_000)},
			})
			defer testRecord.Release()

			icebergSchema, err := ArrowSchemaToIcebergWithFreshIDs(testRecord.Schema(), false)
			s.Require().NoError(err)

			sourceField, ok := icebergSchema.FindFieldByName("created_ts")
			s.Require().True(ok)
			s.True(tt.expectedSource.Equals(sourceField.Type))

			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{sourceField.ID},
				FieldID:   1000,
				Transform: iceberg.DayTransform{},
				Name:      "created_day",
			})

			partitions, err := getRecordPartitions(spec, icebergSchema, testRecord)
			s.Require().NoError(err)
			s.Require().Len(partitions, 1)

			partitionPath := spec.PartitionToPath(partitions[0].partitionRec, icebergSchema)
			s.Equal("created_day=2023-11-14", partitionPath)
		})
	}
}

func (s *FanoutWriterTestSuite) TestTimestampPartitionFloorsNegativeNanoseconds() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "created_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), arrow.Timestamp(-1_500)},
	})
	defer testRecord.Release()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "created_ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Transform: iceberg.IdentityTransform{},
		Name:      "created_ts",
	})

	partitions, err := getRecordPartitions(spec, icebergSchema, testRecord)
	s.Require().NoError(err)
	s.Require().Len(partitions, 1)
	s.Equal(iceberg.Timestamp(-2), partitions[0].partitionRec.Get(0))

	partitionPath := spec.PartitionToPath(partitions[0].partitionRec, icebergSchema)
	s.Equal("created_ts=1969-12-31T23%3A59%3A59.999998", partitionPath)
}

func (s *FanoutWriterTestSuite) TestTimestampPartitionRejectsOverflowWhenScalingArrowUnit() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "created_ts", Type: &arrow.TimestampType{Unit: arrow.Second}, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(1), arrow.Timestamp(math.MaxInt64)},
	})
	defer testRecord.Release()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "created_ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Transform: iceberg.DayTransform{},
		Name:      "created_day",
	})

	_, err := getRecordPartitions(spec, icebergSchema, testRecord)
	s.Require().ErrorContains(err, "overflows int64")
}

func (s *FanoutWriterTestSuite) TestGetRecordPartitionsWithDroppedLeadingSourceColumn() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "bar", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "baz", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	testRecord := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(7), true},
	})
	defer testRecord.Release()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool},
	)

	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.IdentityTransform{}, Name: "foo",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
			Transform: iceberg.IdentityTransform{}, Name: "bar",
		},
		iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1002,
			Transform: iceberg.IdentityTransform{}, Name: "baz",
		},
	)

	partitions, err := getRecordPartitions(spec, icebergSchema, testRecord)
	s.Require().NoError(err)
	s.Require().Len(partitions, 1)
	s.Nil(partitions[0].partitionRec.Get(0))
	s.Equal(int32(7), partitions[0].partitionRec.Get(1))
	s.Equal(true, partitions[0].partitionRec.Get(2))
	s.Equal("foo=null/bar=7/baz=true", spec.PartitionToPath(partitions[0].partitionRec, icebergSchema))
}

func (s *FanoutWriterTestSuite) TestInitialPartitionRowCapacity() {
	tests := []struct {
		name           string
		rows           int64
		partitionCount int
		expected       int
	}{
		{name: "first low cardinality partition", rows: 32_768, expected: 128},
		{name: "keep cap for 256 partitions", rows: 32_768, partitionCount: 255, expected: 128},
		{name: "round up near max capacity", rows: 32_768, partitionCount: 256, expected: 128},
		{name: "keep growth-safe capacity at boundary", rows: 32_768, partitionCount: 511, expected: 64},
		{name: "round up below boundary", rows: 32_768, partitionCount: 512, expected: 64},
		{name: "estimate 32 rows per partition", rows: 32_768, partitionCount: 1023, expected: 32},
		{name: "one row per partition", rows: 32_768, partitionCount: 32_767, expected: 1},
		{name: "more partitions than rows", rows: 10, partitionCount: 100, expected: 1},
		{name: "zero rows", rows: 0, expected: 1},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.Equal(test.expected, initialPartitionRowCapacity(test.rows, test.partitionCount))
		})
	}
}

func (s *FanoutWriterTestSuite) TestPartitionRowCapacityUsesBatchPartitionCount() {
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "region", Type: arrow.PrimitiveTypes.Int32},
		{Name: "bucket", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	record := s.createCustomTestRecord(arrSchema, [][]any{
		{int32(0), int32(0)},
		{int32(0), int32(1)},
		{int32(1), int32(0)},
		{int32(1), int32(1)},
	})
	defer record.Release()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "bucket", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "region"},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Transform: iceberg.IdentityTransform{}, Name: "bucket"},
	)

	partitions, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().NoError(err)
	s.Require().Len(partitions, 4)

	capacities := make([]int, 0, len(partitions))
	for _, partition := range partitions {
		capacities = append(capacities, cap(partition.rows))
	}

	s.ElementsMatch([]int{4, 2, 1, 1}, capacities)
}

func (s *FanoutWriterTestSuite) TestPartitionRowCapacityLateDiscoveryIsGrowthSafe() {
	const (
		rows                      = 32_768
		firstDiscoveredPartitions = 300
		latePartitionStart        = 256
		latePartitionRows         = 128
		latePartitionCount        = firstDiscoveredPartitions - latePartitionStart
		lateRows                  = latePartitionCount * (latePartitionRows - 1)
	)

	arrSchema := arrow.NewSchema([]arrow.Field{{Name: "part", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewInt32Builder(s.mem)
	for row := range rows {
		var value int32
		switch {
		case row < firstDiscoveredPartitions:
			value = int32(row)
		case row < firstDiscoveredPartitions+lateRows:
			value = int32(latePartitionStart + (row-firstDiscoveredPartitions)%latePartitionCount)
		}
		builder.Append(value)
	}
	column := builder.NewArray()
	builder.Release()

	record := array.NewRecordBatch(arrSchema, []arrow.Array{column}, rows)
	column.Release()
	defer record.Release()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})

	partitions, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().NoError(err)
	s.Require().Len(partitions, firstDiscoveredPartitions)

	partitionByValue := make(map[int32]*partitionInfo, len(partitions))
	for _, partition := range partitions {
		value, ok := partition.partitionRec[0].(int32)
		s.Require().True(ok)
		partitionByValue[value] = partition
	}

	for value := int32(latePartitionStart); value < firstDiscoveredPartitions; value++ {
		partition := partitionByValue[value]
		s.Require().NotNil(partition, "missing partition %d", value)
		s.Len(partition.rows, latePartitionRows)
		s.Equal(maxInitialPartitionRowCapacity, cap(partition.rows))
	}
}

func (s *FanoutWriterTestSuite) TestPartitionedWriterReusesExtractionPlan() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "part",
	})

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.PrimitiveTypes.Int32},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)
	firstRecord := s.createCustomTestRecord(arrowSchema, [][]any{{int32(7), "a"}})
	defer firstRecord.Release()

	writer := newPartitionedFanoutWriter(spec, icebergSchema, nil, nil)
	partitions, err := writer.getPartitions(firstRecord)
	s.Require().NoError(err)
	s.Require().Len(partitions, 1)
	s.Equal(int32(7), partitions[0].partitionRec.Get(0))
	firstPlan := writer.plan
	s.Require().NotNil(firstPlan)

	equivalentSchema := arrow.NewSchema(arrowSchema.Fields(), nil)
	secondRecord := s.createCustomTestRecord(equivalentSchema, [][]any{{int32(8), "b"}})
	defer secondRecord.Release()

	partitions, err = writer.getPartitions(secondRecord)
	s.Require().NoError(err)
	s.Require().Len(partitions, 1)
	s.Equal(int32(8), partitions[0].partitionRec.Get(0))
	s.Same(firstPlan, writer.plan)
}

func (s *FanoutWriterTestSuite) TestPartitionExtractionPlanMatchesSchema() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "part",
	})

	originalSchema := arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.PrimitiveTypes.Int32},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)
	plan, err := newPartitionExtractionPlan(spec, icebergSchema, originalSchema)
	s.Require().NoError(err)

	tests := []struct {
		name   string
		schema *arrow.Schema
		match  bool
	}{
		{name: "same schema pointer", schema: originalSchema, match: true},
		{name: "equivalent schema", schema: arrow.NewSchema(originalSchema.Fields(), nil), match: true},
		{
			name: "reordered fields",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "value", Type: arrow.BinaryTypes.String},
				{Name: "part", Type: arrow.PrimitiveTypes.Int32},
			}, nil),
			match: false,
		},
		{
			name: "added field",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "part", Type: arrow.PrimitiveTypes.Int32},
				{Name: "value", Type: arrow.BinaryTypes.String},
				{Name: "extra", Type: arrow.FixedWidthTypes.Boolean},
			}, nil),
			match: false,
		},
		{
			name: "changed field type",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "part", Type: arrow.PrimitiveTypes.Int64},
				{Name: "value", Type: arrow.BinaryTypes.String},
			}, nil),
			match: false,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.Equal(test.match, plan.matchesSchema(test.schema))
		})
	}
}

func (s *FanoutWriterTestSuite) TestPartitionExtractionPlanHandlesReorderedRecordSchema() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "part",
	})

	originalSchema := arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.PrimitiveTypes.Int32},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)
	plan, err := newPartitionExtractionPlan(spec, icebergSchema, originalSchema)
	s.Require().NoError(err)
	s.Equal(0, plan.fields[0].columnIndex)

	reorderedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String},
		{Name: "part", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	record := s.createCustomTestRecord(reorderedSchema, [][]any{{"a", int32(7)}, {"b", int32(8)}})
	defer record.Release()

	partitions, err := plan.getRecordPartitions(record)
	s.Require().NoError(err)
	s.Require().Len(partitions, 2)
	values := []int32{
		partitions[0].partitionRec.Get(0).(int32),
		partitions[1].partitionRec.Get(0).(int32),
	}
	s.ElementsMatch([]int32{7, 8}, values)
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
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 4008, Transform: iceberg.IdentityTransform{}, Name: "decimal_col"},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 4009, Transform: iceberg.IdentityTransform{}, Name: "time_col"},
		iceberg.PartitionField{SourceIDs: []int{4}, FieldID: 4010, Transform: iceberg.IdentityTransform{}, Name: "timestamp_col"},
		iceberg.PartitionField{SourceIDs: []int{5}, FieldID: 4011, Transform: iceberg.IdentityTransform{}, Name: "timestamptz_col"},
		iceberg.PartitionField{SourceIDs: []int{6}, FieldID: 4014, Transform: iceberg.IdentityTransform{}, Name: "uuid_col"},
		iceberg.PartitionField{SourceIDs: []int{7}, FieldID: 4015, Transform: iceberg.IdentityTransform{}, Name: "date_col"},
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

	bldr := array.NewRecordBuilder(pool, arrSchema)
	defer bldr.Release()

	for i := range 4 {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		if i%2 == 0 {
			val := fmt.Sprintf("%d.%06d", 123, i)
			arrowDec, _ := arrowdecimal.Decimal128FromString(val, 10, 6)
			bldr.Field(1).(*array.Decimal128Builder).Append(arrowDec)
			bldr.Field(2).(*array.Time64Builder).Append(arrow.Time64(time.Duration(i * 1_000_000)))
			bldr.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(1_600_000_000_000_000 + int64(i)*1_000_000))
			bldr.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(1_600_000_000_000_000 + int64(i)*1_000_000))
			bldr.Field(5).(*extensions.UUIDBuilder).Append(uuid.New())
			bldr.Field(6).(*array.Date32Builder).Append(arrow.Date32(20000 + i))
		} else {
			for j := 1; j <= 6; j++ {
				bldr.Field(j).AppendNull()
			}
		}
	}

	return bldr.NewRecordBatch()
}

func (s *FanoutWriterTestSuite) TestGetArrowValueAsIcebergLiteralTime64() {
	// time64[us]: value passes through unchanged.
	usBldr := array.NewTime64Builder(s.mem, &arrow.Time64Type{Unit: arrow.Microsecond})
	defer usBldr.Release()
	usBldr.Append(arrow.Time64(5_000_000))
	usArr := usBldr.NewTime64Array()
	defer usArr.Release()

	lit, err := getArrowValueAsIcebergLiteral(usArr, 0, iceberg.PrimitiveTypes.Time)
	s.Require().NoError(err)
	s.Equal(iceberg.NewLiteral(iceberg.Time(5_000_000)), lit)

	// time64[ns]: explicitly rejected to avoid silently producing wrong partition keys.
	nsBldr := array.NewTime64Builder(s.mem, &arrow.Time64Type{Unit: arrow.Nanosecond})
	defer nsBldr.Release()
	nsBldr.Append(arrow.Time64(5_000_000_000))
	nsArr := nsBldr.NewTime64Array()
	defer nsArr.Release()

	_, err = getArrowValueAsIcebergLiteral(nsArr, 0, iceberg.PrimitiveTypes.Time)
	s.Require().ErrorIs(err, iceberg.ErrInvalidSchema)
	s.Contains(err.Error(), "time64[ns]")
}
