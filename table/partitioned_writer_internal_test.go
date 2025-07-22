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
	"runtime"
	"strings"
	"testing"
	"time"

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

	ctx        context.Context
	mem        memory.Allocator
	location   string
	schema     *iceberg.Schema
	arrSchema  *arrow.Schema
	testRecord arrow.Record
}

func TestFanoutWriter(t *testing.T) {
	suite.Run(t, new(FanoutWriterTestSuite))
}

func (s *FanoutWriterTestSuite) SetupSuite() {
	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.schema = iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "amount", Type: iceberg.PrimitiveTypes.Float32, Required: true},
		iceberg.NestedField{ID: 4, Name: "created_date", Type: iceberg.PrimitiveTypes.Date, Required: true},
		iceberg.NestedField{ID: 5, Name: "created_ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 6, Name: "uuid_str", Type: iceberg.PrimitiveTypes.UUID, Required: true},
		iceberg.NestedField{ID: 7, Name: "bin_data", Type: iceberg.PrimitiveTypes.Binary, Required: true},
		iceberg.NestedField{ID: 8, Name: "nothing", Type: iceberg.PrimitiveTypes.Int32, Required: true})

	s.arrSchema = arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "created_date", Type: arrow.PrimitiveTypes.Date32, Nullable: false},
		{Name: "created_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: false},
		{Name: "uuid_str", Type: extensions.NewUUIDType(), Nullable: false},
		{Name: "bin_data", Type: arrow.BinaryTypes.Binary, Nullable: false},
		{Name: "nothing", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	s.testRecord = s.createTestRecord()
}

func (s *FanoutWriterTestSuite) SetupTest() {
	s.location = filepath.ToSlash(strings.Replace(s.T().TempDir(), "#", "", -1))
}

func (s *FanoutWriterTestSuite) TearDownSuite() {
	if s.testRecord != nil {
		s.testRecord.Release()
	}
}

func (s *FanoutWriterTestSuite) createTestRecord() arrow.Record {
	bldr := array.NewRecordBuilder(s.mem, s.arrSchema)
	defer bldr.Release()

	numRows := 10
	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < numRows; i++ {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		bldr.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("name_%d", i%3))
		bldr.Field(2).(*array.Float32Builder).Append(float32(i) * 1.5)
		bldr.Field(3).(*array.Date32Builder).Append(arrow.Date32(18262 + int32(i)))

		tm := baseTime.AddDate(0, 0, i)
		ts, _ := arrow.TimestampFromTime(tm, arrow.Microsecond)
		bldr.Field(4).(*array.TimestampBuilder).Append(ts)

		uuidVal := uuid.New()
		bldr.Field(5).(*extensions.UUIDBuilder).Append(uuidVal)

		bldr.Field(6).(*array.BinaryBuilder).Append([]byte(fmt.Sprintf("data_%d", i)))
		bldr.Field(7).(*array.Int32Builder).Append(int32(i * 10))
	}

	return bldr.NewRecord()
}

func (s *FanoutWriterTestSuite) createMetadataBuilder() *MetadataBuilder {
	meta, err := NewMetadata(s.schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		s.location, iceberg.Properties{"format-version": "2"})
	s.Require().NoError(err)

	builder, err := MetadataBuilderFromBase(meta)
	s.Require().NoError(err)

	return builder
}

func (s *FanoutWriterTestSuite) createRecordWritingArgs() recordWritingArgs {
	records := []arrow.Record{s.testRecord}
	s.testRecord.Retain()

	return recordWritingArgs{
		sc: s.arrSchema,
		itr: func(yield func(arrow.Record, error) bool) {
			for _, rec := range records {
				if !yield(rec, nil) {
					return
				}
			}
		},
		fs:        iceio.LocalFS{},
		writeUUID: func() *uuid.UUID { u := uuid.New(); return &u }(),
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					return
				}
			}
		},
	}
}

func (s *FanoutWriterTestSuite) TestDifferentTransforms() {
	tests := []struct {
		name      string
		transform iceberg.Transform
		sourceID  int
	}{
		{"identity", iceberg.IdentityTransform{}, 2},
		{"bucket", iceberg.BucketTransform{NumBuckets: 3}, 2},
		{"truncate", iceberg.TruncateTransform{Width: 5}, 2},
		{"year", iceberg.YearTransform{}, 4},
		{"month", iceberg.MonthTransform{}, 4},
		{"day", iceberg.DayTransform{}, 4},
		{"hour", iceberg.HourTransform{}, 4},
		{"void", iceberg.VoidTransform{}, 8},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			spec := iceberg.NewPartitionSpec(
				iceberg.PartitionField{
					SourceID:  tt.sourceID,
					FieldID:   1000,
					Transform: tt.transform,
					Name:      fmt.Sprintf("test_%s", tt.name),
				},
			)

			meta := s.createMetadataBuilder()
			args := s.createRecordWritingArgs()

			nextCount, stopCount := iter.Pull(args.counter)
			partitionWriter := NewPartitionedFanoutWriter(spec, s.schema, args.itr)
			rollingDataWriters := NewWriterFactory(s.location, args, meta, s.schema, 1024*1024)
			rollingDataWriters.nextCount = nextCount
			rollingDataWriters.stopCount = stopCount

			partitionWriter.writers = &rollingDataWriters
			workers := meta.props.GetInt(FanoutWriterWorkersKey, FanoutWriterWorkersDefault)
			if workers <= 0 {
				workers = runtime.NumCPU()
			}

			dataFiles := partitionWriter.Write(s.ctx, workers)

			fileCount := 0
			for dataFile, err := range dataFiles {
				s.Require().NoError(err, "Transform %s should work", tt.name)
				s.NotNil(dataFile)
				fileCount++
			}

			s.Greater(fileCount, 0, "Should create at least one file for transform %s", tt.name)
		})
	}
}
