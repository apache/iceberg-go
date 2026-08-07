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
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/DataDog/iceberg-go"
)

func (s *FanoutWriterTestSuite) TestBinaryPartitionValuesDoNotAliasArrowStorage() {
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
			record := s.createCustomTestRecord(arrowSchema, [][]any{
				{[]byte{1, 2, 3, 4}},
				{[]byte{1, 2, 3, 4}},
				{[]byte{5, 6, 7, 8}},
			})
			defer record.Release()

			icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "part", Type: test.icebergType})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
			})

			partitions, err := getRecordPartitions(spec, icebergSchema, record)
			s.Require().NoError(err)
			s.Require().Len(partitions, 2)

			var arrowValue []byte
			switch values := record.Column(0).(type) {
			case *array.Binary:
				arrowValue = values.Value(0)
			case *array.FixedSizeBinary:
				arrowValue = values.Value(0)
			default:
				s.FailNow("unsupported byte-slice column", "%T", record.Column(0))
			}

			var storedValue []byte
			for _, partition := range partitions {
				value, ok := partition.partitionRec[0].([]byte)
				s.Require().True(ok)
				if string(value) == string([]byte{1, 2, 3, 4}) {
					storedValue = value

					break
				}
			}
			s.Require().NotNil(storedValue)
			s.NotEqual(
				uintptr(unsafe.Pointer(unsafe.SliceData(arrowValue))),
				uintptr(unsafe.Pointer(unsafe.SliceData(storedValue))),
				"stored partition value must not alias Arrow-owned storage",
			)

			arrowValue[0] = 9
			s.Equal([]byte{1, 2, 3, 4}, storedValue)
		})
	}
}

func (s *FanoutWriterTestSuite) TestFixedSizeBinaryPartitionReportsWidthMismatch() {
	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "part",
		Type: &arrow.FixedSizeBinaryType{ByteWidth: 4},
	}}, nil)
	record := s.createCustomTestRecord(arrowSchema, [][]any{{[]byte{1, 2, 3, 4}}})
	defer record.Release()

	icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "part", Type: iceberg.FixedTypeOf(3),
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})

	_, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().Error(err)
	s.ErrorContains(err, `source column "part"`)
	s.ErrorContains(err, "field ID 1")
	s.ErrorContains(err, "fixed[3]")
}

func (s *FanoutWriterTestSuite) TestFixedSizeBinaryPartitionRejectsUnsupportedIcebergType() {
	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "part",
		Type: &arrow.FixedSizeBinaryType{ByteWidth: 4},
	}}, nil)
	record := s.createCustomTestRecord(arrowSchema, [][]any{{[]byte{1, 2, 3, 4}}})
	defer record.Release()

	icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "part", Type: iceberg.PrimitiveTypes.String,
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})

	_, err := getRecordPartitions(spec, icebergSchema, record)
	s.Require().ErrorIs(err, iceberg.ErrInvalidSchema)
	s.ErrorContains(err, `source column "part"`)
	s.ErrorContains(err, "field ID 1")
	s.ErrorContains(err, "fixed_size_binary[4]")
	s.ErrorContains(err, "string")
}
